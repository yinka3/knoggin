from typing import Any, Optional

from loguru import logger
from psycopg import Error as PsycopgError

from common.exceptions import StorageWriteError
from common.scoping import IDENTITY_ENTITY_ID, require_scope_value
from core.knowledge.db.projection_rebuilder import GraphBuilder
from core.knowledge.db.writers.age_projection_writer import AgeProjectionWriter
from infrastructure.postgres_client import PostgresClient


class ProjectDeletionWriter:
    """Delete one project and preserve identities still used by other projects."""

    def __init__(self, client: PostgresClient) -> None:
        self.client = client
        self.projection = AgeProjectionWriter(client)
        self.projection_rebuilder = GraphBuilder(client)

    @staticmethod
    def _raise_storage_write(operation: str, exc: Exception) -> None:
        logger.error("Storage write failed for {}: {}", operation, exc)
        raise StorageWriteError(
            operation,
            details={"error_type": type(exc).__name__},
        ) from exc

    @staticmethod
    def _name_key(value: str) -> str:
        return value.strip().casefold()

    @staticmethod
    def _choose_supported_canonical_name(
        names_by_key: dict[str, list[str]],
        supports_by_key: dict[str, list[dict[str, Any]]],
    ) -> str:
        """Choose one current name from the surviving source support."""

        choices: list[tuple[int, int, str, str]] = []
        for name_key, supports in supports_by_key.items():
            if not supports or name_key not in names_by_key:
                continue
            name = min(
                names_by_key[name_key],
                key=lambda value: (value.casefold(), value),
            )
            has_user_support = any(
                support["source_kind"] == "user" for support in supports
            )
            project_count = len(
                {
                    str(support["project_id"])
                    for support in supports
                    if support["source_kind"] == "project"
                    and support["project_id"] is not None
                }
            )
            choices.append(
                (
                    0 if has_user_support else 1,
                    -project_count,
                    name.casefold(),
                    name,
                )
            )
        if not choices:
            raise RuntimeError("No supported name is available for canonical selection")
        return min(choices)[-1]

    async def _capture_affected_entity_ids(
        self,
        cur,
        *,
        user_name: str,
        project_id: str,
    ) -> list[int]:
        """Lock identities whose context or name support belongs to the project."""

        await cur.execute(
            """
            SELECT entity.entity_id
            FROM public.entities AS entity
            WHERE entity.user_name = %s
              AND (
                  EXISTS (
                      SELECT 1
                      FROM public.project_entity_contexts AS context
                      WHERE context.entity_id = entity.entity_id
                        AND context.project_id = %s
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM public.entity_name_supports AS support
                      WHERE support.entity_id = entity.entity_id
                        AND support.project_id = %s
                  )
              )
            ORDER BY entity.entity_id
            FOR UPDATE OF entity
            """,
            (user_name, project_id, project_id),
        )
        return [int(row["entity_id"]) for row in await cur.fetchall()]

    async def _capture_context_project_ids(
        self,
        cur,
        *,
        entity_ids: list[int],
        deleted_project_id: str,
    ) -> list[str]:
        if not entity_ids:
            return []
        await cur.execute(
            """
            SELECT DISTINCT context.project_id
            FROM public.project_entity_contexts AS context
            WHERE context.entity_id = ANY(%s)
              AND context.project_id <> %s
            ORDER BY context.project_id
            """,
            (entity_ids, deleted_project_id),
        )
        return [str(row["project_id"]) for row in await cur.fetchall()]

    async def _capture_reader_project_ids(
        self,
        cur,
        *,
        user_name: str,
        project_id: str,
    ) -> list[str]:
        await cur.execute(
            """
            SELECT project_id
            FROM public.project_read_scopes
            WHERE user_name = %s
              AND readable_project_id = %s
            ORDER BY project_id
            FOR UPDATE
            """,
            (user_name, project_id),
        )
        return [str(row["project_id"]) for row in await cur.fetchall()]

    async def _invalidate_project_merge_audits(
        self,
        cur,
        *,
        user_name: str,
        project_id: str,
    ) -> list[str]:
        """Remove rollback payload whose source project is being deleted."""

        await cur.execute(
            """
            SELECT merge_id
            FROM public.entity_global_merge_audits
            WHERE user_name = %s
              AND affected_project_ids @> jsonb_build_array(%s::text)
            ORDER BY merge_id
            FOR UPDATE
            """,
            (user_name, project_id),
        )
        merge_ids = [str(row["merge_id"]) for row in await cur.fetchall()]
        if not merge_ids:
            return []

        await cur.execute(
            """
            DELETE FROM public.entity_global_merge_mutations
            WHERE merge_id = ANY(%s)
            """,
            (merge_ids,),
        )
        await cur.execute(
            """
            UPDATE public.entity_global_merge_audits AS audit
            SET status = 'failed',
                plan = '{}'::jsonb,
                affected_project_ids = (
                    SELECT COALESCE(
                        jsonb_agg(scope.value ORDER BY scope.ordinal),
                        '[]'::jsonb
                    )
                    FROM jsonb_array_elements_text(audit.affected_project_ids)
                         WITH ORDINALITY AS scope(value, ordinal)
                    WHERE scope.value <> %s
                ),
                completed_at = COALESCE(completed_at, now()),
                failure_reason = 'project_deleted'
            WHERE merge_id = ANY(%s)
            """,
            (project_id, merge_ids),
        )
        return merge_ids

    async def _cleanup_entity_names(
        self,
        cur,
        *,
        entity_id: int,
        user_name: str,
    ) -> dict[str, Any] | None:
        """Keep only current names with surviving independent support."""

        await cur.execute(
            """
            SELECT entity_id, user_name, canonical_name
            FROM public.entities
            WHERE entity_id = %s AND user_name = %s
            """,
            (entity_id, user_name),
        )
        entity = await cur.fetchone()
        if entity is None:
            return None

        canonical_name = str(entity["canonical_name"])
        await cur.execute(
            """
            SELECT alias
            FROM public.entity_aliases
            WHERE entity_id = %s
            ORDER BY alias
            """,
            (entity_id,),
        )
        aliases = [str(row["alias"]) for row in await cur.fetchall()]
        await cur.execute(
            """
            SELECT project_id
            FROM public.project_entity_contexts
            WHERE entity_id = %s
            """,
            (entity_id,),
        )
        context_project_ids = {
            str(row["project_id"]) for row in await cur.fetchall()
        }
        await cur.execute(
            """
            SELECT name, project_id, source_kind, source_key
            FROM public.entity_name_supports
            WHERE entity_id = %s
            ORDER BY name, source_kind, source_key
            """,
            (entity_id,),
        )
        supports = [dict(row) for row in await cur.fetchall()]

        names_by_key: dict[str, list[str]] = {}
        for name in [canonical_name, *aliases]:
            key = self._name_key(name)
            if key:
                names_by_key.setdefault(key, []).append(name)

        supports_by_key: dict[str, list[dict[str, Any]]] = {}
        valid_support_keys: set[tuple[str, str | None, str, str]] = set()
        for support in supports:
            source_kind = str(support["source_kind"])
            project_id = support["project_id"]
            valid_source = source_kind == "user" or (
                source_kind == "project"
                and project_id is not None
                and str(project_id) in context_project_ids
            )
            name_key = self._name_key(str(support["name"]))
            if not valid_source or name_key not in names_by_key:
                continue
            supports_by_key.setdefault(name_key, []).append(support)
            valid_support_keys.add(
                (
                    str(support["name"]),
                    str(project_id) if project_id is not None else None,
                    source_kind,
                    str(support["source_key"]),
                )
            )

        supported_name_keys = set(supports_by_key)
        canonical_key = self._name_key(canonical_name)
        if entity_id != IDENTITY_ENTITY_ID and not supported_name_keys:
            return None
        if canonical_key in supported_name_keys:
            retained_canonical_name = canonical_name
        elif supported_name_keys:
            retained_canonical_name = self._choose_supported_canonical_name(
                names_by_key,
                supports_by_key,
            )
        else:
            # The reserved identity has a user-authored canonical name in a
            # current database. Retain it defensively if that invariant was
            # already violated; it is never a project-owned identity.
            retained_canonical_name = canonical_name

        retained_aliases = [
            alias
            for alias in aliases
            if self._name_key(alias) in supported_name_keys
        ]
        retained_name_keys = {
            self._name_key(retained_canonical_name),
            *(self._name_key(alias) for alias in retained_aliases),
        }

        aliases_to_delete = [
            alias
            for alias in aliases
            if alias not in retained_aliases
        ]
        if aliases_to_delete:
            await cur.execute(
                """
                DELETE FROM public.entity_aliases
                WHERE entity_id = %s AND alias = ANY(%s)
                """,
                (entity_id, aliases_to_delete),
            )

        for support in supports:
            key = (
                str(support["name"]),
                (
                    str(support["project_id"])
                    if support["project_id"] is not None
                    else None
                ),
                str(support["source_kind"]),
                str(support["source_key"]),
            )
            if key in valid_support_keys and self._name_key(
                str(support["name"])
            ) in retained_name_keys:
                continue
            await cur.execute(
                """
                DELETE FROM public.entity_name_supports
                WHERE entity_id = %s
                  AND name = %s
                  AND source_kind = %s
                  AND source_key = %s
                """,
                (
                    entity_id,
                    support["name"],
                    support["source_kind"],
                    support["source_key"],
                ),
            )

        if retained_canonical_name != canonical_name:
            await cur.execute(
                "SELECT set_config(%s, 'on', true)",
                ("knoggin.project_deletion_name_cleanup",),
            )
            await cur.execute(
                """
                UPDATE public.entities
                SET canonical_name = %s
                WHERE entity_id = %s AND user_name = %s
                """,
                (retained_canonical_name, entity_id, user_name),
            )

        return {
            "id": entity_id,
            "user_name": user_name,
            "canonical_name": retained_canonical_name,
            "aliases": retained_aliases,
        }

    async def _expanded_redirect_ids(
        self,
        cur,
        *,
        entity_ids: list[int],
    ) -> list[int]:
        """Delete retired merge identities with an unsupported survivor."""

        if not entity_ids:
            return []
        await cur.execute(
            """
            WITH RECURSIVE redirect_targets(entity_id) AS (
                SELECT unnest(%s::bigint[])
                UNION
                SELECT child.entity_id
                FROM public.entities AS child
                JOIN redirect_targets AS target
                  ON child.redirect_entity_id = target.entity_id
            )
            SELECT entity.entity_id
            FROM public.entities AS entity
            JOIN redirect_targets AS target
              ON target.entity_id = entity.entity_id
            WHERE entity.entity_id <> %s
            ORDER BY entity.entity_id
            FOR UPDATE OF entity
            """,
            (entity_ids, IDENTITY_ENTITY_ID),
        )
        return [int(row["entity_id"]) for row in await cur.fetchall()]

    async def list_pending_file_cleanup_project_ids(
        self,
        *,
        user_name: str,
    ) -> list[str]:
        """List committed project-file removals that still need local cleanup."""

        user_name = require_scope_value(
            user_name,
            "user_name",
            "list_pending_file_cleanup_project_ids",
        )
        try:
            rows = await self.client.fetch_all(
                """
                SELECT project_id
                FROM public.project_file_cleanup_tasks
                WHERE user_name = %s
                ORDER BY created_at, project_id
                """,
                (user_name,),
            )
        except PsycopgError as exc:
            self._raise_storage_write("list_pending_file_cleanup_project_ids", exc)
        return [str(row["project_id"]) for row in rows]

    async def has_pending_file_cleanup(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> bool:
        """Whether a deleted project still has an owned-file cleanup task."""

        user_name = require_scope_value(
            user_name,
            "user_name",
            "has_pending_file_cleanup",
        )
        project_id = require_scope_value(
            project_id,
            "project_id",
            "has_pending_file_cleanup",
        )
        try:
            row = await self.client.fetch_one(
                """
                SELECT 1 AS pending
                FROM public.project_file_cleanup_tasks
                WHERE user_name = %s AND project_id = %s
                """,
                (user_name, project_id),
            )
        except PsycopgError as exc:
            self._raise_storage_write("has_pending_file_cleanup", exc)
        return row is not None

    async def complete_file_cleanup(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> None:
        """Clear a task only after its owned directory has been removed."""

        user_name = require_scope_value(
            user_name,
            "user_name",
            "complete_file_cleanup",
        )
        project_id = require_scope_value(
            project_id,
            "project_id",
            "complete_file_cleanup",
        )
        try:
            await self.client.execute(
                """
                DELETE FROM public.project_file_cleanup_tasks
                WHERE user_name = %s AND project_id = %s
                """,
                (user_name, project_id),
            )
        except PsycopgError as exc:
            self._raise_storage_write("complete_file_cleanup", exc)

    async def delete_project(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> Optional[dict[str, Any]]:
        user_name = require_scope_value(user_name, "user_name", "delete_project")
        project_id = require_scope_value(project_id, "project_id", "delete_project")

        try:
            async with self.client.transaction() as cur:
                await cur.execute(
                    """
                    SELECT project_id
                    FROM public.projects
                    WHERE user_name = %s AND project_id = %s
                    FOR UPDATE
                    """,
                    (user_name, project_id),
                )
                if await cur.fetchone() is None:
                    return None

                # Entity merges and deletion both mutate user-global identity
                # state and audit records. Share the merge transaction lock.
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"entity-merge:{user_name}",),
                )
                affected_entity_ids = await self._capture_affected_entity_ids(
                    cur,
                    user_name=user_name,
                    project_id=project_id,
                )
                projection_project_ids = await self._capture_context_project_ids(
                    cur,
                    entity_ids=affected_entity_ids,
                    deleted_project_id=project_id,
                )
                reader_project_ids = await self._capture_reader_project_ids(
                    cur,
                    user_name=user_name,
                    project_id=project_id,
                )
                invalidated_merge_ids = await self._invalidate_project_merge_audits(
                    cur,
                    user_name=user_name,
                    project_id=project_id,
                )
                await self.projection.clear_project_projection(cur, project_id)

                # Assistant memberships restrict deletion of their exchange's
                # user message. Remove the project-owned memberships first so
                # aggregate deletion can then rely on the project cascades.
                await cur.execute(
                    """
                    DELETE FROM public.project_semantic_window_messages
                    WHERE project_id = %s
                    """,
                    (project_id,),
                )

                # This row intentionally has no project foreign key. It is the
                # durable handoff from the committed database deletion to the
                # filesystem cleanup that follows it.
                await cur.execute(
                    """
                    INSERT INTO public.project_file_cleanup_tasks (
                        project_id, user_name
                    ) VALUES (%s, %s)
                    """,
                    (project_id, user_name),
                )

                await cur.execute(
                    """
                    DELETE FROM public.projects
                    WHERE user_name = %s AND project_id = %s
                    RETURNING project_id
                    """,
                    (user_name, project_id),
                )
                if await cur.fetchone() is None:
                    raise RuntimeError("Project disappeared during aggregate deletion")

                surviving_entities: list[dict[str, Any]] = []
                entity_ids_to_delete: list[int] = []
                for entity_id in affected_entity_ids:
                    entity = await self._cleanup_entity_names(
                        cur,
                        entity_id=entity_id,
                        user_name=user_name,
                    )
                    if entity is None:
                        entity_ids_to_delete.append(entity_id)
                    else:
                        surviving_entities.append(entity)

                deleted_entity_ids = await self._expanded_redirect_ids(
                    cur,
                    entity_ids=entity_ids_to_delete,
                )
                if deleted_entity_ids:
                    await cur.execute(
                        """
                        DELETE FROM public.entities entity
                        WHERE entity.entity_id = ANY(%s)
                        RETURNING entity_id
                        """,
                        (deleted_entity_ids,),
                    )
                    deleted_entity_ids = [
                        int(row["entity_id"]) for row in await cur.fetchall()
                    ]
                    await self.projection.delete_entities_projection(
                        cur,
                        deleted_entity_ids,
                        project_id,
                    )

                non_identity_entities = [
                    entity
                    for entity in surviving_entities
                    if int(entity["id"]) != IDENTITY_ENTITY_ID
                ]
                if non_identity_entities:
                    await self.projection.project_entities(cur, non_identity_entities)
                identity = next(
                    (
                        entity
                        for entity in surviving_entities
                        if int(entity["id"]) == IDENTITY_ENTITY_ID
                    ),
                    None,
                )
                if identity is not None:
                    await self.projection.project_identity(cur, identity)

                for affected_project_id in projection_project_ids:
                    await self.projection_rebuilder.rebuild_project_projection(
                        affected_project_id,
                        user_name,
                        cur=cur,
                    )

                affected_entity_ids = sorted(
                    set(affected_entity_ids) | set(deleted_entity_ids)
                )
                return {
                    "entities": len(deleted_entity_ids),
                    "projects": 1,
                    "affected_entity_ids": affected_entity_ids,
                    "projection_project_ids": projection_project_ids,
                    "cache_project_ids": sorted(
                        set(projection_project_ids) | set(reader_project_ids)
                    ),
                    "merge_audits_invalidated": len(invalidated_merge_ids),
                }
        except PsycopgError as exc:
            self._raise_storage_write("delete_project", exc)
