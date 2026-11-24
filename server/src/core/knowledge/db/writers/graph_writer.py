from contextlib import asynccontextmanager
from functools import wraps
from typing import Dict, List, Optional, Sequence

from loguru import logger
from psycopg import Error as PsycopgError

from common.exceptions import StorageWriteError
from common.scoping import (
    IDENTITY_ENTITY_ID,
    IDENTITY_SCOPE,
    require_scope_value,
)
from common.utils.time_utils import get_now_ms
from infrastructure.postgres_client import PostgresClient


def _storage_write(operation: str):
    """Translate infrastructure failures without hiding contract violations."""

    def decorate(method):
        @wraps(method)
        async def wrapped(self, *args, **kwargs):
            try:
                return await method(self, *args, **kwargs)
            except (StorageWriteError, TypeError, ValueError):
                raise
            except PsycopgError as exc:
                self._raise_storage_write(operation, exc)

        return wrapped

    return decorate


class GraphWriter:
    def __init__(self, client: PostgresClient):
        self.client = client

    def _current_time_ms(self) -> int:
        return get_now_ms()

    @staticmethod
    def _raise_storage_write(operation: str, exc: Exception) -> None:
        logger.error("Storage write failed for {}: {}", operation, exc)
        raise StorageWriteError(
            operation,
            details={"error_type": type(exc).__name__},
        ) from exc

    @staticmethod
    def _require_project_id(project_id: str, operation: str) -> str:
        return require_scope_value(project_id, "project_id", operation)

    @staticmethod
    def _normalized_identity_value(value: object) -> str:
        return str(value or "").strip().strip('"').casefold()

    @asynccontextmanager
    async def _cursor_context(self, cur=None):
        if cur is not None:
            yield cur
            return
        async with self.client.transaction() as transaction_cursor:
            yield transaction_cursor

    @_storage_write("ensure_identity_entity")
    async def ensure_identity_entity(
        self, user_name: str, aliases: Optional[List[str]] = None
    ) -> Dict:
        """Persist and validate the identity-scoped entity reserved at ID 1."""
        user_name = str(user_name or "").strip()
        if not user_name:
            raise ValueError("Identity requires a non-empty configured user name")
        canonical_key = self._normalized_identity_value(user_name)
        clean_aliases = []
        seen_aliases = {canonical_key}
        for alias in aliases or []:
            clean_alias = str(alias or "").strip()
            alias_key = self._normalized_identity_value(clean_alias)
            if clean_alias and alias_key not in seen_aliases:
                clean_aliases.append(clean_alias)
                seen_aliases.add(alias_key)

        now_ms = self._current_time_ms()
        identity = {
            "id": IDENTITY_ENTITY_ID,
            "user_name": user_name,
            "project_id": IDENTITY_SCOPE,
            "canonical_name": user_name,
            "aliases": clean_aliases,
            "type": "person",
            "topic": "Identity",
            "now": now_ms,
        }

        async with self.client.transaction() as cur:
            await cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (IDENTITY_ENTITY_ID,),
            )
            await cur.execute(
                """
                SELECT entity_id, user_name, canonical_name
                FROM entities
                WHERE entity_id = %s
                FOR UPDATE
                """,
                (IDENTITY_ENTITY_ID,),
            )
            existing = await cur.fetchone()
            if existing and (
                self._normalized_identity_value(existing["user_name"])
                != canonical_key
                or self._normalized_identity_value(existing["canonical_name"])
                != canonical_key
            ):
                raise RuntimeError(
                    "Entity ID 1 is occupied by a non-identity entity; "
                    "reset the development database before startup"
                )
            stored_canonical_name = (
                str(existing["canonical_name"]) if existing else user_name
            )
            identity["canonical_name"] = stored_canonical_name

            await cur.execute(
                """
                INSERT INTO entities (
                    entity_id,
                    user_name,
                    canonical_name
                )
                VALUES (%s, %s, %s)
                ON CONFLICT (entity_id) DO NOTHING
                """,
                (
                    IDENTITY_ENTITY_ID,
                    user_name,
                    user_name,
                ),
            )
            current_names = [stored_canonical_name, *clean_aliases]
            # Settings own only the names they wrote (source_kind 'user'). A
            # name taken out of settings goes, with all its support; names the
            # identity learned elsewhere (a merge, a JEV identity decision) stay.
            await cur.execute(
                """
                SELECT DISTINCT name FROM public.entity_name_supports
                WHERE entity_id = %s AND source_kind = 'user'
                """,
                (IDENTITY_ENTITY_ID,),
            )
            removed = sorted(
                {row["name"] for row in await cur.fetchall()} - set(current_names)
            )
            if removed:
                await cur.execute(
                    "DELETE FROM entity_aliases WHERE entity_id = %s AND alias = ANY(%s)",
                    (IDENTITY_ENTITY_ID, removed),
                )
                await cur.execute(
                    """
                    DELETE FROM public.entity_name_supports
                    WHERE entity_id = %s AND name = ANY(%s)
                    """,
                    (IDENTITY_ENTITY_ID, removed),
                )
            await cur.execute(
                """
                DELETE FROM public.entity_name_supports
                WHERE entity_id = %s
                  AND source_kind = 'user'
                """,
                (IDENTITY_ENTITY_ID,),
            )
            for alias in clean_aliases:
                await cur.execute(
                    """
                    INSERT INTO entity_aliases (entity_id, alias)
                    VALUES (%s, %s)
                    ON CONFLICT (entity_id, alias) DO NOTHING
                    """,
                    (IDENTITY_ENTITY_ID, alias),
                )
            for name in current_names:
                await cur.execute(
                    """
                    INSERT INTO public.entity_name_supports (
                        entity_id, name, project_id, source_kind, source_key
                    ) VALUES (%s, %s, NULL, 'user', %s)
                    ON CONFLICT (entity_id, name, source_kind, source_key)
                    DO NOTHING
                    """,
                    (IDENTITY_ENTITY_ID, name, user_name),
                )

        return identity


    @_storage_write("forget_graph")
    async def forget_graph(self, *, user_name: str, project_id: str) -> list[int]:
        """Remove unsupported graph evidence and retain names atomically for retries."""
        user_name = require_scope_value(user_name, "user_name", "forget_graph")
        project_id = require_scope_value(project_id, "project_id", "forget_graph")
        async with self.client.transaction() as cur:
            await cur.execute(
                """SELECT forget_id FROM project_forget_records
                   WHERE project_id = %s AND user_name = %s
                     AND cleanup_completed_at IS NULL FOR UPDATE""", (project_id, user_name),
            )
            if not await cur.fetchall():
                return []
            await cur.execute(
                """DELETE FROM relationship_observations observation
                   WHERE observation.project_id = %s
                     AND EXISTS (SELECT 1 FROM relationship_observation_messages support
                         WHERE support.observation_id = observation.observation_id)
                     AND NOT EXISTS (
                         SELECT 1 FROM relationship_observation_messages support
                         JOIN messages message USING (message_id)
                         WHERE support.observation_id = observation.observation_id
                           AND message.lifecycle_state <> 'forgotten')
                   RETURNING relationship_id""", (project_id,),
            )
            affected_relationships = [row["relationship_id"] for row in await cur.fetchall()]
            await cur.execute(
                """DELETE FROM relationships relationship WHERE project_id = %s
                   AND relationship_id = ANY(%s)
                   AND NOT EXISTS (SELECT 1 FROM relationship_observations observation
                       WHERE observation.relationship_id = relationship.relationship_id)""",
                (project_id, affected_relationships),
            )
            # Map every merged name to its final identity before testing support.
            # Context entity occurrences were removed by migration 0003; graph
            # support now comes from messages, never historical Context versions.
            await cur.execute(
                """WITH RECURSIVE canonical AS (
                       SELECT entity_id AS member_id, entity_id, redirect_entity_id
                       FROM entities WHERE user_name = %s
                       UNION ALL
                       SELECT canonical.member_id, entity.entity_id, entity.redirect_entity_id
                       FROM canonical JOIN entities entity
                         ON entity.entity_id = canonical.redirect_entity_id
                   ), mentions AS (
                       SELECT coalesce(corrected_entity_id, resolved_entity_id) AS entity_id,
                              message_id FROM message_entity_occurrences WHERE project_id = %s
                       UNION
                       SELECT ref.entity_id, ref.message_id FROM message_entity_refs ref
                       JOIN messages message USING (message_id) WHERE message.project_id = %s
                   ), forgotten AS (
                       SELECT canonical.entity_id, array_agg(DISTINCT message.message_id) AS message_ids
                       FROM mentions JOIN canonical ON canonical.member_id = mentions.entity_id
                         AND canonical.redirect_entity_id IS NULL
                       JOIN messages message USING (message_id)
                       GROUP BY canonical.entity_id
                       HAVING bool_and(message.lifecycle_state = 'forgotten')
                   )
                   SELECT forgotten.entity_id, forgotten.message_ids,
                          array_agg(DISTINCT canonical.member_id) AS family_ids
                   FROM forgotten JOIN canonical USING (entity_id)
                   WHERE canonical.redirect_entity_id IS NULL AND forgotten.entity_id <> %s
                   GROUP BY forgotten.entity_id, forgotten.message_ids""",
                (user_name, project_id, project_id, IDENTITY_ENTITY_ID),
            )
            candidates = await cur.fetchall()
            deleted = []
            for candidate in candidates:
                family = candidate['family_ids']
                await cur.execute(
                    """SELECT canonical_name AS name FROM entities WHERE entity_id = ANY(%s)
                       UNION SELECT alias FROM entity_aliases WHERE entity_id = ANY(%s)""",
                    (family, family),
                )
                names = sorted({row['name'] for row in await cur.fetchall()})
                await cur.execute(
                    """UPDATE project_forget_records record
                       SET deleted_names = ARRAY(SELECT DISTINCT unnest(record.deleted_names || %s::text[]) ORDER BY 1),
                           deleted_entity_ids = ARRAY(SELECT DISTINCT unnest(record.deleted_entity_ids || %s::bigint[]) ORDER BY 1)
                       WHERE project_id = %s AND user_name = %s
                         AND message_ids && %s::bigint[]""",
                    (names, family, project_id, user_name, candidate['message_ids']),
                )
                await self._delete_entity_aggregate_with_cursor(cur, family, project_id)
                deleted.extend(family)
            # Forgotten mentions must also disappear for entities that survive.
            await cur.execute(
                """DELETE FROM message_entity_occurrences occurrence USING messages message
                   WHERE occurrence.message_id = message.message_id AND occurrence.project_id = %s
                     AND message.lifecycle_state = 'forgotten'""", (project_id,),
            )
            await cur.execute(
                """DELETE FROM message_entity_refs ref USING messages message
                   WHERE ref.message_id = message.message_id AND message.project_id = %s
                     AND message.lifecycle_state = 'forgotten'""", (project_id,),
            )
            # Return saved IDs too: a failed cache publication must be retried.
            await cur.execute(
                """SELECT DISTINCT unnest(deleted_entity_ids) AS entity_id FROM project_forget_records
                   WHERE project_id = %s AND user_name = %s AND cleanup_completed_at IS NULL""",
                (project_id, user_name),
            )
            return sorted({int(row['entity_id']) for row in await cur.fetchall()} | set(deleted))

    async def _delete_entity_aggregate(
        self,
        entity_ids: List[int],
        project_id: str,
    ) -> List[int]:
        unique_ids = sorted(
            {
                int(entity_id)
                for entity_id in entity_ids
                if int(entity_id) != IDENTITY_ENTITY_ID
            }
        )
        if not unique_ids:
            return []

        async with self.client.transaction() as cur:
            return await self._delete_entity_aggregate_with_cursor(
                cur,
                unique_ids,
                project_id,
            )

    async def _delete_entity_aggregate_with_cursor(
        self,
        cur,
        entity_ids: List[int],
        project_id: str,
    ) -> List[int]:
        if not entity_ids:
            return []

        await cur.execute(
            """
            DELETE FROM relationship_observations
            WHERE relationship_id IN (
                SELECT relationship_id
                FROM relationships
                WHERE project_id = %s
                  AND (entity_a_id = ANY(%s) OR entity_b_id = ANY(%s))
            )
            """,
            (project_id, entity_ids, entity_ids),
        )
        await cur.execute(
            """
            DELETE FROM relationships
            WHERE project_id = %s
              AND (entity_a_id = ANY(%s) OR entity_b_id = ANY(%s))
            """,
            (project_id, entity_ids, entity_ids),
        )
        await cur.execute(
            """
            DELETE FROM message_entity_refs ref
            USING messages message
            WHERE ref.message_id = message.message_id
              AND message.project_id = %s
              AND ref.entity_id = ANY(%s)
            """,
            (project_id, entity_ids),
        )
        await cur.execute(
            """
            DELETE FROM episode_entities
            WHERE project_id = %s AND entity_id = ANY(%s)
            """,
            (project_id, entity_ids),
        )
        # Finalize rebuilds Episode links from occurrences; remove them too so
        # a retry cannot link the deleted entity again.
        await cur.execute(
            """
            DELETE FROM message_entity_occurrences
            WHERE project_id = %s
              AND coalesce(corrected_entity_id, resolved_entity_id) = ANY(%s)
            """,
            (project_id, entity_ids),
        )
        await cur.execute(
            """
            DELETE FROM project_entity_contexts
            WHERE project_id = %s AND entity_id = ANY(%s)
            """,
            (project_id, entity_ids),
        )
        await cur.execute(
            "DELETE FROM entity_name_supports WHERE project_id = %s AND entity_id = ANY(%s)",
            (project_id, entity_ids),
        )
        await cur.execute(
            """DELETE FROM entity_aliases alias WHERE entity_id = ANY(%s)
               AND NOT EXISTS (SELECT 1 FROM entity_name_supports support
                   WHERE support.entity_id = alias.entity_id AND support.name = alias.alias)""",
            (entity_ids,),
        )
        # Delete redirect leaves first. An alias used by another project keeps
        # its canonical row alive, but never this project's removed context.
        for _ in entity_ids:
            await cur.execute(
                """DELETE FROM entities entity WHERE entity_id = ANY(%s)
                   AND NOT EXISTS (SELECT 1 FROM project_entity_contexts context
                       WHERE context.entity_id = entity.entity_id)
                   AND NOT EXISTS (SELECT 1 FROM entities redirect
                       WHERE redirect.redirect_entity_id = entity.entity_id)
                   RETURNING entity_id""", (entity_ids,),
            )
            if not await cur.fetchall():
                break
        return sorted(entity_ids)

    @_storage_write("delete_selected_project_entities")
    async def delete_selected_project_entities(
        self,
        entity_ids: Sequence[int],
        *,
        user_name: str,
        project_id: str,
    ) -> List[int]:
        """Delete explicitly selected, project-owned derived entities atomically."""

        project_id = self._require_project_id(
            project_id,
            "delete_selected_project_entities",
        )
        if not user_name or not user_name.strip():
            raise ValueError("delete_selected_project_entities requires user_name")
        normalized_ids: list[int] = []
        for entity_id in entity_ids:
            if not isinstance(entity_id, int) or isinstance(entity_id, bool):
                raise TypeError("entity_ids must contain integer IDs")
            if entity_id <= 0:
                raise ValueError("entity_ids must contain positive IDs")
            normalized_ids.append(entity_id)
        selected_ids = sorted(set(normalized_ids))
        if not selected_ids:
            raise ValueError("Entity cleanup requires at least one selected entity")
        if IDENTITY_ENTITY_ID in selected_ids:
            raise ValueError("The reserved identity entity cannot be deleted")

        async with self.client.transaction() as cur:
            await cur.execute(
                """
            SELECT entity.entity_id
                FROM public.entities entity
                JOIN public.project_entity_contexts context
                  ON context.entity_id = entity.entity_id
                WHERE entity.entity_id = ANY(%s)
                  AND entity.user_name = %s
                  AND context.project_id = %s
                FOR UPDATE
                """,
                (selected_ids, user_name, project_id),
            )
            owned_ids = {int(row["entity_id"]) for row in await cur.fetchall()}
            missing_ids = sorted(set(selected_ids) - owned_ids)
            if missing_ids:
                raise ValueError(
                    "Entity cleanup selection contains IDs outside the project: "
                    f"{missing_ids}"
                )
            return await self._delete_entity_aggregate_with_cursor(
                cur,
                selected_ids,
                project_id,
            )
