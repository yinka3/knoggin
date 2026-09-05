import json
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
from core.knowledge.db.writers.age_projection_writer import (
    AgeProjectionWriter,
)
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
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name
        self.projection = AgeProjectionWriter(client, graph_name=graph_name)

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

            await cur.execute(
                """
                INSERT INTO entities (
                    entity_id,
                    user_name,
                    canonical_name,
                    embedding
                )
                VALUES (%s, %s, %s, NULL)
                ON CONFLICT (entity_id) DO NOTHING
                """,
                (
                    IDENTITY_ENTITY_ID,
                    user_name,
                    user_name,
                ),
            )
            await cur.execute(
                "DELETE FROM entity_aliases WHERE entity_id = %s",
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
            await self.projection.project_identity(cur, identity)

        return identity

    @_storage_write("update_entity_embedding")
    async def update_entity_embedding(
        self, entity_id: int, embedding: List[float], *, project_id: str
    ) -> None:
        project_id = self._require_project_id(project_id, "update_entity_embedding")
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE entities
                SET embedding = %s::vector
                WHERE entity_id = %s
                  AND (
                      entity_id = %s
                      OR EXISTS (
                          SELECT 1 FROM project_entity_contexts context
                          WHERE context.entity_id = entities.entity_id
                            AND context.project_id = %s
                      )
                  )
                """,
                (json.dumps(embedding), entity_id, IDENTITY_ENTITY_ID, project_id),
            )
    @_storage_write("update_entity_aliases")
    async def update_entity_aliases(
        self, alias_updates: Dict[int, List[str]], *, project_id: str, cur=None
    ) -> None:
        project_id = self._require_project_id(project_id, "update_entity_aliases")
        if not alias_updates:
            return

        params = [
            {"id": entity_id, "aliases": aliases}
            for entity_id, aliases in alias_updates.items()
            if aliases
        ]
        if not params:
            return

        async with self._cursor_context(cur) as cur:
            for item in params:
                for alias in item["aliases"]:
                    await cur.execute(
                        """
                        INSERT INTO entity_aliases (entity_id, alias)
                        SELECT %s, %s
                        WHERE EXISTS (
                            SELECT 1
                            FROM entities entity
                            WHERE entity.entity_id = %s
                              AND (
                                  entity.entity_id = %s
                                  OR EXISTS (
                                      SELECT 1 FROM project_entity_contexts context
                                      WHERE context.entity_id = entity.entity_id
                                        AND context.project_id = %s
                                  )
                              )
                        )
                        ON CONFLICT (entity_id, alias) DO NOTHING
                        """,
                        (
                            item["id"],
                            alias,
                            item["id"],
                            IDENTITY_ENTITY_ID,
                            project_id,
                        ),
                    )

            cypher = """
            UNWIND $batch AS data
            MATCH (e:Entity {id: data.id})
            WITH e,
                coalesce(e.aliases, []) + coalesce(data.aliases, [])
                AS all_aliases
            WITH e,
                CASE WHEN size(all_aliases) = 0
                     THEN [null]
                     ELSE all_aliases
                END AS safe_aliases
            UNWIND safe_aliases AS alias
            WITH e, collect(DISTINCT alias) AS merged_aliases
            WITH e,
                [x IN merged_aliases WHERE x IS NOT NULL] AS final_aliases
            SET e.aliases = final_aliases
            RETURN count(e)
            """
            await cur.execute(
                self.client.build_cypher(cypher),
                (
                    json.dumps(
                        {
                            "batch": params,
                        }
                    ),
                ),
            )

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
        await cur.execute(
            """
            DELETE FROM project_entity_contexts
            WHERE project_id = %s AND entity_id = ANY(%s)
            """,
            (project_id, entity_ids),
        )
        await cur.execute(
            """
            DELETE FROM entities entity
            WHERE entity_id = ANY(%s)
              AND NOT EXISTS (
                  SELECT 1 FROM project_entity_contexts context
                  WHERE context.entity_id = entity.entity_id
              )
            RETURNING entity_id
            """,
            (entity_ids,),
        )
        orphaned_ids = sorted(int(row["entity_id"]) for row in await cur.fetchall())
        await self.projection.delete_entities_projection(
            cur,
            orphaned_ids,
            project_id,
        )
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
