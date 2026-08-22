from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Dict, List

from loguru import logger

from common.exceptions import StorageWriteError
from common.scoping import IDENTITY_ENTITY_ID, require_scope_value
from core.knowledge.db.writers.age_projection_writer import (
    AgeProjectionWriter,
)
from infrastructure.postgres_client import PostgresClient


class GraphBuilder:
    """Rebuilds AGE traversal projection from canonical Postgres tables."""

    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.projection = AgeProjectionWriter(client, graph_name=graph_name)

    @asynccontextmanager
    async def _projection_cursor(self, cur):
        if cur is not None:
            yield cur
            return
        async with self.client.transaction() as transaction_cursor:
            yield transaction_cursor

    @staticmethod
    def _to_iso(value):
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        return value

    @classmethod
    def _relationship_projection_params(cls, rows: List[Dict]) -> List[Dict]:
        params = []
        for row in rows:
            params.append(
                {
                    "relationship_id": row["relationship_id"],
                    "project_id": row["project_id"],
                    "entity_a_id": int(row["entity_a_id"]),
                    "entity_b_id": int(row["entity_b_id"]),
                    "relationship_type": row["relationship_type"],
                }
            )
            params[-1]["symmetric"] = bool(row.get("symmetric", False))
        return params

    async def rebuild_project_projection(
        self,
        project_id: str,
        user_name: str,
        *,
        cur=None,
    ) -> Dict[str, int]:
        project_id = require_scope_value(
            project_id,
            "project_id",
            "rebuild_project_projection",
        )
        user_name = require_scope_value(
            user_name,
            "user_name",
            "rebuild_project_projection",
        )
        using_existing_cursor = cur is not None
        try:
            async with self._projection_cursor(cur) as cur:
                await self.projection.clear_project_projection(cur, project_id)

                entities = await self._fetch_entities(
                    cur,
                    project_id,
                    user_name,
                )
                relationships = await self._fetch_relationships(
                    cur,
                    project_id,
                    user_name,
                )
                await self.projection.project_entities(cur, entities)
                await self.projection.replace_relationships_for_entities(
                    cur,
                    project_id,
                    [entity["id"] for entity in entities],
                    self._relationship_projection_params(relationships),
                )

                summary = {
                    "entities": len(entities),
                    "relationships": len(relationships),
                }
                logger.info(
                    f"Rebuilt AGE projection for project {project_id}: {summary}"
                )
                return summary
        except Exception as e:
            if using_existing_cursor:
                raise
            logger.error("AGE projection rebuild failed for {}: {}", project_id, e)
            raise StorageWriteError(
                "rebuild_project_projection",
                details={"error_type": type(e).__name__},
            ) from e

    async def _fetch_entities(
        self,
        cur,
        project_id: str,
        user_name: str,
    ) -> List[Dict]:
        await cur.execute(
            """
            SELECT
                e.entity_id AS id,
                e.user_name,
                e.project_id,
                e.canonical_name,
                e.type,
                e.topic,
                e.last_mentioned_ms AS last_mentioned,
                COALESCE(
                    array_agg(DISTINCT a.alias)
                    FILTER (WHERE a.alias IS NOT NULL),
                    ARRAY[]::text[]
                ) AS aliases
            FROM entities e
            LEFT JOIN entity_aliases a
              ON a.entity_id = e.entity_id
            WHERE (e.project_id = %s OR e.entity_id = %s)
              AND (e.user_name = %s OR e.entity_id = %s)
            GROUP BY e.entity_id
            ORDER BY e.entity_id
            """,
            (project_id, IDENTITY_ENTITY_ID, user_name, IDENTITY_ENTITY_ID),
        )
        return list(await cur.fetchall())

    async def _fetch_relationships(
        self,
        cur,
        project_id: str,
        user_name: str,
    ) -> List[Dict]:
        await cur.execute(
            """
            SELECT
                rel.relationship_id,
                rel.user_name,
                rel.project_id,
                rel.entity_a_id,
                rel.entity_b_id,
                rel.relationship_type,
                rel."symmetric" AS symmetric
            FROM relationships rel
            WHERE rel.project_id = %s
              AND rel.user_name = %s
            ORDER BY rel.relationship_id
            """,
            (project_id, user_name),
        )
        return list(await cur.fetchall())
