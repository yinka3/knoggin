import json
from datetime import datetime
from typing import List, Optional

from loguru import logger

from common.schema.primitives import FactRecord
from infrastructure.postgres_client import PostgresClient
from knoggin_server.knowledge.db.writers.age_projection_writer import (
    AgeProjectionWriter,
)


class FactWriter:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name
        self.projection = AgeProjectionWriter(client, graph_name=graph_name)

    @staticmethod
    def _clean_string(value):
        if isinstance(value, str):
            return value.strip('"')
        return value

    async def create_facts_batch(
        self,
        entity_id: int,
        facts: List[FactRecord],
        user_name: Optional[str] = None,
        session_id: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> int:
        if not facts:
            return 0

        if not self.client.async_pool:
            raise RuntimeError("PostgresClient async_pool is not initialized")
        if not user_name or not project_id:
            raise ValueError(
                "create_facts_batch requires user_name and project_id scope"
            )

        fact_params = []
        for f in facts:
            source_user_name = f.source_user_name or user_name
            source_session_id = f.source_session_id or session_id
            if f.source_msg_id is not None and (
                not source_user_name or not source_session_id
            ):
                raise ValueError(
                    "Cannot link facts to source messages without user/session scope"
                )

            fact_params.append(
                {
                    "id": f.id,
                    "content": f.content,
                    "valid_at": f.valid_at.isoformat(),
                    "invalid_at": f.invalid_at.isoformat() if f.invalid_at else None,
                    "confidence": f.confidence,
                    "source_msg_id": f.source_msg_id,
                    "source_user_name": source_user_name,
                    "source_session_id": source_session_id,
                    "source": f.source,
                }
            )

        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    count = 0
                    for item in fact_params:
                        await cur.execute(
                            """
                            INSERT INTO facts (
                                fact_id,
                                entity_id,
                                user_name,
                                project_id,
                                content,
                                valid_at,
                                invalid_at,
                                confidence,
                                source_msg_id,
                                source_user_name,
                                source_session_id,
                                source
                            )
                            SELECT
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            WHERE EXISTS (
                                SELECT 1
                                FROM entities
                                WHERE entity_id = %s
                                  AND project_id = %s
                            )
                            ON CONFLICT (fact_id) DO UPDATE SET
                                entity_id = EXCLUDED.entity_id,
                                user_name = EXCLUDED.user_name,
                                project_id = EXCLUDED.project_id,
                                content = EXCLUDED.content,
                                valid_at = EXCLUDED.valid_at,
                                invalid_at = EXCLUDED.invalid_at,
                                confidence = EXCLUDED.confidence,
                                source_msg_id = EXCLUDED.source_msg_id,
                                source_user_name = EXCLUDED.source_user_name,
                                source_session_id = EXCLUDED.source_session_id,
                                source = EXCLUDED.source
                            RETURNING fact_id
                            """,
                            (
                                item["id"],
                                entity_id,
                                user_name,
                                project_id,
                                item["content"],
                                item["valid_at"],
                                item["invalid_at"],
                                item["confidence"],
                                item["source_msg_id"],
                                item["source_user_name"],
                                item["source_session_id"],
                                item["source"],
                                entity_id,
                                project_id,
                            ),
                        )
                        record = await cur.fetchone()
                        if record:
                            count += 1

                    if count == 0:
                        raise Exception(
                            "Failed to create facts for entity "
                            f"{entity_id} (parent may not exist)"
                        )

                    projected_count = await self.projection.project_facts(
                        cur,
                        entity_id,
                        fact_params,
                        user_name,
                        session_id,
                        project_id,
                    )
                    if projected_count == 0:
                        raise Exception(
                            f"Failed to project facts for entity {entity_id}"
                        )
                    await self.projection.project_fact_message_links(cur, fact_params)

                    # Write to Postgres fact_search table (Vectors)
                    for f in facts:
                        if f.embedding:
                            await cur.execute(
                                """
                                INSERT INTO fact_search (
                                    fact_id,
                                    entity_id,
                                    user_name,
                                    project_id,
                                    embedding,
                                    invalid_at
                                )
                                VALUES (%s, %s, %s, %s, %s::vector, %s)
                                ON CONFLICT (fact_id) DO UPDATE SET
                                    entity_id = EXCLUDED.entity_id,
                                    user_name = EXCLUDED.user_name,
                                    project_id = EXCLUDED.project_id,
                                    invalid_at = EXCLUDED.invalid_at,
                                    embedding = COALESCE(
                                        EXCLUDED.embedding,
                                        fact_search.embedding
                                    )
                                """,
                                (
                                    f.id,
                                    entity_id,
                                    user_name,
                                    project_id,
                                    json.dumps(f.embedding),
                                    f.invalid_at,
                                ),
                    )

        return count

    async def invalidate_fact(
        self, fact_id: str, invalid_at: datetime, project_id: Optional[str] = None
    ) -> bool:
        if not project_id:
            raise ValueError("invalidate_fact requires project_id scope")
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE facts
                        SET invalid_at = %s
                        WHERE fact_id = %s
                          AND project_id = %s
                        RETURNING fact_id
                        """,
                        (invalid_at, fact_id, project_id),
                    )
                    record = await cur.fetchone()

                    if record:
                        await self.projection.invalidate_fact(
                            cur,
                            fact_id,
                            invalid_at.isoformat(),
                            project_id,
                        )
                        await cur.execute(
                            """
                            UPDATE fact_search
                            SET invalid_at = %s
                            WHERE fact_id = %s
                              AND project_id = %s
                            """,
                            (invalid_at, fact_id, project_id),
                        )
                        return True
        return False

    async def delete_old_invalidated_facts(
        self, cutoff: datetime, project_id: Optional[str] = None
    ) -> int:
        if not project_id:
            raise ValueError("delete_old_invalidated_facts requires project_id scope")
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        DELETE FROM facts
                        WHERE invalid_at IS NOT NULL
                          AND invalid_at < %s
                          AND project_id = %s
                        RETURNING fact_id
                        """,
                        (cutoff, project_id),
                    )
                    records = await cur.fetchall()
                    deleted_ids = [
                        str(self._clean_string(r["fact_id"]))
                        for r in records
                    ]

                    if deleted_ids:
                        await self.projection.delete_facts(
                            cur,
                            deleted_ids,
                            project_id,
                        )
                        logger.info(f"Deleted {len(deleted_ids)} old invalidated facts")

        return len(deleted_ids) if records else 0
