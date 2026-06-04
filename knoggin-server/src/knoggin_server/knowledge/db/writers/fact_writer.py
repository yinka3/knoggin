import json
from datetime import datetime
from typing import List, Optional

from loguru import logger

from common.schema.primitives import FactRecord
from infrastructure.postgres_client import PostgresClient


class FactWriter:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name

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
            raise ValueError("create_facts_batch requires user_name and project_id scope")

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
                    # Write to AGE Graph
                    cypher = """
                    MATCH (e:Entity {id: $entity_id})
                    WHERE e.project_id = $project_id
                    UNWIND $batch AS item
                    CREATE (f:Fact {
                        id: item.id,
                        source_entity_id: $entity_id,
                        content: item.content,
                        valid_at: item.valid_at,
                        invalid_at: item.invalid_at,
                        confidence: item.confidence,
                        user_name: $user_name,
                        project_id: $project_id,
                        source_msg_id: item.source_msg_id,
                        source_user_name: item.source_user_name,
                        source_session_id: item.source_session_id,
                        source: item.source
                    })
                    CREATE (e)-[:HAS_FACT]->(f)
                    WITH f, item
                    RETURN count(f)
                    """

                    await cur.execute(
                        self.client.build_cypher(cypher, "created_count agtype"),
                        (
                            json.dumps(
                                {
                                    "entity_id": entity_id,
                                    "batch": fact_params,
                                    "user_name": user_name,
                                    "session_id": session_id,
                                    "project_id": project_id,
                                }
                            ),
                        ),
                    )
                    record = await cur.fetchone()
                    count = int(record["created_count"]) if record else 0

                    if count == 0:
                        raise Exception(
                            f"Failed to create facts for entity {entity_id} (parent may not exist)"
                        )

                    # Handle message links without FOREACH
                    msg_params = [p for p in fact_params if p["source_msg_id"] is not None]
                    if msg_params:
                        cypher_m = """
                        UNWIND $batch AS item
                        MATCH (f:Fact {id: item.id})
                        MERGE (m:Message {
                            user_name: item.source_user_name,
                            session_id: item.source_session_id,
                            id: item.source_msg_id
                        })
                        MERGE (f)-[:EXTRACTED_FROM]->(m)
                        RETURN count(f)
                        """
                        await cur.execute(
                            self.client.build_cypher(cypher_m),
                            (json.dumps({"batch": msg_params}),),
                        )


                    # Write to Postgres fact_search table (Vectors)
                    for f in facts:
                        if f.embedding:
                            await cur.execute(
                                """
                                INSERT INTO fact_search (fact_id, entity_id, user_name, project_id, embedding, invalid_at)
                                VALUES (%s, %s, %s, %s, %s::vector, %s)
                                ON CONFLICT (fact_id) DO UPDATE SET
                                    user_name = EXCLUDED.user_name,
                                    project_id = EXCLUDED.project_id,
                                    invalid_at = EXCLUDED.invalid_at,
                                    embedding = COALESCE(EXCLUDED.embedding, fact_search.embedding)
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
                    # Update Graph
                    cypher = """
                    MATCH (f:Fact {id: $fact_id})
                    WHERE f.project_id = $project_id
                    SET f.invalid_at = $invalid_at
                    RETURN f.id
                    """
                    await cur.execute(
                        self.client.build_cypher(cypher, "id agtype"),
                        (
                            json.dumps(
                                {
                                    "fact_id": fact_id,
                                    "invalid_at": invalid_at.isoformat(),
                                    "project_id": project_id,
                                }
                            ),
                        ),
                    )
                    record = await cur.fetchone()

                    # Update Vector Table
                    if record:
                        await cur.execute(
                            "UPDATE fact_search SET invalid_at = %s WHERE fact_id = %s AND project_id = %s",
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
                    # Delete from Graph
                    cypher = """
                    MATCH (f:Fact)
                    WHERE f.invalid_at IS NOT NULL AND f.invalid_at < $cutoff
                      AND f.project_id = $project_id
                    WITH f, f.id as fact_id
                    DETACH DELETE f
                    RETURN fact_id
                    """
                    # We return the IDs to delete them from Postgres table easily
                    await cur.execute(
                        self.client.build_cypher(cypher, "fact_id agtype"),
                        (
                            json.dumps(
                                {"cutoff": cutoff.isoformat(), "project_id": project_id}
                            ),
                        ),
                    )
                    records = await cur.fetchall()
                    deleted_ids = [
                        r["fact_id"].strip('"')
                        if isinstance(r["fact_id"], str)
                        else r["fact_id"]
                        for r in records
                    ]

                    # Delete from Vector Table
                    if deleted_ids:
                        await cur.execute(
                            "DELETE FROM fact_search WHERE fact_id = ANY(%s) AND project_id = %s",
                            (deleted_ids, project_id),
                        )
                        logger.info(f"Deleted {len(deleted_ids)} old invalidated facts")

        return len(deleted_ids) if records else 0
