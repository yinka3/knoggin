import json
from typing import Dict, List, Optional

from loguru import logger

from common.scoping import IDENTITY_ENTITY_ID
from common.utils.time_utils import get_now_ms
from infrastructure.postgres_client import PostgresClient
from knoggin_server.knowledge.db.writers.age_projection_writer import (
    AgeProjectionWriter,
)


class EntityWriter:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name
        self.projection = AgeProjectionWriter(client, graph_name=graph_name)

    def _current_time_ms(self) -> int:
        return get_now_ms()

    @staticmethod
    def _require_scope(data: Dict, fields: List[str], label: str) -> None:
        missing = [field for field in fields if not data.get(field)]
        if missing:
            raise ValueError(f"{label} missing required scope fields: {missing}")

    @staticmethod
    def _require_project_id(project_id: Optional[str], operation: str) -> str:
        if not project_id:
            raise ValueError(f"{operation} requires project_id scope")
        return project_id

    @staticmethod
    def _parse_message_id(message_id) -> int:
        if isinstance(message_id, str):
            if message_id.startswith("msg_"):
                return int(message_id.split("_", 1)[1])
            if message_id.startswith("turn_"):
                return int(message_id.split("_", 1)[1]) + 1_000_000_000
        return int(message_id)

    @classmethod
    def _build_evidence_ref(cls, rel: Dict) -> Dict:
        evidence_ref = rel.get("evidence_ref")
        if isinstance(evidence_ref, dict):
            return {
                "user_name": evidence_ref["user_name"],
                "session_id": evidence_ref["session_id"],
                "message_id": cls._parse_message_id(evidence_ref["message_id"]),
            }

        if not rel.get("user_name") or not rel.get("session_id"):
            raise ValueError(
                "Relationship evidence requires user_name and session_id scope"
            )

        return {
            "user_name": rel["user_name"],
            "session_id": rel["session_id"],
            "message_id": cls._parse_message_id(rel["message_id"]),
        }

    @staticmethod
    def _relationship_id(project_id: str, entity_a_id: int, entity_b_id: int) -> str:
        return f"{project_id}:{entity_a_id}:{entity_b_id}"

    async def write_batch(self, entities: List[Dict], relationships: List[Dict]):
        # We need a transaction for both Graph and Hybrid tables
        if not self.client.async_pool:
            raise RuntimeError("PostgresClient async_pool is not initialized")

        now_ms = self._current_time_ms()

        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    # Write Entities to Graph
                    if entities:
                        entity_params = []
                        for e in entities:
                            self._require_scope(
                                e,
                                ["user_name", "session_id", "project_id"],
                                f"Entity {e.get('id')}",
                            )
                            e_clean = e.copy()
                            e_clean["aliases"] = e.get("aliases") or []
                            e_clean["now"] = now_ms
                            entity_params.append(e_clean)

                            await cur.execute(
                                """
                                INSERT INTO entities (
                                    entity_id,
                                    user_name,
                                    project_id,
                                    session_id,
                                    canonical_name,
                                    type,
                                    topic,
                                    confidence,
                                    last_mentioned_ms,
                                    last_updated_ms,
                                    last_profiled_msg_id
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (entity_id) DO UPDATE SET
                                    user_name = EXCLUDED.user_name,
                                    project_id = EXCLUDED.project_id,
                                    session_id = EXCLUDED.session_id,
                                    canonical_name = EXCLUDED.canonical_name,
                                    type = COALESCE(entities.type, EXCLUDED.type),
                                    topic = EXCLUDED.topic,
                                    confidence = EXCLUDED.confidence,
                                    last_mentioned_ms = EXCLUDED.last_mentioned_ms,
                                    last_updated_ms = EXCLUDED.last_updated_ms,
                                    last_profiled_msg_id = COALESCE(
                                        entities.last_profiled_msg_id,
                                        EXCLUDED.last_profiled_msg_id
                                    )
                                """,
                                (
                                    e_clean["id"],
                                    e_clean["user_name"],
                                    e_clean["project_id"],
                                    e_clean["session_id"],
                                    e_clean["canonical_name"],
                                    e_clean.get("type"),
                                    e_clean.get("topic", "General"),
                                    e_clean.get("confidence", 1.0),
                                    now_ms,
                                    now_ms,
                                    e_clean.get("last_profiled_msg_id"),
                                ),
                            )

                            for alias in e_clean["aliases"]:
                                if not alias:
                                    continue
                                await cur.execute(
                                    """
                                    INSERT INTO entity_aliases (entity_id, alias)
                                    VALUES (%s, %s)
                                    ON CONFLICT (entity_id, alias) DO NOTHING
                                    """,
                                    (e_clean["id"], alias),
                                )

                        await self.projection.project_entities(cur, entity_params)

                        # Handle topics in AGE without FOREACH
                        topic_params = [
                            {"id": e["id"], "topic": e["topic"]}
                            for e in entities
                            if e.get("topic")
                        ]
                        await self.projection.project_entity_topics(cur, topic_params)

                        # 2. Write Hybrid Search Data (Vectors)
                        for e in entities:
                            if "embedding" in e and e["embedding"]:
                                await cur.execute(
                                    """
                                    INSERT INTO entity_search (
                                        entity_id,
                                        canonical_name,
                                        user_name,
                                        project_id,
                                        embedding
                                    )
                                    VALUES (%s, %s, %s, %s, %s::vector)
                                    ON CONFLICT (entity_id) DO UPDATE SET
                                        canonical_name = EXCLUDED.canonical_name,
                                        user_name = EXCLUDED.user_name,
                                        project_id = EXCLUDED.project_id,
                                        embedding = COALESCE(
                                            EXCLUDED.embedding,
                                            entity_search.embedding
                                        )
                                    """,
                                    (
                                        e["id"],
                                        e["canonical_name"],
                                        e["user_name"],
                                        e["project_id"],
                                        json.dumps(e["embedding"]),
                                    ),
                                )

                    # 3. Write Relationships to Graph
                    if relationships:
                        rel_params = []
                        for r in relationships:
                            label = (
                                f"Relationship {r.get('entity_a_id')}:"
                                f"{r.get('entity_b_id')}"
                            )
                            self._require_scope(
                                r,
                                ["user_name", "session_id", "project_id"],
                                label,
                            )
                            r_clean = r.copy()
                            # AGE MERGE can be touchy when sorting IDs in Cypher.
                            a_id, b_id = r["entity_a_id"], r["entity_b_id"]
                            if a_id > b_id:
                                a_id, b_id = b_id, a_id
                            r_clean["entity_a_id"] = a_id
                            r_clean["entity_b_id"] = b_id

                            evidence_ref = self._build_evidence_ref(r)
                            relationship_id = self._relationship_id(
                                r["project_id"],
                                a_id,
                                b_id,
                            )
                            await cur.execute(
                                """
                                INSERT INTO relationships (
                                    relationship_id,
                                    user_name,
                                    project_id,
                                    entity_a_id,
                                    entity_b_id,
                                    weight,
                                    confidence,
                                    context,
                                    last_seen_ms
                                )
                                SELECT
                                    %s, %s, %s, %s, %s, 1, %s, %s, %s
                                WHERE EXISTS (
                                    SELECT 1
                                    FROM entities
                                    WHERE entity_id = %s
                                      AND (
                                          project_id = %s
                                          OR entity_id = %s
                                      )
                                )
                                AND EXISTS (
                                    SELECT 1
                                    FROM entities
                                    WHERE entity_id = %s
                                      AND (
                                          project_id = %s
                                          OR entity_id = %s
                                      )
                                )
                                ON CONFLICT (relationship_id) DO UPDATE SET
                                    user_name = EXCLUDED.user_name,
                                    project_id = EXCLUDED.project_id,
                                    entity_a_id = EXCLUDED.entity_a_id,
                                    entity_b_id = EXCLUDED.entity_b_id,
                                    weight = relationships.weight + 1,
                                    confidence = GREATEST(
                                        relationships.confidence,
                                        EXCLUDED.confidence
                                    ),
                                    context = COALESCE(
                                        EXCLUDED.context,
                                        relationships.context
                                    ),
                                    last_seen_ms = EXCLUDED.last_seen_ms
                                RETURNING relationship_id
                                """,
                                (
                                    relationship_id,
                                    r["user_name"],
                                    r["project_id"],
                                    a_id,
                                    b_id,
                                    r.get("confidence", 1.0),
                                    r.get("context"),
                                    now_ms,
                                    a_id,
                                    r["project_id"],
                                    IDENTITY_ENTITY_ID,
                                    b_id,
                                    r["project_id"],
                                    IDENTITY_ENTITY_ID,
                                ),
                            )
                            record = await cur.fetchone()
                            if not record:
                                continue

                            await cur.execute(
                                """
                                INSERT INTO relationship_evidence_refs (
                                    relationship_id,
                                    user_name,
                                    session_id,
                                    message_id
                                )
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (
                                    relationship_id,
                                    user_name,
                                    session_id,
                                    message_id
                                ) DO NOTHING
                                """,
                                (
                                    relationship_id,
                                    evidence_ref["user_name"],
                                    evidence_ref["session_id"],
                                    evidence_ref["message_id"],
                                ),
                            )

                            r_clean["evidence_ref"] = json.dumps(evidence_ref)
                            r_clean["confidence"] = r.get("confidence", 1.0)
                            r_clean["now"] = now_ms
                            rel_params.append(r_clean)

                        await self.projection.project_relationships(cur, rel_params)

        return True

    async def update_entity_profile(
        self,
        entity_id: int,
        canonical_name: str,
        embedding: List[float],
        last_msg_id: int,
        project_id: Optional[str] = None,
    ):
        project_id = self._require_project_id(project_id, "update_entity_profile")
        now_ms = self._current_time_ms()
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE entities
                        SET canonical_name = %s,
                            last_updated_ms = %s,
                            last_profiled_msg_id = %s
                        WHERE entity_id = %s
                          AND (project_id = %s OR entity_id = %s)
                        """,
                        (
                            canonical_name,
                            now_ms,
                            last_msg_id,
                            entity_id,
                            project_id,
                            IDENTITY_ENTITY_ID,
                        ),
                    )

                    cypher = """
                    MATCH (e:Entity {id: $id})
                    WHERE e.project_id = $project_id OR e.id = $identity_entity_id
                    SET e.canonical_name = $canonical_name,
                        e.last_updated = $now,
                        e.last_profiled_msg_id = $last_msg_id
                    RETURN e.id
                    """
                    await cur.execute(
                        self.client.build_cypher(cypher),
                        (
                            json.dumps(
                                {
                                    "id": entity_id,
                                    "canonical_name": canonical_name,
                                    "now": now_ms,
                                    "last_msg_id": last_msg_id,
                                    "project_id": project_id,
                                    "identity_entity_id": IDENTITY_ENTITY_ID,
                                }
                            ),
                        ),
                    )

                    # Update Vector Table
                    await cur.execute(
                        """
                        UPDATE entity_search
                        SET canonical_name = %s, embedding = %s::vector
                        WHERE entity_id = %s
                          AND (project_id = %s OR entity_id = %s)
                        """,
                        (
                            canonical_name,
                            json.dumps(embedding),
                            entity_id,
                            project_id,
                            IDENTITY_ENTITY_ID,
                        ),
                    )
        logger.info(f"Updated entity {entity_id} (checkpoint: msg_{last_msg_id})")

    async def update_entity_canonical_name(
        self, entity_id: int, canonical_name: str, project_id: Optional[str] = None
    ) -> None:
        project_id = self._require_project_id(
            project_id,
            "update_entity_canonical_name",
        )
        now_ms = self._current_time_ms()
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE entities
                        SET canonical_name = %s,
                            last_updated_ms = %s
                        WHERE entity_id = %s
                          AND (project_id = %s OR entity_id = %s)
                        """,
                        (
                            canonical_name,
                            now_ms,
                            entity_id,
                            project_id,
                            IDENTITY_ENTITY_ID,
                        ),
                    )

                    cypher = """
                    MATCH (e:Entity {id: $id})
                    WHERE e.project_id = $project_id OR e.id = $identity_entity_id
                    SET e.canonical_name = $canonical_name, e.last_updated = $now
                    RETURN e.id
                    """
                    await cur.execute(
                        self.client.build_cypher(cypher),
                        (
                            json.dumps(
                                {
                                    "id": entity_id,
                                    "canonical_name": canonical_name,
                                    "now": now_ms,
                                    "project_id": project_id,
                                    "identity_entity_id": IDENTITY_ENTITY_ID,
                                }
                            ),
                        ),
                    )

                    # Update Vector Table
                    await cur.execute(
                        """
                        UPDATE entity_search
                        SET canonical_name = %s
                        WHERE entity_id = %s
                          AND (project_id = %s OR entity_id = %s)
                        """,
                        (canonical_name, entity_id, project_id, IDENTITY_ENTITY_ID),
                    )

    async def update_entity_embedding(
        self, entity_id: int, embedding: List[float], project_id: Optional[str] = None
    ) -> None:
        project_id = self._require_project_id(project_id, "update_entity_embedding")
        now_ms = self._current_time_ms()
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE entities
                        SET last_updated_ms = %s
                        WHERE entity_id = %s
                          AND (project_id = %s OR entity_id = %s)
                        """,
                        (now_ms, entity_id, project_id, IDENTITY_ENTITY_ID),
                    )

                    cypher = """
                    MATCH (e:Entity {id: $id})
                    WHERE e.project_id = $project_id OR e.id = $identity_entity_id
                    SET e.last_updated = $now
                    RETURN e.id
                    """
                    await cur.execute(
                        self.client.build_cypher(cypher),
                        (
                            json.dumps(
                                {
                                    "id": entity_id,
                                    "now": now_ms,
                                    "project_id": project_id,
                                    "identity_entity_id": IDENTITY_ENTITY_ID,
                                }
                            ),
                        ),
                    )

                    # Update Vector Table
                    await cur.execute(
                        """
                        UPDATE entity_search
                        SET embedding = %s::vector
                        WHERE entity_id = %s
                          AND (project_id = %s OR entity_id = %s)
                        """,
                        (
                            json.dumps(embedding),
                            entity_id,
                            project_id,
                            IDENTITY_ENTITY_ID,
                        ),
                    )

    async def update_entity_checkpoint(
        self, entity_id: int, last_msg_id: int, project_id: Optional[str] = None
    ) -> None:
        project_id = self._require_project_id(project_id, "update_entity_checkpoint")
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE entities
                        SET last_profiled_msg_id = %s
                        WHERE entity_id = %s
                          AND (project_id = %s OR entity_id = %s)
                        """,
                        (last_msg_id, entity_id, project_id, IDENTITY_ENTITY_ID),
                    )
                    cypher = """
                    MATCH (e:Entity {id: $id})
                    WHERE e.project_id = $project_id OR e.id = $identity_entity_id
                    SET e.last_profiled_msg_id = $last_msg_id
                    RETURN e.id
                    """
                    await cur.execute(
                        self.client.build_cypher(cypher),
                        (
                            json.dumps(
                                {
                                    "id": entity_id,
                                    "last_msg_id": last_msg_id,
                                    "project_id": project_id,
                                    "identity_entity_id": IDENTITY_ENTITY_ID,
                                }
                            ),
                        ),
                    )

    async def update_entity_aliases(
        self, alias_updates: Dict[int, List[str]], project_id: Optional[str] = None
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

        now_ms = self._current_time_ms()
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    for item in params:
                        await cur.execute(
                            """
                            UPDATE entities
                            SET last_updated_ms = %s
                            WHERE entity_id = %s
                              AND (project_id = %s OR entity_id = %s)
                            """,
                            (
                                now_ms,
                                item["id"],
                                project_id,
                                IDENTITY_ENTITY_ID,
                            ),
                        )
                        for alias in item["aliases"]:
                            await cur.execute(
                                """
                                INSERT INTO entity_aliases (entity_id, alias)
                                SELECT %s, %s
                                WHERE EXISTS (
                                    SELECT 1
                                    FROM entities
                                    WHERE entity_id = %s
                                      AND (
                                          project_id = %s
                                          OR entity_id = %s
                                      )
                                )
                                ON CONFLICT (entity_id, alias) DO NOTHING
                                """,
                                (
                                    item["id"],
                                    alias,
                                    item["id"],
                                    project_id,
                                    IDENTITY_ENTITY_ID,
                                ),
                            )

                    cypher = """
                    UNWIND $batch AS data
                    MATCH (e:Entity {id: data.id})
                    WHERE e.project_id = $project_id OR e.id = $identity_entity_id
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
                    SET e.aliases = final_aliases,
                        e.last_updated = $now
                    RETURN count(e)
                    """
                    await cur.execute(
                        self.client.build_cypher(cypher),
                        (
                            json.dumps(
                                {
                                    "batch": params,
                                    "project_id": project_id,
                                    "identity_entity_id": IDENTITY_ENTITY_ID,
                                    "now": now_ms,
                                }
                            ),
                        ),
                    )

    async def cleanup_null_entities(self, project_id: Optional[str] = None) -> int:
        project_id = self._require_project_id(project_id, "cleanup_null_entities")
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT entity_id
                        FROM entities
                        WHERE type IS NULL
                          AND project_id = %s
                        """,
                        (project_id,),
                    )
                    rows = await cur.fetchall()
                    entity_ids = [row["entity_id"] for row in rows]

                    cypher = """
                    MATCH (e:Entity)
                    WHERE e.type IS NULL AND e.project_id = $project_id
                    DETACH DELETE e
                    RETURN count(e)
                    """
                    await cur.execute(
                        self.client.build_cypher(cypher, "deleted agtype"),
                        (json.dumps({"project_id": project_id}),),
                    )

                    if entity_ids:
                        await cur.execute(
                            "DELETE FROM entity_aliases WHERE entity_id = ANY(%s)",
                            (entity_ids,),
                        )
                        await cur.execute(
                            """
                            DELETE FROM entity_search
                            WHERE entity_id = ANY(%s)
                              AND project_id = %s
                            """,
                            (entity_ids, project_id),
                        )
                        await cur.execute(
                            """
                            DELETE FROM entities
                            WHERE entity_id = ANY(%s)
                              AND project_id = %s
                            """,
                            (entity_ids, project_id),
                        )

        return len(entity_ids)

    async def delete_entity(
        self,
        entity_id: int,
        project_id: Optional[str] = None,
    ) -> bool:
        project_id = self._require_project_id(project_id, "delete_entity")
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        DELETE FROM entity_aliases
                        WHERE entity_id = %s
                          AND EXISTS (
                              SELECT 1
                              FROM entities
                              WHERE entities.entity_id = entity_aliases.entity_id
                                AND project_id = %s
                                AND entities.entity_id <> %s
                          )
                        """,
                        (entity_id, project_id, IDENTITY_ENTITY_ID),
                    )
                    await cur.execute(
                        """
                        DELETE FROM entities
                        WHERE entity_id = %s
                          AND project_id = %s
                          AND entity_id <> %s
                        """,
                        (entity_id, project_id, IDENTITY_ENTITY_ID),
                    )

                    # Delete from Graph
                    cypher = """
                    MATCH (e:Entity {id: $id})
                    WHERE e.project_id = $project_id AND e.id <> $identity_entity_id
                    OPTIONAL MATCH (e)-[:HAS_FACT]->(f:Fact)
                    DETACH DELETE e, f
                    RETURN count(e)
                    """
                    await cur.execute(
                        self.client.build_cypher(cypher),
                        (
                            json.dumps(
                                {
                                    "id": entity_id,
                                    "project_id": project_id,
                                    "identity_entity_id": IDENTITY_ENTITY_ID,
                                }
                            ),
                        ),
                    )

                    # Delete from Vector Table
                    await cur.execute(
                        """
                        DELETE FROM entity_search
                        WHERE entity_id = %s
                          AND project_id = %s
                        """,
                        (entity_id, project_id),
                    )
        return True

    async def bulk_delete_entities(
        self, entity_ids: List[int], project_id: Optional[str] = None
    ) -> int:
        if not entity_ids:
            return 0
        project_id = self._require_project_id(project_id, "bulk_delete_entities")

        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        DELETE FROM entity_aliases
                        WHERE entity_id = ANY(%s)
                          AND EXISTS (
                              SELECT 1
                              FROM entities
                              WHERE entities.entity_id = entity_aliases.entity_id
                                AND project_id = %s
                                AND entities.entity_id <> %s
                          )
                        """,
                        (entity_ids, project_id, IDENTITY_ENTITY_ID),
                    )
                    await cur.execute(
                        """
                        DELETE FROM entities
                        WHERE entity_id = ANY(%s)
                          AND project_id = %s
                          AND entity_id <> %s
                        """,
                        (entity_ids, project_id, IDENTITY_ENTITY_ID),
                    )

                    # Delete from Graph
                    cypher = """
                    MATCH (e:Entity)
                    WHERE e.id IN $ids
                      AND e.project_id = $project_id
                      AND e.id <> $identity_entity_id
                    OPTIONAL MATCH (e)-[:HAS_FACT]->(f:Fact)
                    DETACH DELETE e, f
                    RETURN count(DISTINCT e)
                    """
                    await cur.execute(
                        self.client.build_cypher(cypher, "deleted agtype"),
                        (
                            json.dumps(
                                {
                                    "ids": entity_ids,
                                    "project_id": project_id,
                                    "identity_entity_id": IDENTITY_ENTITY_ID,
                                }
                            ),
                        ),
                    )
                    res = await cur.fetchall()
                    deleted = int(res[0]["deleted"]) if res else 0

                    # Delete from Vector Table
                    await cur.execute(
                        """
                        DELETE FROM entity_search
                        WHERE entity_id = ANY(%s)
                          AND project_id = %s
                        """,
                        (entity_ids, project_id),
                    )

        return deleted
