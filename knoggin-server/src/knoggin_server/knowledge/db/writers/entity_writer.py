import json
from typing import Dict, List, Optional

from loguru import logger

from common.scoping import IDENTITY_ENTITY_ID
from common.utils.time_utils import get_now_ms
from infrastructure.postgres_client import PostgresClient


class EntityWriter:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name

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

                        # Notice we omit 'embedding' from the graph properties to save space.
                        # It will only live in the entity_search table.
                        cypher_e = """
                        UNWIND $batch AS data
                        MERGE (e:Entity {id: data.id})
                        SET e.user_name = data.user_name,
                            e.session_id = data.session_id,
                            e.project_id = data.project_id,
                            e.canonical_name = data.canonical_name,
                            e.type = coalesce(e.type, data.type),
                            e.confidence = data.confidence,
                            e.last_updated = data.now,
                            e.last_mentioned = data.now
                        
                        WITH e, data, coalesce(e.aliases, []) + coalesce(data.aliases, []) AS all_aliases
                        WITH e, CASE WHEN size(all_aliases) = 0 THEN [null] ELSE all_aliases END AS safe_aliases
                        UNWIND safe_aliases AS alias
                        WITH e, collect(DISTINCT alias) AS merged_aliases
                        WITH e, [x IN merged_aliases WHERE x IS NOT NULL] AS final_aliases
                        SET e.aliases = final_aliases
                        
                        RETURN e.id
                        """
                        await cur.execute(
                            self.client.build_cypher(cypher_e),
                            (json.dumps({"batch": entity_params}),),
                        )
                        
                        # Handle topics in AGE without FOREACH
                        topic_params = [
                            {"id": e["id"], "topic": e["topic"]}
                            for e in entities
                            if e.get("topic")
                        ]
                        if topic_params:
                            cypher_t = """
                            UNWIND $batch AS data
                            MATCH (e:Entity {id: data.id})
                            MERGE (t:Topic {name: data.topic})
                            MERGE (e)-[:BELONGS_TO]->(t)
                            RETURN count(e)
                            """
                            await cur.execute(
                                self.client.build_cypher(cypher_t),
                                (json.dumps({"batch": topic_params}),),
                            )

                        # 2. Write Hybrid Search Data (Vectors)
                        for e in entities:
                            if "embedding" in e and e["embedding"]:
                                await cur.execute(
                                    """
                                    INSERT INTO entity_search (entity_id, canonical_name, user_name, project_id, embedding)
                                    VALUES (%s, %s, %s, %s, %s::vector)
                                    ON CONFLICT (entity_id) DO UPDATE SET
                                        canonical_name = EXCLUDED.canonical_name,
                                        user_name = EXCLUDED.user_name,
                                        project_id = EXCLUDED.project_id,
                                        embedding = COALESCE(EXCLUDED.embedding, entity_search.embedding)
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
                            self._require_scope(
                                r,
                                ["user_name", "session_id", "project_id"],
                                f"Relationship {r.get('entity_a_id')}:{r.get('entity_b_id')}",
                            )
                            r_clean = r.copy()
                            # Sort IDs in python to avoid AGE MERGE planner bugs with variables
                            a_id, b_id = r["entity_a_id"], r["entity_b_id"]
                            if a_id > b_id:
                                a_id, b_id = b_id, a_id
                            r_clean["entity_a_id"] = a_id
                            r_clean["entity_b_id"] = b_id
                            
                            # Serialize to string to prevent AGE backend crashes when concatenating map objects
                            r_clean["evidence_ref"] = json.dumps(self._build_evidence_ref(r))
                            r_clean["confidence"] = r.get("confidence", 1.0)
                            r_clean["now"] = now_ms
                            rel_params.append(r_clean)

                        cypher_r = """
                        UNWIND $batch AS rel
                        MATCH (a:Entity {id: rel.entity_a_id})
                        MATCH (b:Entity {id: rel.entity_b_id})
                        WHERE (a.project_id = rel.project_id OR a.id = $identity_entity_id)
                          AND (b.project_id = rel.project_id OR b.id = $identity_entity_id)
                        MERGE (a)-[r:RELATED_TO]->(b)
                        SET r.weight = coalesce(r.weight, 0) + 1,
                            r.confidence = CASE 
                                WHEN r.confidence IS NULL THEN rel.confidence 
                                WHEN rel.confidence > r.confidence THEN rel.confidence 
                                ELSE r.confidence 
                            END,
                            r.last_seen = rel.now,
                            r.message_ids = CASE
                                WHEN r.message_ids IS NULL THEN [rel.evidence_ref]
                                WHEN rel.evidence_ref IN coalesce(r.message_ids, []) THEN r.message_ids
                                ELSE coalesce(r.message_ids, []) + [rel.evidence_ref]
                            END,
                            r.context = CASE 
                                WHEN r.context IS NULL THEN rel.context
                                WHEN rel.context IS NOT NULL THEN rel.context 
                                ELSE r.context 
                            END
                        RETURN count(r)
                        """
                        await cur.execute(
                            self.client.build_cypher(cypher_r),
                            (
                                json.dumps(
                                    {
                                        "batch": rel_params,
                                        "identity_entity_id": IDENTITY_ENTITY_ID,
                                    }
                                ),
                            ),
                        )

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
                    # Update Graph
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
        project_id = self._require_project_id(project_id, "update_entity_canonical_name")
        now_ms = self._current_time_ms()
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    # Update Graph
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
                    # Mark updated in Graph
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
                        (json.dumps(embedding), entity_id, project_id, IDENTITY_ENTITY_ID),
                    )

    async def update_entity_checkpoint(
        self, entity_id: int, last_msg_id: int, project_id: Optional[str] = None
    ) -> None:
        project_id = self._require_project_id(project_id, "update_entity_checkpoint")
        cypher = """
        MATCH (e:Entity {id: $id})
        WHERE e.project_id = $project_id OR e.id = $identity_entity_id
        SET e.last_profiled_msg_id = $last_msg_id
        RETURN e.id
        """
        await self.client.execute_write(
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

        cypher = """
        UNWIND $batch AS data
        MATCH (e:Entity {id: data.id})
        WHERE e.project_id = $project_id OR e.id = $identity_entity_id
        WITH e, coalesce(e.aliases, []) + coalesce(data.aliases, []) AS all_aliases
        WITH e, CASE WHEN size(all_aliases) = 0 THEN [null] ELSE all_aliases END AS safe_aliases
        UNWIND safe_aliases AS alias
        WITH e, collect(DISTINCT alias) AS merged_aliases
        WITH e, [x IN merged_aliases WHERE x IS NOT NULL] AS final_aliases
        SET e.aliases = final_aliases,
            e.last_updated = $now
        RETURN count(e)
        """
        await self.client.execute_write(
            self.client.build_cypher(cypher),
            (
                json.dumps(
                    {
                        "batch": params,
                        "project_id": project_id,
                        "identity_entity_id": IDENTITY_ENTITY_ID,
                        "now": self._current_time_ms(),
                    }
                ),
            ),
        )

    async def cleanup_null_entities(self, project_id: Optional[str] = None) -> int:
        project_id = self._require_project_id(project_id, "cleanup_null_entities")
        cypher = """
        MATCH (e:Entity)
        WHERE e.type IS NULL AND e.project_id = $project_id
        DETACH DELETE e
        RETURN count(e)
        """
        res = await self.client.execute_read(
            self.client.build_cypher(cypher, "deleted agtype"),
            (json.dumps({"project_id": project_id}),),
        )
        return int(res[0]["deleted"]) if res else 0

    async def delete_entity(self, entity_id: int, project_id: Optional[str] = None) -> bool:
        project_id = self._require_project_id(project_id, "delete_entity")
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
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
                        "DELETE FROM entity_search WHERE entity_id = %s AND project_id = %s",
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
                    res = await self.client.execute_read(
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
                    deleted = int(res[0]["deleted"]) if res else 0

                    # Delete from Vector Table
                    await cur.execute(
                        "DELETE FROM entity_search WHERE entity_id = ANY(%s) AND project_id = %s",
                        (entity_ids, project_id),
                    )

        return deleted
