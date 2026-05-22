import json
import time
from typing import Dict, List

from loguru import logger

from infrastructure.postgres_client import PostgresClient


class AgeEntityWriter:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name

    def _current_time_ms(self) -> int:
        return int(time.time() * 1000)

    async def write_batch(self, entities: List[Dict], relationships: List[Dict]):
        # We need a transaction for both Graph and Hybrid tables
        if not self.client.async_pool:
            raise RuntimeError("PostgresClient async_pool is not initialized")
            
        now_ms = self._current_time_ms()
        
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    
                    # 1. Write Entities to Graph
                    if entities:
                        entity_params = []
                        for e in entities:
                            e_clean = e.copy()
                            e_clean["aliases"] = e.get("aliases") or []
                            e_clean["now"] = now_ms
                            entity_params.append(e_clean)
                            
                        # Notice we omit 'embedding' from the graph properties to save space.
                        # It will only live in the entity_search table.
                        cypher_e = """
                        UNWIND $batch AS data
                        MERGE (e:Entity {id: data.id})
                        ON CREATE SET
                            e.session_id = data.session_id,
                            e.project_id = data.project_id,
                            e.canonical_name = data.canonical_name,
                            e.aliases = data.aliases,
                            e.type = data.type,
                            e.confidence = data.confidence,
                            e.last_updated = data.now,
                            e.last_mentioned = data.now
                        ON MATCH SET
                            e.canonical_name = data.canonical_name,
                            e.confidence = data.confidence,
                            e.last_updated = data.now,
                            e.last_mentioned = data.now
                            
                        WITH e, data
                        // Handle aliases in AGE (concatenation via python pre-processing is safer, but we try AGE array functions if possible,
                        // or we just overwrite if it's a batch update, but we need to merge aliases)
                        // For AGE, to avoid agtype list concat errors, we'll overwrite with data.aliases if this is new, 
                        // but ideally we'd merge them. For now, we set them.
                        // (We will handle alias merges explicitly in update_entity_aliases)
                        
                        FOREACH (_ IN CASE WHEN data.topic IS NOT NULL AND data.topic <> "" THEN [1] ELSE [] END |
                            MERGE (t:Topic {name: data.topic})
                            MERGE (e)-[:BELONGS_TO]->(t)
                        )
                        RETURN e.id
                        """
                        # We run the graph query
                        await cur.execute(self.client.build_cypher(cypher_e), (json.dumps({"batch": entity_params}),))
                        
                        # 2. Write Hybrid Search Data (Vectors)
                        for e in entities:
                            if "embedding" in e and e["embedding"]:
                                await cur.execute(
                                    """
                                    INSERT INTO entity_search (entity_id, canonical_name, user_name, project_id, embedding)
                                    VALUES (%s, %s, %s, %s, %s::vector)
                                    ON CONFLICT (entity_id) DO UPDATE SET
                                        canonical_name = EXCLUDED.canonical_name,
                                        embedding = COALESCE(EXCLUDED.embedding, entity_search.embedding)
                                    """,
                                    (
                                        e["id"], 
                                        e["canonical_name"], 
                                        e.get("user_name", "default_user"), 
                                        e.get("project_id", "default_project"),
                                        e["embedding"]
                                    )
                                )

                    # 3. Write Relationships to Graph
                    if relationships:
                        rel_params = []
                        for r in relationships:
                            r_clean = r.copy()
                            r_clean["confidence"] = r.get("confidence", 1.0)
                            r_clean["now"] = now_ms
                            rel_params.append(r_clean)
                            
                        cypher_r = """
                        UNWIND $batch AS rel
                        MATCH (a:Entity {id: rel.entity_a_id})
                        MATCH (b:Entity {id: rel.entity_b_id})
                        WITH a, b, rel,
                            CASE WHEN a.id < b.id THEN a ELSE b END AS node_a,
                            CASE WHEN a.id < b.id THEN b ELSE a END AS node_b
                        MERGE (node_a)-[r:RELATED_TO]->(node_b)
                        ON CREATE SET
                            r.weight = 1,
                            r.confidence = rel.confidence,
                            r.last_seen = rel.now,
                            r.message_ids = [rel.message_id],
                            r.context = rel.context
                        ON MATCH SET
                            r.weight = r.weight + 1,
                            r.confidence = CASE WHEN rel.confidence > r.confidence THEN rel.confidence ELSE r.confidence END,
                            r.last_seen = rel.now,
                            r.context = CASE WHEN rel.context IS NOT NULL THEN rel.context ELSE r.context END
                        RETURN count(r)
                        """
                        # Note: message_ids append for ON MATCH SET is omitted here because agtype list append 
                        # can be problematic. We handle complex edge updates in merge_entities.
                        await cur.execute(self.client.build_cypher(cypher_r), (json.dumps({"batch": rel_params}),))
                        
        return True

    async def update_entity_profile(
        self,
        entity_id: int,
        canonical_name: str,
        embedding: List[float],
        last_msg_id: int,
    ):
        now_ms = self._current_time_ms()
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    # Update Graph
                    cypher = """
                    MATCH (e:Entity {id: $id})
                    SET e.canonical_name = $canonical_name,
                        e.last_updated = $now,
                        e.last_profiled_msg_id = $last_msg_id
                    RETURN e.id
                    """
                    await cur.execute(self.client.build_cypher(cypher), (json.dumps({
                        "id": entity_id, "canonical_name": canonical_name, 
                        "now": now_ms, "last_msg_id": last_msg_id
                    }),))
                    
                    # Update Vector Table
                    await cur.execute(
                        """
                        UPDATE entity_search 
                        SET canonical_name = %s, embedding = %s::vector
                        WHERE entity_id = %s
                        """,
                        (canonical_name, embedding, entity_id)
                    )
        logger.info(f"Updated entity {entity_id} (checkpoint: msg_{last_msg_id})")

    async def update_entity_canonical_name(self, entity_id: int, canonical_name: str) -> None:
        now_ms = self._current_time_ms()
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    # Update Graph
                    cypher = "MATCH (e:Entity {id: $id}) SET e.canonical_name = $canonical_name, e.last_updated = $now RETURN e.id"
                    await cur.execute(self.client.build_cypher(cypher), (json.dumps({"id": entity_id, "canonical_name": canonical_name, "now": now_ms}),))
                    
                    # Update Vector Table
                    await cur.execute("UPDATE entity_search SET canonical_name = %s WHERE entity_id = %s", (canonical_name, entity_id))

    async def update_entity_embedding(self, entity_id: int, embedding: List[float]) -> None:
        now_ms = self._current_time_ms()
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    # Mark updated in Graph
                    cypher = "MATCH (e:Entity {id: $id}) SET e.last_updated = $now RETURN e.id"
                    await cur.execute(self.client.build_cypher(cypher), (json.dumps({"id": entity_id, "now": now_ms}),))
                    
                    # Update Vector Table
                    await cur.execute("UPDATE entity_search SET embedding = %s::vector WHERE entity_id = %s", (embedding, entity_id))

    async def update_entity_checkpoint(self, entity_id: int, last_msg_id: int) -> None:
        cypher = "MATCH (e:Entity {id: $id}) SET e.last_profiled_msg_id = $last_msg_id RETURN e.id"
        await self.client.execute_write(self.client.build_cypher(cypher), (json.dumps({"id": entity_id, "last_msg_id": last_msg_id}),))

    async def update_entity_aliases(self, alias_updates: Dict[int, List[str]]):
        if not alias_updates:
            return

        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    # For AGE, the safest way to append arrays without agtype casting nightmares 
                    # is to read existing, combine in Python, write back.
                    for eid, new_aliases in alias_updates.items():
                        # Read existing
                        read_cyp = "MATCH (e:Entity {id: $id}) RETURN e.aliases"
                        res = await self.client.execute_read(self.client.build_cypher(read_cyp, "aliases agtype"), (json.dumps({"id": eid}),))
                        
                        existing = []
                        if res and res[0]["aliases"]:
                            existing = res[0]["aliases"]
                            
                        combined = list(set(existing + new_aliases))
                        
                        # Write back
                        write_cyp = "MATCH (e:Entity {id: $id}) SET e.aliases = $aliases, e.last_updated = $now RETURN e.id"
                        await cur.execute(self.client.build_cypher(write_cyp), (json.dumps({"id": eid, "aliases": combined, "now": self._current_time_ms()}),))
                        
        logger.debug(f"Updated aliases for {len(alias_updates)} entities")

    async def cleanup_null_entities(self) -> int:
        cypher = """
        MATCH (e:Entity)
        WHERE e.type IS NULL
        DETACH DELETE e
        RETURN count(e)
        """
        res = await self.client.execute_read(self.client.build_cypher(cypher, "deleted agtype"), ("{}",))
        return int(res[0]["deleted"]) if res else 0

    async def delete_entity(self, entity_id: int) -> bool:
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    # Delete from Graph
                    cypher = """
                    MATCH (e:Entity {id: $id})
                    OPTIONAL MATCH (e)-[:HAS_FACT]->(f:Fact)
                    DETACH DELETE e, f
                    RETURN count(e)
                    """
                    await cur.execute(self.client.build_cypher(cypher), (json.dumps({"id": entity_id}),))
                    
                    # Delete from Vector Table
                    await cur.execute("DELETE FROM entity_search WHERE entity_id = %s", (entity_id,))
        return True

    async def bulk_delete_entities(self, entity_ids: List[int]) -> int:
        if not entity_ids:
            return 0
            
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    # Delete from Graph
                    cypher = """
                    MATCH (e:Entity)
                    WHERE e.id IN $ids
                    OPTIONAL MATCH (e)-[:HAS_FACT]->(f:Fact)
                    DETACH DELETE e, f
                    RETURN count(DISTINCT e)
                    """
                    res = await self.client.execute_read(self.client.build_cypher(cypher, "deleted agtype"), (json.dumps({"ids": entity_ids}),))
                    deleted = int(res[0]["deleted"]) if res else 0
                    
                    # Delete from Vector Table
                    await cur.execute("DELETE FROM entity_search WHERE entity_id = ANY(%s)", (entity_ids,))
                    
        return deleted
