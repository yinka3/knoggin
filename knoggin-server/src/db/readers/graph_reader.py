from datetime import datetime, timezone, timedelta
from loguru import logger
from typing import Dict, List, Optional, Set, Tuple
from neo4j import AsyncDriver


class GraphReader:
    def __init__(self, driver: AsyncDriver):
        self.driver = driver

    async def get_message_text(self, message_id: int) -> str:
        """
        Fetch message text on demand.
        """
        query = "MATCH (m:Message {id: $id}) RETURN m.content as content"

        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"id": int(message_id)})
                record = await result.single()
                return record["content"] if record else ""
        except Exception as e:
            logger.error(f"Failed to get message text for {message_id}: {e}")
            return ""


    async def get_messages_by_ids(self, ids: List[int]) -> List[Dict]:
        """Batch fetch messages by their IDs."""
        if not ids:
            return []
        query = """
        MATCH (m:Message)
        WHERE m.id IN $ids
        RETURN m.id as id,
               m.role as role,
               m.content as content,
               m.timestamp as timestamp
        ORDER BY id ASC
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"ids": ids})
                return await result.data()
        except Exception as e:
            logger.error(f"Failed to fetch messages by ids: {e}")
            return []


    async def get_surrounding_messages(self, message_id: int, forward: int = 3, target_total: int = 10) -> List[Dict]:
        """Fetch surrounding messages for context from Memgraph."""
        back_limit = max(0, target_total - forward - 1)

        safe_query = """
        MATCH (target:Message {id: $msg_id})
        WITH target.timestamp AS target_ts, target
        
        CALL {
            WITH target_ts, target
            MATCH (prev:Message) 
            WHERE prev.timestamp <= target_ts AND prev.id <> target.id
            RETURN prev
            ORDER BY prev.timestamp DESC
            LIMIT $back_limit
        }
        WITH target_ts, target, collect(prev) AS prev_msgs
        
        CALL {
            WITH target_ts, target
            MATCH (next:Message)
            WHERE next.timestamp >= target_ts AND next.id <> target.id
            RETURN next
            ORDER BY next.timestamp ASC
            LIMIT $forward_limit
        }
        WITH target, prev_msgs, collect(next) AS next_msgs
        
        UNWIND (prev_msgs + [target] + next_msgs) AS m
        WITH m WHERE m IS NOT NULL
        RETURN m.id as id,
               m.role as role,
               m.content as content,
               m.timestamp as timestamp
        ORDER BY timestamp ASC
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(safe_query, {
                    "msg_id": message_id,
                    "back_limit": back_limit,
                    "forward_limit": forward
                })
                return await result.data()
        except Exception as e:
            logger.error(f"Failed to fetch surrounding messages for {message_id}: {e}")
            return []


    async def get_neighbor_ids(self, entity_id: int) -> set[int]:
        query = """
        MATCH (e:Entity {id: $entity_id})-[:RELATED_TO]-(neighbor:Entity)
        RETURN neighbor.id as neighbor_id
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"entity_id": entity_id})
                records = await result.data()
                return {record["neighbor_id"] for record in records}
        except Exception as e:
            logger.error(f"Failed to get neighbor IDs for {entity_id}: {e}")
            return set()
    

    async def get_parent_entities(self, entity_id: int) -> List[Dict]:
        """Get entities this one is PART_OF."""
        query = """
        MATCH (child:Entity {id: $entity_id})-[:PART_OF]->(parent:Entity)
        RETURN parent.id as id,
            parent.canonical_name as canonical_name,
            parent.type as type,
           [(parent)-[:HAS_FACT]->(f) WHERE f.invalid_at IS NULL | f.content] as facts
        """
        
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"entity_id": entity_id})
                return await result.data()
        
        except Exception as e:
            logger.error(f"Failed to get parents for entity {entity_id}: {e}")
            return []


    async def get_neighbor_entities(self, entity_id: int, limit: int = 5) -> List[Dict]:
        """Get canonical names of connected entities."""
        query = """
        MATCH (e:Entity {id: $entity_id})-[:RELATED_TO]-(neighbor:Entity)
        RETURN neighbor.id as id, neighbor.canonical_name as name
        ORDER BY neighbor.last_mentioned DESC
        LIMIT $limit
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"entity_id": entity_id, "limit": limit})
                records = await result.data()
                return [{"id": record["id"], "name": record["name"]} for record in records]
        except Exception as e:
            logger.error(f"Failed to get neighbor entities for {entity_id}: {e}")
            return []
        

    async def get_child_entities(self, entity_id: int) -> List[Dict]:
        """Get entities that are PART_OF this one."""
        query = """
        MATCH (child:Entity)-[:PART_OF]->(parent:Entity {id: $entity_id})
        RETURN child.id as id,
            child.canonical_name as canonical_name,
            child.type as type,
            [(child)-[:HAS_FACT]->(f) WHERE f.invalid_at IS NULL | f.content] as facts
        """
        
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"entity_id": entity_id})
                return await result.data()
        
        except Exception as e:
            logger.error(f"Failed to get children for entity {entity_id}: {e}")
            return []
    

    async def has_direct_edge(self, id_a: int, id_b: int) -> bool:
        query = """
        MATCH (a:Entity {id: $id_a})-[r:RELATED_TO]-(b:Entity {id: $id_b})
        RETURN count(r) > 0 as connected
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"id_a": id_a, "id_b": id_b})
                record = await result.single()
                return record["connected"] if record else False
        except Exception as e:
            logger.error(f"Failed to check direct edge between {id_a} and {id_b}: {e}")
            return False


    async def has_hierarchy_edge(self, id_a: int, id_b: int) -> bool:
        """Check if PART_OF relationship exists in either direction."""
        query = """
        MATCH (a:Entity {id: $id_a}), (b:Entity {id: $id_b})
        WHERE (a)-[:PART_OF]->(b) OR (b)-[:PART_OF]->(a)
        RETURN true as exists
        LIMIT 1
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"id_a": id_a, "id_b": id_b})
                return await result.single() is not None
        except Exception as e:
            logger.error(f"Failed to check hierarchy edge between {id_a} and {id_b}: {e}")
            return False
    

    async def search_messages_vector(self, query_embedding: List[float], limit: int = 50) -> List[Tuple[int, float]]:
        query = """
        CALL vector_search.search('message_vec', $limit, $embedding)
        YIELD node, similarity
        WITH node, similarity
        RETURN node.id as id, similarity
        """

        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"embedding": query_embedding, "limit": limit})
                records = await result.data()
                return [(r["id"], r["similarity"]) for r in records]
        except Exception as e:
            logger.error(f"Failed to search messages by vector: {e}")
            return []
    


    async def get_hierarchy_candidates(
        self,
        topic: str,
        parent_type: str,
        child_types: List[str],
        min_weight: int = 2
    ) -> List[Dict]:
        """
        Get candidate pairs for hierarchy detection.
        Returns pairs where RELATED_TO exists but PART_OF doesn't.
        """
        query = """
        MATCH (parent:Entity)-[:BELONGS_TO]->(t:Topic {name: $topic})
        MATCH (child:Entity)-[:BELONGS_TO]->(t)
        MATCH (parent)-[r:RELATED_TO]-(child)
        WHERE parent.type = $parent_type
        AND child.type IN $child_types
        AND r.weight >= $min_weight
        AND NOT (child)-[:PART_OF]->(parent)
        RETURN 
        parent.id AS parent_id,
        parent.canonical_name AS parent_name,
        parent.embedding AS parent_embedding,
        child.id AS child_id,
        child.canonical_name AS child_name,
        child.embedding AS child_embedding,
        r.weight AS weight
        """
        
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {
                    "topic": topic,
                    "parent_type": parent_type,
                    "child_types": child_types,
                    "min_weight": min_weight
                })
                return await result.data()
        except Exception as e:
            logger.error(f"Hierarchy candidate query failed: {e}")
            return []
    

    async def list_preferences(self, session_id: str, kind: Optional[str] = None) -> List[Dict]:
        where_kind = "AND p.kind = $kind" if kind else ""
        query = f"""
        MATCH (p:Preference {{session_id: $session_id}})
        WHERE true {where_kind}
        RETURN p.id AS id, p.content AS content, p.kind AS kind, p.created_at AS created_at
        ORDER BY p.created_at DESC
        """
        params = {"session_id": session_id}
        if kind:
            params["kind"] = kind
        
        try:
            async with self.driver.session() as session:
                result = await session.run(query, params)
                return await result.data()
        except Exception as e:
            logger.error(f"Failed to list preferences: {e}")
            return []
    

    async def get_graph_stats(self) -> Dict[str, int]:
        """Get aggregate counts for dashboard."""
        query = """
        MATCH (e:Entity) WITH count(e) as entities
        MATCH (f:Fact) WHERE f.invalid_at IS NULL WITH entities, count(f) as facts
        MATCH ()-[r:RELATED_TO]->() WITH entities, facts, count(r) as relationships
        RETURN entities, facts, relationships
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query)
                record = await result.single()
                if not record:
                    return {"entities": 0, "facts": 0, "relationships": 0}
                return {
                    "entities": record["entities"] or 0,
                    "facts": record["facts"] or 0,
                    "relationships": record["relationships"] or 0
                }
        except Exception as e:
            logger.error(f"Failed to get graph stats: {e}")
            return {"entities": 0, "facts": 0, "relationships": 0}
    

