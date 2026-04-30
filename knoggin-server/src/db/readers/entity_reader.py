from datetime import datetime, timezone, timedelta
from loguru import logger
from typing import Dict, List, Optional, Set, Tuple
from neo4j import AsyncDriver


class EntityReader:
    def __init__(self, driver: AsyncDriver):
        self.driver = driver

    async def get_max_entity_id(self) -> int:
        """
        Returns the highest entity ID currently in the graph.
        Used on startup to sync Redis counters.
        """
        query = "MATCH (e:Entity) RETURN max(e.id) as max_id"
        try:
            async with self.driver.session() as session:
                result = await session.run(query)
                record = await result.single()
                return record["max_id"] if record and record["max_id"] is not None else 0
        except Exception as e:
            logger.error(f"Failed to get max entity ID: {e}")
            raise
    


    async def get_entity_embedding(self, entity_id: int) -> List[float]:
        query = "MATCH (e:Entity {id: $id}) RETURN e.embedding as embedding"
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"id": entity_id})
                record = await result.single()
                return record["embedding"] if record else []
        except Exception as e:
            logger.error(f"Failed to get embedding for entity {entity_id}: {e}")
            return []
    
    

    async def list_entities(
        self,
        limit: int = 20,
        offset: int = 0,
        topic: Optional[str] = None,
        entity_type: Optional[str] = None,
        search: Optional[str] = None
    ) -> Tuple[List[Dict], int]:
        """Paginated entity listing with optional filters."""
        
        where_clauses = []
        params = {"limit": limit, "offset": offset}
        
        if entity_type:
            where_clauses.append("e.type = $entity_type")
            params["entity_type"] = entity_type
        
        if search:
            where_clauses.append("toLower(e.canonical_name) CONTAINS toLower($search)")
            params["search"] = search
        
        if topic:
            where_clauses.append("t.name = $topic")
            params["topic"] = topic
        
        topic_match = "MATCH (e)-[:BELONGS_TO]->(t:Topic)" if topic else "OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)"
        where_str = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        count_query = f"""
        MATCH (e:Entity)
        {topic_match}
        {where_str}
        RETURN count(e) AS total
        """
        
        data_query = f"""
        MATCH (e:Entity)
        {topic_match}
        {where_str}
        WITH e, t,
            [(e)-[:HAS_FACT]->(f) WHERE f.invalid_at IS NULL | f.content][0..2] AS fact_snippets
        RETURN e.id AS id,
            e.session_id AS session_id,
            e.canonical_name AS canonical_name,
            e.type AS type,
            t.name AS topic,
            e.last_mentioned / 1000 AS last_mentioned,
            CASE WHEN size(fact_snippets) > 0
                THEN reduce(s = '', x IN fact_snippets | s + CASE WHEN s = '' THEN '' ELSE '. ' END + x)
                ELSE null
            END AS summary
        ORDER BY last_mentioned DESC
        SKIP $offset
        LIMIT $limit
        """
        
        try:
            async def _read_tx(tx):
                count_result = await tx.run(count_query, params)
                count_record = await count_result.single()
                total = count_record["total"] if count_record else 0
                
                if total == 0:
                    return [], 0
                
                result = await tx.run(data_query, params)
                entities = await result.data()
                return entities, total

            async with self.driver.session() as session:
                return await session.execute_read(_read_tx)
        except Exception as e:
            logger.error(f"Failed to list entities: {e}")
            return [], 0
    

    async def get_entity_by_id(self, entity_id: int) -> Optional[Dict]:
        """Get single entity by ID with topic."""
        query = """
        MATCH (e:Entity {id: $entity_id})
        OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)
        RETURN e.id AS id,
            e.session_id AS session_id,
            e.canonical_name AS canonical_name,
            e.aliases AS aliases,
            e.type AS type,
            t.name AS topic,
            e.last_mentioned / 1000 AS last_mentioned,
            e.last_updated / 1000 AS last_updated,
            e.last_profiled_msg_id AS last_profiled_msg_id
        """
        
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"entity_id": entity_id})
                record = await result.single()
                return dict(record) if record else None
        except Exception as e:
            logger.error(f"Failed to get entity {entity_id}: {e}")
            return None


    async def get_entities_by_ids(self, entity_ids: List[int]) -> List[Dict]:
        """Batch fetch entities by their IDs."""
        if not entity_ids:
            return []
            
        query = """
        MATCH (e:Entity)
        WHERE e.id IN $entity_ids
        OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)
        RETURN e.id AS id,
            e.session_id AS session_id,
            e.canonical_name AS canonical_name,
            e.aliases AS aliases,
            e.type AS type,
            t.name AS topic,
            e.last_mentioned / 1000 AS last_mentioned,
            e.last_updated / 1000 AS last_updated,
            e.last_profiled_msg_id AS last_profiled_msg_id,
            e.embedding AS embedding
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"entity_ids": entity_ids})
                return await result.data()
        except Exception as e:
            logger.error(f"Failed to fetch entities by ids: {e}")
            return []
    

    async def get_all_entities_for_hydration(self) -> list[dict]:
        """
        Fetch entity data needed to hydrate EntityResolver.
        Facts are fetched separately via get_facts_for_entity.
        """
        query = """
        MATCH (e:Entity)
        WHERE e.id IS NOT NULL
        OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)
        WITH e, t
        RETURN e.id AS id,
            e.canonical_name AS canonical_name,
            e.aliases AS aliases,
            e.type AS type,
            t.name AS topic,
            e.session_id AS session_id,
            e.embedding AS embedding
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query)
                return await result.data()
        except Exception as e:
            logger.error(f"Failed to hydrate entities: {e}")
            return []
    

    async def find_alias_collisions(self) -> List[Tuple[int, int]]:
        """Find entity pairs sharing exact canonical names or aliases."""
        query = """
        MATCH (e:Entity)
        UNWIND (e.aliases + [e.canonical_name]) AS name
        WITH toLower(name) AS lower_name, collect(e.id) AS ids
        WHERE size(ids) > 1
        UNWIND ids AS id_a
        UNWIND ids AS id_b
        WITH id_a, id_b WHERE id_a < id_b
        RETURN DISTINCT id_a, id_b
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query)
                records = await result.data()
                return [(r["id_a"], r["id_b"]) for r in records]
        except Exception as e:
            logger.error(f"Failed to find alias collisions: {e}")
            return []
    


    async def get_orphan_entities(
        self, 
        protected_id: int = 1, 
        orphan_cutoff_ms: int = 0, 
        stale_junk_cutoff_ms: int = 0
    ) -> List[int]:
        """
        Find entities to delete based on two criteria:
        1. Pure Orphans: 0 relationships, older than orphan_cutoff.
        2. Stale Junk: Only 1 relationship (to User), NO facts, older than stale_junk_cutoff.
        """
        query = """
        MATCH (e:Entity)
        WHERE e.id <> $protected_id
        AND NOT EXISTS { MATCH (e)-[:HAS_FACT]->(f_active:Fact) WHERE f_active.invalid_at IS NULL }

        OPTIONAL MATCH (e)-[r:RELATED_TO]-(neighbor)
        WITH e, collect(neighbor.id) as neighbors, $stale_cutoff as stale_limit, $orphan_cutoff as orphan_limit

        WHERE
            (size(neighbors) = 0 AND e.last_mentioned < orphan_limit)
            OR
            (size(neighbors) = 1 AND neighbors[0] = $protected_id AND e.last_mentioned < stale_limit)

        RETURN e.id as id
        """
        
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {
                    "protected_id": protected_id, 
                    "orphan_cutoff": orphan_cutoff_ms,
                    "stale_cutoff": stale_junk_cutoff_ms
                })
                records = await result.data()
                return [record["id"] for record in records]
        except Exception as e:
            logger.error(f"Failed to fetch orphans: {e}")
            return []
    

    async def get_entities_by_names(self, names: List[str]) -> List[Dict]:
        lower_names = [n.lower() for n in names]
        query = """
        MATCH (e:Entity)
        WHERE toLower(e.canonical_name) IN $names
            OR any(alias IN e.aliases WHERE toLower(alias) IN $names)
        RETURN e.id as id, e.canonical_name as canonical_name, 
            e.type as type, e.aliases as aliases,
            [(e)-[:HAS_FACT]->(f) WHERE f.invalid_at IS NULL | f.content] as facts
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"names": lower_names})
                return await result.data()
        except Exception as e:
            logger.error(f"Failed to get entities by names: {e}")
            return []
    

    async def search_similar_entities(self, entity_id: int, limit: int = 50) -> List[Tuple[int, float]]:
        query = """
        MATCH (e:Entity {id: $id})
        CALL vector_search.search('entity_vec', $limit, e.embedding)
        YIELD node, similarity
        WITH node, similarity
        WHERE node.id <> $id
        RETURN node.id as id, similarity
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"id": entity_id, "limit": limit})
                records = await result.data()
                return [(r["id"], r["similarity"]) for r in records]
        except Exception as e:
            logger.error(f"Failed to search similar entities for {entity_id}: {e}")
            return []
    

    async def search_entities_by_embedding(self, embedding: List[float], limit: int = 10, score_threshold: float = 0.8) -> List[Tuple[int, float]]:
        """
        Find entities based on semantic description.
        Used when fuzzy string matching fails (e.g., "The plumber" -> "John Smith").
        """
        query = """
        CALL vector_search.search('entity_vec', $limit, $embedding)
        YIELD node, similarity
        WITH node, similarity
        WHERE similarity >= $threshold
        RETURN node.id as id, similarity
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {
                    "embedding": embedding, 
                    "limit": limit,
                    "threshold": score_threshold
                })
                records = await result.data()
                return [(r["id"], r["similarity"]) for r in records]
        except Exception as e:
            logger.error(f"Entity vector search failed: {e}")
            return []
    

    async def validate_existing_ids(self, ids: List[int]) -> Optional[Set[int]]:
        """
        Liveness Check: Returns the subset of IDs that actually exist in the DB.
        Used to prevent 'Zombie Resurrection' of deleted entities during writes.
        """
        if not ids:
            return set()
            
        query = """
        MATCH (e:Entity)
        WHERE e.id IN $ids
        RETURN e.id as id
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"ids": list(ids)})
                records = await result.data()
                return {record["id"] for record in records}
        except Exception as e:
            logger.error(f"Liveness check failed: {e}")
            return None


    async def get_entity_count_by_type(self) -> List[Dict]:
        """Get entity counts grouped by type for charts."""
        query = """
        MATCH (e:Entity)
        WHERE e.type IS NOT NULL
        RETURN e.type AS type, count(e) AS count
        ORDER BY count DESC
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query)
                return await result.data()
        except Exception as e:
            logger.error(f"Failed to get entity count by type: {e}")
            return []


    async def get_entity_count_by_topic(self) -> List[Dict]:
        """Get entity counts grouped by topic for charts."""
        query = """
        MATCH (e:Entity)-[:BELONGS_TO]->(t:Topic)
        RETURN t.name AS topic, count(e) AS count
        ORDER BY count DESC
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query)
                return await result.data()
        except Exception as e:
            logger.error(f"Failed to get entity count by topic: {e}")
            return []


    async def get_top_connected_entities(self, limit: int = 10) -> List[Dict]:
        """Get entities with the most connections."""
        query = """
        MATCH (e:Entity)-[r:RELATED_TO]-()
        WITH e, count(r) AS connections
        ORDER BY connections DESC
        LIMIT $limit
        RETURN e.canonical_name AS name, e.type AS type, connections
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"limit": limit})
                return await result.data()
        except Exception as e:
            logger.error(f"Failed to get top connected entities: {e}")
            return []
    

    async def get_entity_relationships(self, entity_id: int) -> List[Dict]:
        """Get all RELATED_TO edges for an entity with full metadata."""
        query = """
        MATCH (e:Entity {id: $entity_id})-[r:RELATED_TO]-(neighbor:Entity)
        RETURN neighbor.id as neighbor_id,
            neighbor.canonical_name as neighbor_name,
            r.weight as weight,
            r.message_ids as message_ids,
            r.context as context,
            r.confidence as confidence
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"entity_id": entity_id})
                return await result.data()
        except Exception as e:
            logger.error(f"Failed to get relationships for entity {entity_id}: {e}")
            return []
    

