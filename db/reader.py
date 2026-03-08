from datetime import datetime, timezone, timedelta
from loguru import logger
from typing import Dict, List, Optional, Set, Tuple
from neo4j import Driver

from shared.models.schema.dtypes import Fact


class GraphReader:
    def __init__(self, driver: Driver):
        self.driver = driver

    def _hydrate_fact(self, record) -> Fact:
        """Convert DB record to Fact dataclass."""
        return Fact.from_record(record)

    def get_message_text(self, message_id: int) -> str:
        """
        Fetch message text on demand.
        """
        query = "MATCH (m:Message {id: $id}) RETURN m.content as content"

        try:
            with self.driver.session() as session:
                result = session.run(query, {"id": int(message_id)}).single()
                return result["content"] if result else ""
        except Exception as e:
            logger.error(f"Failed to get message text for {message_id}: {e}")
            return ""

    def get_messages_by_ids(self, ids: List[int]) -> List[Dict]:
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
            with self.driver.session() as session:
                result = session.run(query, {"ids": ids})
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Failed to fetch messages by ids: {e}")
            return []

    def get_surrounding_messages(self, message_id: int, forward: int = 3, target_total: int = 10) -> List[Dict]:
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
            with self.driver.session() as session:
                result = session.run(safe_query, {
                    "msg_id": message_id,
                    "back_limit": back_limit,
                    "forward_limit": forward
                })
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Failed to fetch surrounding messages for {message_id}: {e}")
            return []

    def validate_existing_ids(self, ids: List[int]) -> Optional[Set[int]]:
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
            with self.driver.session() as session:
                result = session.run(query, {"ids": list(ids)})
                return {record["id"] for record in result}
        except Exception as e:
            logger.error(f"Liveness check failed: {e}")
            return None

    def get_facts_for_entity(self, entity_id: int, active_only: bool = True):
        """Get facts from an entity."""
        base = """
        MATCH (e:Entity {id: $entity_id})-[:HAS_FACT]->(f:Fact)
        """
        
        where = "WHERE f.invalid_at IS NULL" if active_only else ""
        
        tail = """
        OPTIONAL MATCH (f)-[:EXTRACTED_FROM]->(m:Message)
        RETURN f.id as id, f.source_entity_id as source_entity_id, f.content as content, 
            f.valid_at as valid_at, f.invalid_at as invalid_at, f.confidence as confidence, 
            f.embedding as embedding, m.id as source_msg_id
        ORDER BY f.created_at DESC
        """
        
        query = base + where + tail

        try:
            with self.driver.session() as session:
                result = session.run(query, {"entity_id": entity_id, "active_only": active_only})
                return [self._hydrate_fact(record) for record in result]
        except Exception as e:
            logger.error(f"Failed to get facts for entity {entity_id}: {e}")
            return []

    def get_facts_for_entities(self, entity_ids: List[int], active_only: bool = True) -> Dict[int, List[Fact]]:
        """Batch fetch facts for multiple entities. Returns {entity_id: [Fact, ...]}."""
        if not entity_ids:
            return {}
        
        where_clause = "AND f.invalid_at IS NULL" if active_only else ""
    
        query = f"""
        MATCH (e:Entity)-[:HAS_FACT]->(f:Fact)
        WHERE e.id IN $entity_ids
        {where_clause}
        OPTIONAL MATCH (f)-[:EXTRACTED_FROM]->(m:Message)
        RETURN e.id as entity_id, f.id as id, f.source_entity_id as source_entity_id,
            f.content as content, f.valid_at as valid_at, f.invalid_at as invalid_at, 
            f.confidence as confidence, f.embedding as embedding,
            m.id as source_msg_id
        ORDER BY e.id, f.created_at DESC
        """
        
        try:
            with self.driver.session() as session:
                result = session.run(query, {"entity_ids": entity_ids})
                
                facts_by_entity: Dict[int, List[Fact]] = {eid: [] for eid in entity_ids}
                
                for record in result:
                    eid = record["entity_id"]
                    fact = self._hydrate_fact(record)
                    facts_by_entity[eid].append(fact)
                
                return facts_by_entity
                
        except Exception as e:
            logger.error(f"Failed to batch fetch facts: {e}")
            return {eid: [] for eid in entity_ids}

    def get_facts_from_message(self, msg_id: int) -> List[Fact]:
        """Fetch all facts extracted from a message."""
        query = """
        MATCH (f:Fact)-[:EXTRACTED_FROM]->(m:Message {id: $msg_id})
        RETURN f.id as id, 
            f.source_entity_id as source_entity_id,
            f.content as content, 
            f.valid_at as valid_at,
            f.invalid_at as invalid_at, 
            f.confidence as confidence,
            f.embedding as embedding, 
            $msg_id as source_msg_id
        """
        try:
            with self.driver.session() as session:
                result = session.run(query, {"msg_id": msg_id})
                return [self._hydrate_fact(record) for record in result]
        except Exception as e:
            logger.error(f"Failed to get facts from message {msg_id}: {e}")
            return []
    
    def get_max_entity_id(self) -> int:
        """
        Returns the highest entity ID currently in the graph.
        Used on startup to sync Redis counters.
        """
        query = "MATCH (e:Entity) RETURN max(e.id) as max_id"
        try:
            with self.driver.session() as session:
                result = session.run(query).single()
                return result["max_id"] if result and result["max_id"] is not None else 0
        except Exception as e:
            logger.error(f"Failed to get max entity ID: {e}")
            return 0
    

    def get_entity_embedding(self, entity_id: int) -> List[float]:
        query = "MATCH (e:Entity {id: $id}) RETURN e.embedding as embedding"
        try:
            with self.driver.session() as session:
                result = session.run(query, {"id": entity_id}).single()
                return result["embedding"] if result else []
        except Exception as e:
            logger.error(f"Failed to get embedding for entity {entity_id}: {e}")
            return []
    
    
    def get_all_entities_for_hydration(self) -> list[dict]:
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
            e.session_id AS session_id
        """
        try:
            with self.driver.session() as session:
                result = session.run(query)
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Failed to hydrate entities: {e}")
            return []
    
    def find_alias_collisions(self) -> List[Tuple[int, int]]:
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
            with self.driver.session() as session:
                result = session.run(query)
                return [(r["id_a"], r["id_b"]) for r in result]
        except Exception as e:
            logger.error(f"Failed to find alias collisions: {e}")
            return []
    

    def get_orphan_entities(
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
        AND NOT (e)-[:HAS_FACT]->()
        AND e.last_mentioned < $orphan_cutoff 

        OPTIONAL MATCH (e)-[r:RELATED_TO]-(neighbor)
        WITH e, collect(neighbor.id) as neighbors, $stale_cutoff as stale_limit, $orphan_cutoff as orphan_limit

        WHERE
            (size(neighbors) = 0 AND e.last_mentioned < orphan_limit)
            OR
            (size(neighbors) = 1 AND neighbors[0] = $protected_id AND e.last_mentioned < stale_limit)

        RETURN e.id as id
        """
        
        try:
            with self.driver.session() as session:
                result = session.run(query, {
                    "protected_id": protected_id, 
                    "orphan_cutoff": orphan_cutoff_ms,
                    "stale_cutoff": stale_junk_cutoff_ms
                })
                return [record["id"] for record in result]
        except Exception as e:
            logger.error(f"Failed to fetch orphans: {e}")
            return []
    
    def get_neighbor_ids(self, entity_id: int) -> set[int]:
        query = """
        MATCH (e:Entity {id: $entity_id})-[:RELATED_TO]-(neighbor:Entity)
        RETURN neighbor.id as neighbor_id
        """
        try:
            with self.driver.session() as session:
                result = session.run(query, {"entity_id": entity_id})
                return {record["neighbor_id"] for record in result}
        except Exception as e:
            logger.error(f"Failed to get neighbor IDs for {entity_id}: {e}")
            return set()
    
    def get_entities_by_names(self, names: List[str]) -> List[Dict]:
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
            with self.driver.session() as session:
                result = session.run(query, {"names": lower_names})
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Failed to get entities by names: {e}")
            return []
    
    def get_parent_entities(self, entity_id: int) -> List[Dict]:
        """Get entities this one is PART_OF."""
        query = """
        MATCH (child:Entity {id: $entity_id})-[:PART_OF]->(parent:Entity)
        RETURN parent.id as id,
            parent.canonical_name as canonical_name,
            parent.type as type,
           [(parent)-[:HAS_FACT]->(f) WHERE f.invalid_at IS NULL | f.content] as facts
        """
        
        try:
            with self.driver.session() as session:
                result = session.run(query, {"entity_id": entity_id})
                return [dict(record) for record in result]
        
        except Exception as e:
            logger.error(f"Failed to get parents for entity {entity_id}: {e}")
            return []

    def get_neighbor_entities(self, entity_id: int, limit: int = 5) -> List[Dict]:
        """Get canonical names of connected entities."""
        query = """
        MATCH (e:Entity {id: $entity_id})-[:RELATED_TO]-(neighbor:Entity)
        RETURN neighbor.id as id, neighbor.canonical_name as name
        ORDER BY neighbor.last_mentioned DESC
        LIMIT $limit
        """
        try:
            with self.driver.session() as session:
                result = session.run(query, {"entity_id": entity_id, "limit": limit})
                return [{"id": record["id"], "name": record["name"]} for record in result]
        except Exception as e:
            logger.error(f"Failed to get neighbor entities for {entity_id}: {e}")
            return []
        
    def get_child_entities(self, entity_id: int) -> List[Dict]:
        """Get entities that are PART_OF this one."""
        query = """
        MATCH (child:Entity)-[:PART_OF]->(parent:Entity {id: $entity_id})
        RETURN child.id as id,
            child.canonical_name as canonical_name,
            child.type as type,
            [(child)-[:HAS_FACT]->(f) WHERE f.invalid_at IS NULL | f.content] as facts
        """
        
        try:
            with self.driver.session() as session:
                result = session.run(query, {"entity_id": entity_id})
                return [dict(record) for record in result]
        
        except Exception as e:
            logger.error(f"Failed to get children for entity {entity_id}: {e}")
            return []
    
    def has_direct_edge(self, id_a: int, id_b: int) -> bool:
        query = """
        MATCH (a:Entity {id: $id_a})-[r:RELATED_TO]-(b:Entity {id: $id_b})
        RETURN count(r) > 0 as connected
        """
        try:
            with self.driver.session() as session:
                result = session.run(query, {"id_a": id_a, "id_b": id_b}).single()
                return result["connected"] if result else False
        except Exception as e:
            logger.error(f"Failed to check direct edge between {id_a} and {id_b}: {e}")
            return False

    def has_hierarchy_edge(self, id_a: int, id_b: int) -> bool:
        """Check if PART_OF relationship exists in either direction."""
        query = """
        MATCH (a:Entity {id: $id_a}), (b:Entity {id: $id_b})
        WHERE (a)-[:PART_OF]->(b) OR (b)-[:PART_OF]->(a)
        RETURN true as exists
        LIMIT 1
        """
        try:
            with self.driver.session() as session:
                return session.run(query, {"id_a": id_a, "id_b": id_b}).single() is not None
        except Exception as e:
            logger.error(f"Failed to check hierarchy edge between {id_a} and {id_b}: {e}")
            return False
    
    def search_similar_entities(self, entity_id: int, limit: int = 50) -> List[Tuple[int, float]]:
        query = """
        MATCH (e:Entity {id: $id})
        CALL vector_search.search('entity_vec', $limit, e.embedding)
        YIELD node, similarity
        WITH node, similarity
        WHERE node.id <> $id
        RETURN node.id as id, similarity
        """
        try:
            with self.driver.session() as session:
                result = session.run(query, {"id": entity_id, "limit": limit})
                return [(r["id"], r["similarity"]) for r in result]
        except Exception as e:
            logger.error(f"Failed to search similar entities for {entity_id}: {e}")
            return []
    
    def search_entities_by_embedding(self, embedding: List[float], limit: int = 10, score_threshold: float = 0.8) -> List[Tuple[int, float]]:
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
            with self.driver.session() as session:
                result = session.run(query, {
                    "embedding": embedding, 
                    "limit": limit,
                    "threshold": score_threshold
                })
                return [(r["id"], r["similarity"]) for r in result]
        except Exception as e:
            logger.error(f"Entity vector search failed: {e}")
            return []
    
    def search_messages_vector(self, query_embedding: List[float], limit: int = 50) -> List[Tuple[int, float]]:
        query = """
        CALL vector_search.search('message_vec', $limit, $embedding)
        YIELD node, similarity
        WITH node, similarity
        RETURN node.id as id, similarity
        """

        try:
            with self.driver.session() as session:
                result = session.run(query, {"embedding": query_embedding, "limit": limit})
                return [(r["id"], r["similarity"]) for r in result]
        except Exception as e:
            logger.error(f"Failed to search messages by vector: {e}")
            return []
    

    def get_hierarchy_candidates(
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
            with self.driver.session() as session:
                result = session.run(query, {
                    "topic": topic,
                    "parent_type": parent_type,
                    "child_types": child_types,
                    "min_weight": min_weight
                })
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Hierarchy candidate query failed: {e}")
            return []
    
    def list_entities(
        self,
        limit: int = 20,
        offset: int = 0,
        topic: str = None,
        entity_type: str = None,
        search: str = None
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
            [(e)-[:HAS_FACT]->(f) | f.content][0..2] AS fact_snippets
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
        """
        
        try:
            with self.driver.session() as session:
                count_result = session.run(count_query, params).single()
                total = count_result["total"] if count_result else 0
                
                if total == 0:
                    return [], 0
                
                result = session.run(data_query, params)
                entities = [dict(record) for record in result]
                
                return entities, total
        except Exception as e:
            logger.error(f"Failed to list entities: {e}")
            return [], 0
    
    def get_entity_by_id(self, entity_id: int) -> Optional[Dict]:
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
            with self.driver.session() as session:
                result = session.run(query, {"entity_id": entity_id}).single()
                return dict(result) if result else None
        except Exception as e:
            logger.error(f"Failed to get entity {entity_id}: {e}")
            return None
    
    def list_preferences(self, session_id: str, kind: str = None) -> List[Dict]:
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
            with self.driver.session() as session:
                result = session.run(query, params)
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Failed to list preferences: {e}")
            return []
    
    def get_graph_stats(self) -> Dict[str, int]:
        """Get aggregate counts for dashboard."""
        query = """
        MATCH (e:Entity) WITH count(e) as entities
        MATCH (f:Fact) WHERE f.invalid_at IS NULL WITH entities, count(f) as facts
        MATCH ()-[r:RELATED_TO]->() WITH entities, facts, count(r) as relationships
        RETURN entities, facts, relationships
        """
        with self.driver.session() as session:
            result = session.run(query).single()
            if not result:
                return {"entities": 0, "facts": 0, "relationships": 0}
            return {
                "entities": result["entities"] or 0,
                "facts": result["facts"] or 0,
                "relationships": result["relationships"] or 0
            }
    
    def get_entity_count_by_type(self) -> List[Dict]:
        """Get entity counts grouped by type for charts."""
        query = """
        MATCH (e:Entity)
        WHERE e.type IS NOT NULL
        RETURN e.type AS type, count(e) AS count
        ORDER BY count DESC
        """
        try:
            with self.driver.session() as session:
                result = session.run(query)
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Failed to get entity count by type: {e}")
            return []

    def get_entity_count_by_topic(self) -> List[Dict]:
        """Get entity counts grouped by topic for charts."""
        query = """
        MATCH (e:Entity)-[:BELONGS_TO]->(t:Topic)
        RETURN t.name AS topic, count(e) AS count
        ORDER BY count DESC
        """
        try:
            with self.driver.session() as session:
                result = session.run(query)
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Failed to get entity count by topic: {e}")
            return []

    def get_top_connected_entities(self, limit: int = 10) -> List[Dict]:
        """Get entities with the most connections."""
        query = """
        MATCH (e:Entity)-[r:RELATED_TO]-()
        WITH e, count(r) AS connections
        ORDER BY connections DESC
        LIMIT $limit
        RETURN e.canonical_name AS name, e.type AS type, connections
        """
        try:
            with self.driver.session() as session:
                result = session.run(query, {"limit": limit})
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Failed to get top connected entities: {e}")
            return []
    
    def get_entity_relationships(self, entity_id: int) -> List[Dict]:
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
            with self.driver.session() as session:
                result = session.run(query, {"entity_id": entity_id})
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Failed to get relationships for entity {entity_id}: {e}")
            return []
    
    def get_recent_facts(self, days: int = 7, limit: int = 20) -> List[Dict]:
        """Get recently created facts."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        
        query = """
        MATCH (e:Entity)-[:HAS_FACT]->(f:Fact)
        WHERE f.valid_at > $cutoff
        AND f.invalid_at IS NULL
        RETURN f.id as id,
            f.content as content,
            f.valid_at as created_at,
            e.canonical_name as entity_name,
            e.type as entity_type
        ORDER BY f.valid_at DESC
        LIMIT $limit
        """
        with self.driver.session() as session:
            result = session.run(query, cutoff=cutoff, limit=limit)
            return [dict(r) for r in result]

    def get_recently_active_entities(self, days: int = 7, limit: int = 10) -> List[Dict]:
        """Get entities with recent fact activity."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        
        query = """
        MATCH (e:Entity)-[:HAS_FACT]->(f:Fact)
        WHERE f.valid_at > $cutoff
        AND f.invalid_at IS NULL
        WITH e, count(f) as recent_facts, max(f.valid_at) as last_activity
        RETURN e.id as id,
            e.canonical_name as name,
            e.type as type,
            e.topic as topic,
            recent_facts,
            last_activity
        ORDER BY recent_facts DESC, last_activity DESC
        LIMIT $limit
        """
        with self.driver.session() as session:
            result = session.run(query, cutoff=cutoff, limit=limit)
            return [dict(r) for r in result]

    def get_notable_entities(self, limit: int = 10) -> List[Dict]:
        """Get top entities by connection count with summary info."""
        query = """
        MATCH (e:Entity)
        WHERE e.canonical_name IS NOT NULL
        OPTIONAL MATCH (e)-[r]-()
        WITH e, count(DISTINCT r) as connection_count
        OPTIONAL MATCH (e)-[:HAS_FACT]->(f:Fact)
        WHERE f.invalid_at IS NULL
        WITH e, connection_count, count(f) as fact_count
        RETURN e.id as id,
            e.canonical_name as name,
            e.type as type,
            e.topic as topic,
            connection_count,
            fact_count
        ORDER BY connection_count DESC
        LIMIT $limit
        """
        with self.driver.session() as session:
            result = session.run(query, limit=limit)
            return [dict(r) for r in result]