from datetime import datetime
import re
import time
from loguru import logger
from typing import Dict, List, Set, Tuple
from neo4j import GraphDatabase, ManagedTransaction
from dotenv import load_dotenv
import os

from schema.dtypes import Fact
load_dotenv()

MEMGRAPH_USER=os.environ.get("MEMGRAPH_USER")
MEMGRAPH_PASSWORD=os.environ.get("MEMGRAPH_PASSWORD")
MEMGRAPH_HOST=os.environ.get("MEMGRAPH_HOST")
MEMGRAPH_PORT=os.environ.get("MEMGRAPH_PORT")

class MemGraphStore:
    def __init__(self, uri: str = None):
        if uri is None:
            uri = f"bolt://{MEMGRAPH_HOST}:{MEMGRAPH_PORT}"
        self.driver = GraphDatabase.driver(
            uri,
            auth=(MEMGRAPH_USER, MEMGRAPH_PASSWORD)
        )
        self.verify_conn()
        self._setup_schema()
        logger.info("Graph store initialized")

    def close(self):
        if self.driver:
            self.driver.close()
    
    def verify_conn(self):
        max_retries = 5
        for i in range(max_retries):
            try:
                self.driver.verify_connectivity()
                return
            except Exception as e:
                if i == max_retries - 1:
                    raise e
                logger.warning(f"Waiting for Memgraph... ({e})")
                time.sleep(2)
    
    def get_max_entity_id(self) -> int:
        """
        Returns the highest entity ID currently in the graph.
        Used on startup to sync Redis counters.
        """
        query = "MATCH (e:Entity) RETURN max(e.id) as max_id"
        with self.driver.session() as session:
            result = session.run(query).single()
            return result["max_id"] if result and result["max_id"] is not None else 0
    
    def _setup_schema(self):
        """
        Create indices and constraints using Memgraph syntax.
        """
        constraints = [
            "CREATE CONSTRAINT ON (e:Entity) ASSERT e.id IS UNIQUE;",
            "CREATE CONSTRAINT ON (t:Topic) ASSERT t.name IS UNIQUE;",
            "CREATE CONSTRAINT ON (m:Message) ASSERT m.id IS UNIQUE;",
            "CREATE CONSTRAINT ON (f:Fact) ASSERT f.id IS UNIQUE;",
        ]

        indices = [
            "CREATE INDEX ON :Fact(invalid_at);",
            "CREATE INDEX ON :Fact(created_at);",
            "CREATE INDEX ON :Message(timestamp);",
            "CREATE INDEX ON :MoodCheckpoint(timestamp);",
            "CREATE INDEX ON :Entity(canonical_name);",
        ]

        vector_indices = [
            """
            CREATE VECTOR INDEX entity_vec ON :Entity(embedding) 
            WITH CONFIG {"dimension": 1024, "capacity": 500000, "metric": "cos"}
            """,
            """
            CREATE VECTOR INDEX fact_vec ON :Fact(embedding) 
            WITH CONFIG {"dimension": 1024, "capacity": 5000000, "metric": "cos"}
            """,
            """
            CREATE VECTOR INDEX message_vec ON :Message(embedding) 
            WITH CONFIG {"dimension": 1024, "capacity": 5000000, "metric": "cos"}
            """
        ]
        
        text_indices = [
            "CREATE TEXT INDEX message_search ON :Message(content)",
            "CREATE TEXT INDEX entity_search ON :Entity(canonical_name, aliases)"
        ]
        
        with self.driver.session() as session:
            for q in constraints + indices + vector_indices + text_indices:
                try:
                    session.run(q)
                except Exception as e:
                    logger.debug(f"Schema setup note: {e}")
        
        logger.info("Memgraph schema indices verified.")
    
    def save_message_logs(self, messages: List[Dict]):
        """
        Persist message texts to the graph.
        """
        if not messages:
            return
        
        query = """
        UNWIND $batch AS msg
        MERGE (m:Message {id: msg.id})
        SET m.content = msg.content,
            m.role = msg.role,
            m.timestamp = msg.timestamp,
            m.embedding = msg.embedding
        """
        
        with self.driver.session() as session:
            try:
                session.run(query, {"batch": messages}).consume()
            except Exception as e:
                logger.error(f"Failed to save message logs: {e}")
                return False
        
        logger.info(f"Saved {len(messages)} message logs to Memgraph.")
        return True
    
    def get_entity_embedding(self, entity_id: int) -> List[float]:
        query = "MATCH (e:Entity {id: $id}) RETURN e.embedding as embedding"
        with self.driver.session() as session:
            result = session.run(query, {"id": entity_id}).single()
            return result["embedding"] if result else []
    
    def search_similar_entities(self, entity_id: int, limit: int = 50) -> List[Tuple[int, float]]:
        query = """
        MATCH (e:Entity {id: $id})
        CALL vector_search.search('entity_vec', $limit, e.embedding)
        YIELD node, similarity
        WHERE node.id <> $id
        RETURN node.id as id, similarity
        """
        with self.driver.session() as session:
            result = session.run(query, {"id": entity_id, "limit": limit})
            return [(r["id"], r["similarity"]) for r in result]
    
    def search_messages_vector(self, query_embedding: List[float], limit: int = 50) -> List[Tuple[int, float]]:
        query = """
        CALL vector_search.search('message_vec', $limit, $embedding)
        YIELD node, similarity
        RETURN node.id as id, similarity
        """
        with self.driver.session() as session:
            result = session.run(query, {"embedding": query_embedding, "limit": limit})
            return [(r["id"], r["similarity"]) for r in result]
    
    def get_all_message_embeddings(self) -> Dict[int, List[float]]:
        """
        Fetch all message embeddings for rapid FAISS hydration.
        """
        query = """
        MATCH (m:Message) 
        WHERE m.embedding IS NOT NULL
        RETURN m.id as id, m.embedding as embedding
        """
        try:
            with self.driver.session() as session:
                result = session.run(query)
                return {record["id"]: record["embedding"] for record in result}
        except Exception as e:
            logger.error(f"Failed to fetch message embeddings: {e}")
            return {}

    def get_message_text(self, message_id: int) -> str:
        """
        Fetch message text on demand.
        """
        query = "MATCH (m:Message {id: $id}) RETURN m.content as content"
        with self.driver.session() as session:
            result = session.run(query, {"id": int(message_id)}).single()
            return result["content"] if result else ""
    
    def _hydrate_fact(self, record) -> Fact:
        """Convert DB record to Fact dataclass."""
        return Fact(
            id=record["id"],
            source_entity_id=record["source_entity_id"],
            content=record["content"],
            valid_at=datetime.fromisoformat(record["valid_at"]),
            invalid_at=datetime.fromisoformat(record["invalid_at"]) if record["invalid_at"] else None,
            confidence=record["confidence"],
            embedding=record["embedding"] or [],
            source_msg_id=record["source_msg_id"]
        )

    def create_facts_batch(self, entity_id: int, facts: List[Fact]) -> int:
        """
        Atomically create multiple facts for an entity.
        Returns number of facts created.
        Raises Exception if ANY fact fails (All-or-Nothing).
        """
        if not facts:
            return 0

        fact_params = []
        for f in facts:
            fact_params.append({
                "id": f.id,
                "content": f.content,
                "valid_at": f.valid_at.isoformat(),
                "invalid_at": f.invalid_at.isoformat() if f.invalid_at else None,
                "confidence": f.confidence,
                "embedding": f.embedding,
                "source_msg_id": f.source_msg_id
            })
        
        def _execute_batch(tx: ManagedTransaction):
            query = """
            MATCH (e:Entity {id: $entity_id})
            
            UNWIND $batch AS item
            
            CREATE (f:Fact {
                id: item.id,
                source_entity_id: $entity_id,
                content: item.content,
                valid_at: item.valid_at,
                invalid_at: item.invalid_at,
                confidence: item.confidence,
                created_at: timestamp(),
                embedding: item.embedding
            })
            CREATE (e)-[:HAS_FACT]->(f)
            
            WITH f, item
            FOREACH (_ IN CASE WHEN item.source_msg_id IS NOT NULL THEN [1] ELSE [] END |
                MERGE (m:Message {id: item.source_msg_id})
                MERGE (f)-[:EXTRACTED_FROM]->(m)
            )
            
            RETURN count(f) as created_count
            """
            
            result = tx.run(query, {
                "entity_id": entity_id,
                "batch": fact_params
            }).single()
            
            return result["created_count"] if result else 0

        try:
            with self.driver.session() as session:
                return session.execute_write(_execute_batch)
        except Exception as e:
            logger.error(f"Batch write failed for entity {entity_id}: {e}")
            raise e
    
    def invalidate_fact(self, fact_id: str, invalid_at: datetime) -> bool:
        """Mark fact as invalid."""
        query = """
        MATCH (f:Fact {id: $fact_id})
        SET f.invalid_at = $invalid_at
        RETURN f.id as id
        """
        try:
            with self.driver.session() as session:
                result = session.run(query, {
                    "fact_id": fact_id,
                    "invalid_at": invalid_at.isoformat()
                }).single()
                return result is not None
        except Exception as e:
            logger.error(f"Failed to invalidate fact {fact_id}: {e}")
            return False
    

    def get_facts_for_entity(self, entity_id: int, valid_only: bool = True):
        """Get a fact from an entity."""

        query = """
        MATCH (e:Entity {id: $entity_id})-[:HAS_FACT]->(f:Fact)
        """ + ("WHERE f.invalid_at IS NULL" if valid_only else "") + """
        OPTIONAL MATCH (f)-[:EXTRACTED_FROM]->(m:Message)
        RETURN f.id as id, f.source_entity_id as source_entity_id, f.content as content, f.valid_at as valid_at,
            f.invalid_at as invalid_at, f.confidence as confidence, f.embedding as embedding,
            m.id as source_msg_id
        ORDER BY f.created_at DESC
        """

        try:
            with self.driver.session() as session:
                result= session.run(query, {"entity_id": entity_id})
                return [self._hydrate_fact(record) for record in result]
        except Exception as e:
            logger.error(f"Failed to get facts for entity {entity_id}: {e}")
            return []


    def get_facts_from_message(self, msg_id: str) -> List[Fact]:
        """Fetch all facts extracted from a message."""
        query = """
        MATCH (f:Fact)-[:EXTRACTED_FROM]->(m:Message {id: $msg_id})
        RETURN f.id as id, f.content as content, f.valid_at as valid_at,
            f.invalid_at as invalid_at, f.confidence as confidence,
            f.embedding as embedding, $msg_id as source_msg_id
        """
        try:
            with self.driver.session() as session:
                result = session.run(query, {"msg_id": msg_id})
                return [self._hydrate_fact(record) for record in result]
        except Exception as e:
            logger.error(f"Failed to get facts from message {msg_id}: {e}")
            return []
    
    def delete_old_invalidated_facts(self, cutoff: datetime) -> int:
        """Delete Fact nodes invalidated before cutoff date."""
        query = """
        MATCH (f:Fact)
        WHERE f.invalid_at IS NOT NULL 
        AND f.invalid_at < $cutoff
        DETACH DELETE f
        RETURN count(f) as deleted
        """
        try:
            with self.driver.session() as session:
                result = session.run(query, {"cutoff": cutoff.isoformat()}).single()
                deleted = result["deleted"] if result else 0
                if deleted > 0:
                    logger.info(f"Deleted {deleted} old invalidated facts")
                return deleted
        except Exception as e:
            logger.error(f"Failed to delete old facts: {e}")
            return 0
    
    def get_facts_for_entities(self, entity_ids: List[int], active_only: bool = True) -> Dict[int, List[Fact]]:
        """Batch fetch facts for multiple entities. Returns {entity_id: [Fact, ...]}."""
        if not entity_ids:
            return {}
        
        query = """
        MATCH (e:Entity)-[:HAS_FACT]->(f:Fact)
        WHERE e.id IN $entity_ids
        WHERE ($active_only = false OR f.invalid_at IS NULL)
        OPTIONAL MATCH (f)-[:EXTRACTED_FROM]->(m:Message)
        RETURN e.id as entity_id, f.id as id, f.content as content, 
            f.valid_at as valid_at, f.invalid_at as invalid_at, 
            f.confidence as confidence, f.embedding as embedding,
            m.id as source_msg_id
        ORDER BY e.id, f.created_at DESC
        """
        
        try:
            with self.driver.session() as session:
                result = session.run(query, {"entity_ids": entity_ids, "active_only": active_only})
                
                facts_by_entity: Dict[int, List[Fact]] = {eid: [] for eid in entity_ids}
                
                for record in result:
                    eid = record["entity_id"]
                    fact = self._hydrate_fact(record)
                    facts_by_entity[eid].append(fact)
                
                return facts_by_entity
                
        except Exception as e:
            logger.error(f"Failed to batch fetch facts: {e}")
            return {eid: [] for eid in entity_ids}
    
    def write_batch(self, entities: List[Dict], relationships: List[Dict]):
        entity_params = []
        for e in entities:
            e_clean = e.copy()
            e_clean["aliases"] = e.get("aliases") or []
            entity_params.append(e_clean)

        relationship_params = []
        for r in relationships:
            r_clean = r.copy()
            r_clean["confidence"] = r.get("confidence", 1.0)
            relationship_params.append(r_clean)

        def _write(tx: 'ManagedTransaction'):
            if entity_params:
                tx.run("""
                    UNWIND $batch AS data
                    MERGE (e:Entity {id: data.id})
                    ON CREATE SET
                        e.session_id = data.session_id,
                        e.canonical_name = data.canonical_name,
                        e.aliases = data.aliases,
                        e.type = data.type,
                        e.confidence = data.confidence,
                        e.last_updated = timestamp(),
                        e.last_mentioned = timestamp(),
                        e.embedding = data.embedding
                    ON MATCH SET 
                        e.canonical_name = data.canonical_name,
                        e.confidence = data.confidence,
                        e.last_updated = timestamp(),
                        e.last_mentioned = timestamp()

                    WITH e, data
                    UNWIND coalesce(e.aliases, []) + data.aliases AS alias
                    WITH e, data, collect(DISTINCT alias) AS unique_aliases
                    SET e.aliases = unique_aliases

                    WITH e, data
                    FOREACH (_ IN CASE WHEN data.topic IS NOT NULL AND data.topic <> "" THEN [1] ELSE [] END |
                        MERGE (t:Topic {name: data.topic})
                        MERGE (e)-[:BELONGS_TO]->(t)
                    )
                """, batch=entity_params)

            if relationship_params:
                tx.run("""
                    UNWIND $batch AS rel
                    MATCH (a:Entity {canonical_name: rel.entity_a})
                    MATCH (b:Entity {canonical_name: rel.entity_b})
                    WITH a, b, rel,
                        CASE WHEN a.id < b.id THEN a ELSE b END AS node_a,
                        CASE WHEN a.id < b.id THEN b ELSE a END AS node_b
                    MERGE (node_a)-[r:RELATED_TO]->(node_b)
                    
                    ON CREATE SET 
                        r.weight = 1, 
                        r.confidence = rel.confidence,
                        r.last_seen = timestamp(), 
                        r.message_ids = [rel.message_id]
                        
                    ON MATCH SET 
                        r.weight = r.weight + 1,
                        r.confidence = CASE WHEN rel.confidence > r.confidence THEN rel.confidence ELSE r.confidence END,
                        r.last_seen = timestamp()
                    
                    WITH r, rel
                    UNWIND coalesce(r.message_ids, []) + [rel.message_id] AS mid
                    WITH r, collect(DISTINCT mid) AS unique_ids
                    SET r.message_ids = unique_ids
                """, batch=relationship_params)

        with self.driver.session() as session:
            session.execute_write(_write)
    
    def find_alias_collisions(self) -> List[Tuple[int, int]]:
        """Find entity pairs sharing exact canonical names or aliases."""
        query = """
        MATCH (a:Entity), (b:Entity)
        WHERE a.id < b.id 
        AND (toLower(a.canonical_name) = toLower(b.canonical_name)
            OR ANY(alias IN a.aliases WHERE toLower(alias) IN [x IN b.aliases | toLower(x)])
            OR toLower(a.canonical_name) IN [x IN b.aliases | toLower(x)]
            OR toLower(b.canonical_name) IN [x IN a.aliases | toLower(x)])
        RETURN a.id AS id_a, b.id AS id_b
        """
        with self.driver.session() as session:
            result = session.run(query)
            return [(r["id_a"], r["id_b"]) for r in result]
    
    def validate_existing_ids(self, ids: List[int]) -> Set[int]:
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
            return set()
    
    
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
            e.embedding AS embedding,
            e.session_id AS session_id
        """
        with self.driver.session() as session:
            result = session.run(query)
            return [dict(record) for record in result]
    
    def update_entity_profile(
        self, 
        entity_id: int, 
        canonical_name: str,
        embedding: List[float], 
        last_msg_id: int
    ):
        """
        Update entity metadata and embedding.
        """
        def _update(tx: 'ManagedTransaction'):
            tx.run("""
                MATCH (e:Entity {id: $id})
                SET e.canonical_name = $canonical_name,
                    e.embedding = $embedding,
                    e.last_updated = timestamp(),
                    e.last_profiled_msg_id = $last_msg_id
            """, 
            id=entity_id, 
            canonical_name=canonical_name, 
            embedding=embedding,
            last_msg_id=last_msg_id
            )
        
        with self.driver.session() as session:
            session.execute_write(_update)
        logger.info(f"Updated entity {entity_id} (checkpoint: msg_{last_msg_id})")
    
    def update_entity_embedding(self, entity_id: int, embedding: List[float]):
        """
        Persists a new embedding for an entity.
        """
        query = """
        MATCH (e:Entity {id: $id})
        SET e.embedding = $embedding,
            e.last_updated = timestamp()
        """
        with self.driver.session() as session:
            session.run(query, {"id": entity_id, "embedding": embedding}).consume()

    def cleanup_null_entities(self) -> int:
        """Remove entities with null type and their relationships."""
        query = """
        MATCH (e:Entity)
        WHERE e.type IS NULL
        DETACH DELETE e
        RETURN count(e) as deleted
        """
        with self.driver.session() as session:
            result = session.run(query)
            record = result.single()
            deleted = record["deleted"] if record else 0
            if deleted > 0:
                logger.info(f"Cleaned up {deleted} null-type entities")
            return deleted
    
    def get_orphan_entities(self, protected_id: int = 1, cutoff_ms: int = 0) -> List[int]:
        """Find entity IDs with NO relationships AND NO facts."""
        query = """
        MATCH (e:Entity)
        WHERE NOT (e)-[:RELATED_TO]-() 
        AND NOT (e)-[:HAS_FACT]->()
        AND e.id <> $protected_id
        AND e.last_mentioned < $cutoff
        RETURN e.id as id
        """
        with self.driver.session() as session:
            result = session.run(query, {
                "protected_id": protected_id, 
                "cutoff": cutoff_ms
            })
            return [record["id"] for record in result]
    
    def bulk_delete_entities(self, entity_ids: List[int]) -> int:
        """DETACH DELETE entities by ID list. Returns count deleted."""
        if not entity_ids:
            return 0
        query = """
        MATCH (e:Entity)
        WHERE e.id IN $ids
        DETACH DELETE e
        RETURN count(e) as deleted
        """
        with self.driver.session() as session:
            result = session.run(query, {"ids": entity_ids})
            record = result.single()
            deleted = record["deleted"] if record else 0
            if deleted > 0:
                logger.info(f"Bulk deleted {deleted} orphan entities")
            return deleted
    
    def has_direct_edge(self, id_a: int, id_b: int) -> bool:
        query = """
        MATCH (a:Entity {id: $id_a})-[r:RELATED_TO]-(b:Entity {id: $id_b})
        RETURN count(r) > 0 as connected
        """
        with self.driver.session() as session:
            result = session.run(query, {"id_a": id_a, "id_b": id_b}).single()
            return result["connected"] if result else False
    
    def get_neighbor_ids(self, entity_id: int) -> set[int]:
        query = """
        MATCH (e:Entity {id: $entity_id})-[:RELATED_TO]-(neighbor:Entity)
        RETURN neighbor.id as neighbor_id
        """
        with self.driver.session() as session:
            result = session.run(query, {"entity_id": entity_id})
            return {record["neighbor_id"] for record in result}
    
    def get_entities_by_names(self, names: List[str]) -> List[Dict]:
        lower_names = [n.lower() for n in names]
        query = """
        MATCH (e:Entity)
        WHERE toLower(e.canonical_name) IN $names
            OR any(alias IN e.aliases WHERE toLower(alias) IN $names)
        RETURN e.id as id, e.canonical_name as canonical_name, 
            e.type as type, e.aliases as aliases, e.facts as facts
        """
        with self.driver.session() as session:
            result = session.run(query, {"names": lower_names})
            return [dict(record) for record in result]
    
    def log_mood_checkpoint(
        self,
        user_name: str,
        primary: str,
        primary_count: int,
        secondary: str,
        secondary_count: int,
        message_count: int
    ):
        query = """
        MATCH (u:Entity {canonical_name: $user_name, type: 'person'})
        CREATE (m:MoodCheckpoint {
            timestamp: timestamp(),
            primary_emotion: $primary,
            primary_count: $primary_count,
            secondary_emotion: $secondary,
            secondary_count: $secondary_count,
            message_count: $message_count
        })
        MERGE (u)-[:FELT]->(m)
        """
        with self.driver.session() as session:
            session.run(query, {
                "user_name": user_name,
                "primary": primary,
                "primary_count": primary_count,
                "secondary": secondary,
                "secondary_count": secondary_count,
                "message_count": message_count
            }).consume()
    
    
    def get_hot_topic_context_with_messages(self, hot_topic_names: List[str], msg_limit: int = 5, slim: bool = False) -> dict:
        """
        Get top entities + recent message IDs per hot topic.
        slim=True: returns name + aliases only (no summaries)
        """
        if slim:
            entity_projection = "{name: e.canonical_name, aliases: e.aliases}"
            msg_limit = 20
        else:
            entity_projection = "{name: e.canonical_name, facts: e.facts}"
        
        query = f"""
        MATCH (t:Topic) WHERE t.name IN $hot_topics
        MATCH (e:Entity)-[:BELONGS_TO]->(t)
        OPTIONAL MATCH (e)-[r:RELATED_TO]-()
        
        WITH t, e, r ORDER BY e.last_mentioned DESC
        WITH t, 
            collect(DISTINCT {entity_projection})[..3] as entities,
            reduce(flat = [], arr IN collect(DISTINCT r.message_ids) | flat + arr) as flat_msgs
        
        RETURN t.name as topic, 
            entities,
            flat_msgs[..$msg_limit] as message_ids
        """
        
        with self.driver.session() as session:
            result = session.run(query, {"hot_topics": hot_topic_names, "msg_limit": msg_limit})
            return {
                record["topic"]: {
                    "entities": record["entities"],
                    "message_ids": record["message_ids"] or []
                }
                for record in result
            }
    
    def get_neighbor_names(self, entity_id: int, limit: int = 5) -> List[str]:
        """Get canonical names of connected entities."""
        query = """
        MATCH (e:Entity {id: $entity_id})-[:RELATED_TO]-(neighbor:Entity)
        RETURN neighbor.canonical_name as name
        ORDER BY neighbor.last_mentioned DESC
        LIMIT $limit
        """
        with self.driver.session() as session:
            result = session.run(query, {"entity_id": entity_id, "limit": limit})
            return [record["name"] for record in result]
    
    def search_messages_fts(self, query: str, limit: int = 50) -> List[Tuple[int, float]]:
        """
        Perform native Full-Text Search on Message nodes.
        Returns list of (message_id, score).
        """

        cypher = """
        CALL text_search.search('message_search', $q) YIELD node, score
        RETURN node.id as id, score
        ORDER BY score DESC LIMIT $limit
        """
        
        try:
            with self.driver.session() as session:
                result = session.run(cypher, {"q": query, "limit": limit})
                return [(record["id"], record["score"]) for record in result]
        except Exception as e:
            logger.error(f"FTS Message search failed: {e}")
            return []
    
    def search_entity(self, query: str, active_topics: List[str] = None, limit: int = 5, connections_limit: int = 5, evidence_limit: int = 5) -> list[dict]:
        """
        Search for entities by name/alias with top connections included.
        """
        clean_query = re.sub(r'[\W_]+', ' ', query).strip()
        if not clean_query:
             return []

        cypher = """
        CALL text_search.search('entity_search', $q) YIELD node as e, score
        
        OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)
        WITH e, t, score
        WHERE ($filter_topics = false) OR (t IS NULL) OR (t.name IN $active_topics)
        
        OPTIONAL MATCH (e)-[:PART_OF]->(parent:Entity)
        OPTIONAL MATCH (child:Entity)-[:PART_OF]->(e)
        OPTIONAL MATCH (e)-[r:RELATED_TO]-(conn:Entity)
        
        WITH e, t, parent, count(DISTINCT child) as children_count, r, conn, score
        ORDER BY score DESC, r.weight DESC
        
        RETURN e.id AS id,
            e.canonical_name AS canonical_name,
            e.aliases AS aliases,
            e.type AS type,
            t.name AS topic,
            e.last_mentioned AS last_mentioned,
            e.last_updated AS last_updated,
            [(e)-[:HAS_FACT]->(f) WHERE f.invalid_at IS NULL | f.content] AS facts,
            conn.canonical_name AS conn_name,
            conn.aliases AS conn_aliases,
            r.weight AS conn_weight,
            r.message_ids AS evidence_ids,
            [(conn)-[:HAS_FACT]->(cf) WHERE cf.invalid_at IS NULL | cf.content] AS conn_facts,
            parent.canonical_name AS parent_name,
            children_count
        LIMIT $limit
        """

        params = {
            "q": clean_query, 
            "limit": limit,
            "active_topics": active_topics if active_topics is not None else [],
            "filter_topics": active_topics is not None
        }
        
        with self.driver.session() as session:
            result = session.run(cypher, params)
            
            entities = {}
            for row in result:
                eid = row["id"]
                
                if eid not in entities:
                    entities[eid] = {
                        "id": eid,
                        "canonical_name": row["canonical_name"],
                        "aliases": row["aliases"] or [],
                        "type": row["type"],
                        "facts": row["facts"] or [],
                        "topic": row["topic"],
                        "last_mentioned": row["last_mentioned"],
                        "last_updated": row["last_updated"],
                        "top_connections": [],
                        "hierarchy": {
                            "parent": row["parent_name"],
                            "children_count": row["children_count"]
                        }
                    }
                
                if row["conn_name"] and len(entities[eid]["top_connections"]) < connections_limit:
                    entities[eid]["top_connections"].append({
                        "canonical_name": row["conn_name"],
                        "aliases": row["conn_aliases"] or [],
                        "facts": row["conn_facts"] or [],
                        "weight": row["conn_weight"],
                        "evidence_ids": (row["evidence_ids"] or [])[:evidence_limit]
                    })
            return list(entities.values())

    def get_related_entities(self, entity_names: List[str], active_topics: List[str] = None, limit: int = 50):
        """
        Find all entities connected to the given entities.
        Use this when the user asks about someone's connections, relationships, network, or "who/what is related to X".
        Set active_only=False if the user wants to include entities from inactive topics.
        Returns: connected entities with connection strength and supporting message references.
        """

        query = """
        MATCH (source:Entity) WHERE source.canonical_name IN $names
        MATCH (source)-[r:RELATED_TO]-(target:Entity)
        OPTIONAL MATCH (target)-[:BELONGS_TO]->(t:Topic)
        WITH source, r, target, t
        WHERE
            ($filter_topics = false) OR
            (t IS NULL) OR
            (t.name IN $active_topics)
        RETURN
            source.canonical_name as source,
            target.canonical_name as target,
            target.facts as target_facts,
            r.weight as connection_strength,
            r.message_ids as evidence_ids,
            r.confidence as confidence,
            r.last_seen as last_seen
        ORDER BY r.weight DESC, r.last_seen DESC
        LIMIT $limit
        """
        params = {
            "names": entity_names, 
            "active_topics": active_topics if active_topics is not None else [],
            "filter_topics": active_topics is not None,
            "limit": limit
        }
        with self.driver.session() as session:
            res = session.run(query, params)
            return [record.data() for record in res]
        
    
    def get_recent_activity(self, entity_name: str, active_topics: List[str] = None, hours: int = 24):
        """
        Get recent interactions. Filtered by active_topics if provided.
        """
        cutoff_ms = int((time.time() - (hours * 3600)) * 1000)

        query = """
        MATCH (e:Entity {canonical_name: $name})-[r:RELATED_TO]-(target:Entity)
        OPTIONAL MATCH (target)-[:BELONGS_TO]->(t:Topic)
        WHERE r.last_seen > $cutoff
        AND (($filter_topics = false) OR (t IS NULL) OR (t.name IN $active_topics))
        RETURN target.canonical_name as entity, r.message_ids as evidence_ids, r.last_seen as time
        ORDER BY r.last_seen DESC
        """

        params = {
            "name": entity_name, 
            "cutoff": cutoff_ms,
            "active_topics": active_topics if active_topics is not None else [],
            "filter_topics": active_topics is not None
        }

        with self.driver.session() as session:
            result = session.run(query, params)
            return [record.data() for record in result]
    
    def _build_path_data(self, names: List[str], topics: List[str], evidence: List[List[str]]) -> List[Dict]:
        """
        Convert raw query results into path structure.
        """
        return [
            {
                "step": i,
                "entity_a": names[i],
                "entity_b": names[i + 1],
                "topic_a": topics[i] if i < len(topics) else None,
                "topic_b": topics[i+1] if i+1 < len(topics) else None,
                "evidence_refs": evidence[i]
            }
            for i in range(len(evidence))
        ]


    def _find_shortest_path(self, start_name: str, end_name: str, active_topics: List[str] = None) -> tuple[List[str], List[str], List[List[str]], bool] | None:
        """
        Find shortest path. Calculates 'has_inactive' dynamically based on passed active_topics list.
        Returns: (names, topics, evidence_ids, has_inactive)
        """
        query = """
        MATCH (start:Entity {canonical_name: $start_name})
        MATCH (end:Entity {canonical_name: $end_name})
        MATCH p = (start)-[:RELATED_TO *BFS ..4]-(end)
        UNWIND nodes(p) AS n
        OPTIONAL MATCH (n)-[:BELONGS_TO]->(t:Topic)
        WITH p, 
            collect(COALESCE(t.name, 'General')) AS node_topics,
            [node IN nodes(p) | node.canonical_name] AS names,
            [r IN relationships(p) | r.message_ids] AS evidence_ids
        WITH names, node_topics, evidence_ids,
             ANY(topic IN node_topics WHERE NOT ($filter_topics = false OR topic IN $active_topics)) as has_inactive
        
        RETURN names, node_topics, evidence_ids, has_inactive
        LIMIT 1
        """
        
        params = {
            "start_name": start_name, 
            "end_name": end_name,
            "active_topics": active_topics if active_topics is not None else [],
            "filter_topics": active_topics is not None
        }
        
        with self.driver.session() as session:
            record = session.run(query, params).single()
            if not record:
                return None
            return record["names"], record["node_topics"], record["evidence_ids"], record["has_inactive"]


    def _find_active_only_path(self, start_name: str, end_name: str, active_topics: List[str] = None) -> tuple[List[str], List[List[str]]] | None:
        """
        Find shortest path excluding inactive-topic entities.
        Returns: (names, evidence_ids) or None if no path.
        """
        query = """
        MATCH (start:Entity {canonical_name: $start_name})
        MATCH (end:Entity {canonical_name: $end_name})
        MATCH p = (start)-[:RELATED_TO *BFS ..4]-(end)
        WHERE ALL(n IN nodes(p) WHERE
            EXISTS {
                MATCH (n)-[:BELONGS_TO]->(t:Topic)
                WHERE t.name IN $active_topics OR t IS NULL
            }
        )
        UNWIND nodes(p) AS n
        OPTIONAL MATCH (n)-[:BELONGS_TO]->(t:Topic)
        WITH p, collect(COALESCE(t.name, 'General')) AS node_topics
        
        RETURN [n IN nodes(p) | n.canonical_name] AS names,
            node_topics,
            [r IN relationships(p) | r.message_ids] AS evidence_ids
        LIMIT 1
        """
        
        params = {
            "start_name": start_name, 
            "end_name": end_name,
            "active_topics": active_topics
        }
        
        with self.driver.session() as session:
            record = session.run(query, params).single()
            if not record:
                return None
            return record["names"], record["node_topics"], record["evidence_ids"]


    def _find_path_filtered(self, start_name: str, end_name: str, active_topics: List[str] = None) -> tuple[List[Dict], bool]:
        """
        Find path between entities with topic filtering.
        Returns: (path_data, has_inactive_shortcut)
        """
        
        shortest = self._find_shortest_path(start_name, end_name, active_topics)
        
        if not shortest:
            return [], False
        
        names, topics, evidence, has_inactive = shortest
        
        if not has_inactive:
            return self._build_path_data(names, topics, evidence), False
        
        active_path = self._find_active_only_path(start_name, end_name, active_topics)
        
        if active_path:
            active_names, active_topics_list, active_evidence = active_path
            return self._build_path_data(active_names, active_topics_list, active_evidence), True
        
        # No active path exists, only the inactive one
        return [], True
    
    
    def get_mood_history(self, user_name: str, limit: int = 10) -> list[dict]:
        query = """
        MATCH (u:Entity {canonical_name: $user_name})-[:FELT]->(m:MoodCheckpoint)
        RETURN m.primary_emotion as primary_emotion,
            m.primary_count as primary_count,
            m.secondary_emotion as secondary_emotion,
            m.secondary_count as secondary_count,
            m.timestamp as timestamp,
            m.message_count as message_count
        ORDER BY m.timestamp DESC
        LIMIT $limit
        """
        with self.driver.session() as session:
            result = session.run(query, {"user_name": user_name, "limit": limit})
            return [dict(record) for record in result]
    
    def create_hierarchy_edge(self, parent_id: int, child_id: int) -> bool:
        """
        Create PART_OF relationship: (child)-[:PART_OF]->(parent)
        
        Returns True if created, False if already exists or failed.
        """
        query = """
        MATCH (child:Entity {id: $child_id})
        MATCH (parent:Entity {id: $parent_id})
        WHERE NOT (child)-[:PART_OF]->(parent)
        CREATE (child)-[:PART_OF {created_at: timestamp()}]->(parent)
        RETURN true as created
        """
        
        try:
            with self.driver.session() as session:
                result = session.run(query, {
                    "child_id": child_id,
                    "parent_id": parent_id
                })
                return len(list(result)) > 0
            
        except Exception as e:
            logger.error(f"Failed to create hierarchy edge ({child_id})-[:PART_OF]->({parent_id}): {e}")
            return False


    def get_parent_entities(self, entity_id: int) -> List[Dict]:
        """Get entities this one is PART_OF."""
        query = """
        MATCH (child:Entity {id: $entity_id})-[:PART_OF]->(parent:Entity)
        RETURN parent.id as id,
            parent.canonical_name as canonical_name,
            parent.type as type,
            parent.facts as facts
        """
        
        try:
            with self.driver.session() as session:
                result = session.run(query, {"entity_id": entity_id})
                return [dict(record) for record in result]
        
        except Exception as e:
            logger.error(f"Failed to get parents for entity {entity_id}: {e}")
            return []


    def get_child_entities(self, entity_id: int) -> List[Dict]:
        """Get entities that are PART_OF this one."""
        query = """
        MATCH (child:Entity)-[:PART_OF]->(parent:Entity {id: $entity_id})
        RETURN child.id as id,
            child.canonical_name as canonical_name,
            child.type as type,
            child.facts as facts
        """
        
        try:
            with self.driver.session() as session:
                result = session.run(query, {"entity_id": entity_id})
                return [dict(record) for record in result]
        
        except Exception as e:
            logger.error(f"Failed to get children for entity {entity_id}: {e}")
            return []
    

    def has_hierarchy_edge(self, id_a: int, id_b: int) -> bool:
        """Check if PART_OF relationship exists in either direction."""
        query = """
        MATCH (a:Entity {id: $id_a}), (b:Entity {id: $id_b})
        WHERE (a)-[:PART_OF]->(b) OR (b)-[:PART_OF]->(a)
        RETURN true as exists
        LIMIT 1
        """
        with self.driver.session() as session:
            return session.run(query, {"id_a": id_a, "id_b": id_b}).single() is not None
    
    def merge_entities(self, primary_id: int, secondary_id: int) -> bool:
        """
        Merge secondary entity into primary (single transaction).
        Transfers RELATED_TO and HAS_FACT edges, then deletes secondary.
        """
        
        def _execute_merge(tx):
            # Step 1: Validate both exist
            check = tx.run("""
                MATCH (p:Entity {id: $primary_id})
                MATCH (s:Entity {id: $secondary_id})
                RETURN p.canonical_name as p_name, 
                    p.aliases as p_aliases,
                    s.canonical_name as s_name, 
                    s.aliases as s_aliases,
                    s.confidence as s_conf,
                    s.last_mentioned as s_last
            """, primary_id=primary_id, secondary_id=secondary_id).single()
            
            if not check:
                logger.error(f"Merge failed: one or both entities not found ({primary_id}, {secondary_id})")
                return False
            
            # Step 2: Update primary with merged aliases
            combined_aliases = list(set(
                (check["p_aliases"] or []) + 
                (check["s_aliases"] or []) + 
                [check["s_name"]]
            ))
            
            tx.run("""
                MATCH (p:Entity {id: $primary_id})
                MATCH (s:Entity {id: $secondary_id})
                SET p.aliases = $aliases,
                    p.last_updated = timestamp(),
                    p.confidence = CASE 
                        WHEN coalesce(s.confidence, 0) > coalesce(p.confidence, 0) 
                        THEN s.confidence ELSE p.confidence END,
                    p.last_mentioned = CASE 
                        WHEN coalesce(s.last_mentioned, 0) > coalesce(p.last_mentioned, 0) 
                        THEN s.last_mentioned ELSE p.last_mentioned END
            """, primary_id=primary_id, secondary_id=secondary_id, aliases=combined_aliases)
            
            # Step 3: Transfer RELATED_TO edges
            tx.run("""
                MATCH (s:Entity {id: $secondary_id})-[r_old:RELATED_TO]-(target:Entity)
                WHERE target.id <> $primary_id
                MATCH (p:Entity {id: $primary_id})
                WITH p, target, r_old,
                    CASE WHEN p.id < target.id THEN p ELSE target END AS node_a,
                    CASE WHEN p.id < target.id THEN target ELSE p END AS node_b
                MERGE (node_a)-[r_new:RELATED_TO]->(node_b)
                ON CREATE SET
                    r_new.weight = r_old.weight,
                    r_new.confidence = r_old.confidence,
                    r_new.message_ids = r_old.message_ids,
                    r_new.last_seen = r_old.last_seen
                ON MATCH SET
                    r_new.weight = r_new.weight + r_old.weight,
                    r_new.confidence = CASE 
                        WHEN r_old.confidence > r_new.confidence 
                        THEN r_old.confidence ELSE r_new.confidence END,
                    r_new.last_seen = CASE 
                        WHEN r_old.last_seen > r_new.last_seen 
                        THEN r_old.last_seen ELSE r_new.last_seen END,
                    r_new.message_ids = coalesce(r_new.message_ids, []) + coalesce(r_old.message_ids, [])
            """, primary_id=primary_id, secondary_id=secondary_id)
            
            # Step 4: Transfer HAS_FACT edges
            tx.run("""
                MATCH (s:Entity {id: $secondary_id})-[r:HAS_FACT]->(f:Fact)
                MATCH (p:Entity {id: $primary_id})
                DELETE r
                CREATE (p)-[:HAS_FACT]->(f)
            """, primary_id=primary_id, secondary_id=secondary_id)

             # Step 4a: Transfer Topic memberships (BELONGS_TO)
            tx.run("""
                MATCH (s:Entity {id: $secondary_id})-[r:BELONGS_TO]->(t:Topic)
                MATCH (p:Entity {id: $primary_id})
                MERGE (p)-[:BELONGS_TO]->(t)
                DELETE r
            """, primary_id=primary_id, secondary_id=secondary_id)

            # Step 4b: Transfer Hierarchy Children (Entities that are PART_OF secondary)
            # The children now become part of the primary
            tx.run("""
                MATCH (child:Entity)-[r:PART_OF]->(s:Entity {id: $secondary_id})
                MATCH (p:Entity {id: $primary_id})
                MERGE (child)-[:PART_OF]->(p)
                ON CREATE SET r.transferred = true
                DELETE r
            """, primary_id=primary_id, secondary_id=secondary_id)

            # Step 4c: Transfer Hierarchy Parent (Who the secondary is PART_OF)
            # Only transfer if Primary doesn't already have a parent to avoid conflicts
            tx.run("""
                MATCH (s:Entity {id: $secondary_id})-[r:PART_OF]->(parent:Entity)
                MATCH (p:Entity {id: $primary_id})
                WHERE NOT (p)-[:PART_OF]->() 
                MERGE (p)-[:PART_OF]->(parent)
                DELETE r
            """, primary_id=primary_id, secondary_id=secondary_id)
            
            # Step 5: Delete secondary entity
            result = tx.run("""
                MATCH (s:Entity {id: $secondary_id})
                DETACH DELETE s
                RETURN count(*) as deleted
            """, secondary_id=secondary_id).single()
            
            return result and result["deleted"] > 0

        with self.driver.session() as session:
            try:
                success = session.execute_write(_execute_merge)
                if success:
                    logger.info(f"Merged entity {secondary_id} into {primary_id}")
                return success
            except Exception as e:
                logger.error(f"Merge transaction failed: {e}")
                return False
    
    