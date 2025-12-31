import time
from loguru import logger
from typing import Dict, List
from neo4j import GraphDatabase, ManagedTransaction
from dotenv import load_dotenv
import os
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
        self.driver.verify_connectivity()
    
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
        Create indices and constraints to ensure performance and data integrity.
        """
        queries = [
            "CREATE CONSTRAINT ON (e:Entity) ASSERT e.id IS UNIQUE;",
            "CREATE CONSTRAINT ON (t:Topic) ASSERT t.name IS UNIQUE",
            "CREATE INDEX ON :MoodCheckpoint(timestamp);"
            "CREATE INDEX ON :Entity(canonical_name);"
        ]
        
        with self.driver.session() as session:
            for q in queries:
                try:
                    session.run(q)
                except Exception as e:
                    logger.debug(f"Schema setup note: {e}")
        logger.info("Memgraph schema indices verified.")
    
    def write_batch(self, entities: List[Dict], relationships: List[Dict], is_user_message: bool = False):
        def _write(tx: 'ManagedTransaction'):
            for ent in entities:
                tx.run("""
                    MERGE (e:Entity {id: $id})
                    ON CREATE SET
                        e.canonical_name = $canonical_name,
                        e.aliases = $aliases,
                        e.type = $type,
                        e.summary = $summary,
                        e.confidence = $confidence,
                        e.last_updated = timestamp(),
                        e.last_mentioned = timestamp(),
                        e.embedding = $embedding
                    ON MATCH SET 
                        e.canonical_name = $canonical_name,
                        e.confidence = $confidence,
                        e.last_updated = timestamp(),
                        e.last_mentioned = timestamp()

                    WITH e
                    UNWIND coalesce(e.aliases, []) + $aliases AS alias
                    WITH e, collect(DISTINCT alias) AS unique_aliases
                    SET e.aliases = unique_aliases

                    WITH e
                    FOREACH (_ IN CASE WHEN $topic IS NOT NULL AND $topic <> "" THEN [1] ELSE [] END |
                        MERGE (t:Topic {name: $topic})
                        MERGE (e)-[:BELONGS_TO]->(t)
                    )
                """, **ent, is_user_message=is_user_message)

            for rel in relationships:
                tx.run("""
                    MATCH (a:Entity {canonical_name: $entity_a})
                    MATCH (b:Entity {canonical_name: $entity_b})
                    MERGE (a)-[r:RELATED_TO]-(b)
                    
                    ON CREATE SET 
                        r.weight = 1, 
                        r.confidence = $confidence,
                        r.last_seen = timestamp(), 
                        r.message_ids = [$message_id]
                        
                    ON MATCH SET 
                        r.weight = r.weight + 1,
                        r.confidence = CASE WHEN $confidence > r.confidence THEN $confidence ELSE r.confidence END,
                        r.last_seen = timestamp()
                       
                    WITH r
                    UNWIND coalesce(r.message_ids, []) + [$message_id] AS mid
                    WITH r, collect(DISTINCT mid) AS unique_ids
                    SET r.message_ids = unique_ids
                """, **rel)

        with self.driver.session() as session:
            session.execute_write(_write)
    
    def get_all_entities_for_hydration(self) -> list[dict]:
        """
        Fetch all entity data needed to hydrate EntityResolver.
        Single query, single pass.
        """
        query = """
        MATCH (e:Entity)
        WHERE e.id IS NOT NULL
        OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)
        WITH e, t
        WHERE t IS NULL OR t.status IS NULL OR t.status <> 'inactive'
        RETURN e.id AS id,
            e.canonical_name AS canonical_name,
            e.aliases AS aliases,
            e.type AS type,
            e.topic AS topic,
            e.summary AS summary,
            e.embedding AS embedding
        """
        with self.driver.session() as session:
            result = session.run(query)
            return [dict(record) for record in result]
    

    def update_entity_profile(self, entity_id: int, canonical_name: str, 
                        summary: str, embedding: List[float], 
                        last_msg_id: int, topic: str = "General"):
        """
        Update an existing entity's profile without touching relationships.
        Called by GraphBuilder when processing PROFILE_UPDATE messages.
        """
        def _update(tx: 'ManagedTransaction'):
            tx.run("""
                MERGE (e:Entity {id: $id})
                
                ON CREATE SET
                    e.canonical_name = $canonical_name,
                    e.summary = $summary,
                    e.embedding = $embedding,
                    e.last_profiled_msg_id = $last_msg_id,
                    e.last_updated = timestamp(),
                    e.created_by = 'profile_stream'

                ON MATCH SET
                    e.canonical_name = $canonical_name,
                    e.summary = $summary,
                    e.embedding = $embedding,
                    e.last_updated = timestamp(),
                    e.last_profiled_msg_id = $last_msg_id
                
                WITH e
                FOREACH (_ IN CASE WHEN $topic IS NOT NULL AND $topic <> "" THEN [1] ELSE [] END |
                    MERGE (t:Topic {name: $topic})
                    MERGE (e)-[:BELONGS_TO]->(t)
                )
            """, 
            id=entity_id, 
            canonical_name=canonical_name, 
            summary=summary,
            embedding=embedding,
            last_msg_id=last_msg_id,
            topic=topic
            )
        
        with self.driver.session() as session:
            session.execute_write(_update)
            logger.info(f"Updated entity {entity_id} profile (checkpoint: msg_{last_msg_id})")

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
    
    def get_orphan_entities(self, protected_id: int = 1) -> List[int]:
        """Find entity IDs with no relationships, excluding protected (user)."""
        query = """
        MATCH (e:Entity)
        WHERE NOT (e)-[:RELATED_TO]-() AND e.id <> $protected_id
        RETURN e.id as id
        """
        with self.driver.session() as session:
            result = session.run(query, {"protected_id": protected_id})
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
    
    def get_entities_by_name(self, name: str) -> List[Dict]:
        query = """
        MATCH (e:Entity)
        WHERE toLower(e.canonical_name) = toLower($name)
        OR any(alias IN e.aliases WHERE toLower(alias) = toLower($name))
        RETURN e.id as id, e.canonical_name as canonical_name, 
            e.type as type, e.aliases as aliases, e.summary as summary
        """
        with self.driver.session() as session:
            result = session.run(query, {"name": name})
            return [dict(record) for record in result]

    def set_topic_status(self, topic_name: str, status: str):
        """Handles Topic State (active/inactive/hot)"""

        query = "MERGE (t:Topic {name: $name}) SET t.status = $status"
        with self.driver.session() as session:
            session.run(query, {"name": topic_name, "status": status}).consume()
    
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
    
    
    def get_hot_topic_context(self, hot_topic_names: List[str]):
        """
        Retrieves the top 3 most recently active entities for each Hot Topic.
        """
        query = """
        MATCH (t:Topic) WHERE t.name IN $hot_topics
        MATCH (e:Entity)-[:BELONGS_TO]->(t)

        WITH t, e ORDER BY e.last_mentioned DESC 
        WITH t, collect(e)[..3] as top_entities
        UNWIND top_entities as e
        RETURN t.name as topic, e.canonical_name as name, e.summary as summary
        """
        
        with self.driver.session() as session:
            result = session.run(query, {"hot_topics": hot_topic_names})
            
            grouped = {}
            for record in result:
                topic = record["topic"]
                if topic not in grouped:
                    grouped[topic] = []
                grouped[topic].append({
                    "name": record["name"],
                    "summary": record["summary"]
                })
            
            return grouped
    
    def search_entity(self, query: str, limit: int = 5):
        """
        Search for entities by name or alias.
        """
        query_cypher = """
        MATCH (e:Entity)
        WHERE (e.canonical_name CONTAINS $query 
            OR ANY(alias IN e.aliases WHERE alias CONTAINS $query))
        OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)
        WITH e, t
        WHERE t IS NULL OR t.status IS NULL OR t.status <> 'inactive'
        RETURN e.id as id, e.canonical_name as name, e.summary as summary, e.type as type
        ORDER BY e.last_mentioned DESC
        LIMIT $limit
        """
        with self.driver.session() as session:
            result = session.run(query_cypher, {"query": query, "limit": limit})
            return [record.data() for record in result]
    
    def get_entity_profile(self, entity_name: str):
        """
        Get the full profile for a specific entity.
        """

        query = """
        MATCH (e:Entity {canonical_name: $name})
        OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)
        WITH e, t
        WHERE t IS NULL OR t.status IS NULL OR t.status <> 'inactive'
        RETURN e.id as id,
            e.canonical_name as canonical_name,
            e.aliases as aliases,
            e.type as type,
            e.summary as summary,
            e.last_mentioned as last_mentioned,
            e.last_updated as last_updated,
            t.name as topic
        """
        with self.driver.session() as session:
            result = session.run(query, {"name": entity_name})
            record = result.single()
            return dict(record) if record else None

    def get_related_entities(self, entity_names: List[str], active_only: bool = True):
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
            ($active_only = false) OR
            (t IS NULL) OR
            (t.status IS NULL) OR
            (t.status <> 'inactive')
        RETURN
            source.canonical_name as source,
            target.canonical_name as target,
            target.summary as target_summary,
            r.weight as connection_strength,
            r.message_ids as evidence_ids,
            r.confidence as confidence,
            r.last_seen as last_seen
        ORDER BY r.weight DESC, r.last_seen DESC
        LIMIT 50
        """
        with self.driver.session() as session:
            res = session.run(query, {"names": entity_names, "active_only": active_only})
            return [record.data() for record in res]
        
    
    def get_recent_activity(self, entity_name: str, hours: int = 24):
        """
        Get recent interactions involving an entity within a time window.
        """
        cutoff_ms = int((time.time() - (hours * 3600)) * 1000)
        query = """
        MATCH (e:Entity {canonical_name: $name})-[r:RELATED_TO]-(target:Entity)
        WHERE r.last_seen > $cutoff
        RETURN target.canonical_name as entity, r.message_ids as evidence_ids, r.last_seen as time
        ORDER BY r.last_seen DESC
        """
        with self.driver.session() as session:
            result = session.run(query, {"name": entity_name, "cutoff": cutoff_ms})
            return [record.data() for record in result]
    
    
    def _find_path_filtered(self, start_name: str, end_name: str, active_only: bool = True) -> List[Dict]:
        query = """
        MATCH (start:Entity {canonical_name: $start_name})
        MATCH (end:Entity {canonical_name: $end_name})
        MATCH p = (start)-[:RELATED_TO *BFS ..4]-(end)
        RETURN [n in nodes(p) | n.canonical_name] as names,
            [r in relationships(p) | r.message_ids] as evidence_ids
        LIMIT 1
        """
        with self.driver.session() as session:
            result = session.run(query, {
                "start_name": start_name, 
                "end_name": end_name,
                "active_only": active_only
            })
            record = result.single()
            if not record:
                return []
            
            path_data = []
            names = record["names"]
            evidence = record["evidence_ids"]
            for i in range(len(evidence)):
                path_data.append({
                    "step": i,
                    "entity_a": names[i],
                    "entity_b": names[i+1],
                    "evidence_refs": evidence[i]
                })
            return path_data
    
    def get_topics_by_status(self) -> dict:
        query = """
        MATCH (t:Topic)
        RETURN t.name as name, coalesce(t.status, 'active') as status
        """
        with self.driver.session() as session:
            result = session.run(query)
            grouped = {"active": [], "hot": [], "inactive": []}
            for record in result:
                status = record["status"]
                if status in grouped:
                    grouped[status].append(record["name"])
            return grouped
    
    def get_entities_list(self, topic: str = None, limit: int = 50) -> list[dict]:
        query = """
        MATCH (e:Entity)
        OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)
        WHERE ($topic IS NULL OR t.name = $topic)
        AND (t IS NULL OR t.status IS NULL OR t.status <> 'inactive')
        RETURN e.id as id,
            e.canonical_name as canonical_name,
            e.type as type,
            e.summary as summary,
            t.name as topic
        ORDER BY e.last_mentioned DESC
        LIMIT $limit
        """
        with self.driver.session() as session:
            result = session.run(query, {"topic": topic, "limit": limit})
            return [dict(record) for record in result]
    
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
    
    def merge_entities(self, primary_id: int, secondary_id: int, merged_summary: str) -> bool:
        """
        Merge secondary entity into primary (single transaction).
        Primary survives with combined data, secondary is deleted.
        
        Args:
            primary_id: Entity that survives
            secondary_id: Entity that gets merged and deleted
            merged_summary: Pre-computed summary (from LLM or concat)
        """
        
        def _execute_merge(tx):
            # Step 1: Get both entities and validate they exist
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
            
            # Step 2: Update primary with merged data
            combined_aliases = list(set(
                (check["p_aliases"] or []) + 
                (check["s_aliases"] or []) + 
                [check["s_name"]]
            ))
            
            tx.run("""
                MATCH (p:Entity {id: $primary_id})
                SET p.aliases = $aliases,
                    p.summary = $summary,
                    p.last_updated = timestamp()
                WITH p
                MATCH (s:Entity {id: $secondary_id})
                SET p.confidence = CASE 
                        WHEN coalesce(s.confidence, 0) > coalesce(p.confidence, 0) 
                        THEN s.confidence ELSE p.confidence END,
                    p.last_mentioned = CASE 
                        WHEN coalesce(s.last_mentioned, 0) > coalesce(p.last_mentioned, 0) 
                        THEN s.last_mentioned ELSE p.last_mentioned END
            """, primary_id=primary_id, secondary_id=secondary_id, 
                aliases=combined_aliases, summary=merged_summary)
            
            # Step 3: Transfer relationships from secondary to primary
            tx.run("""
                MATCH (s:Entity {id: $secondary_id})-[r_old:RELATED_TO]-(target:Entity)
                WHERE target.id <> $primary_id
                MATCH (p:Entity {id: $primary_id})
                MERGE (p)-[r_new:RELATED_TO]-(target)
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
                    r_new.message_ids = r_new.message_ids + r_old.message_ids
            """, primary_id=primary_id, secondary_id=secondary_id)
            
            # Step 4: Delete secondary entity
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