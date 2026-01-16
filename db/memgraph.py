import time
from loguru import logger
from typing import Dict, List, Tuple
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
            "CREATE CONSTRAINT ON (m:Message) ASSERT m.id IS UNIQUE;",
            "CREATE CONSTRAINT ON (f:Fact) ASSERT f.id IS UNIQUE;",
            "CREATE INDEX ON :Fact(invalid_at);",
            "CREATE INDEX ON :Fact(created_at);",
            "CREATE INDEX ON :Message(timestamp);",
            "CREATE INDEX ON :MoodCheckpoint(timestamp);",
            "CREATE INDEX ON :Entity(canonical_name);"
        ]
        
        with self.driver.session() as session:
            for q in queries:
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
            m.timestamp = msg.timestamp
        """
        with self.driver.session() as session:
            session.run(query, {"batch": messages}).consume()

    def get_message_text(self, message_id: int) -> str:
        """
        Fetch message text on demand.
        """
        query = "MATCH (m:Message {id: $id}) RETURN m.content as content"
        with self.driver.session() as session:
            result = session.run(query, {"id": int(message_id)}).single()
            return result["content"] if result else ""
    
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
                        e.facts = [],
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
            t.name AS topic,
            e.facts AS facts,
            e.embedding AS embedding
        """
        with self.driver.session() as session:
            result = session.run(query)
            return [dict(record) for record in result]
    

    def update_entity_profile(self, entity_id: int, canonical_name: str, 
                        facts: List[str], embedding: List[float], 
                        last_msg_id: int):
        """
        Update an existing entity's fact ledger.
        """
        def _update(tx: 'ManagedTransaction'):
            tx.run("""
                MERGE (e:Entity {id: $id})
                
                ON CREATE SET
                    e.canonical_name = $canonical_name,
                    e.facts = $facts,
                    e.embedding = $embedding,
                    e.last_profiled_msg_id = $last_msg_id,
                    e.last_updated = timestamp(),
                    e.created_by = 'profile_stream'

                ON MATCH SET
                    e.canonical_name = $canonical_name,
                    e.facts = $facts,
                    e.embedding = $embedding,
                    e.last_updated = timestamp(),
                    e.last_profiled_msg_id = $last_msg_id
            """, 
            id=entity_id, 
            canonical_name=canonical_name, 
            facts=facts,
            embedding=embedding,
            last_msg_id=last_msg_id
            )
        
        with self.driver.session() as session:
            session.execute_write(_update)
        logger.info(f"Updated entity {entity_id} ledger (checkpoint: msg_{last_msg_id})")

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
        """Find entity IDs with no relationships, excluding protected (user)."""
        query = """
        MATCH (e:Entity)
        WHERE NOT (e)-[:RELATED_TO]-() 
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
    
    def reset_all_topics_to_active(self):
        """Set all topics to active status."""
        query = "MATCH (t:Topic) SET t.status = NULL"
        with self.driver.session() as session:
            session.run(query).consume()
    
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
    
    def search_entity(self, query: str, limit: int = 5, connections_limit: int = 5, evidence_limit: int = 5) -> list[dict]:
        """
        Search for entities by name/alias with top connections included.
        """
        cypher = """
        MATCH (e:Entity)
        WHERE toLower(e.canonical_name) CONTAINS toLower($query)
        OR ANY(alias IN e.aliases WHERE toLower(alias) CONTAINS toLower($query))
        OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)
        WITH e, t
        WHERE t IS NULL OR t.status IS NULL OR t.status <> 'inactive'
        WITH e, t
        LIMIT $limit
        OPTIONAL MATCH (e)-[r:RELATED_TO]-(conn:Entity)
        OPTIONAL MATCH (e)-[:PART_OF]->(parent:Entity)
        OPTIONAL MATCH (child:Entity)-[:PART_OF]->(e)
        WITH e, t, r, conn, parent,
            count(DISTINCT child) as children_count
        RETURN e.id AS id,
            e.canonical_name AS canonical_name,
            e.aliases AS aliases,
            e.type AS type,
            e.facts AS facts,
            t.name AS topic,
            e.last_mentioned AS last_mentioned,
            e.last_updated AS last_updated,
            conn.canonical_name AS conn_name,
            conn.aliases AS conn_aliases,
            conn.facts AS conn_facts,
            r.weight AS conn_weight,
            r.message_ids AS evidence_ids,
            parent.canonical_name AS parent_name,
            children_count
        ORDER BY e.last_mentioned DESC, conn_weight DESC
        """
        
        with self.driver.session() as session:
            result = session.run(cypher, {"query": query, "limit": limit})
            
            entities = {}
            for row in result:
                eid = row["id"]
                
                if eid not in entities:
                    entities[eid] = {
                        "id": eid,
                        "canonical_name": row["canonical_name"],
                        "aliases": row["aliases"] or [],
                        "type": row["type"],
                        "facts": row["facts"],
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
            target.facts as target_facts,
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
        OPTIONAL MATCH (target)-[:BELONGS_TO]->(t:Topic)
        WHERE r.last_seen > $cutoff
        AND (t IS NULL OR t.status IS NULL OR t.status <> 'inactive')
        RETURN target.canonical_name as entity, r.message_ids as evidence_ids, r.last_seen as time
        ORDER BY r.last_seen DESC
        """

        with self.driver.session() as session:
            result = session.run(query, {"name": entity_name, "cutoff": cutoff_ms})
            return [record.data() for record in result]
    
    
    def _build_path_data(self, names: List[str], evidence: List[List[str]]) -> List[Dict]:
        """Convert raw query results into path structure."""
        return [
            {
                "step": i,
                "entity_a": names[i],
                "entity_b": names[i + 1],
                "evidence_refs": evidence[i]
            }
            for i in range(len(evidence))
        ]


    def _find_shortest_path(self, start_name: str, end_name: str) -> tuple[List[str], List[List[str]], bool] | None:
        """
        Find shortest path regardless of topic status.
        Returns: (names, evidence_ids, has_inactive) or None if no path.
        """
        query = """
        MATCH (start:Entity {canonical_name: $start_name})
        MATCH (end:Entity {canonical_name: $end_name})
        MATCH p = (start)-[:RELATED_TO *BFS ..4]-(end)
        UNWIND nodes(p) AS n
        OPTIONAL MATCH (n)-[:BELONGS_TO]->(t:Topic)
        WITH p, collect(COALESCE(t.status, 'active')) AS statuses,
            [node IN nodes(p) | node.canonical_name] AS names,
            [r IN relationships(p) | r.message_ids] AS evidence_ids
        RETURN names, evidence_ids,
            ANY(s IN statuses WHERE s = 'inactive') AS has_inactive
        LIMIT 1
        """
        
        with self.driver.session() as session:
            record = session.run(query, {"start_name": start_name, "end_name": end_name}).single()
            if not record:
                return None
            return record["names"], record["evidence_ids"], record["has_inactive"]


    def _find_active_only_path(self, start_name: str, end_name: str) -> tuple[List[str], List[List[str]]] | None:
        """
        Find shortest path excluding inactive-topic entities.
        Returns: (names, evidence_ids) or None if no path.
        """
        query = """
        MATCH (start:Entity {canonical_name: $start_name})
        MATCH (end:Entity {canonical_name: $end_name})
        MATCH p = (start)-[:RELATED_TO *BFS ..4]-(end)
        WHERE ALL(n IN nodes(p) WHERE
            NOT EXISTS {
                MATCH (n)-[:BELONGS_TO]->(t:Topic)
                WHERE t.status = 'inactive'
            }
        )
        RETURN [n IN nodes(p) | n.canonical_name] AS names,
            [r IN relationships(p) | r.message_ids] AS evidence_ids
        LIMIT 1
        """
        
        with self.driver.session() as session:
            record = session.run(query, {"start_name": start_name, "end_name": end_name}).single()
            if not record:
                return None
            return record["names"], record["evidence_ids"]


    def _find_path_filtered(self, start_name: str, end_name: str, active_only: bool = True) -> tuple[List[Dict], bool]:
        """
        Find path between entities with topic filtering.
        
        Returns: (path_data, has_inactive_shortcut)
        - path_data: usable path (empty if none found)
        - has_inactive_shortcut: True if shorter path exists through inactive topics
        """
        shortest = self._find_shortest_path(start_name, end_name)
        
        if not shortest:
            return [], False
        
        names, evidence, has_inactive = shortest
        
        if not has_inactive:
            return self._build_path_data(names, evidence), False
        
        if not active_only:
            return self._build_path_data(names, evidence), False
        
        active_path = self._find_active_only_path(start_name, end_name)
        
        if active_path:
            active_names, active_evidence = active_path
            return self._build_path_data(active_names, active_evidence), True
        
        # No active path, only inactive exists
        return [], True
    
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
    
    def get_entities_with_invalidated_facts(self) -> List[Dict]:
        """Find entities holding facts marked as [INVALIDATED: ...]"""
        query = """
        MATCH (e:Entity)
        WHERE any(f IN e.facts WHERE f CONTAINS '[INVALIDATED:')
        RETURN e.id as id, e.facts as facts
        """
        with self.driver.session() as session:
            result = session.run(query)
            return [dict(record) for record in result]


    def commit_fact_archival(self, entity_id: int, active_facts: List[str], archived_facts: List[str]):
        """
        Move old facts to a linked History node and update the main entity.
        """
        query = """
        MATCH (e:Entity {id: $id})
        MERGE (e)-[:HAS_HISTORY]->(h:FactHistory)
        ON CREATE SET 
            h.facts = $archived_facts, 
            h.last_archived = timestamp()
        ON MATCH SET 
            h.facts = h.facts + $archived_facts, 
            h.last_archived = timestamp()
        
        SET e.facts = $active_facts, e.last_updated = timestamp()
        """
        with self.driver.session() as session:
            session.run(query, {
                "id": entity_id, 
                "active_facts": active_facts, 
                "archived_facts": archived_facts
            }).consume()
    
    def get_archived_facts(self, entity_id: int) -> List[str]:
        """
        Retrieve the full history of archived facts for an entity.
        Used by the 'get_entity_history' tool.
        """
        query = """
        MATCH (e:Entity {id: $id})-[:HAS_HISTORY]->(h:FactHistory)
        RETURN h.facts as facts
        """
        with self.driver.session() as session:
            result = session.run(query, {"id": entity_id}).single()
            if result and result["facts"]:
                return result["facts"]
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
    
    def merge_entities(self, primary_id: int, secondary_id: int, merged_facts: List[str]) -> bool:
        """
        Merge secondary entity into primary (single transaction).
        Primary survives with combined data, secondary is deleted.
        
        Args:
            primary_id: Entity that survives
            secondary_id: Entity that gets merged and deleted
            merged_facts: Pre-computed facts
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
                    p.facts = $facts,
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
                aliases=combined_aliases, facts=merged_facts)
            
            # Step 3: Transfer relationships from secondary to primary
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
    
    