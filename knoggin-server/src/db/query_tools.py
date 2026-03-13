import re
import time
from loguru import logger
from typing import Dict, List, Tuple, Any, Optional
from neo4j import AsyncDriver


class GraphToolQueries:
    def __init__(self, driver: AsyncDriver):
        self.driver = driver

    def _build_path_data(self, names: List[str], topics: List[str], evidence: List[List[str]]) -> List[Dict]:
        """Convert raw query results into path structure."""
        return [
            {
                "step": i,
                "entity_a": names[i],
                "entity_b": names[i + 1],
                "topic_a": topics[i] if i < len(topics) else None,
                "topic_b": topics[i + 1] if i + 1 < len(topics) else None,
                "evidence_refs": evidence[i]
            }
            for i in range(len(evidence))
        ]
    
    async def get_hot_topic_context_with_messages(self, hot_topic_names: List[str], msg_limit: int = 5, slim: bool = False) -> Dict[str, Dict[str, Any]]:
        """
        Get top entities + recent message IDs per hot topic.
        slim=True: returns name + aliases only (no summaries)
        """
        if slim:
            entity_projection = "{name: e.canonical_name, aliases: e.aliases}"
            msg_limit = 20
        else:
            entity_projection = "{name: e.canonical_name, facts: [(e)-[:HAS_FACT]->(f) WHERE f.invalid_at IS NULL | f.content]}"
        
        query = f"""
        MATCH (t:Topic) WHERE t.name IN $hot_topics
        MATCH (e:Entity)-[:BELONGS_TO]->(t)
        OPTIONAL MATCH (e)-[r:RELATED_TO]-()
        
        WITH t, e, r ORDER BY e.last_mentioned DESC
        WITH t, 
            collect(DISTINCT {entity_projection})[..3] as entities,
            reduce(flat = [], arr IN collect(DISTINCT r.message_ids)[..5] | flat + arr) as flat_msgs
        
        RETURN t.name as topic, 
            entities,
            flat_msgs[..$msg_limit] as message_ids
        """
        
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"hot_topics": hot_topic_names, "msg_limit": msg_limit})
                return {
                    record["topic"]: {
                        "entities": record["entities"],
                        "message_ids": record["message_ids"] or []
                    }
                    async for record in result
                }
        except Exception as e:
            logger.error(f"Failed to get hot topic context: {e}")
            return {}
    
    @staticmethod
    def _sanitize_fts_query(query: str) -> str:
        """Strip characters that can break Memgraph full-text search."""
        # Remove FTS operators and special chars
        sanitized = re.sub(r'[+\-"*~^\\:(){}[\]]', ' ', query)
        # Collapse whitespace
        sanitized = re.sub(r'\s+', ' ', sanitized).strip()
        return sanitized

    async def search_messages_fts(self, query: str, limit: int = 50) -> List[Tuple[int, float]]:
        """
        Perform native Full-Text Search on Message nodes.
        Returns list of (message_id, score).
        """

        sanitized = self._sanitize_fts_query(query)
        if not sanitized:
            return []

        cypher = """
        CALL text_search.search_all('message_search', $q, $limit) YIELD node, score
        RETURN node.id as id, score
        ORDER BY score DESC LIMIT $limit
        """
        
        try:
            async with self.driver.session() as session:
                result = await session.run(cypher, {"q": sanitized, "limit": limit})
                return [(record["id"], record["score"]) async for record in result]
        except Exception as e:
            logger.error(f"FTS Message search failed: {e}")
            return []
    
    async def search_entity(self, query: str, active_topics: Optional[List[str]] = None, limit: int = 5, connections_limit: int = 5, evidence_limit: int = 5) -> List[Dict[str, Any]]:
        """
        Search for entities by name/alias with top connections included.
        """
        clean_query = re.sub(r"[^\w\s.\-']", '', query).strip()
        if not clean_query:
             return []

        cypher = """
        CALL text_search.search_all('entity_search', $q) YIELD node as e, score
        
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
            r.context AS conn_context,
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
        
        try:
            async with self.driver.session() as session:
                result = await session.run(cypher, params)
                
                entities: Dict[int, Any] = {}
                async for row in result:
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
                            "context": row["conn_context"],
                            "evidence_ids": list(row["evidence_ids"] or [])[:evidence_limit]
                        })
                return list(entities.values())
        except Exception as e:
            logger.error(f"Entity search failed: {e}")
            return []

    async def get_related_entities(self, entity_names: List[str], active_topics: Optional[List[str]] = None, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Find all entities connected to the given entities.
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
            [(target)-[:HAS_FACT]->(f) WHERE f.invalid_at IS NULL | f.content] as target_facts,
            r.weight as connection_strength,
            r.message_ids as evidence_ids,
            r.confidence as confidence,
            r.last_seen as last_seen,
            r.context as context
        ORDER BY r.weight DESC, r.last_seen DESC
        LIMIT $limit
        """
        params = {
            "names": entity_names, 
            "active_topics": active_topics if active_topics is not None else [],
            "filter_topics": active_topics is not None,
            "limit": limit
        }
        try:
            async with self.driver.session() as session:
                result = await session.run(query, params)
                return [record.data() async for record in result]
        except Exception as e:
            logger.error(f"Failed to get related entities: {e}")
            return []
        
    
    async def get_recent_activity(self, entity_name: str, active_topics: Optional[List[str]] = None, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get recent interactions. Filtered by active_topics if provided.
        """
        cutoff_ms = int((time.time() - (hours * 3600)) * 1000)

        query = """
        MATCH (e:Entity {canonical_name: $name})-[r:RELATED_TO]-(target:Entity)
        WHERE r.last_seen > $cutoff
        OPTIONAL MATCH (target)-[:BELONGS_TO]->(t:Topic)
        WITH e, r, target, t
        WHERE ($filter_topics = false) OR (t IS NULL) OR (t.name IN $active_topics)
        RETURN target.canonical_name as entity, r.message_ids as evidence_ids, r.last_seen as time
        ORDER BY r.last_seen DESC
        """

        params = {
            "name": entity_name, 
            "cutoff": cutoff_ms,
            "active_topics": active_topics if active_topics is not None else [],
            "filter_topics": active_topics is not None
        }

        try:
            async with self.driver.session() as session:
                result = await session.run(query, params)
                return [record.data() async for record in result]
        except Exception as e:
            logger.error(f"Failed to get recent activity for {entity_name}: {e}")
            return []
    
    async def _find_shortest_path(self, start_name: str, end_name: str, active_topics: Optional[List[str]] = None, max_depth: int = 4) -> Optional[Tuple[List[str], List[str], List[List[str]], bool]]:
        """
        Find shortest path. Calculates 'has_inactive' dynamically based on passed active_topics list.
        Returns: (names, topics, evidence_ids, has_inactive)
        """
        query = f"""
        MATCH (start:Entity {{canonical_name: $start_name}})
        MATCH (end:Entity {{canonical_name: $end_name}})
        MATCH p = (start)-[:RELATED_TO *BFS ..{max_depth}]-(end)
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
        
        try:
            async with self.driver.session() as session:
                result = await session.run(query, params)
                record = await result.single()
                if not record:
                    return None
                return record["names"], record["node_topics"], record["evidence_ids"], record["has_inactive"]
        except Exception as e:
            logger.error(f"Shortest path search failed: {e}")
            return None

    async def _find_active_only_path(self, start_name: str, end_name: str, active_topics: Optional[List[str]] = None, max_depth: int = 4) -> Optional[Tuple[List[str], List[str], List[List[str]]]]:
        """
        Find shortest path excluding inactive-topic entities.
        Returns: (names, topics, evidence_ids) or None if no path.
        """
        query = f"""
        MATCH (start:Entity {{canonical_name: $start_name}})
        MATCH (end:Entity {{canonical_name: $end_name}})
        MATCH p = (start)-[:RELATED_TO *BFS ..{max_depth}]-(end)
        
        UNWIND nodes(p) AS n
        OPTIONAL MATCH (n)-[:BELONGS_TO]->(t:Topic)
        
        WITH p, n, t
        WHERE t IS NULL OR t.name IN $active_topics
        
        WITH p, count(DISTINCT n) AS valid_count
        WHERE valid_count = size(nodes(p))
        
        WITH p LIMIT 1
        UNWIND nodes(p) AS n
        OPTIONAL MATCH (n)-[:BELONGS_TO]->(t:Topic)
        
        WITH p, collect(COALESCE(t.name, 'General')) AS node_topics
        RETURN [n IN nodes(p) | n.canonical_name] AS names,
            node_topics,
            [r IN relationships(p) | r.message_ids] AS evidence_ids
        """
        
        params = {
            "start_name": start_name, 
            "end_name": end_name,
            "active_topics": active_topics
        }
        
        try:
            async with self.driver.session() as session:
                result = await session.run(query, params)
                record = await result.single()
                if not record:
                    return None
                return record["names"], record["node_topics"], record["evidence_ids"]
        except Exception as e:
            logger.error(f"Failed to find active-only path: {e}")
            return None


    async def _find_path_filtered(self, start_name: str, end_name: str, active_topics: Optional[List[str]] = None, max_depth: int = 4) -> Tuple[List[Dict], bool]:
        """
        Find path between entities with topic filtering.
        Returns: (path_data, has_inactive_shortcut)
        """
        
        shortest = await self._find_shortest_path(start_name, end_name, active_topics, max_depth)
        
        if not shortest:
            return [], False
        
        names, topics, evidence, has_inactive = shortest
        
        if not has_inactive:
            return self._build_path_data(names, topics, evidence), False
        
        active_path = await self._find_active_only_path(start_name, end_name, active_topics, max_depth)
        
        if active_path:
            active_names, active_topics_list, active_evidence = active_path
            return self._build_path_data(active_names, active_topics_list, active_evidence), True
        
        # No active path exists, only the inactive one
        return [], True
    