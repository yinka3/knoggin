import json
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from infrastructure.db_client import DBClient


class ToolQueries:
    def __init__(self, client: DBClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name

    def _build_path_data(
        self, names: List[str], topics: List[str], evidence: List[List[str]]
    ) -> List[Dict]:
        return [
            {
                "step": i,
                "entity_a": names[i],
                "entity_b": names[i + 1],
                "topic_a": topics[i] if i < len(topics) else None,
                "topic_b": topics[i + 1] if i + 1 < len(topics) else None,
                "evidence_refs": evidence[i] if i < len(evidence) else [],
            }
            for i in range(len(evidence))
        ]

    async def get_hot_topic_context_with_messages(
        self, hot_topic_names: List[str], msg_limit: int = 5, slim: bool = False
    ) -> Dict[str, Dict[str, Any]]:
        # In AGE, list comprehensions like `[(e)-[:HAS_FACT]->(f) ... | f.content]` work if formatted carefully,
        # but subquery limitations can trigger. A safer fallback is multiple queries, but let's try standard standard Cypher first.

        # It's better to fetch entities and relationships, then group in python to avoid complex AGE reduce failures.
        cypher = """
        MATCH (t:Topic) WHERE t.name IN $hot_topics
        MATCH (e:Entity)-[:BELONGS_TO]->(t)
        OPTIONAL MATCH (e)-[r:RELATED_TO]-()
        WITH t, e, r
        ORDER BY e.last_mentioned DESC
        RETURN t.name as topic, e.canonical_name as name, e.aliases as aliases,
               [(e)-[:HAS_FACT]->(f) WHERE f.invalid_at IS NULL | f.content] as facts,
               r.message_ids as msg_ids
        """
        query = self.client.build_cypher(
            cypher,
            "topic agtype, name agtype, aliases agtype, facts agtype, msg_ids agtype",
        )

        try:
            data = await self.client.execute_read(
                query, (json.dumps({"hot_topics": hot_topic_names}),)
            )

            topics_map = {}
            for row in data:
                t_name = (
                    row["topic"].strip('"')
                    if isinstance(row["topic"], str)
                    else row["topic"]
                )
                if t_name not in topics_map:
                    topics_map[t_name] = {
                        "entities": [],
                        "message_ids": set(),
                        "_entity_names": set(),
                    }

                e_name = (
                    row["name"].strip('"')
                    if isinstance(row["name"], str)
                    else row["name"]
                )

                if (
                    e_name not in topics_map[t_name]["_entity_names"]
                    and len(topics_map[t_name]["entities"]) < 3
                ):
                    topics_map[t_name]["_entity_names"].add(e_name)
                    ent = {"name": e_name, "aliases": row["aliases"] or []}
                    if not slim:
                        ent["facts"] = row["facts"] or []
                    topics_map[t_name]["entities"].append(ent)

                if row["msg_ids"]:
                    for m_id in row["msg_ids"]:
                        if len(topics_map[t_name]["message_ids"]) < msg_limit:
                            topics_map[t_name]["message_ids"].add(m_id)

            # Convert sets to lists
            result = {}
            for t, val in topics_map.items():
                result[t] = {
                    "entities": val["entities"],
                    "message_ids": list(val["message_ids"]),
                }
            return result
        except Exception as e:
            logger.error(f"Failed to get hot topic context: {e}")
            return {}

    @staticmethod
    def _sanitize_fts_query(query: str) -> str:
        sanitized = re.sub(r'[+\-"*~^\\:(){}[\]!&|?]', " ", query)
        sanitized = re.sub(r"\s+", " ", sanitized).strip()
        # Convert to postgres valid tsquery (e.g. "foo bar" -> "foo | bar")
        if not sanitized:
            return ""
        return " | ".join(sanitized.split())

    async def search_messages_fts(
        self, query: str, limit: int = 50
    ) -> List[Tuple[int, float]]:
        sanitized = self._sanitize_fts_query(query)
        if not sanitized:
            return []

        sql = """
        SELECT message_id, ts_rank(content_tsvector, to_tsquery('english', %s)) as score
        FROM message_search
        WHERE content_tsvector @@ to_tsquery('english', %s)
        ORDER BY score DESC LIMIT %s
        """
        try:
            res = await self.client.execute_read(sql, (sanitized, sanitized, limit))
            return [(int(row["message_id"]), float(row["score"])) for row in res]
        except Exception as e:
            logger.error(f"Postgres FTS search failed: {e}")
            return []

    async def search_entity(
        self,
        query: str,
        active_topics: Optional[List[str]] = None,
        limit: int = 5,
        connections_limit: int = 5,
        evidence_limit: int = 5,
    ) -> List[Dict[str, Any]]:
        clean_query = re.sub(r"[^\w\s.\-']", "", query).strip()
        if not clean_query:
            return []

        # 1. Search Postgres for top entity IDs
        # We can just use ILIKE on canonical_name
        search_sql = """
        SELECT entity_id FROM entity_search
        WHERE canonical_name ILIKE %s
        LIMIT %s
        """
        try:
            id_res = await self.client.execute_read(
                search_sql, (f"%{clean_query}%", limit * 2)
            )
            if not id_res:
                return []
            entity_ids = [int(r["entity_id"]) for r in id_res]

            # 2. Fetch Graph data for those IDs
            cypher = """
            MATCH (e:Entity) WHERE e.id IN $ids
            OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)
            WITH e, t
            WHERE ($filter_topics = false) OR (t IS NULL) OR (t.name IN $active_topics)
            
            OPTIONAL MATCH (e)-[:PART_OF]->(parent:Entity)
            OPTIONAL MATCH (child:Entity)-[:PART_OF]->(e)
            OPTIONAL MATCH (e)-[r:RELATED_TO]-(conn:Entity)
            
            WITH e, t, parent, count(DISTINCT child) as children_count, r, conn
            ORDER BY r.weight DESC
            
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
            """

            q = self.client.build_cypher(
                cypher,
                "id agtype, canonical_name agtype, aliases agtype, type agtype, topic agtype, last_mentioned agtype, last_updated agtype, facts agtype, conn_name agtype, conn_aliases agtype, conn_weight agtype, evidence_ids agtype, conn_context agtype, conn_facts agtype, parent_name agtype, children_count agtype",
            )

            data = await self.client.execute_read(
                q,
                (
                    json.dumps(
                        {
                            "ids": entity_ids,
                            "filter_topics": active_topics is not None,
                            "active_topics": active_topics
                            if active_topics is not None
                            else [],
                        }
                    ),
                ),
            )

            entities: Dict[int, Any] = {}
            for row in data:
                eid = int(row["id"])

                if eid not in entities:
                    entities[eid] = {
                        "id": eid,
                        "canonical_name": row["canonical_name"].strip('"')
                        if isinstance(row["canonical_name"], str)
                        else row["canonical_name"],
                        "aliases": row["aliases"] or [],
                        "type": row["type"].strip('"')
                        if isinstance(row["type"], str)
                        else row["type"],
                        "facts": row["facts"] or [],
                        "topic": row["topic"].strip('"')
                        if isinstance(row["topic"], str)
                        else row["topic"],
                        "last_mentioned": row["last_mentioned"],
                        "last_updated": row["last_updated"],
                        "top_connections": [],
                        "hierarchy": {
                            "parent": row["parent_name"].strip('"')
                            if isinstance(row["parent_name"], str)
                            else row["parent_name"],
                            "children_count": int(row["children_count"] or 0),
                        },
                    }

                if (
                    row["conn_name"]
                    and len(entities[eid]["top_connections"]) < connections_limit
                ):
                    entities[eid]["top_connections"].append(
                        {
                            "canonical_name": row["conn_name"].strip('"')
                            if isinstance(row["conn_name"], str)
                            else row["conn_name"],
                            "aliases": row["conn_aliases"] or [],
                            "facts": row["conn_facts"] or [],
                            "weight": float(row["conn_weight"] or 1.0),
                            "context": row["conn_context"].strip('"')
                            if isinstance(row["conn_context"], str)
                            else row["conn_context"],
                            "evidence_ids": list(row["evidence_ids"] or [])[
                                :evidence_limit
                            ],
                        }
                    )
            return list(entities.values())[:limit]

        except Exception as e:
            logger.error(f"Failed search_entity: {e}")
            return []

    async def get_related_entities(
        self,
        entity_names: List[str],
        active_topics: Optional[List[str]] = None,
        limit: int = 25,
    ) -> List[Dict[str, Any]]:
        cypher = """
        MATCH (source:Entity) WHERE source.canonical_name IN $names
        MATCH (source)-[r:RELATED_TO]-(target:Entity)
        OPTIONAL MATCH (target)-[:BELONGS_TO]->(t:Topic)
        WITH source, r, target, t
        WHERE ($filter_topics = false) OR (t IS NULL) OR (t.name IN $active_topics)
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
        q = self.client.build_cypher(
            cypher,
            "source agtype, target agtype, target_facts agtype, connection_strength agtype, evidence_ids agtype, confidence agtype, last_seen agtype, context agtype",
        )
        try:
            data = await self.client.execute_read(
                q,
                (
                    json.dumps(
                        {
                            "names": entity_names,
                            "filter_topics": active_topics is not None,
                            "active_topics": active_topics
                            if active_topics is not None
                            else [],
                            "limit": limit,
                        }
                    ),
                ),
            )
            return [
                {
                    "source": r["source"].strip('"')
                    if isinstance(r["source"], str)
                    else r["source"],
                    "target": r["target"].strip('"')
                    if isinstance(r["target"], str)
                    else r["target"],
                    "target_facts": r["target_facts"] or [],
                    "connection_strength": float(r["connection_strength"] or 1.0),
                    "evidence_ids": r["evidence_ids"] or [],
                    "confidence": float(r["confidence"] or 1.0),
                    "last_seen": r["last_seen"],
                    "context": r["context"].strip('"')
                    if isinstance(r["context"], str)
                    else r["context"],
                }
                for r in data
            ]
        except Exception as e:
            logger.error(f"Failed get_related_entities: {e}")
            return []

    async def get_recent_activity(
        self,
        entity_name: str,
        active_topics: Optional[List[str]] = None,
        hours: int = 24,
    ) -> List[Dict[str, Any]]:
        cutoff_ms = int((time.time() - (hours * 3600)) * 1000)
        cypher = """
        MATCH (e:Entity {canonical_name: $name})-[r:RELATED_TO]-(target:Entity)
        WHERE r.last_seen > $cutoff
        OPTIONAL MATCH (target)-[:BELONGS_TO]->(t:Topic)
        WITH e, r, target, t
        WHERE ($filter_topics = false) OR (t IS NULL) OR (t.name IN $active_topics)
        RETURN target.canonical_name as entity, r.message_ids as evidence_ids, r.last_seen as time
        ORDER BY r.last_seen DESC
        """
        q = self.client.build_cypher(
            cypher, "entity agtype, evidence_ids agtype, time agtype"
        )
        try:
            data = await self.client.execute_read(
                q,
                (
                    json.dumps(
                        {
                            "name": entity_name,
                            "cutoff": cutoff_ms,
                            "filter_topics": active_topics is not None,
                            "active_topics": active_topics
                            if active_topics is not None
                            else [],
                        }
                    ),
                ),
            )
            return [
                {
                    "entity": r["entity"].strip('"')
                    if isinstance(r["entity"], str)
                    else r["entity"],
                    "evidence_ids": r["evidence_ids"] or [],
                    "time": r["time"],
                }
                for r in data
            ]
        except Exception as e:
            logger.error(f"Failed get_recent_activity: {e}")
            return []

    async def _find_shortest_path(
        self,
        start_name: str,
        end_name: str,
        active_topics: Optional[List[str]] = None,
        max_depth: int = 4,
    ) -> Optional[Tuple[List[str], List[str], List[List[str]], bool]]:
        # Using AGE standard variable-length path
        cypher = f"""
        MATCH (start:Entity {{canonical_name: $start_name}})
        MATCH (end:Entity {{canonical_name: $end_name}})
        MATCH p = (start)-[rels:RELATED_TO*1..{max_depth}]-(end)
        
        WITH p, nodes(p) as path_nodes, relationships(p) as path_rels
        ORDER BY length(p) ASC LIMIT 1
        
        UNWIND path_nodes AS n
        OPTIONAL MATCH (n)-[:BELONGS_TO]->(t:Topic)
        
        WITH p, path_nodes, path_rels, collect(COALESCE(t.name, 'General')) AS node_topics
        WITH p, path_nodes, path_rels, node_topics,
             [node IN path_nodes | node.canonical_name] AS names,
             [r IN path_rels | r.message_ids] AS evidence_ids
             
        WITH names, node_topics, evidence_ids,
             ANY(topic IN node_topics WHERE NOT ($filter_topics = false OR topic IN $active_topics)) as has_inactive
             
        RETURN names, node_topics, evidence_ids, has_inactive
        """
        q = self.client.build_cypher(
            cypher,
            "names agtype, node_topics agtype, evidence_ids agtype, has_inactive agtype",
        )
        try:
            data = await self.client.execute_read(
                q,
                (
                    json.dumps(
                        {
                            "start_name": start_name,
                            "end_name": end_name,
                            "filter_topics": active_topics is not None,
                            "active_topics": active_topics
                            if active_topics is not None
                            else [],
                        }
                    ),
                ),
            )
            if not data:
                return None
            row = data[0]
            return (
                row["names"],
                row["node_topics"],
                row["evidence_ids"],
                bool(row["has_inactive"]),
            )
        except Exception as e:
            logger.error(f"Failed _find_shortest_path: {e}")
            return None

    async def _find_active_only_path(
        self,
        start_name: str,
        end_name: str,
        active_topics: Optional[List[str]] = None,
        max_depth: int = 4,
    ) -> Optional[Tuple[List[str], List[str], List[List[str]]]]:
        cypher = f"""
        MATCH (start:Entity {{canonical_name: $start_name}})
        MATCH (end:Entity {{canonical_name: $end_name}})
        MATCH p = (start)-[rels:RELATED_TO*1..{max_depth}]-(end)
        
        WITH p, nodes(p) as path_nodes, relationships(p) as path_rels
        ORDER BY length(p) ASC
        
        // Filter out paths containing inactive nodes
        WHERE ALL(n IN path_nodes WHERE 
            EXISTS {{ MATCH (n)-[:BELONGS_TO]->(t:Topic) WHERE t.name IN $active_topics }} OR
            NOT EXISTS {{ MATCH (n)-[:BELONGS_TO]->(:Topic) }}
        )
        
        WITH p, path_nodes, path_rels LIMIT 1
        
        UNWIND path_nodes AS n
        OPTIONAL MATCH (n)-[:BELONGS_TO]->(t:Topic)
        
        WITH p, collect(COALESCE(t.name, 'General')) AS node_topics, path_nodes, path_rels
        RETURN [n IN path_nodes | n.canonical_name] AS names,
               node_topics,
               [r IN path_rels | r.message_ids] AS evidence_ids
        """
        q = self.client.build_cypher(
            cypher, "names agtype, node_topics agtype, evidence_ids agtype"
        )
        try:
            data = await self.client.execute_read(
                q,
                (
                    json.dumps(
                        {
                            "start_name": start_name,
                            "end_name": end_name,
                            "active_topics": active_topics
                            if active_topics is not None
                            else [],
                        }
                    ),
                ),
            )
            if not data:
                return None
            row = data[0]
            return (row["names"], row["node_topics"], row["evidence_ids"])
        except Exception as e:
            logger.error(f"Failed _find_active_only_path: {e}")
            return None

    async def find_path_filtered(
        self,
        start_name: str,
        end_name: str,
        active_topics: Optional[List[str]] = None,
        max_depth: int = 4,
    ) -> Tuple[List[Dict], bool]:
        shortest = await self._find_shortest_path(
            start_name, end_name, active_topics, max_depth
        )
        if not shortest:
            return [], False
        names, topics, evidence, has_inactive = shortest

        if not has_inactive:
            return self._build_path_data(names, topics, evidence), False

        active_path = await self._find_active_only_path(
            start_name, end_name, active_topics, max_depth
        )
        if active_path:
            active_names, active_topics_list, active_evidence = active_path
            return self._build_path_data(
                active_names, active_topics_list, active_evidence
            ), True

        return [], True
