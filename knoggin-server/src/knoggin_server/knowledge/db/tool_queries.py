import json
import re
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from common.scoping import IDENTITY_ENTITY_ID
from common.utils.time_utils import get_now_ms
from infrastructure.postgres_client import PostgresClient


class ToolQueries:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
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

    def _scope_params(self, visible_project_ids: Optional[List[str]] = None) -> Dict:
        return {
            "filter_projects": bool(visible_project_ids),
            "visible_project_ids": visible_project_ids or [],
            "identity_entity_id": IDENTITY_ENTITY_ID,
        }

    async def get_hot_topic_context_with_messages(
        self,
        hot_topic_names: List[str],
        msg_limit: int = 5,
        slim: bool = False,
        visible_project_ids: Optional[List[str]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        if not hot_topic_names:
            return {}

        filter_projects = bool(visible_project_ids)
        visible_project_ids = visible_project_ids or []

        query = """
        SELECT
            e.topic,
            e.canonical_name as name,
            COALESCE(
                (SELECT array_agg(alias) FROM entity_aliases ea WHERE ea.entity_id = e.entity_id),
                '{}'::text[]
            ) as aliases,
            CASE WHEN %s THEN '{}'::text[] ELSE COALESCE(
                (SELECT array_agg(content) FROM facts f WHERE f.entity_id = e.entity_id AND f.invalid_at IS NULL),
                '{}'::text[]
            ) END as facts,
            COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'user_name', rer.user_name,
                            'session_id', rer.session_id,
                            'message_id', rer.message_id
                        )
                    )
                    FROM (
                        SELECT r.relationship_id
                        FROM relationships r
                        WHERE r.entity_a_id = e.entity_id OR r.entity_b_id = e.entity_id
                        LIMIT 10
                    ) rels
                    JOIN relationship_evidence_refs rer ON rer.relationship_id = rels.relationship_id
                ),
                '[]'::jsonb
            ) as msg_ids
        FROM entities e
        WHERE e.topic = ANY(%s)
          AND (%s = false OR e.project_id = ANY(%s) OR e.entity_id = %s)
        ORDER BY e.last_mentioned_ms DESC
        """

        try:
            data = await self.client.execute_read(
                query,
                (
                    slim,
                    hot_topic_names,
                    filter_projects,
                    visible_project_ids,
                    IDENTITY_ENTITY_ID,
                ),
            )

            topics_map = {}
            for row in data:
                t_name = row["topic"]
                if t_name not in topics_map:
                    topics_map[t_name] = {
                        "entities": [],
                        "message_refs": [],
                        "_message_ref_keys": set(),
                        "_entity_names": set(),
                    }

                e_name = row["name"]
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
                    for msg_ref in row["msg_ids"]:
                        ref_key = (
                            msg_ref.get("user_name"),
                            msg_ref.get("session_id"),
                            msg_ref.get("message_id"),
                        )
                        if (
                            ref_key not in topics_map[t_name]["_message_ref_keys"]
                            and len(topics_map[t_name]["message_refs"]) < msg_limit
                        ):
                            topics_map[t_name]["_message_ref_keys"].add(ref_key)
                            topics_map[t_name]["message_refs"].append(msg_ref)

            result = {}
            for t, val in topics_map.items():
                result[t] = {
                    "entities": val["entities"],
                    "message_refs": val["message_refs"],
                }
            return result
        except Exception as e:
            logger.error(f"Failed to get hot topic context: {e}")
            return {}

    @staticmethod
    def _sanitize_fts_query(query: str) -> str:
        tokens = re.findall(r"\w+", query or "")
        # Convert to postgres valid tsquery (e.g. "foo bar" -> "foo | bar")
        if not tokens:
            return ""
        return " | ".join(tokens)

    async def search_messages_fts(
        self,
        query: str,
        limit: int = 50,
        user_name: Optional[str] = None,
        session_ids: Optional[List[str]] = None,
        project_ids: Optional[List[str]] = None,
    ) -> List[Tuple[int, float, str]]:
        sanitized = self._sanitize_fts_query(query)
        if not sanitized:
            return []

        scope_sql = ""
        params = [sanitized, sanitized]
        if user_name and project_ids:
            scope_sql = "AND user_name = %s AND project_id = ANY(%s)"
            params.extend([user_name, project_ids])
        elif user_name and session_ids:
            scope_sql = "AND user_name = %s AND session_id = ANY(%s)"
            params.extend([user_name, session_ids])

        sql = f"""
        SELECT message_id, session_id, ts_rank(content_tsvector, to_tsquery('english', %s)) as score
        FROM message_search
        WHERE content_tsvector @@ to_tsquery('english', %s)
        {scope_sql}
        ORDER BY score DESC LIMIT %s
        """
        try:
            params.append(limit)
            res = await self.client.execute_read(sql, tuple(params))
            return [
                (int(row["message_id"]), float(row["score"]), row["session_id"])
                for row in res
            ]
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
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        clean_query = re.sub(r"[^\w\s.\-']", "", query).strip()
        if not clean_query:
            return []

        # 1. Search Postgres for top entity IDs
        # We can just use ILIKE on canonical_name
        scope_sql = ""
        search_params = [f"%{clean_query}%", limit * 2]
        if visible_project_ids:
            scope_sql = "AND (project_id = ANY(%s) OR entity_id = %s)"
            search_params = [f"%{clean_query}%", visible_project_ids, IDENTITY_ENTITY_ID, limit * 2]

        search_sql = f"""
        SELECT entity_id FROM entity_search
        WHERE canonical_name ILIKE %s
        {scope_sql}
        LIMIT %s
        """
        try:
            id_res = await self.client.execute_read(
                search_sql, tuple(search_params)
            )
            if not id_res:
                return []
            entity_ids = [int(r["entity_id"]) for r in id_res]

            # 2. Fetch basic entity data from Postgres
            entity_sql = """
            SELECT
                e.entity_id as id,
                e.canonical_name,
                e.type,
                e.topic,
                e.last_mentioned_ms as last_mentioned,
                e.last_updated_ms as last_updated,
                COALESCE(
                    (SELECT array_agg(alias) FROM entity_aliases ea WHERE ea.entity_id = e.entity_id),
                    '{}'::text[]
                ) as aliases,
                COALESCE(
                    (SELECT array_agg(content) FROM facts f WHERE f.entity_id = e.entity_id AND f.invalid_at IS NULL),
                    '{}'::text[]
                ) as facts,
                (SELECT canonical_name FROM entities p JOIN hierarchy_edges he ON he.parent_id = p.entity_id WHERE he.child_id = e.entity_id LIMIT 1) as parent_name,
                (SELECT count(*) FROM hierarchy_edges WHERE parent_id = e.entity_id) as children_count
            FROM entities e
            WHERE e.entity_id = ANY(%s)
            """
            if active_topics:
                entity_sql += " AND e.topic = ANY(%s)"
                params = (entity_ids, active_topics)
            else:
                params = (entity_ids,)

            entity_data = await self.client.execute_read(entity_sql, params)

            entities: Dict[int, Any] = {}
            for row in entity_data:
                eid = int(row["id"])
                entities[eid] = {
                    "id": eid,
                    "canonical_name": row["canonical_name"],
                    "aliases": row["aliases"] or [],
                    "type": row["type"],
                    "facts": row["facts"] or [],
                    "topic": row["topic"],
                    "last_mentioned": row["last_mentioned"],
                    "last_updated": row["last_updated"],
                    "hierarchy": {
                        "parent": row["parent_name"],
                        "children_count": row["children_count"],
                    },
                    "top_connections": [],
                    "_conn_names": set(),
                }

            if not entities:
                return []

            # 3. Fetch relationships
            rel_sql = """
            SELECT
                CASE WHEN r.entity_a_id = ANY(%s) THEN r.entity_a_id ELSE r.entity_b_id END as source_id,
                conn.canonical_name as conn_name,
                r.weight as conn_weight,
                r.context as conn_context,
                COALESCE(
                    (SELECT array_agg(alias) FROM entity_aliases ea WHERE ea.entity_id = conn.entity_id),
                    '{}'::text[]
                ) as conn_aliases,
                COALESCE(
                    (SELECT array_agg(content) FROM facts f WHERE f.entity_id = conn.entity_id AND f.invalid_at IS NULL),
                    '{}'::text[]
                ) as conn_facts,
                COALESCE(
                    (
                        SELECT jsonb_agg(
                            jsonb_build_object(
                                'user_name', rer.user_name,
                                'session_id', rer.session_id,
                                'message_id', rer.message_id
                            )
                        )
                        FROM relationship_evidence_refs rer 
                        WHERE rer.relationship_id = r.relationship_id
                    ),
                    '[]'::jsonb
                ) as evidence_refs
            FROM relationships r
            JOIN entities conn ON (
                CASE WHEN r.entity_a_id = ANY(%s) THEN r.entity_b_id ELSE r.entity_a_id END
            ) = conn.entity_id
            WHERE r.entity_a_id = ANY(%s) OR r.entity_b_id = ANY(%s)
            ORDER BY r.weight DESC
            """
            
            valid_ids = list(entities.keys())
            rel_data = await self.client.execute_read(rel_sql, (valid_ids, valid_ids, valid_ids, valid_ids))

            for row in rel_data:
                eid = int(row["source_id"])
                if eid not in entities:
                    continue
                
                ent = entities[eid]
                if len(ent["top_connections"]) >= connections_limit:
                    continue
                    
                c_name = row["conn_name"]
                if c_name not in ent["_conn_names"]:
                    ent["_conn_names"].add(c_name)
                    ent["top_connections"].append(
                        {
                            "canonical_name": c_name,
                            "aliases": row["conn_aliases"] or [],
                            "weight": row["conn_weight"],
                            "evidence_refs": (row["evidence_refs"] or [])[:evidence_limit],
                            "context": row["conn_context"],
                            "facts": row["conn_facts"] or [],
                        }
                    )

            # Cleanup processing keys
            for ent in entities.values():
                ent.pop("_conn_names", None)

            # Sort by last mentioned
            result = list(entities.values())
            result.sort(key=lambda x: x.get("last_mentioned") or 0, reverse=True)
            return result[:limit]

        except Exception as e:
            logger.error(f"Failed search_entity: {e}")
            return []

    async def get_related_entities(
        self,
        entity_names: List[str],
        active_topics: Optional[List[str]] = None,
        limit: int = 25,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        if not entity_names:
            return []

        filter_projects = bool(visible_project_ids)
        visible_project_ids = visible_project_ids or []

        query = """
        SELECT
            source.canonical_name as source,
            target.canonical_name as target,
            COALESCE(
                (SELECT array_agg(content) FROM facts f WHERE f.entity_id = target.entity_id AND f.invalid_at IS NULL),
                '{}'::text[]
            ) as target_facts,
            r.weight as connection_strength,
            COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'user_name', rer.user_name,
                            'session_id', rer.session_id,
                            'message_id', rer.message_id
                        )
                    )
                    FROM relationship_evidence_refs rer 
                    WHERE rer.relationship_id = r.relationship_id
                ),
                '[]'::jsonb
            ) as evidence_refs,
            r.confidence as confidence,
            r.last_seen_ms as last_seen,
            r.context as context
        FROM entities source
        JOIN relationships r ON (r.entity_a_id = source.entity_id OR r.entity_b_id = source.entity_id)
        JOIN entities target ON (
            target.entity_id = (CASE WHEN r.entity_a_id = source.entity_id THEN r.entity_b_id ELSE r.entity_a_id END)
        )
        WHERE source.canonical_name = ANY(%s)
          AND (%s = false OR source.project_id = ANY(%s) OR source.entity_id = %s)
          AND (%s = false OR target.project_id = ANY(%s) OR target.entity_id = %s)
        """
        
        if active_topics is not None:
            query += " AND target.topic = ANY(%s)"
            params = (
                entity_names, filter_projects, visible_project_ids, IDENTITY_ENTITY_ID, 
                filter_projects, visible_project_ids, IDENTITY_ENTITY_ID, active_topics
            )
        else:
            params = (
                entity_names, filter_projects, visible_project_ids, IDENTITY_ENTITY_ID, 
                filter_projects, visible_project_ids, IDENTITY_ENTITY_ID
            )
            
        query += " ORDER BY r.weight DESC, r.last_seen_ms DESC LIMIT %s"
        params = (*params, limit)

        try:
            data = await self.client.execute_read(query, params)
            return [
                {
                    "source": r["source"],
                    "target": r["target"],
                    "target_facts": r["target_facts"] or [],
                    "connection_strength": float(r["connection_strength"] or 1.0),
                    "evidence_refs": r["evidence_refs"] or [],
                    "confidence": float(r["confidence"] or 1.0),
                    "last_seen": r["last_seen"],
                    "context": r["context"],
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
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        if not entity_name or not entity_name.strip():
            return []

        cutoff_ms = get_now_ms() - (hours * 3600 * 1000)
        filter_projects = bool(visible_project_ids)
        visible_project_ids = visible_project_ids or []

        query = """
        SELECT
            target.canonical_name as entity,
            COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'user_name', rer.user_name,
                            'session_id', rer.session_id,
                            'message_id', rer.message_id
                        )
                    )
                    FROM relationship_evidence_refs rer 
                    WHERE rer.relationship_id = r.relationship_id
                ),
                '[]'::jsonb
            ) as evidence_refs,
            r.last_seen_ms as time
        FROM entities source
        JOIN relationships r ON (r.entity_a_id = source.entity_id OR r.entity_b_id = source.entity_id)
        JOIN entities target ON (
            target.entity_id = (CASE WHEN r.entity_a_id = source.entity_id THEN r.entity_b_id ELSE r.entity_a_id END)
        )
        WHERE source.canonical_name = %s
          AND r.last_seen_ms > %s
          AND (%s = false OR source.project_id = ANY(%s) OR source.entity_id = %s)
          AND (%s = false OR target.project_id = ANY(%s) OR target.entity_id = %s)
        """

        if active_topics is not None:
            query += " AND target.topic = ANY(%s)"
            params = (
                entity_name, cutoff_ms,
                filter_projects, visible_project_ids, IDENTITY_ENTITY_ID,
                filter_projects, visible_project_ids, IDENTITY_ENTITY_ID,
                active_topics
            )
        else:
            params = (
                entity_name, cutoff_ms,
                filter_projects, visible_project_ids, IDENTITY_ENTITY_ID,
                filter_projects, visible_project_ids, IDENTITY_ENTITY_ID
            )

        query += " ORDER BY r.last_seen_ms DESC LIMIT 50"

        try:
            data = await self.client.execute_read(query, params)
            return [
                {
                    "entity": r["entity"],
                    "evidence_refs": r["evidence_refs"] or [],
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
        visible_project_ids: Optional[List[str]] = None,
    ) -> Optional[Tuple[List[str], List[str], List[List[str]], bool]]:
        # Using AGE standard variable-length path
        cypher = f"""
        MATCH (start:Entity {{canonical_name: $start_name}})
        MATCH (end:Entity {{canonical_name: $end_name}})
        WHERE ($filter_projects = false OR start.project_id IN $visible_project_ids OR start.id = $identity_entity_id)
          AND ($filter_projects = false OR end.project_id IN $visible_project_ids OR end.id = $identity_entity_id)
        MATCH p = (start)-[rels:RELATED_TO*1..{max_depth}]-(end)
        
        WITH p, nodes(p) as path_nodes, relationships(p) as path_rels
        WHERE ALL(n IN path_nodes WHERE $filter_projects = false OR n.project_id IN $visible_project_ids OR n.id = $identity_entity_id)
        ORDER BY length(p) ASC LIMIT 1
        
        UNWIND path_nodes AS n
        OPTIONAL MATCH (n)-[:BELONGS_TO]->(t:Topic)
        
        WITH p, path_nodes, path_rels, collect(COALESCE(t.name, 'General')) AS node_topics
        WITH p, path_nodes, path_rels, node_topics,
             [node IN path_nodes | node.canonical_name] AS names,
             [r IN path_rels | r.message_ids] AS evidence_refs
             
        WITH names, node_topics, evidence_refs,
             ANY(topic IN node_topics WHERE NOT ($filter_topics = false OR topic IN $active_topics)) as has_inactive
             
        RETURN names, node_topics, evidence_refs, has_inactive
        """
        q = self.client.build_cypher(
            cypher,
            "names agtype, node_topics agtype, evidence_refs agtype, has_inactive agtype",
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
                            **self._scope_params(visible_project_ids),
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
                row["evidence_refs"],
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
        visible_project_ids: Optional[List[str]] = None,
    ) -> Optional[Tuple[List[str], List[str], List[List[str]]]]:
        cypher = f"""
        MATCH (start:Entity {{canonical_name: $start_name}})
        MATCH (end:Entity {{canonical_name: $end_name}})
        WHERE ($filter_projects = false OR start.project_id IN $visible_project_ids OR start.id = $identity_entity_id)
          AND ($filter_projects = false OR end.project_id IN $visible_project_ids OR end.id = $identity_entity_id)
        MATCH p = (start)-[rels:RELATED_TO*1..{max_depth}]-(end)
        
        WITH p, nodes(p) as path_nodes, relationships(p) as path_rels
        WHERE ALL(n IN path_nodes WHERE $filter_projects = false OR n.project_id IN $visible_project_ids OR n.id = $identity_entity_id)
          AND ALL(n IN path_nodes WHERE
            EXISTS {{ MATCH (n)-[:BELONGS_TO]->(t:Topic) WHERE t.name IN $active_topics }} OR
            NOT EXISTS {{ MATCH (n)-[:BELONGS_TO]->(:Topic) }}
        )
        ORDER BY length(p) ASC
        
        WITH p, path_nodes, path_rels LIMIT 1
        
        UNWIND path_nodes AS n
        OPTIONAL MATCH (n)-[:BELONGS_TO]->(t:Topic)
        
        WITH p, collect(COALESCE(t.name, 'General')) AS node_topics, path_nodes, path_rels
        RETURN [n IN path_nodes | n.canonical_name] AS names,
               node_topics,
               [r IN path_rels | r.message_ids] AS evidence_refs
        """
        q = self.client.build_cypher(
            cypher, "names agtype, node_topics agtype, evidence_refs agtype"
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
                            **self._scope_params(visible_project_ids),
                        }
                    ),
                ),
            )
            if not data:
                return None
            row = data[0]
            return (row["names"], row["node_topics"], row["evidence_refs"])
        except Exception as e:
            logger.error(f"Failed _find_active_only_path: {e}")
            return None

    async def find_path_filtered(
        self,
        start_name: str,
        end_name: str,
        active_topics: Optional[List[str]] = None,
        max_depth: int = 4,
        visible_project_ids: Optional[List[str]] = None,
    ) -> Tuple[List[Dict], bool]:
        if (
            not start_name
            or not start_name.strip()
            or not end_name
            or not end_name.strip()
        ):
            return [], False

        shortest = await self._find_shortest_path(
            start_name, end_name, active_topics, max_depth, visible_project_ids
        )
        if not shortest:
            return [], False
        names, topics, evidence, has_inactive = shortest

        if not has_inactive:
            return self._build_path_data(names, topics, evidence), False

        active_path = await self._find_active_only_path(
            start_name, end_name, active_topics, max_depth, visible_project_ids
        )
        if active_path:
            active_names, active_topics_list, active_evidence = active_path
            return self._build_path_data(
                active_names, active_topics_list, active_evidence
            ), True

        return [], True
