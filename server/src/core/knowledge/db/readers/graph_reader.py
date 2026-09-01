import json
from typing import Dict, List, Optional, Tuple

from loguru import logger

from common.exceptions import StorageReadError
from common.scoping import (
    IDENTITY_ENTITY_ID,
    require_scope_value,
    require_visible_project_ids,
)
from infrastructure.postgres_client import PostgresClient

_MAX_PATH_DEPTH = 4


class GraphReader:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name

    @staticmethod
    def _raise_storage_read(operation: str, exc: Exception) -> None:
        logger.error(f"Storage query failed for {operation}: {exc}")
        raise StorageReadError(
            operation,
            details={"error_type": type(exc).__name__},
        ) from exc

    @staticmethod
    def _clean_string(value):
        if isinstance(value, str):
            return value.strip('"')
        return value

    @staticmethod
    def _parse_boolean(value) -> bool:
        """Decode native or string-backed AGE boolean values safely."""
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                return value.strip().strip('"').casefold() == "true"
            if isinstance(value, str):
                return value.strip().strip('"').casefold() == "true"
        return bool(value)

    def _parse_message_row(self, row: Dict) -> Dict:
        return {
            "id": int(row["id"]),
            "user_name": self._clean_string(row["user_name"]),
            "session_id": self._clean_string(row["session_id"]),
            "role": self._clean_string(row["role"]),
            "content": self._clean_string(row["content"]),
            "timestamp": row["timestamp"],
        }

    def _parse_vector(self, val) -> List[float]:
        if val is None:
            return []
        if hasattr(val, "tolist"):
            return [float(x) for x in val.tolist()]
        if isinstance(val, str):
            raw = val.strip()
            if not raw:
                return []
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                parsed = raw.strip("[]").split(",")
            return [float(x) for x in parsed if str(x).strip()]
        return [float(x) for x in val]

    @staticmethod
    def _validate_path_depth(max_depth: int, operation: str) -> int:
        if (
            not isinstance(max_depth, int)
            or isinstance(max_depth, bool)
            or not 1 <= max_depth <= _MAX_PATH_DEPTH
        ):
            raise ValueError(
                f"{operation}: max_depth must be an integer between 1 and "
                f"{_MAX_PATH_DEPTH}"
            )
        return max_depth

    @staticmethod
    def _build_path_data(
        names: List[str],
        topics: List[str],
        evidence: List[List[Dict]],
    ) -> List[Dict]:
        return [
            {
                "step": index,
                "entity_a": names[index],
                "entity_b": names[index + 1],
                "topic_a": topics[index] if index < len(topics) else None,
                "topic_b": topics[index + 1] if index + 1 < len(topics) else None,
                "evidence_refs": evidence[index] if index < len(evidence) else [],
            }
            for index in range(len(evidence))
        ]

    async def _relationship_observation_refs(
        self,
        relationship_ids: List[str],
        visible_project_ids: List[str],
    ) -> List[List[Dict]]:
        if not relationship_ids:
            return []
        rows = await self.client.fetch_all(
            """
            SELECT
                relationship_id,
                json_agg(
                    json_build_object(
                        'project_id', project_id,
                        'user_name', user_name,
                        'session_id', session_id,
                        'message_id', message_id
                    )
                    ORDER BY observed_at_ms, observation_id
                ) AS evidence_refs
            FROM relationship_observations
            WHERE relationship_id = ANY(%s)
              AND project_id = ANY(%s)
            GROUP BY relationship_id
            """,
            (relationship_ids, visible_project_ids),
        )
        by_relationship = {
            row["relationship_id"]: row.get("evidence_refs") or []
            for row in rows
        }
        return [
            by_relationship.get(relationship_id, [])
            for relationship_id in relationship_ids
        ]

    def _path_scope_params(self, visible_project_ids: List[str]) -> Dict:
        return {
            "filter_projects": True,
            "visible_project_ids": visible_project_ids,
            "identity_entity_id": IDENTITY_ENTITY_ID,
        }

    async def _find_shortest_path(
        self,
        start_name: str,
        end_name: str,
        *,
        visible_project_ids: List[str],
        active_topics: Optional[List[str]] = None,
        max_depth: int = 4,
    ) -> Optional[Tuple[List[str], List[str], List[List[Dict]], bool]]:
        max_depth = self._validate_path_depth(max_depth, "_find_shortest_path")
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "_find_shortest_path",
        )
        cypher = f"""
        MATCH (start:Entity {{canonical_name: $start_name}})
        MATCH (end:Entity {{canonical_name: $end_name}})
        MATCH p = (start)-[rels:RELATED_TO*1..{max_depth}]-(end)
        WITH p, nodes(p) AS path_nodes, relationships(p) AS path_rels
        WHERE ALL(relationship IN path_rels WHERE relationship.project_id IN $visible_project_ids)
        ORDER BY length(p) ASC LIMIT 1
        WITH path_nodes, path_rels,
             [node IN path_nodes | 'General'] AS node_topics,
             [node IN path_nodes | node.canonical_name] AS names,
             [relationship IN path_rels | relationship.relationship_id] AS relationship_ids
        RETURN names, node_topics, relationship_ids,
               ANY(topic IN node_topics WHERE NOT ($filter_topics = false OR topic IN $active_topics)) AS has_inactive
        """
        query = self.client.build_cypher(
            cypher,
            "names agtype, node_topics agtype, relationship_ids agtype, has_inactive agtype",
        )
        try:
            rows = await self.client.fetch_all(
                query,
                (
                    json.dumps(
                        {
                            "start_name": start_name,
                            "end_name": end_name,
                            "filter_topics": active_topics is not None,
                            "active_topics": active_topics or [],
                            **self._path_scope_params(visible_project_ids),
                        }
                    ),
                ),
            )
            if not rows:
                return None
            row = rows[0]
            return (
                row["names"],
                row["node_topics"],
                await self._relationship_observation_refs(
                    row["relationship_ids"],
                    visible_project_ids,
                ),
                self._parse_boolean(row["has_inactive"]),
            )
        except Exception as exc:
            self._raise_storage_read("find_shortest_path", exc)

    async def _find_active_only_path(
        self,
        start_name: str,
        end_name: str,
        *,
        visible_project_ids: List[str],
        active_topics: Optional[List[str]] = None,
        max_depth: int = 4,
    ) -> Optional[Tuple[List[str], List[str], List[List[Dict]]]]:
        max_depth = self._validate_path_depth(max_depth, "_find_active_only_path")
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "_find_active_only_path",
        )
        cypher = f"""
        MATCH (start:Entity {{canonical_name: $start_name}})
        MATCH (end:Entity {{canonical_name: $end_name}})
        MATCH p = (start)-[rels:RELATED_TO*1..{max_depth}]-(end)
        WITH p, nodes(p) AS path_nodes, relationships(p) AS path_rels
        WHERE ALL(relationship IN path_rels WHERE relationship.project_id IN $visible_project_ids)
        ORDER BY length(p) ASC LIMIT 1
        RETURN [node IN path_nodes | node.canonical_name] AS names,
               [node IN path_nodes | 'General'] AS node_topics,
               [relationship IN path_rels | relationship.relationship_id] AS relationship_ids
        """
        query = self.client.build_cypher(
            cypher,
            "names agtype, node_topics agtype, relationship_ids agtype",
        )
        try:
            rows = await self.client.fetch_all(
                query,
                (
                    json.dumps(
                        {
                            "start_name": start_name,
                            "end_name": end_name,
                            "active_topics": active_topics or [],
                            **self._path_scope_params(visible_project_ids),
                        }
                    ),
                ),
            )
            if not rows:
                return None
            row = rows[0]
            return (
                row["names"],
                row["node_topics"],
                await self._relationship_observation_refs(
                    row["relationship_ids"],
                    visible_project_ids,
                ),
            )
        except Exception as exc:
            self._raise_storage_read("find_active_only_path", exc)

    async def find_path_filtered(
        self,
        start_name: str,
        end_name: str,
        *,
        visible_project_ids: List[str],
        active_topics: Optional[List[str]] = None,
        max_depth: int = 4,
    ) -> Tuple[List[Dict], bool]:
        max_depth = self._validate_path_depth(max_depth, "find_path_filtered")
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "find_path_filtered",
        )
        if not start_name or not start_name.strip() or not end_name or not end_name.strip():
            return [], False
        shortest = await self._find_shortest_path(
            start_name,
            end_name,
            visible_project_ids=visible_project_ids,
            active_topics=active_topics,
            max_depth=max_depth,
        )
        if not shortest:
            return [], False
        names, topics, evidence, has_inactive = shortest
        if not has_inactive:
            return self._build_path_data(names, topics, evidence), False
        active_path = await self._find_active_only_path(
            start_name,
            end_name,
            visible_project_ids=visible_project_ids,
            active_topics=active_topics,
            max_depth=max_depth,
        )
        if active_path:
            active_names, active_topics_list, active_evidence = active_path
            return self._build_path_data(
                active_names,
                active_topics_list,
                active_evidence,
            ), True
        return [], True

    async def get_message_text(
        self,
        message_id: int,
        *,
        user_name: str,
        session_id: str,
        visible_project_ids: List[str],
    ) -> str:
        user_name = require_scope_value(user_name, "user_name", "get_message_text")
        session_id = require_scope_value(session_id, "session_id", "get_message_text")
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_message_text",
        )

        query = """
        SELECT content
        FROM messages
        WHERE user_name = %s
          AND session_id = %s
          AND message_id = %s
          AND project_id = ANY(%s)
        """
        try:
            row = await self.client.fetch_one(
                query,
                (user_name, session_id, message_id, visible_project_ids),
            )
            if not row:
                return ""
            content = row["content"]
            return self._clean_string(content)
        except Exception as e:
            logger.error(f"Failed to get message text for {message_id}: {e}")
            self._raise_storage_read("get_message_text", e)

    async def get_messages_by_ids(
        self,
        ids: List[int],
        *,
        user_name: str,
        session_ids: List[str],
        visible_project_ids: List[str],
        discoverable_only: bool = False,
    ) -> List[Dict]:
        user_name = require_scope_value(
            user_name,
            "user_name",
            "get_messages_by_ids",
        )
        if not session_ids:
            raise ValueError("get_messages_by_ids requires session_ids scope")
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_messages_by_ids",
        )
        if not ids:
            return []

        params = {"ids": ids, "user_name": user_name, "session_ids": session_ids}

        discovery_clause = (
            """
          AND lifecycle_state = 'sealed'
          AND EXISTS (
              SELECT 1
              FROM sessions
              WHERE sessions.session_id = messages.session_id
                AND sessions.project_id = messages.project_id
                AND sessions.user_name = messages.user_name
                AND sessions.status = 'open'
          )
            """
            if discoverable_only
            else ""
        )
        query = f"""
        SELECT
            message_id AS id,
            user_name,
            session_id,
            role,
            content,
            timestamp_ms AS timestamp
        FROM messages
        WHERE message_id = ANY(%s)
          AND user_name = %s
          AND session_id = ANY(%s)
          AND project_id = ANY(%s)
          {discovery_clause}
        ORDER BY message_id ASC
        """
        try:
            res = await self.client.fetch_all(
                query,
                (
                    params["ids"],
                    params["user_name"],
                    params["session_ids"],
                    visible_project_ids,
                ),
            )
            return [self._parse_message_row(row) for row in res]
        except Exception as e:
            logger.error(f"Failed to fetch messages by ids: {e}")
            self._raise_storage_read("get_messages_by_ids", e)

    async def get_recent_project_messages(
        self,
        user_name: str,
        project_id: str,
        limit: int,
        before_message_id: Optional[int] = None,
    ) -> List[Dict]:
        user_name = require_scope_value(
            user_name,
            "user_name",
            "get_recent_project_messages",
        )
        project_id = require_scope_value(
            project_id,
            "project_id",
            "get_recent_project_messages",
        )
        if limit <= 0:
            return []

        params = {
            "user_name": user_name,
            "project_id": project_id,
            "limit": limit,
            "before_message_id": before_message_id,
        }
        before_clause = (
            "AND message_id < %s"
            if before_message_id is not None
            else ""
        )
        query = f"""
        SELECT
            message_id AS id,
            user_name,
            session_id,
            role,
            content,
            timestamp_ms AS timestamp
        FROM messages
        WHERE user_name = %s
        AND project_id = %s
        {before_clause}
        ORDER BY message_id DESC
        LIMIT %s
        """
        query_params = (
            (params["user_name"], params["project_id"], params["before_message_id"])
            if before_message_id is not None
            else (params["user_name"], params["project_id"])
        )
        query_params = (*query_params, params["limit"])
        try:
            rows = await self.client.fetch_all(query, query_params)
            return [self._parse_message_row(row) for row in reversed(rows)]
        except Exception as e:
            logger.error(f"Failed to fetch recent project messages: {e}")
            self._raise_storage_read("get_recent_project_messages", e)

    async def get_surrounding_messages(
        self,
        message_id: int,
        *,
        user_name: str,
        session_id: str,
        visible_project_ids: List[str],
        forward: int = 3,
        target_total: int = 10,
        discoverable_only: bool = False,
    ) -> List[Dict]:
        user_name = require_scope_value(
            user_name,
            "user_name",
            "get_surrounding_messages",
        )
        session_id = require_scope_value(
            session_id,
            "session_id",
            "get_surrounding_messages",
        )
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_surrounding_messages",
        )

        back_limit = max(0, target_total - forward - 1)
        session_ids = [session_id]
        params_base = (user_name, session_id)

        try:
            target_res = await self.get_messages_by_ids(
                [message_id],
                user_name=user_name,
                session_ids=session_ids,
                visible_project_ids=visible_project_ids,
                discoverable_only=discoverable_only,
            )
            if not target_res:
                return []
            target = target_res[0]
            target_ts = target["timestamp"]

            discovery_clause = (
                """
              AND lifecycle_state = 'sealed'
              AND EXISTS (
                  SELECT 1
                  FROM sessions
                  WHERE sessions.session_id = messages.session_id
                    AND sessions.project_id = messages.project_id
                    AND sessions.user_name = messages.user_name
                    AND sessions.status = 'open'
              )
                """
                if discoverable_only
                else ""
            )

            back_query = f"""
            SELECT
                message_id AS id,
                user_name,
                session_id,
                role,
                content,
                timestamp_ms AS timestamp
            FROM messages
            WHERE (
                    timestamp_ms < %s
                 OR (timestamp_ms = %s AND message_id < %s)
                 OR (%s::BIGINT IS NULL AND timestamp_ms IS NOT NULL)
                 OR (
                        %s::BIGINT IS NULL
                    AND timestamp_ms IS NULL
                    AND message_id < %s
                 )
              )
              AND user_name = %s
              AND session_id = %s
              AND project_id = ANY(%s)
              {discovery_clause}
            ORDER BY timestamp_ms DESC NULLS FIRST, message_id DESC
            LIMIT %s
            """

            fwd_query = f"""
            SELECT
                message_id AS id,
                user_name,
                session_id,
                role,
                content,
                timestamp_ms AS timestamp
            FROM messages
            WHERE (
                    timestamp_ms > %s
                 OR (timestamp_ms = %s AND message_id > %s)
                 OR (%s::BIGINT IS NOT NULL AND timestamp_ms IS NULL)
                 OR (
                        %s::BIGINT IS NULL
                    AND timestamp_ms IS NULL
                    AND message_id > %s
                 )
              )
              AND user_name = %s
              AND session_id = %s
              AND project_id = ANY(%s)
              {discovery_clause}
            ORDER BY timestamp_ms ASC NULLS LAST, message_id ASC
            LIMIT %s
            """

            back_data = await self.client.fetch_all(
                back_query,
                (
                    target_ts,
                    target_ts,
                    message_id,
                    target_ts,
                    target_ts,
                    message_id,
                    *params_base,
                    visible_project_ids,
                    back_limit,
                ),
            )
            fwd_data = await self.client.fetch_all(
                fwd_query,
                (
                    target_ts,
                    target_ts,
                    message_id,
                    target_ts,
                    target_ts,
                    message_id,
                    *params_base,
                    visible_project_ids,
                    forward,
                ),
            )

            prev_msgs = [self._parse_message_row(r) for r in reversed(back_data)]
            next_msgs = [self._parse_message_row(r) for r in fwd_data]

            return prev_msgs + [target] + next_msgs
        except Exception as e:
            logger.error(f"Failed to fetch surrounding messages for {message_id}: {e}")
            self._raise_storage_read("get_surrounding_messages", e)

    async def get_neighbor_ids(
        self, entity_id: int, *, visible_project_ids: List[str]
    ) -> set[int]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_neighbor_ids",
        )
        cypher = """
        MATCH (e:Entity {id: $entity_id})-[r:RELATED_TO]-(neighbor:Entity)
        WHERE r.project_id IN $visible_project_ids
        RETURN neighbor.id
        """
        query = self.client.build_cypher(cypher, "neighbor_id agtype")
        try:
            res = await self.client.fetch_all(
                query,
                (
                    json.dumps(
                        {
                            "entity_id": entity_id,
                            "visible_project_ids": visible_project_ids,
                            "identity_entity_id": IDENTITY_ENTITY_ID,
                        }
                    ),
                ),
            )
            return {int(row["neighbor_id"]) for row in res}
        except Exception as e:
            logger.error(f"Failed to get neighbor IDs for {entity_id}: {e}")
            self._raise_storage_read("get_neighbor_ids", e)

    async def get_neighbor_entities(
        self,
        entity_id: int,
        *,
        visible_project_ids: List[str],
        limit: int = 5,
    ) -> List[Dict]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_neighbor_entities",
        )
        cypher = """
        MATCH (e:Entity {id: $entity_id})-[r:RELATED_TO]-(neighbor:Entity)
        WHERE r.project_id IN $visible_project_ids
        RETURN DISTINCT neighbor.id, neighbor.canonical_name
        ORDER BY neighbor.canonical_name
        LIMIT $limit
        """
        query = self.client.build_cypher(cypher, "id agtype, name agtype")
        try:
            res = await self.client.fetch_all(
                query,
                (
                    json.dumps(
                        {
                            "entity_id": entity_id,
                            "limit": limit,
                            "visible_project_ids": visible_project_ids,
                            "identity_entity_id": IDENTITY_ENTITY_ID,
                        }
                    ),
                ),
            )
            return [{"id": int(r["id"]), "name": r["name"]} for r in res]
        except Exception as e:
            logger.error(f"Failed to get neighbor entities for {entity_id}: {e}")
            self._raise_storage_read("get_neighbor_entities", e)

    async def has_direct_edge(
        self, id_a: int, id_b: int, *, visible_project_ids: List[str]
    ) -> bool:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "has_direct_edge",
        )
        cypher = """
        MATCH (a:Entity {id: $id_a})-[r:RELATED_TO]-(b:Entity {id: $id_b})
        WHERE r.project_id IN $visible_project_ids
        RETURN count(r) > 0 as connected
        """
        query = self.client.build_cypher(cypher, "connected agtype")
        try:
            row = await self.client.fetch_one(
                query,
                (
                    json.dumps(
                        {
                            "id_a": id_a,
                            "id_b": id_b,
                            "visible_project_ids": visible_project_ids,
                            "identity_entity_id": IDENTITY_ENTITY_ID,
                        }
                    ),
                ),
            )
            return self._parse_boolean(row["connected"]) if row else False
        except Exception as e:
            logger.error(f"Failed to check direct edge between {id_a} and {id_b}: {e}")
            self._raise_storage_read("has_direct_edge", e)

    async def get_merge_topic_strength(
        self,
        primary_id: int,
        secondary_id: int,
        project_id: str,
    ) -> Dict:
        project_id = require_scope_value(
            project_id,
            "project_id",
            "get_merge_topic_strength",
        )

        query = """
        SELECT
            p_context.topic AS p_topic,
            p_context.last_mentioned_ms AS p_last,
            s_context.topic AS s_topic,
            s_context.last_mentioned_ms AS s_last,
            (
                SELECT count(*)
                FROM episode_entities episode_entity
                JOIN episodes episode ON episode.episode_id = episode_entity.episode_id
                WHERE episode.project_id = %s
                  AND episode_entity.entity_id = %s
            ) AS p_episode_count,
            (
                SELECT count(*)
                FROM episode_entities episode_entity
                JOIN episodes episode ON episode.episode_id = episode_entity.episode_id
                WHERE episode.project_id = %s
                  AND episode_entity.entity_id = %s
            ) AS s_episode_count,
            (
                SELECT count(*)
                FROM relationships
                WHERE project_id = %s
                  AND (
                      entity_a_id = %s
                      OR entity_b_id = %s
                  )
                  AND NOT (
                      (
                          entity_a_id = %s
                          AND entity_b_id = %s
                      )
                      OR (
                          entity_a_id = %s
                          AND entity_b_id = %s
                      )
                  )
            ) AS p_relationship_count,
            (
                SELECT count(*)
                FROM relationships
                WHERE project_id = %s
                  AND (
                      entity_a_id = %s
                      OR entity_b_id = %s
                  )
                  AND NOT (
                      (
                          entity_a_id = %s
                          AND entity_b_id = %s
                      )
                      OR (
                          entity_a_id = %s
                          AND entity_b_id = %s
                      )
                  )
            ) AS s_relationship_count
        FROM entities p
        JOIN project_entity_contexts p_context
          ON p_context.entity_id = p.entity_id
         AND p_context.project_id = %s
        JOIN entities s
          ON s.entity_id = %s
        JOIN project_entity_contexts s_context
          ON s_context.entity_id = s.entity_id
         AND s_context.project_id = %s
        WHERE p.entity_id = %s
        """
        try:
            row = await self.client.fetch_one(
                query,
                (
                    project_id,
                    primary_id,
                    project_id,
                    secondary_id,
                    project_id,
                    primary_id,
                    primary_id,
                    primary_id,
                    secondary_id,
                    secondary_id,
                    primary_id,
                    project_id,
                    secondary_id,
                    secondary_id,
                    primary_id,
                    secondary_id,
                    secondary_id,
                    primary_id,
                    project_id,
                    secondary_id,
                    project_id,
                    primary_id,
                ),
            )
            return dict(row) if row else {}
        except Exception as e:
            logger.error(
                "Failed to get merge topic strength for "
                f"{primary_id}<-{secondary_id}: {e}"
            )
            self._raise_storage_read("get_merge_topic_strength", e)

    async def get_graph_stats(
        self, *, visible_project_ids: List[str]
    ) -> Dict[str, int]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_graph_stats",
        )
        query = """
        SELECT
            (
                SELECT count(*)
                FROM project_entity_contexts
                WHERE project_id = ANY(%s)
            ) AS entities,
            (
                SELECT count(*)
                FROM episodes
                WHERE project_id = ANY(%s)
            ) AS episodes,
            (
                SELECT count(*)
                FROM relationships
                WHERE project_id = ANY(%s)
            ) AS relationships
        """
        try:
            row = await self.client.fetch_one(
                query,
                (
                    visible_project_ids,
                    visible_project_ids,
                    visible_project_ids,
                ),
            )
            if not row:
                return {"entities": 0, "episodes": 0, "relationships": 0}
            return {
                "entities": int(row["entities"] or 0),
                "episodes": int(row["episodes"] or 0),
                "relationships": int(row["relationships"] or 0),
            }
        except Exception as e:
            logger.error(f"Failed to get graph stats: {e}")
            self._raise_storage_read("get_graph_stats", e)

    async def get_neighbor_ids_batch(
        self,
        entity_ids: List[int],
        *,
        visible_project_ids: List[str],
    ) -> Dict[int, set[int]]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_neighbor_ids_batch",
        )
        if not entity_ids:
            return {}
        cypher = """
        MATCH (e:Entity)-[r:RELATED_TO]-(neighbor:Entity)
        WHERE e.id IN $ids
          AND r.project_id IN $visible_project_ids
        RETURN e.id as entity_id, collect(DISTINCT neighbor.id) as neighbor_ids
        """
        query = self.client.build_cypher(
            cypher, "entity_id agtype, neighbor_ids agtype"
        )
        try:
            res = await self.client.fetch_all(
                query,
                (
                    json.dumps(
                        {
                            "ids": entity_ids,
                            "visible_project_ids": visible_project_ids,
                            "identity_entity_id": IDENTITY_ENTITY_ID,
                        }
                    ),
                ),
            )
            result_map = {eid: set() for eid in entity_ids}
            for row in res:
                if row["neighbor_ids"]:
                    result_map[int(row["entity_id"])] = {
                        int(x) for x in row["neighbor_ids"]
                    }
            return result_map
        except Exception as e:
            logger.error(f"Failed to batch fetch neighbor IDs: {e}")
            self._raise_storage_read("get_neighbor_ids_batch", e)
