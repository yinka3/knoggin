import json
from typing import Dict, List, Optional

from loguru import logger

from common.exceptions import StorageUnavailableError
from common.scoping import (
    IDENTITY_ENTITY_ID,
    require_scope_value,
    require_visible_project_ids,
)
from infrastructure.postgres_client import PostgresClient


class GraphReader:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name

    @staticmethod
    def _raise_storage_unavailable(operation: str, exc: Exception) -> None:
        logger.error(f"Storage query failed for {operation}: {exc}")
        raise StorageUnavailableError(
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
            self._raise_storage_unavailable("get_message_text", e)

    async def get_messages_by_ids(
        self,
        ids: List[int],
        *,
        user_name: str,
        session_ids: List[str],
        visible_project_ids: List[str],
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

        query = """
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
            self._raise_storage_unavailable("get_messages_by_ids", e)

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
            self._raise_storage_unavailable("get_recent_project_messages", e)

    async def get_surrounding_messages(
        self,
        message_id: int,
        *,
        user_name: str,
        session_id: str,
        visible_project_ids: List[str],
        forward: int = 3,
        target_total: int = 10,
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
            )
            if not target_res:
                return []
            target = target_res[0]
            target_ts = target["timestamp"]

            back_query = """
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
            ORDER BY timestamp_ms DESC NULLS FIRST, message_id DESC
            LIMIT %s
            """

            fwd_query = """
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
            self._raise_storage_unavailable("get_surrounding_messages", e)

    async def get_neighbor_ids(
        self, entity_id: int, *, visible_project_ids: List[str]
    ) -> set[int]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_neighbor_ids",
        )
        cypher = """
        MATCH (e:Entity {id: $entity_id})-[r:RELATED_TO]-(neighbor:Entity)
        WHERE (e.project_id IN $visible_project_ids OR e.id = $identity_entity_id)
          AND (neighbor.project_id IN $visible_project_ids OR neighbor.id = $identity_entity_id)
          AND r.project_id IN $visible_project_ids
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
            self._raise_storage_unavailable("get_neighbor_ids", e)

    async def get_parent_entities(
        self, entity_id: int, *, visible_project_ids: List[str]
    ) -> List[Dict]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_parent_entities",
        )
        query = """
        SELECT
            parent.entity_id AS id,
            parent.canonical_name,
            parent.type
        FROM hierarchy_edges edge
        JOIN entities parent ON parent.entity_id = edge.parent_id
        WHERE edge.child_id = %s
          AND edge.project_id = ANY(%s)
          AND (parent.project_id = ANY(%s) OR parent.entity_id = %s)
        GROUP BY parent.entity_id, parent.canonical_name, parent.type
        ORDER BY parent.canonical_name
        """
        try:
            res = await self.client.fetch_all(
                query,
                (
                    entity_id,
                    visible_project_ids,
                    visible_project_ids,
                    IDENTITY_ENTITY_ID,
                ),
            )
            return [
                {
                    "id": int(r["id"]),
                    "canonical_name": self._clean_string(r["canonical_name"]),
                    "type": self._clean_string(r["type"]),
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get parents for entity {entity_id}: {e}")
            self._raise_storage_unavailable("get_parent_entities", e)

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
        WHERE (e.project_id IN $visible_project_ids OR e.id = $identity_entity_id)
          AND (neighbor.project_id IN $visible_project_ids OR neighbor.id = $identity_entity_id)
          AND r.project_id IN $visible_project_ids
        RETURN DISTINCT neighbor.id, neighbor.canonical_name
        ORDER BY neighbor.last_mentioned DESC
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
            self._raise_storage_unavailable("get_neighbor_entities", e)

    async def get_child_entities(
        self, entity_id: int, *, visible_project_ids: List[str]
    ) -> List[Dict]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_child_entities",
        )
        query = """
        SELECT
            child.entity_id AS id,
            child.canonical_name,
            child.type
        FROM hierarchy_edges edge
        JOIN entities child ON child.entity_id = edge.child_id
        WHERE edge.parent_id = %s
          AND edge.project_id = ANY(%s)
          AND (child.project_id = ANY(%s) OR child.entity_id = %s)
        GROUP BY child.entity_id, child.canonical_name, child.type
        ORDER BY child.canonical_name
        """
        try:
            res = await self.client.fetch_all(
                query,
                (
                    entity_id,
                    visible_project_ids,
                    visible_project_ids,
                    IDENTITY_ENTITY_ID,
                ),
            )
            return [
                {
                    "id": int(r["id"]),
                    "canonical_name": self._clean_string(r["canonical_name"]),
                    "type": self._clean_string(r["type"]),
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get children for entity {entity_id}: {e}")
            self._raise_storage_unavailable("get_child_entities", e)

    async def has_direct_edge(
        self, id_a: int, id_b: int, *, visible_project_ids: List[str]
    ) -> bool:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "has_direct_edge",
        )
        cypher = """
        MATCH (a:Entity {id: $id_a})-[r:RELATED_TO]-(b:Entity {id: $id_b})
        WHERE (a.project_id IN $visible_project_ids OR a.id = $identity_entity_id)
          AND (b.project_id IN $visible_project_ids OR b.id = $identity_entity_id)
          AND r.project_id IN $visible_project_ids
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
            self._raise_storage_unavailable("has_direct_edge", e)

    async def has_hierarchy_edge(
        self, id_a: int, id_b: int, *, visible_project_ids: List[str]
    ) -> bool:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "has_hierarchy_edge",
        )
        query = """
        SELECT EXISTS (
            SELECT 1
            FROM hierarchy_edges
            WHERE project_id = ANY(%s)
              AND (
                  (parent_id = %s AND child_id = %s)
                  OR (parent_id = %s AND child_id = %s)
              )
        ) AS exists
        """
        try:
            row = await self.client.fetch_one(
                query,
                (visible_project_ids, id_a, id_b, id_b, id_a),
            )
            return bool(row["exists"]) if row else False
        except Exception as e:
            logger.error(
                f"Failed to check hierarchy edge between {id_a} and {id_b}: {e}"
            )
            self._raise_storage_unavailable("has_hierarchy_edge", e)

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
            p.topic AS p_topic,
            p.confidence AS p_conf,
            p.last_mentioned_ms AS p_last,
            s.topic AS s_topic,
            s.confidence AS s_conf,
            s.last_mentioned_ms AS s_last,
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
        JOIN entities s
          ON s.entity_id = %s
         AND s.project_id = %s
        WHERE p.entity_id = %s
          AND p.project_id = %s
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
                    secondary_id,
                    project_id,
                    primary_id,
                    project_id,
                ),
            )
            return dict(row) if row else {}
        except Exception as e:
            logger.error(
                "Failed to get merge topic strength for "
                f"{primary_id}<-{secondary_id}: {e}"
            )
            self._raise_storage_unavailable("get_merge_topic_strength", e)

    async def get_hierarchy_candidates(
        self,
        project_id: str,
        topic: str,
        parent_type: str,
        child_types: List[str],
        min_weight: int = 2,
    ) -> List[Dict]:
        project_id = require_scope_value(
            project_id,
            "project_id",
            "get_hierarchy_candidates",
        )

        query = """
        SELECT
            parent.entity_id AS parent_id,
            parent.canonical_name AS parent_name,
            parent.type AS parent_type,
            child.entity_id AS child_id,
            child.canonical_name AS child_name,
            child.type AS child_type,
            rel.weight
        FROM relationships rel
        JOIN entities parent
          ON parent.entity_id IN (rel.entity_a_id, rel.entity_b_id)
         AND parent.project_id = rel.project_id
        JOIN entities child
          ON child.entity_id = CASE
              WHEN parent.entity_id = rel.entity_a_id THEN rel.entity_b_id
              ELSE rel.entity_a_id
           END
         AND child.project_id = rel.project_id
        WHERE rel.project_id = %s
          AND parent.topic = %s
          AND child.topic = %s
          AND parent.type = %s
          AND child.type = ANY(%s)
          AND rel.weight >= %s
          AND NOT EXISTS (
              SELECT 1
              FROM hierarchy_edges edge
              WHERE edge.project_id = rel.project_id
                AND edge.parent_id = parent.entity_id
                AND edge.child_id = child.entity_id
          )
        ORDER BY rel.weight DESC, parent.canonical_name, child.canonical_name
        """
        try:
            graph_res = await self.client.fetch_all(
                query,
                (
                    project_id,
                    topic,
                    topic,
                    parent_type,
                    child_types,
                    min_weight,
                ),
            )

            if not graph_res:
                return []

            # 2. Fetch embeddings from relational table for those candidates
            entity_ids = list(
                {int(r["parent_id"]) for r in graph_res}
                | {int(r["child_id"]) for r in graph_res}
            )
            emb_res = await self.client.fetch_all(
                """
                SELECT entity_id, embedding
                FROM entity_search
                WHERE entity_id = ANY(%s)
                  AND project_id = %s
                """,
                (entity_ids, project_id),
            )
            embs = {
                r["entity_id"]: self._parse_vector(r["embedding"])
                for r in emb_res
            }

            return [
                {
                    "parent_id": int(r["parent_id"]),
                    "parent_name": self._clean_string(r["parent_name"]),
                    "parent_type": self._clean_string(r["parent_type"]),
                    "parent_embedding": embs.get(int(r["parent_id"]), []),
                    "child_id": int(r["child_id"]),
                    "child_name": self._clean_string(r["child_name"]),
                    "child_type": self._clean_string(r["child_type"]),
                    "child_embedding": embs.get(int(r["child_id"]), []),
                    "weight": r["weight"],
                }
                for r in graph_res
            ]

        except Exception as e:
            logger.error(f"Hierarchy candidate query failed: {e}")
            self._raise_storage_unavailable("get_hierarchy_candidates", e)

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
                FROM entities
                WHERE project_id = ANY(%s) OR entity_id = %s
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
                    IDENTITY_ENTITY_ID,
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
            self._raise_storage_unavailable("get_graph_stats", e)

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
          AND (e.project_id IN $visible_project_ids OR e.id = $identity_entity_id)
          AND (neighbor.project_id IN $visible_project_ids OR neighbor.id = $identity_entity_id)
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
            self._raise_storage_unavailable("get_neighbor_ids_batch", e)
