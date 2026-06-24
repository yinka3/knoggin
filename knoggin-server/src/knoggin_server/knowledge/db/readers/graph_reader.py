import json
from typing import Dict, List, Optional

from loguru import logger

from infrastructure.postgres_client import PostgresClient


class GraphReader:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name

    @staticmethod
    def _clean_string(value):
        if isinstance(value, str):
            return value.strip('"')
        return value

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
        self, message_id: int, user_name: str, session_id: str
    ) -> str:
        if not user_name or not session_id:
            logger.warning(
                "Refusing unsafe message text lookup without user/session scope"
            )
            return ""

        query = """
        SELECT content
        FROM messages
        WHERE user_name = %s
          AND session_id = %s
          AND message_id = %s
        """
        try:
            res = await self.client.execute_read(
                query,
                (user_name, session_id, message_id),
            )
            if not res:
                return ""
            content = res[0]["content"]
            return self._clean_string(content)
        except Exception as e:
            logger.error(f"Failed to get message text for {message_id}: {e}")
            return ""

    async def get_messages_by_ids(
        self,
        ids: List[int],
        user_name: Optional[str] = None,
        session_ids: Optional[List[str]] = None,
    ) -> List[Dict]:
        if not ids:
            return []
        if not user_name or not session_ids:
            logger.warning("Refusing unsafe message lookup without user/session scope")
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
        ORDER BY message_id ASC
        """
        try:
            res = await self.client.execute_read(
                query,
                (params["ids"], params["user_name"], params["session_ids"]),
            )
            return [self._parse_message_row(row) for row in res]
        except Exception as e:
            logger.error(f"Failed to fetch messages by ids: {e}")
            return []

    async def get_recent_project_messages(
        self,
        user_name: str,
        project_id: str,
        limit: int,
        before_message_id: Optional[int] = None,
    ) -> List[Dict]:
        if not user_name or not project_id:
            logger.warning(
                "Refusing unsafe project message lookup without user/project scope"
            )
            return []
        if limit <= 0:
            return []

        params = {
            "user_name": user_name,
            "project_id": project_id,
            "limit": limit,
            "before_message_id": before_message_id,
        }
        before_clause = (
            "AND message_id <= %s"
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
            rows = await self.client.execute_read(query, query_params)
            return [self._parse_message_row(row) for row in reversed(rows)]
        except Exception as e:
            logger.error(f"Failed to fetch recent project messages: {e}")
            return []

    async def get_surrounding_messages(
        self,
        message_id: int,
        forward: int = 3,
        target_total: int = 10,
        user_name: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> List[Dict]:
        if not user_name or not session_id:
            logger.warning(
                "Refusing unsafe surrounding-message lookup without user/session scope"
            )
            return []

        back_limit = max(0, target_total - forward - 1)
        session_ids = [session_id]
        params_base = (user_name, session_id)

        try:
            target_res = await self.get_messages_by_ids(
                [message_id], user_name=user_name, session_ids=session_ids
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
            WHERE timestamp_ms <= %s
              AND message_id <> %s
              AND user_name = %s
              AND session_id = %s
            ORDER BY timestamp_ms DESC
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
            WHERE timestamp_ms >= %s
              AND message_id <> %s
              AND user_name = %s
              AND session_id = %s
            ORDER BY timestamp_ms ASC
            LIMIT %s
            """

            back_data = await self.client.execute_read(
                back_query,
                (target_ts, message_id, *params_base, back_limit),
            )
            fwd_data = await self.client.execute_read(
                fwd_query,
                (target_ts, message_id, *params_base, forward),
            )

            prev_msgs = [self._parse_message_row(r) for r in reversed(back_data)]
            next_msgs = [self._parse_message_row(r) for r in fwd_data]

            return prev_msgs + [target] + next_msgs
        except Exception as e:
            logger.error(f"Failed to fetch surrounding messages for {message_id}: {e}")
            return []

    async def get_neighbor_ids(self, entity_id: int) -> set[int]:
        cypher = """
        MATCH (e:Entity {id: $entity_id})-[:RELATED_TO]-(neighbor:Entity)
        RETURN neighbor.id
        """
        query = self.client.build_cypher(cypher, "neighbor_id agtype")
        try:
            res = await self.client.execute_read(
                query, (json.dumps({"entity_id": entity_id}),)
            )
            return {int(row["neighbor_id"]) for row in res}
        except Exception as e:
            logger.error(f"Failed to get neighbor IDs for {entity_id}: {e}")
            return set()

    async def get_parent_entities(self, entity_id: int) -> List[Dict]:
        query = """
        SELECT
            parent.entity_id AS id,
            parent.canonical_name,
            parent.type,
            COALESCE(
                array_agg(f.content ORDER BY f.valid_at DESC)
                    FILTER (WHERE f.content IS NOT NULL),
                '{}'
            ) AS facts
        FROM hierarchy_edges edge
        JOIN entities parent ON parent.entity_id = edge.parent_id
        LEFT JOIN facts f
          ON f.entity_id = parent.entity_id
         AND f.invalid_at IS NULL
        WHERE edge.child_id = %s
        GROUP BY parent.entity_id, parent.canonical_name, parent.type
        ORDER BY parent.canonical_name
        """
        try:
            res = await self.client.execute_read(query, (entity_id,))
            return [
                {
                    "id": int(r["id"]),
                    "canonical_name": self._clean_string(r["canonical_name"]),
                    "type": self._clean_string(r["type"]),
                    "facts": r["facts"] or [],
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get parents for entity {entity_id}: {e}")
            return []

    async def get_neighbor_entities(self, entity_id: int, limit: int = 5) -> List[Dict]:
        cypher = """
        MATCH (e:Entity {id: $entity_id})-[:RELATED_TO]-(neighbor:Entity)
        RETURN neighbor.id, neighbor.canonical_name
        ORDER BY neighbor.last_mentioned DESC
        LIMIT $limit
        """
        query = self.client.build_cypher(cypher, "id agtype, name agtype")
        try:
            res = await self.client.execute_read(
                query, (json.dumps({"entity_id": entity_id, "limit": limit}),)
            )
            return [{"id": int(r["id"]), "name": r["name"]} for r in res]
        except Exception as e:
            logger.error(f"Failed to get neighbor entities for {entity_id}: {e}")
            return []

    async def get_child_entities(self, entity_id: int) -> List[Dict]:
        query = """
        SELECT
            child.entity_id AS id,
            child.canonical_name,
            child.type,
            COALESCE(
                array_agg(f.content ORDER BY f.valid_at DESC)
                    FILTER (WHERE f.content IS NOT NULL),
                '{}'
            ) AS facts
        FROM hierarchy_edges edge
        JOIN entities child ON child.entity_id = edge.child_id
        LEFT JOIN facts f
          ON f.entity_id = child.entity_id
         AND f.invalid_at IS NULL
        WHERE edge.parent_id = %s
        GROUP BY child.entity_id, child.canonical_name, child.type
        ORDER BY child.canonical_name
        """
        try:
            res = await self.client.execute_read(query, (entity_id,))
            return [
                {
                    "id": int(r["id"]),
                    "canonical_name": self._clean_string(r["canonical_name"]),
                    "type": self._clean_string(r["type"]),
                    "facts": r["facts"] or [],
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get children for entity {entity_id}: {e}")
            return []

    async def has_direct_edge(self, id_a: int, id_b: int) -> bool:
        cypher = """
        MATCH (a:Entity {id: $id_a})-[r:RELATED_TO]-(b:Entity {id: $id_b})
        RETURN count(r) > 0 as connected
        """
        query = self.client.build_cypher(cypher, "connected agtype")
        try:
            res = await self.client.execute_read(
                query, (json.dumps({"id_a": id_a, "id_b": id_b}),)
            )
            return bool(res[0]["connected"]) if res else False
        except Exception as e:
            logger.error(f"Failed to check direct edge between {id_a} and {id_b}: {e}")
            return False

    async def has_hierarchy_edge(self, id_a: int, id_b: int) -> bool:
        query = """
        SELECT EXISTS (
            SELECT 1
            FROM hierarchy_edges
            WHERE (parent_id = %s AND child_id = %s)
               OR (parent_id = %s AND child_id = %s)
        ) AS exists
        """
        try:
            res = await self.client.execute_read(query, (id_a, id_b, id_b, id_a))
            return bool(res[0]["exists"]) if res else False
        except Exception as e:
            logger.error(
                f"Failed to check hierarchy edge between {id_a} and {id_b}: {e}"
            )
            return False

    async def get_merge_topic_strength(
        self,
        primary_id: int,
        secondary_id: int,
        project_id: str,
    ) -> Dict:
        if not project_id:
            raise ValueError("get_merge_topic_strength requires project_id scope")

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
                FROM facts
                WHERE project_id = %s
                  AND entity_id = %s
                  AND invalid_at IS NULL
            ) AS p_fact_count,
            (
                SELECT count(*)
                FROM facts
                WHERE project_id = %s
                  AND entity_id = %s
                  AND invalid_at IS NULL
            ) AS s_fact_count,
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
            res = await self.client.execute_read(
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
            return dict(res[0]) if res else {}
        except Exception as e:
            logger.error(
                "Failed to get merge topic strength for "
                f"{primary_id}<-{secondary_id}: {e}"
            )
            raise

    async def get_hierarchy_candidates(
        self,
        project_id: str,
        topic: str,
        parent_type: str,
        child_types: List[str],
        min_weight: int = 2,
    ) -> List[Dict]:
        if not project_id:
            raise ValueError("get_hierarchy_candidates requires project_id scope")

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
            graph_res = await self.client.execute_read(
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
            emb_res = await self.client.execute_read(
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
            return []



    async def get_graph_stats(self) -> Dict[str, int]:
        query = """
        SELECT
            (SELECT count(*) FROM entities) AS entities,
            (
                SELECT count(*)
                FROM facts
                WHERE invalid_at IS NULL
            ) AS facts,
            (SELECT count(*) FROM relationships) AS relationships
        """
        try:
            res = await self.client.execute_read(query)
            if not res:
                return {"entities": 0, "facts": 0, "relationships": 0}
            return {
                "entities": int(res[0]["entities"] or 0),
                "facts": int(res[0]["facts"] or 0),
                "relationships": int(res[0]["relationships"] or 0),
            }
        except Exception as e:
            logger.error(f"Failed to get graph stats: {e}")
            return {"entities": 0, "facts": 0, "relationships": 0}

    async def get_neighbor_ids_batch(
        self, entity_ids: List[int]
    ) -> Dict[int, set[int]]:
        if not entity_ids:
            return {}
        cypher = """
        MATCH (e:Entity)-[:RELATED_TO]-(neighbor:Entity)
        WHERE e.id IN $ids
        RETURN e.id as entity_id, collect(neighbor.id) as neighbor_ids
        """
        query = self.client.build_cypher(
            cypher, "entity_id agtype, neighbor_ids agtype"
        )
        try:
            res = await self.client.execute_read(
                query, (json.dumps({"ids": entity_ids}),)
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
            return {eid: set() for eid in entity_ids}
