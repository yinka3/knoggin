import json
from typing import Dict, List, Optional

from loguru import logger

from infrastructure.postgres_client import PostgresClient


class GraphReader:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name

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
            logger.warning("Refusing unsafe message text lookup without user/session scope")
            return ""

        cypher = """
        MATCH (m:Message {user_name: $user_name, session_id: $session_id, id: $id})
        RETURN m.content
        """
        query = self.client.build_cypher(cypher, "content agtype")
        try:
            res = await self.client.execute_read(
                query,
                (
                    json.dumps(
                        {
                            "id": message_id,
                            "user_name": user_name,
                            "session_id": session_id,
                        }
                    ),
                ),
            )
            if not res:
                return ""
            content = res[0]["content"]
            return content.strip('"') if isinstance(content, str) else content
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

        cypher = """
        MATCH (m:Message)
        WHERE m.id IN $ids
        AND m.user_name = $user_name
        AND m.session_id IN $session_ids
        RETURN m.id, m.user_name, m.session_id, m.role, m.content, m.timestamp
        ORDER BY m.id ASC
        """
        query = self.client.build_cypher(
            cypher,
            "id agtype, user_name agtype, session_id agtype, role agtype, content agtype, timestamp agtype",
        )
        try:
            res = await self.client.execute_read(query, (json.dumps(params),))
            return [
                {
                    "id": int(row["id"]),
                    "user_name": row["user_name"].strip('"')
                    if isinstance(row["user_name"], str)
                    else row["user_name"],
                    "session_id": row["session_id"].strip('"')
                    if isinstance(row["session_id"], str)
                    else row["session_id"],
                    "role": row["role"].strip('"')
                    if isinstance(row["role"], str)
                    else row["role"],
                    "content": row["content"].strip('"')
                    if isinstance(row["content"], str)
                    else row["content"],
                    "timestamp": row["timestamp"],
                }
                for row in res
            ]
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
            logger.warning("Refusing unsafe project message lookup without user/project scope")
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
            "AND m.id <= $before_message_id"
            if before_message_id is not None
            else ""
        )
        cypher = f"""
        MATCH (m:Message)
        WHERE m.user_name = $user_name
        AND m.project_id = $project_id
        {before_clause}
        RETURN m.id, m.user_name, m.session_id, m.role, m.content, m.timestamp
        ORDER BY m.id DESC
        LIMIT $limit
        """
        query = self.client.build_cypher(
            cypher,
            "id agtype, user_name agtype, session_id agtype, role agtype, content agtype, timestamp agtype",
        )
        try:
            rows = await self.client.execute_read(query, (json.dumps(params),))

            def parse(row):
                return {
                    "id": int(row["id"]),
                    "user_name": row["user_name"].strip('"')
                    if isinstance(row["user_name"], str)
                    else row["user_name"],
                    "session_id": row["session_id"].strip('"')
                    if isinstance(row["session_id"], str)
                    else row["session_id"],
                    "role": row["role"].strip('"')
                    if isinstance(row["role"], str)
                    else row["role"],
                    "content": row["content"].strip('"')
                    if isinstance(row["content"], str)
                    else row["content"],
                    "timestamp": row["timestamp"],
                }

            return [parse(row) for row in reversed(rows)]
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
        params_base = {"user_name": user_name, "session_id": session_id}

        # In AGE, complex correlated CALL subqueries can be brittle.
        # It's cleaner and safer to do the backwards/forwards search sequentially in python,
        # or use simple independent Cypher fetches.
        # We will fetch the target first.
        try:
            target_res = await self.get_messages_by_ids(
                [message_id], user_name=user_name, session_ids=session_ids
            )
            if not target_res:
                return []
            target = target_res[0]
            target_ts = target["timestamp"]

            back_cypher = """
            MATCH (prev:Message)
            WHERE prev.timestamp <= $ts AND prev.id <> $id
            AND prev.user_name = $user_name
            AND prev.session_id = $session_id
            RETURN prev.id, prev.user_name, prev.session_id, prev.role, prev.content, prev.timestamp
            ORDER BY prev.timestamp DESC
            LIMIT $limit
            """

            fwd_cypher = """
            MATCH (next:Message)
            WHERE next.timestamp >= $ts AND next.id <> $id
            AND next.user_name = $user_name
            AND next.session_id = $session_id
            RETURN next.id, next.user_name, next.session_id, next.role, next.content, next.timestamp
            ORDER BY next.timestamp ASC
            LIMIT $limit
            """

            back_q = self.client.build_cypher(
                back_cypher,
                "id agtype, user_name agtype, session_id agtype, role agtype, content agtype, timestamp agtype",
            )
            fwd_q = self.client.build_cypher(
                fwd_cypher,
                "id agtype, user_name agtype, session_id agtype, role agtype, content agtype, timestamp agtype",
            )

            back_data = await self.client.execute_read(
                back_q,
                (
                    json.dumps(
                        {
                            "ts": target_ts,
                            "id": message_id,
                            "limit": back_limit,
                            **params_base,
                        }
                    ),
                ),
            )
            fwd_data = await self.client.execute_read(
                fwd_q,
                (
                    json.dumps(
                        {
                            "ts": target_ts,
                            "id": message_id,
                            "limit": forward,
                            **params_base,
                        }
                    ),
                ),
            )

            def parse(row):
                return {
                    "id": int(row["id"]),
                    "user_name": row["user_name"].strip('"')
                    if isinstance(row["user_name"], str)
                    else row["user_name"],
                    "session_id": row["session_id"].strip('"')
                    if isinstance(row["session_id"], str)
                    else row["session_id"],
                    "role": row["role"].strip('"')
                    if isinstance(row["role"], str)
                    else row["role"],
                    "content": row["content"].strip('"')
                    if isinstance(row["content"], str)
                    else row["content"],
                    "timestamp": row["timestamp"],
                }

            prev_msgs = [parse(r) for r in reversed(back_data)]
            next_msgs = [parse(r) for r in fwd_data]

            return prev_msgs + [target] + next_msgs
        except Exception as e:
            logger.error(f"Failed to fetch surrounding messages for {message_id}: {e}")
            return []

    async def get_neighbor_ids(self, entity_id: int) -> set[int]:
        cypher = "MATCH (e:Entity {id: $entity_id})-[:RELATED_TO]-(neighbor:Entity) RETURN neighbor.id"
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
        cypher = """
        MATCH (child:Entity {id: $entity_id})-[:PART_OF]->(parent:Entity)
        OPTIONAL MATCH (parent)-[:HAS_FACT]->(f) WHERE f.invalid_at IS NULL
        RETURN parent.id, parent.canonical_name, parent.type, collect(f.content) as facts
        """
        query = self.client.build_cypher(
            cypher, "id agtype, canonical_name agtype, type agtype, facts agtype"
        )
        try:
            res = await self.client.execute_read(
                query, (json.dumps({"entity_id": entity_id}),)
            )
            return [
                {
                    "id": int(r["id"]),
                    "canonical_name": r["canonical_name"],
                    "type": r["type"],
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
        cypher = """
        MATCH (child:Entity)-[:PART_OF]->(parent:Entity {id: $entity_id})
        OPTIONAL MATCH (child)-[:HAS_FACT]->(f) WHERE f.invalid_at IS NULL
        RETURN child.id, child.canonical_name, child.type, collect(f.content) as facts
        """
        query = self.client.build_cypher(
            cypher, "id agtype, canonical_name agtype, type agtype, facts agtype"
        )
        try:
            res = await self.client.execute_read(
                query, (json.dumps({"entity_id": entity_id}),)
            )
            return [
                {
                    "id": int(r["id"]),
                    "canonical_name": r["canonical_name"],
                    "type": r["type"],
                    "facts": r["facts"] or [],
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get children for entity {entity_id}: {e}")
            return []

    async def has_direct_edge(self, id_a: int, id_b: int) -> bool:
        cypher = "MATCH (a:Entity {id: $id_a})-[r:RELATED_TO]-(b:Entity {id: $id_b}) RETURN count(r) > 0 as connected"
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
        cypher = """
        MATCH (a:Entity {id: $id_a}), (b:Entity {id: $id_b})
        WHERE (a)-[:PART_OF]->(b) OR (b)-[:PART_OF]->(a)
        RETURN count(a) > 0 as exists
        """
        query = self.client.build_cypher(cypher, "exists agtype")
        try:
            res = await self.client.execute_read(
                query, (json.dumps({"id_a": id_a, "id_b": id_b}),)
            )
            return bool(res[0]["exists"]) if res else False
        except Exception as e:
            logger.error(
                f"Failed to check hierarchy edge between {id_a} and {id_b}: {e}"
            )
            return False

    async def get_hierarchy_candidates(
        self, topic: str, parent_type: str, child_types: List[str], min_weight: int = 2
    ) -> List[Dict]:

        # 1. Fetch graph candidates
        cypher = """
        MATCH (parent:Entity)-[:BELONGS_TO]->(t:Topic {name: $topic})
        MATCH (child:Entity)-[:BELONGS_TO]->(t)
        MATCH (parent)-[r:RELATED_TO]-(child)
        WHERE parent.type = $parent_type
        AND child.type IN $child_types
        AND r.weight >= $min_weight
        AND NOT (child)-[:PART_OF]->(parent)
        RETURN parent.id, parent.canonical_name, child.id, child.canonical_name, r.weight
        """
        query = self.client.build_cypher(
            cypher,
            "parent_id agtype, parent_name agtype, child_id agtype, child_name agtype, weight agtype",
        )
        try:
            graph_res = await self.client.execute_read(
                query,
                (
                    json.dumps(
                        {
                            "topic": topic,
                            "parent_type": parent_type,
                            "child_types": child_types,
                            "min_weight": min_weight,
                        }
                    ),
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
                "SELECT entity_id, embedding FROM entity_search WHERE entity_id = ANY(%s)",
                (entity_ids,),
            )
            embs = {
                r["entity_id"]: self._parse_vector(r["embedding"])
                for r in emb_res
            }

            return [
                {
                    "parent_id": int(r["parent_id"]),
                    "parent_name": r["parent_name"],
                    "parent_embedding": embs.get(int(r["parent_id"]), []),
                    "child_id": int(r["child_id"]),
                    "child_name": r["child_name"],
                    "child_embedding": embs.get(int(r["child_id"]), []),
                    "weight": r["weight"],
                }
                for r in graph_res
            ]

        except Exception as e:
            logger.error(f"Hierarchy candidate query failed: {e}")
            return []

    async def list_preferences(
        self, session_id: str, kind: Optional[str] = None
    ) -> List[Dict]:
        where_kind = "AND p.kind = $kind" if kind else ""
        cypher = f"""
        MATCH (p:Preference {{session_id: $session_id}})
        WHERE true {where_kind}
        RETURN p.id, p.content, p.kind, p.created_at
        ORDER BY p.created_at DESC
        """
        query = self.client.build_cypher(
            cypher, "id agtype, content agtype, kind agtype, created_at agtype"
        )
        params = {"session_id": session_id}
        if kind:
            params["kind"] = kind

        try:
            res = await self.client.execute_read(query, (json.dumps(params),))
            return [
                {
                    "id": r["id"].strip('"') if isinstance(r["id"], str) else r["id"],
                    "content": r["content"].strip('"')
                    if isinstance(r["content"], str)
                    else r["content"],
                    "kind": r["kind"].strip('"')
                    if isinstance(r["kind"], str)
                    else r["kind"],
                    "created_at": r["created_at"],
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to list preferences: {e}")
            return []

    async def get_graph_stats(self) -> Dict[str, int]:
        cypher = """
        MATCH (e:Entity) WITH count(e) as entities
        MATCH (f:Fact) WHERE f.invalid_at IS NULL WITH entities, count(f) as facts
        MATCH ()-[r:RELATED_TO]->() WITH entities, facts, count(r) as relationships
        RETURN entities, facts, relationships
        """
        query = self.client.build_cypher(
            cypher, "entities agtype, facts agtype, relationships agtype"
        )
        try:
            res = await self.client.execute_read(query, ("{}",))
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
