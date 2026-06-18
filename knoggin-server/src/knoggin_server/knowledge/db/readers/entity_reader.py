import json
from datetime import timedelta
from typing import Dict, List, Optional, Set, Tuple

from loguru import logger

from common.scoping import IDENTITY_ENTITY_ID
from common.utils.time_utils import get_now
from infrastructure.postgres_client import PostgresClient


class EntityReader:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name

    def _parse_agtype(self, val):
        """Unwrap agtype returned by psycopg when needed."""
        if not val:
            return val
        if isinstance(val, str):
            try:
                return json.loads(val)
            except json.JSONDecodeError:
                pass
        return val

    @staticmethod
    def _clean_string(value):
        if isinstance(value, str):
            return value.strip('"')
        return value

    @staticmethod
    def _ms_to_seconds(value) -> float:
        return float(value or 0) / 1000

    def _parse_aliases(self, value) -> List[str]:
        aliases = self._parse_agtype(value) or []
        if isinstance(aliases, str):
            cleaned = self._clean_string(aliases)
            return [cleaned] if cleaned else []
        return [self._clean_string(alias) for alias in aliases if alias]

    async def _fetch_embeddings(self, entity_ids: List[int]) -> Dict[int, List[float]]:
        if not entity_ids:
            return {}
        emb_query = (
            "SELECT entity_id, embedding FROM entity_search WHERE entity_id = ANY(%s)"
        )
        emb_res = await self.client.execute_read(emb_query, (entity_ids,))
        return {
            int(row["entity_id"]): self._parse_vector(row["embedding"])
            for row in emb_res
        }

    def _hydrate_entity_row(
        self,
        row: Dict,
        embedding: List[float] = None,
        include_project_id: bool = True,
    ) -> Dict:
        entity = {
            "id": int(row["id"]) if row["id"] else None,
            "session_id": self._clean_string(row["session_id"]),
            "canonical_name": self._clean_string(row["canonical_name"]),
            "aliases": self._parse_aliases(row.get("aliases")),
            "type": self._clean_string(row["type"]),
            "topic": self._clean_string(row["topic"]),
            "last_mentioned": self._ms_to_seconds(row.get("last_mentioned")),
            "last_updated": self._ms_to_seconds(row.get("last_updated")),
            "last_profiled_msg_id": row.get("last_profiled_msg_id"),
            "embedding": embedding or [],
        }
        if include_project_id:
            entity["project_id"] = self._clean_string(row["project_id"])
        return entity

    def _parse_vector(self, val) -> List[float]:
        """Normalize pgvector values across adapter and text-returning drivers."""
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

    def _scope_params(self, visible_project_ids: Optional[List[str]] = None) -> Dict:
        return {
            "filter_projects": bool(visible_project_ids),
            "visible_project_ids": visible_project_ids or [],
            "identity_entity_id": IDENTITY_ENTITY_ID,
        }

    async def get_max_entity_id(self) -> int:
        """Returns the highest entity ID currently in the DB."""
        query = "SELECT COALESCE(MAX(entity_id), 0) as max_id FROM entities"
        try:
            result = await self.client.execute_read(query)
            return result[0]["max_id"] if result else 0
        except Exception as e:
            logger.error(f"Failed to get max entity ID: {e}")
            raise

    async def get_entity_embedding(self, entity_id: int) -> List[float]:
        query = "SELECT embedding FROM entity_search WHERE entity_id = %s"
        try:
            result = await self.client.execute_read(query, (entity_id,))
            if result and result[0]["embedding"]:
                return self._parse_vector(result[0]["embedding"])
            return []
        except Exception as e:
            logger.error(f"Failed to get embedding for entity {entity_id}: {e}")
            return []

    async def list_entities(
        self,
        limit: int = 20,
        offset: int = 0,
        topic: Optional[str] = None,
        entity_type: Optional[str] = None,
        search: Optional[str] = None,
    ) -> Tuple[List[Dict], int]:
        """Paginated entity listing with optional filters."""
        where_clauses = []
        params = []

        if entity_type:
            where_clauses.append("e.type = %s")
            params.append(entity_type)

        if search:
            where_clauses.append("lower(e.canonical_name) LIKE lower(%s)")
            params.append(f"%{search}%")

        if topic:
            where_clauses.append("e.topic = %s")
            params.append(topic)

        where_str = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        count_query = f"SELECT count(*) AS total FROM entities e {where_str}"
        data_query = f"""
        SELECT
            e.entity_id AS id,
            e.session_id,
            e.canonical_name,
            e.type,
            e.topic,
            e.last_mentioned_ms AS last_mentioned,
            COALESCE(
                array_agg(f.content ORDER BY f.valid_at DESC)
                    FILTER (WHERE f.content IS NOT NULL),
                '{{}}'
            )[1:2] AS fact_snippets
        FROM entities e
        LEFT JOIN facts f
          ON f.entity_id = e.entity_id
         AND f.invalid_at IS NULL
        {where_str}
        GROUP BY e.entity_id
        ORDER BY e.last_mentioned_ms DESC NULLS LAST
        OFFSET %s
        LIMIT %s
        """

        try:
            count_res = await self.client.execute_read(count_query, tuple(params))
            total = (
                int(count_res[0]["total"]) if count_res and count_res[0]["total"] else 0
            )

            if total == 0:
                return [], 0

            entities_res = await self.client.execute_read(
                data_query,
                (*params, offset, limit),
            )
            entities = []
            for row in entities_res:
                snippets = row["fact_snippets"] or []
                summary = ". ".join(filter(None, snippets)) if snippets else None

                entities.append(
                    {
                        "id": int(row["id"]),
                        "session_id": row["session_id"],
                        "canonical_name": row["canonical_name"],
                        "type": row["type"],
                        "topic": row["topic"],
                        "last_mentioned": self._ms_to_seconds(
                            row["last_mentioned"]
                        ),
                        "summary": summary,
                    }
                )
            return entities, total
        except Exception as e:
            logger.error(f"Failed to list entities: {e}")
            return [], 0

    async def get_entity_by_id(
        self, entity_id: int, visible_project_ids: Optional[List[str]] = None
    ) -> Optional[Dict]:
        scope_sql = ""
        params = [entity_id]
        if visible_project_ids:
            scope_sql = "AND (e.project_id = ANY(%s) OR e.entity_id = %s)"
            params.extend([visible_project_ids, IDENTITY_ENTITY_ID])

        query = f"""
        SELECT
            e.entity_id AS id,
            e.session_id,
            e.project_id,
            e.canonical_name,
            COALESCE(
                array_agg(a.alias ORDER BY a.alias)
                    FILTER (WHERE a.alias IS NOT NULL),
                '{{}}'
            ) AS aliases,
            e.type,
            e.topic,
            e.last_mentioned_ms AS last_mentioned,
            e.last_updated_ms AS last_updated,
            e.last_profiled_msg_id
        FROM entities e
        LEFT JOIN entity_aliases a ON a.entity_id = e.entity_id
        WHERE e.entity_id = %s
        {scope_sql}
        GROUP BY e.entity_id
        """
        try:
            res = await self.client.execute_read(query, tuple(params))
            if not res:
                return None
            row = res[0]
            embedding = await self.get_entity_embedding(entity_id)
            return self._hydrate_entity_row(row, embedding=embedding)
        except Exception as e:
            logger.error(f"Failed to get entity {entity_id}: {e}")
            return None

    async def get_entities_by_ids(self, entity_ids: List[int]) -> List[Dict]:
        if not entity_ids:
            return []

        query = """
        SELECT
            e.entity_id AS id,
            e.session_id,
            e.canonical_name,
            COALESCE(
                array_agg(a.alias ORDER BY a.alias)
                    FILTER (WHERE a.alias IS NOT NULL),
                '{}'
            ) AS aliases,
            e.type,
            e.topic,
            e.last_mentioned_ms AS last_mentioned,
            e.last_updated_ms AS last_updated,
            e.last_profiled_msg_id
        FROM entities e
        LEFT JOIN entity_aliases a ON a.entity_id = e.entity_id
        WHERE e.entity_id = ANY(%s)
        GROUP BY e.entity_id
        ORDER BY e.entity_id
        """

        try:
            res = await self.client.execute_read(query, (entity_ids,))
            embeddings_map = await self._fetch_embeddings(entity_ids)

            entities = []
            for row in res:
                eid = int(row["id"])
                entities.append(
                    self._hydrate_entity_row(
                        row,
                        embedding=embeddings_map.get(eid, []),
                        include_project_id=False,
                    )
                )
            return entities
        except Exception as e:
            logger.error(f"Failed to fetch entities by ids: {e}")
            return []

    async def find_alias_collisions(self) -> List[Tuple[int, int]]:
        query = """
        WITH names AS (
            SELECT entity_id, lower(canonical_name) AS normalized_name
            FROM entities
            WHERE canonical_name IS NOT NULL
            UNION ALL
            SELECT entity_id, lower(alias) AS normalized_name
            FROM entity_aliases
            WHERE alias IS NOT NULL
        )
        SELECT DISTINCT left_name.entity_id AS id_a, right_name.entity_id AS id_b
        FROM names left_name
        JOIN names right_name
          ON left_name.normalized_name = right_name.normalized_name
         AND left_name.entity_id < right_name.entity_id
        ORDER BY id_a, id_b
        """
        try:
            res = await self.client.execute_read(query)
            return [(int(r["id_a"]), int(r["id_b"])) for r in res]
        except Exception as e:
            logger.error(f"Failed to find alias collisions: {e}")
            return []

    async def get_entities_by_names(
        self, names: List[str], visible_project_ids: Optional[List[str]] = None
    ) -> List[Dict]:
        if not names:
            return []

        lower_names = [n.lower() for n in names]
        scope_sql = ""
        params = [lower_names, lower_names]
        if visible_project_ids:
            scope_sql = "AND (e.project_id = ANY(%s) OR e.entity_id = %s)"
            params.extend([visible_project_ids, IDENTITY_ENTITY_ID])

        query = f"""
        SELECT
            e.entity_id AS id,
            e.project_id,
            e.canonical_name,
            e.type,
            COALESCE(
                array_agg(DISTINCT a.alias ORDER BY a.alias)
                    FILTER (WHERE a.alias IS NOT NULL),
                '{{}}'
            ) AS aliases,
            COALESCE(
                array_agg(DISTINCT f.content)
                    FILTER (WHERE f.content IS NOT NULL),
                '{{}}'
            ) AS facts
        FROM entities e
        LEFT JOIN entity_aliases a ON a.entity_id = e.entity_id
        LEFT JOIN facts f
          ON f.entity_id = e.entity_id
         AND f.invalid_at IS NULL
        WHERE (
            lower(e.canonical_name) = ANY(%s)
            OR EXISTS (
                SELECT 1
                FROM entity_aliases ea
                WHERE ea.entity_id = e.entity_id
                  AND lower(ea.alias) = ANY(%s)
            )
        )
        {scope_sql}
        GROUP BY e.entity_id
        """
        try:
            res = await self.client.execute_read(query, tuple(params))
            return [
                {
                    "id": int(row["id"]),
                    "project_id": self._clean_string(row["project_id"]),
                    "canonical_name": self._clean_string(row["canonical_name"]),
                    "type": self._clean_string(row["type"]),
                    "aliases": self._parse_aliases(row["aliases"]),
                    "facts": row["facts"] or [],
                }
                for row in res
            ]
        except Exception as e:
            logger.error(f"Failed to get entities by names: {e}")
            return []

    async def search_similar_entities(
        self,
        entity_id: int,
        limit: int = 50,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[Tuple[int, float]]:
        """Find similar entities using Postgres pgvector."""
        # 1. Get the source vector
        emb = await self.get_entity_embedding(entity_id)
        if not emb:
            return []

        # 2. Search using pgvector cosine distance `<=>`
        # `<=>` returns distance, so we do 1 - distance for similarity
        scope_sql = ""
        params = [emb, entity_id]
        if visible_project_ids:
            scope_sql = "AND (project_id = ANY(%s) OR entity_id = %s)"
            params.extend([visible_project_ids, IDENTITY_ENTITY_ID])
        query = f"""
        SELECT entity_id, 1 - (embedding <=> %s::vector) AS similarity
        FROM entity_search
        WHERE entity_id != %s
        {scope_sql}
        ORDER BY embedding <=> %s::vector
        LIMIT %s
        """
        try:
            params.extend([emb, limit])
            res = await self.client.execute_read(query, tuple(params))
            return [(r["entity_id"], r["similarity"]) for r in res]
        except Exception as e:
            logger.error(f"Failed to search similar entities for {entity_id}: {e}")
            return []

    async def search_entities_by_embedding(
        self,
        embedding: List[float],
        limit: int = 10,
        score_threshold: float = 0.8,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[Tuple[int, float]]:
        scope_sql = ""
        params = [embedding, embedding, score_threshold]
        if visible_project_ids:
            scope_sql = "AND (project_id = ANY(%s) OR entity_id = %s)"
            params.extend([visible_project_ids, IDENTITY_ENTITY_ID])
        query = f"""
        SELECT entity_id, 1 - (embedding <=> %s::vector) AS similarity
        FROM entity_search
        WHERE 1 - (embedding <=> %s::vector) >= %s
        {scope_sql}
        ORDER BY embedding <=> %s::vector
        LIMIT %s
        """
        try:
            params.extend([embedding, limit])
            res = await self.client.execute_read(query, tuple(params))
            return [(r["entity_id"], r["similarity"]) for r in res]
        except Exception as e:
            logger.error(f"Entity vector search failed: {e}")
            return []

    async def validate_existing_ids(self, ids: List[int]) -> Optional[Set[int]]:
        if not ids:
            return set()
        query = """
        SELECT entity_id AS id
        FROM entities
        WHERE entity_id = ANY(%s)
        """
        try:
            res = await self.client.execute_read(query, (ids,))
            return {int(r["id"]) for r in res}
        except Exception as e:
            logger.error(f"Liveness check failed: {e}")
            return None

    async def get_all_entities_for_hydration(self) -> list[dict]:
        query = """
        SELECT
            e.entity_id AS id,
            e.canonical_name,
            COALESCE(
                array_agg(a.alias ORDER BY a.alias)
                    FILTER (WHERE a.alias IS NOT NULL),
                '{}'
            ) AS aliases,
            e.type,
            e.topic,
            e.session_id
        FROM entities e
        LEFT JOIN entity_aliases a ON a.entity_id = e.entity_id
        WHERE e.entity_id IS NOT NULL
        GROUP BY e.entity_id
        ORDER BY e.entity_id
        """
        emb_query = "SELECT entity_id, embedding FROM entity_search"
        try:
            res = await self.client.execute_read(query)
            emb_res = await self.client.execute_read(emb_query)
            embeddings_map = {
                row["entity_id"]: self._parse_vector(row["embedding"])
                for row in emb_res
            }

            entities = []
            for row in res:
                eid = int(row["id"])
                entities.append(
                    {
                        "id": eid,
                        "canonical_name": self._clean_string(
                            row["canonical_name"]
                        ),
                        "aliases": self._parse_aliases(row["aliases"]),
                        "type": self._clean_string(row["type"]),
                        "topic": self._clean_string(row["topic"]),
                        "session_id": self._clean_string(row["session_id"]),
                        "embedding": embeddings_map.get(eid, []),
                    }
                )
            return entities
        except Exception as e:
            logger.error(f"Failed to hydrate entities: {e}")
            return []

    async def get_orphan_entities(
        self,
        protected_id: int = 1,
        orphan_cutoff_ms: int = 0,
        stale_junk_cutoff_ms: int = 0,
        project_id: Optional[str] = None,
    ) -> List[int]:
        if not project_id:
            logger.warning("Refusing unsafe orphan lookup without project scope")
            return []

        query = """
        SELECT e.entity_id AS id
        FROM entities e
        WHERE e.entity_id <> %s
          AND e.project_id = %s
          AND NOT EXISTS (
              SELECT 1
              FROM facts f
              WHERE f.entity_id = e.entity_id
                AND f.project_id = e.project_id
                AND f.invalid_at IS NULL
          )
          AND (
              (
                  e.last_mentioned_ms < %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM relationships r
                      WHERE r.project_id = e.project_id
                        AND (
                            r.entity_a_id = e.entity_id
                            OR r.entity_b_id = e.entity_id
                        )
                  )
              )
              OR
              (
                  e.last_mentioned_ms < %s
                  AND EXISTS (
                      SELECT 1
                      FROM relationships r
                      WHERE r.project_id = e.project_id
                        AND (
                            (
                                r.entity_a_id = e.entity_id
                                AND r.entity_b_id = %s
                            )
                            OR (
                                r.entity_b_id = e.entity_id
                                AND r.entity_a_id = %s
                            )
                        )
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM relationships r
                      WHERE r.project_id = e.project_id
                        AND (
                            r.entity_a_id = e.entity_id
                            OR r.entity_b_id = e.entity_id
                        )
                        AND r.entity_a_id <> %s
                        AND r.entity_b_id <> %s
                  )
              )
          )
        ORDER BY e.entity_id
        """
        try:
            res = await self.client.execute_read(
                query,
                (
                    protected_id,
                    project_id,
                    orphan_cutoff_ms,
                    stale_junk_cutoff_ms,
                    protected_id,
                    protected_id,
                    protected_id,
                    protected_id,
                ),
            )
            return [int(r["id"]) for r in res]
        except Exception as e:
            logger.error(f"Failed to fetch orphans: {e}")
            return []

    async def get_entity_count_by_type(self) -> List[Dict]:
        query = """
        SELECT type, count(*) AS count
        FROM entities
        WHERE type IS NOT NULL
        GROUP BY type
        ORDER BY count DESC
        """
        try:
            res = await self.client.execute_read(query)
            return [
                {
                    "type": self._clean_string(r["type"]),
                    "count": int(r["count"]),
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get entity count by type: {e}")
            return []

    async def get_entity_count_by_topic(self) -> List[Dict]:
        query = """
        SELECT topic, count(*) AS count
        FROM entities
        WHERE topic IS NOT NULL
        GROUP BY topic
        ORDER BY count DESC
        """
        try:
            res = await self.client.execute_read(query)
            return [
                {
                    "topic": self._clean_string(r["topic"]),
                    "count": int(r["count"]),
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get entity count by topic: {e}")
            return []

    async def get_top_connected_entities(self, limit: int = 10) -> List[Dict]:
        query = """
        WITH edge_ends AS (
            SELECT entity_a_id AS entity_id
            FROM relationships
            UNION ALL
            SELECT entity_b_id AS entity_id
            FROM relationships
        )
        SELECT
            e.canonical_name AS name,
            e.type,
            count(*) AS connections
        FROM edge_ends ee
        JOIN entities e ON e.entity_id = ee.entity_id
        GROUP BY e.entity_id, e.canonical_name, e.type
        ORDER BY connections DESC, e.canonical_name
        LIMIT %s
        """
        try:
            res = await self.client.execute_read(query, (limit,))
            return [
                {
                    "name": self._clean_string(r["name"]),
                    "type": self._clean_string(r["type"]),
                    "connections": int(r["connections"]),
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get top connected entities: {e}")
            return []

    async def get_entity_relationships(self, entity_id: int) -> List[Dict]:
        query = """
        SELECT
            CASE
                WHEN r.entity_a_id = %s THEN r.entity_b_id
                ELSE r.entity_a_id
            END AS neighbor_id,
            neighbor.canonical_name AS neighbor_name,
            r.weight,
            COALESCE(
                json_agg(
                    json_build_object(
                        'user_name', ref.user_name,
                        'session_id', ref.session_id,
                        'message_id', ref.message_id
                    )
                    ORDER BY ref.message_id
                ) FILTER (WHERE ref.message_id IS NOT NULL),
                '[]'::json
            ) AS message_refs,
            r.context,
            r.confidence
        FROM relationships r
        JOIN entities neighbor
          ON neighbor.entity_id = CASE
              WHEN r.entity_a_id = %s THEN r.entity_b_id
              ELSE r.entity_a_id
          END
        LEFT JOIN relationship_evidence_refs ref
          ON ref.relationship_id = r.relationship_id
        WHERE %s IN (r.entity_a_id, r.entity_b_id)
        GROUP BY
            r.relationship_id,
            r.entity_a_id,
            r.entity_b_id,
            neighbor.canonical_name,
            r.weight,
            r.context,
            r.confidence,
            r.last_seen_ms
        ORDER BY r.last_seen_ms DESC NULLS LAST
        """
        try:
            res = await self.client.execute_read(
                query,
                (entity_id, entity_id, entity_id),
            )
            return [
                {
                    "neighbor_id": int(r["neighbor_id"]),
                    "neighbor_name": self._clean_string(r["neighbor_name"]),
                    "weight": float(r["weight"] or 1.0),
                    "message_refs": r["message_refs"] or [],
                    "context": self._clean_string(r["context"]),
                    "confidence": float(r["confidence"] or 1.0),
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get relationships for entity {entity_id}: {e}")
            return []

    async def get_recently_active_entities(
        self, days: int = 7, limit: int = 10
    ) -> List[Dict]:
        cutoff = (get_now() - timedelta(days=days)).isoformat()
        cypher = """
        MATCH (e:Entity)-[:HAS_FACT]->(f:Fact)
        WHERE f.valid_at > $cutoff
        AND f.invalid_at IS NULL
        WITH e, count(f) as recent_facts, max(f.valid_at) as last_activity
        OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)
        RETURN e.id as id,
            e.canonical_name as name,
            e.type as type,
            t.name as topic,
            recent_facts,
            last_activity
        ORDER BY recent_facts DESC, last_activity DESC
        LIMIT $limit
        """
        query = self.client.build_cypher(
            cypher,
            "id agtype, name agtype, type agtype, topic agtype, "
            "recent_facts agtype, last_activity agtype",
        )
        try:
            res = await self.client.execute_read(
                query, (json.dumps({"cutoff": cutoff, "limit": limit}),)
            )
            return [
                {
                    "id": int(r["id"]),
                    "name": r["name"].strip('"')
                    if isinstance(r["name"], str)
                    else r["name"],
                    "type": r["type"].strip('"')
                    if isinstance(r["type"], str)
                    else r["type"],
                    "topic": r["topic"].strip('"')
                    if isinstance(r["topic"], str)
                    else r["topic"],
                    "recent_facts": int(r["recent_facts"]),
                    "last_activity": r["last_activity"].strip('"')
                    if isinstance(r["last_activity"], str)
                    else r["last_activity"],
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get recently active entities: {e}")
            return []

    async def get_notable_entities(self, limit: int = 10) -> List[Dict]:
        cypher = """
        MATCH (e:Entity)
        WHERE e.canonical_name IS NOT NULL
        OPTIONAL MATCH (e)-[r]-()
        WITH e, count(DISTINCT r) as connection_count
        OPTIONAL MATCH (e)-[:HAS_FACT]->(f:Fact)
        WHERE f.invalid_at IS NULL
        WITH e, connection_count, count(f) as fact_count
        OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)
        RETURN e.id as id,
            e.canonical_name as name,
            e.type as type,
            t.name as topic,
            connection_count,
            fact_count
        ORDER BY connection_count DESC
        LIMIT $limit
        """
        query = self.client.build_cypher(
            cypher,
            "id agtype, name agtype, type agtype, topic agtype, "
            "connection_count agtype, fact_count agtype",
        )
        try:
            res = await self.client.execute_read(query, (json.dumps({"limit": limit}),))
            return [
                {
                    "id": int(r["id"]),
                    "name": r["name"].strip('"')
                    if isinstance(r["name"], str)
                    else r["name"],
                    "type": r["type"].strip('"')
                    if isinstance(r["type"], str)
                    else r["type"],
                    "topic": r["topic"].strip('"')
                    if isinstance(r["topic"], str)
                    else r["topic"],
                    "connection_count": int(r["connection_count"]),
                    "fact_count": int(r["fact_count"]),
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get notable entities: {e}")
            return []
