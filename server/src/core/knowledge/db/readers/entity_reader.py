import json
from datetime import timedelta
from typing import Dict, List, Optional, Set, Tuple

from loguru import logger

from common.scoping import (
    IDENTITY_ENTITY_ID,
    require_scope_value,
    require_visible_project_ids,
)
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

    async def _fetch_embeddings(
        self,
        entity_ids: List[int],
        visible_project_ids: List[str],
    ) -> Dict[int, List[float]]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "_fetch_embeddings",
        )
        if not entity_ids:
            return {}
        emb_query = """
        SELECT entity_id, embedding
        FROM entity_search
        WHERE entity_id = ANY(%s)
          AND (project_id = ANY(%s) OR entity_id = %s)
        """
        emb_res = await self.client.fetch_all(
            emb_query,
            (entity_ids, visible_project_ids, IDENTITY_ENTITY_ID),
        )
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

    async def get_entity_embedding(
        self,
        entity_id: int,
        *,
        visible_project_ids: List[str],
    ) -> List[float]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_entity_embedding",
        )
        query = """
        SELECT embedding
        FROM entity_search
        WHERE entity_id = %s
          AND (project_id = ANY(%s) OR entity_id = %s)
        """
        try:
            row = await self.client.fetch_one(
                query,
                (entity_id, visible_project_ids, IDENTITY_ENTITY_ID),
            )
            if row and row["embedding"]:
                return self._parse_vector(row["embedding"])
            return []
        except Exception as e:
            logger.error(f"Failed to get embedding for entity {entity_id}: {e}")
            return []

    async def list_entities(
        self,
        limit: int = 20,
        offset: int = 0,
        *,
        visible_project_ids: List[str],
        topic: Optional[str] = None,
        entity_type: Optional[str] = None,
        search: Optional[str] = None,
    ) -> Tuple[List[Dict], int]:
        """Paginated entity listing with optional filters."""
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "list_entities",
        )
        where_clauses = [
            "(e.project_id = ANY(%s) OR e.entity_id = %s)"
        ]
        params = [visible_project_ids, IDENTITY_ENTITY_ID]

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
            e.last_mentioned_ms AS last_mentioned
        FROM entities e
        {where_str}
        ORDER BY e.last_mentioned_ms DESC NULLS LAST
        OFFSET %s
        LIMIT %s
        """

        try:
            count_row = await self.client.fetch_one(count_query, tuple(params))
            total = int(count_row["total"]) if count_row and count_row["total"] else 0

            if total == 0:
                return [], 0

            entities_res = await self.client.fetch_all(
                data_query,
                (*params, offset, limit),
            )
            entities = []
            for row in entities_res:
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
                        "summary": None,
                    }
                )
            return entities, total
        except Exception as e:
            logger.error(f"Failed to list entities: {e}")
            return [], 0

    async def get_entity_by_id(
        self,
        entity_id: int,
        *,
        visible_project_ids: List[str],
    ) -> Optional[Dict]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_entity_by_id",
        )
        params = [entity_id, visible_project_ids, IDENTITY_ENTITY_ID]

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
          AND (e.project_id = ANY(%s) OR e.entity_id = %s)
        GROUP BY e.entity_id
        """
        try:
            row = await self.client.fetch_one(query, tuple(params))
            if not row:
                return None
            embedding = await self.get_entity_embedding(
                entity_id,
                visible_project_ids=visible_project_ids,
            )
            return self._hydrate_entity_row(row, embedding=embedding)
        except Exception as e:
            logger.error(f"Failed to get entity {entity_id}: {e}")
            return None

    async def get_entities_by_ids(
        self,
        entity_ids: List[int],
        *,
        visible_project_ids: List[str],
    ) -> List[Dict]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_entities_by_ids",
        )
        if not entity_ids:
            return []

        query = """
        SELECT
            e.entity_id AS id,
            e.session_id,
            e.project_id,
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
          AND (e.project_id = ANY(%s) OR e.entity_id = %s)
        GROUP BY e.entity_id
        ORDER BY e.entity_id
        """

        try:
            res = await self.client.fetch_all(
                query,
                (entity_ids, visible_project_ids, IDENTITY_ENTITY_ID),
            )
            embeddings_map = await self._fetch_embeddings(
                entity_ids,
                visible_project_ids,
            )

            entities = []
            for row in res:
                eid = int(row["id"])
                entities.append(
                    self._hydrate_entity_row(
                        row,
                        embedding=embeddings_map.get(eid, []),
                        include_project_id=True,
                    )
                )
            return entities
        except Exception as e:
            logger.error(f"Failed to fetch entities by ids: {e}")
            return []

    async def get_entity_ids_for_messages(
        self,
        message_ids: List[int],
        *,
        user_name: str,
        session_id: str,
        project_id: str,
    ) -> Dict[int, List[int]]:
        """Return every resolved entity ID for each scoped canonical message."""

        user_name = require_scope_value(
            user_name, "user_name", "get_entity_ids_for_messages"
        )
        session_id = require_scope_value(
            session_id, "session_id", "get_entity_ids_for_messages"
        )
        project_id = require_scope_value(
            project_id, "project_id", "get_entity_ids_for_messages"
        )
        normalized_message_ids = sorted({int(message_id) for message_id in message_ids})
        if not normalized_message_ids:
            return {}

        query = """
        SELECT mer.message_id, mer.entity_id
        FROM message_entity_refs mer
        JOIN messages m ON m.message_id = mer.message_id
        JOIN entities e ON e.entity_id = mer.entity_id
        WHERE mer.message_id = ANY(%s)
          AND m.user_name = %s
          AND m.session_id = %s
          AND m.project_id = %s
          AND (e.project_id = %s OR e.entity_id = %s)
        ORDER BY mer.message_id, mer.entity_id
        """
        try:
            rows = await self.client.fetch_all(
                query,
                (
                    normalized_message_ids,
                    user_name,
                    session_id,
                    project_id,
                    project_id,
                    IDENTITY_ENTITY_ID,
                ),
            )
        except Exception as e:
            logger.error(f"Failed to fetch message entities: {e}")
            return {}

        entities_by_message = {message_id: [] for message_id in normalized_message_ids}
        for row in rows:
            entities_by_message[int(row["message_id"])].append(
                int(row["entity_id"])
            )
        return entities_by_message

    async def get_entities_by_names(
        self,
        names: List[str],
        *,
        visible_project_ids: List[str],
    ) -> List[Dict]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_entities_by_names",
        )
        if not names:
            return []

        lower_names = [n.lower() for n in names]
        params = [lower_names, lower_names, visible_project_ids, IDENTITY_ENTITY_ID]

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
        FROM entities e
        LEFT JOIN entity_aliases a ON a.entity_id = e.entity_id
        WHERE (
            lower(e.canonical_name) = ANY(%s)
            OR EXISTS (
                SELECT 1
                FROM entity_aliases ea
                WHERE ea.entity_id = e.entity_id
                  AND lower(ea.alias) = ANY(%s)
            )
        )
        AND (e.project_id = ANY(%s) OR e.entity_id = %s)
        GROUP BY e.entity_id
        """
        try:
            res = await self.client.fetch_all(
                query,
                params,
            )
            return [
                {
                    "id": int(row["id"]),
                    "project_id": self._clean_string(row["project_id"]),
                    "canonical_name": self._clean_string(row["canonical_name"]),
                    "type": self._clean_string(row["type"]),
                    "aliases": self._parse_aliases(row["aliases"]),
                }
                for row in res
            ]
        except Exception as e:
            logger.error(f"Failed to get entities by names: {e}")
            return []

    async def search_similar_entities(
        self,
        entity_id: int,
        *,
        visible_project_ids: List[str],
        limit: int = 50,
    ) -> List[Tuple[int, float]]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "search_similar_entities",
        )
        """Find similar entities using Postgres pgvector."""
        # 1. Get the source vector
        emb = await self.get_entity_embedding(
            entity_id,
            visible_project_ids=visible_project_ids,
        )
        if not emb:
            return []

        # 2. Search using pgvector cosine distance `<=>`
        # `<=>` returns distance, so we do 1 - distance for similarity
        params = [
            emb,
            entity_id,
            visible_project_ids,
            IDENTITY_ENTITY_ID,
        ]
        query = f"""
        SELECT entity_id, 1 - (embedding <=> %s::vector) AS similarity
        FROM entity_search
        WHERE entity_id != %s
          AND (project_id = ANY(%s) OR entity_id = %s)
        ORDER BY embedding <=> %s::vector
        LIMIT %s
        """
        try:
            params.extend([emb, limit])
            res = await self.client.fetch_all(query, tuple(params))
            return [(r["entity_id"], r["similarity"]) for r in res]
        except Exception as e:
            logger.error(f"Failed to search similar entities for {entity_id}: {e}")
            return []

    async def search_entities_by_embedding(
        self,
        embedding: List[float],
        *,
        visible_project_ids: List[str],
        limit: int = 10,
        score_threshold: float = 0.8,
    ) -> List[Tuple[int, float]]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "search_entities_by_embedding",
        )
        params = [
            embedding,
            embedding,
            score_threshold,
            visible_project_ids,
            IDENTITY_ENTITY_ID,
        ]
        query = f"""
        SELECT entity_id, 1 - (embedding <=> %s::vector) AS similarity
        FROM entity_search
        WHERE 1 - (embedding <=> %s::vector) >= %s
          AND (project_id = ANY(%s) OR entity_id = %s)
        ORDER BY embedding <=> %s::vector
        LIMIT %s
        """
        try:
            params.extend([embedding, limit])
            res = await self.client.fetch_all(query, tuple(params))
            return [(r["entity_id"], r["similarity"]) for r in res]
        except Exception as e:
            logger.error(f"Entity vector search failed: {e}")
            return []

    async def validate_existing_ids(
        self,
        ids: List[int],
        *,
        visible_project_ids: List[str],
    ) -> Optional[Set[int]]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "validate_existing_ids",
        )
        if not ids:
            return set()
        query = """
        SELECT entity_id AS id
        FROM entities
        WHERE entity_id = ANY(%s)
          AND (project_id = ANY(%s) OR entity_id = %s)
        """
        try:
            res = await self.client.fetch_all(
                query,
                (ids, visible_project_ids, IDENTITY_ENTITY_ID),
            )
            return {int(r["id"]) for r in res}
        except Exception as e:
            logger.error(f"Liveness check failed: {e}")
            return None

    async def get_orphan_entities(
        self,
        protected_id: int = 1,
        orphan_cutoff_ms: int = 0,
        stale_junk_cutoff_ms: int = 0,
        *,
        project_id: str,
    ) -> List[int]:
        project_id = require_scope_value(
            project_id,
            "project_id",
            "get_orphan_entities",
        )

        query = """
        SELECT e.entity_id AS id
        FROM entities e
        WHERE e.entity_id <> %s
          AND e.project_id = %s
          AND NOT EXISTS (
              SELECT 1
              FROM episode_entities ee
              JOIN episodes ep ON ep.episode_id = ee.episode_id
              WHERE ee.entity_id = e.entity_id
                AND ep.project_id = e.project_id
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
            res = await self.client.fetch_all(
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

    async def get_entity_count_by_type(
        self, *, visible_project_ids: List[str]
    ) -> List[Dict]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_entity_count_by_type",
        )
        query = """
        SELECT type, count(*) AS count
        FROM entities
        WHERE type IS NOT NULL
          AND (project_id = ANY(%s) OR entity_id = %s)
        GROUP BY type
        ORDER BY count DESC
        """
        try:
            res = await self.client.fetch_all(
                query,
                (visible_project_ids, IDENTITY_ENTITY_ID),
            )
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

    async def get_entity_count_by_topic(
        self, *, visible_project_ids: List[str]
    ) -> List[Dict]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_entity_count_by_topic",
        )
        query = """
        SELECT topic, count(*) AS count
        FROM entities
        WHERE topic IS NOT NULL
          AND (project_id = ANY(%s) OR entity_id = %s)
        GROUP BY topic
        ORDER BY count DESC
        """
        try:
            res = await self.client.fetch_all(
                query,
                (visible_project_ids, IDENTITY_ENTITY_ID),
            )
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

    async def get_top_connected_entities(
        self, *, visible_project_ids: List[str], limit: int = 10
    ) -> List[Dict]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_top_connected_entities",
        )
        query = """
        WITH edge_ends AS (
            SELECT entity_a_id AS entity_id
            FROM relationships
            WHERE project_id = ANY(%s)
            UNION ALL
            SELECT entity_b_id AS entity_id
            FROM relationships
            WHERE project_id = ANY(%s)
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
            res = await self.client.fetch_all(
                query,
                (visible_project_ids, visible_project_ids, limit),
            )
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

    async def get_entity_relationships(
        self,
        entity_id: int,
        *,
        visible_project_ids: List[str],
    ) -> List[Dict]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_entity_relationships",
        )
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
          AND r.project_id = ANY(%s)
          AND (neighbor.project_id = ANY(%s) OR neighbor.entity_id = %s)
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
            res = await self.client.fetch_all(
                query,
                (
                    entity_id,
                    entity_id,
                    entity_id,
                    visible_project_ids,
                    visible_project_ids,
                    IDENTITY_ENTITY_ID,
                ),
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
        self,
        *,
        visible_project_ids: List[str],
        days: int = 7,
        limit: int = 10,
    ) -> List[Dict]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_recently_active_entities",
        )
        cutoff = get_now() - timedelta(days=days)
        query = """
        SELECT
            e.entity_id AS id,
            e.canonical_name AS name,
            e.type,
            e.topic,
            count(DISTINCT episode_entity.episode_id) AS recent_episode_count,
            max(episode.updated_at) AS last_activity
        FROM entities e
        JOIN episode_entities episode_entity
          ON episode_entity.entity_id = e.entity_id
        JOIN episodes episode
          ON episode.episode_id = episode_entity.episode_id
        WHERE e.project_id = ANY(%s)
          AND episode.project_id = ANY(%s)
          AND episode.updated_at > %s
        GROUP BY e.entity_id
        ORDER BY recent_episode_count DESC, last_activity DESC
        LIMIT %s
        """
        try:
            res = await self.client.fetch_all(
                query,
                (visible_project_ids, visible_project_ids, cutoff, limit),
            )
            return [
                {
                    "id": int(r["id"]),
                    "name": r["name"],
                    "type": r["type"],
                    "topic": r["topic"],
                    "recent_episode_count": int(r["recent_episode_count"]),
                    "last_activity": r["last_activity"],
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get recently active entities: {e}")
            return []

    async def get_notable_entities(
        self, *, visible_project_ids: List[str], limit: int = 10
    ) -> List[Dict]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_notable_entities",
        )
        query = """
        SELECT
            e.entity_id AS id,
            e.canonical_name AS name,
            e.type,
            e.topic,
            (
                SELECT count(*)
                FROM relationships relationship
                WHERE relationship.project_id = ANY(%s)
                  AND (
                      relationship.entity_a_id = e.entity_id
                      OR relationship.entity_b_id = e.entity_id
                  )
            ) AS connection_count,
            (
                SELECT count(*)
                FROM episode_entities episode_entity
                JOIN episodes episode ON episode.episode_id = episode_entity.episode_id
                WHERE episode_entity.entity_id = e.entity_id
                  AND episode.project_id = ANY(%s)
            ) AS episode_count
        FROM entities e
        WHERE e.canonical_name IS NOT NULL
          AND (e.project_id = ANY(%s) OR e.entity_id = %s)
        ORDER BY connection_count DESC, episode_count DESC
        LIMIT %s
        """
        try:
            res = await self.client.fetch_all(
                query,
                (
                    visible_project_ids,
                    visible_project_ids,
                    visible_project_ids,
                    IDENTITY_ENTITY_ID,
                    limit,
                ),
            )
            return [
                {
                    "id": int(r["id"]),
                    "name": r["name"],
                    "type": r["type"],
                    "topic": r["topic"],
                    "connection_count": int(r["connection_count"]),
                    "episode_count": int(r["episode_count"]),
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get notable entities: {e}")
            return []
