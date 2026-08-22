import json
import re
from datetime import timedelta
from typing import Dict, List, Optional, Set, Tuple

from loguru import logger

from common.exceptions import StorageReadError
from common.scoping import (
    IDENTITY_ENTITY_ID,
    require_scope_value,
    require_visible_project_ids,
)
from common.utils.time_utils import get_now
from infrastructure.postgres_client import PostgresClient

_MAX_QUERY_LIMIT = 100
_MAX_ENTITY_LIST_OFFSET = 10_000


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

    @staticmethod
    def _validate_query_limit(limit: int, operation: str) -> int:
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= _MAX_QUERY_LIMIT
        ):
            raise ValueError(
                f"{operation}: limit must be an integer between 1 and "
                f"{_MAX_QUERY_LIMIT}"
            )
        return limit

    @staticmethod
    def _validate_entity_list_offset(offset: int) -> int:
        if (
            not isinstance(offset, int)
            or isinstance(offset, bool)
            or not 0 <= offset <= _MAX_ENTITY_LIST_OFFSET
        ):
            raise ValueError(
                "list_entities: offset must be an integer between 0 and "
                f"{_MAX_ENTITY_LIST_OFFSET}"
            )
        return offset

    @staticmethod
    def _validate_activity_days(days: int) -> int:
        if (
            not isinstance(days, int)
            or isinstance(days, bool)
            or not 1 <= days <= 365
        ):
            raise ValueError(
                "get_recently_active_entities: days must be an integer "
                "between 1 and 365"
            )
        return days

    def _parse_aliases(self, value) -> List[str]:
        aliases = self._parse_agtype(value) or []
        if isinstance(aliases, str):
            cleaned = self._clean_string(aliases)
            return [cleaned] if cleaned else []
        return [self._clean_string(alias) for alias in aliases if alias]

    @staticmethod
    def _raise_storage_read(operation: str, exc: Exception) -> None:
        logger.error("Storage read failed for {}: {}", operation, exc)
        raise StorageReadError(
            operation,
            details={"error_type": type(exc).__name__},
        ) from exc

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
        FROM entities
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
            "canonical_name": self._clean_string(row["canonical_name"]),
            "aliases": self._parse_aliases(row.get("aliases")),
            "type": self._clean_string(row["type"]),
            "topic": self._clean_string(row["topic"]),
            "last_mentioned": self._ms_to_seconds(row.get("last_mentioned")),
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
        FROM entities
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
            self._raise_storage_read("get_entity_embedding", e)

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
        limit = self._validate_query_limit(limit, "list_entities")
        offset = self._validate_entity_list_offset(offset)
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
            async with self.client.transaction() as cur:
                await cur.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
                )
                await cur.execute(count_query, tuple(params))
                count_row = await cur.fetchone()
                total = (
                    int(count_row["total"])
                    if count_row and count_row["total"]
                    else 0
                )

                if total == 0:
                    return [], 0

                await cur.execute(data_query, (*params, offset, limit))
                entities_res = await cur.fetchall()
            entities = []
            for row in entities_res:
                entities.append(
                    {
                        "id": int(row["id"]),
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
            self._raise_storage_read("list_entities", e)

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

        query = """
        SELECT
            e.entity_id AS id,
            e.project_id,
            e.canonical_name,
            COALESCE(
                array_agg(a.alias ORDER BY a.alias)
                    FILTER (WHERE a.alias IS NOT NULL),
                '{}'
            ) AS aliases,
            e.type,
            e.topic,
            e.last_mentioned_ms AS last_mentioned
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
            self._raise_storage_read("get_entity_by_id", e)

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
            e.project_id,
            e.canonical_name,
            COALESCE(
                array_agg(a.alias ORDER BY a.alias)
                    FILTER (WHERE a.alias IS NOT NULL),
                '{}'
            ) AS aliases,
            e.type,
            e.topic,
            e.last_mentioned_ms AS last_mentioned
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
            self._raise_storage_read("get_entities_by_ids", e)

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
            self._raise_storage_read("get_entity_ids_for_messages", e)

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

        query = """
        SELECT
            e.entity_id AS id,
            e.project_id,
            e.canonical_name,
            e.type,
            COALESCE(
                array_agg(DISTINCT a.alias ORDER BY a.alias)
                    FILTER (WHERE a.alias IS NOT NULL),
                '{}'
            ) AS aliases
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
            self._raise_storage_read("get_entities_by_names", e)

    async def search_by_name(
        self,
        query: str,
        *,
        visible_project_ids: List[str],
        active_topics: Optional[List[str]] = None,
        limit: int = 5,
        connections_limit: int = 5,
        evidence_limit: int = 5,
    ) -> List[Dict]:
        """Search entities and hydrate their observed graph connections."""

        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "search_by_name",
        )
        limit = self._validate_query_limit(limit, "search_by_name")
        clean_query = re.sub(r"[^\w\s.\-']", "", query).strip()
        if not clean_query:
            return []

        entity_query = """
        SELECT
            entity.entity_id AS id,
            entity.canonical_name,
            entity.type,
            entity.topic,
            entity.last_mentioned_ms AS last_mentioned,
            COALESCE(
                array_agg(DISTINCT alias.alias) FILTER (WHERE alias.alias IS NOT NULL),
                '{}'::text[]
            ) AS aliases
        FROM entities entity
        LEFT JOIN entity_aliases alias ON alias.entity_id = entity.entity_id
        WHERE (
            entity.canonical_name ILIKE %s
            OR EXISTS (
                SELECT 1
                FROM entity_aliases entity_alias
                WHERE entity_alias.entity_id = entity.entity_id
                  AND entity_alias.alias ILIKE %s
            )
        )
          AND (entity.project_id = ANY(%s) OR entity.entity_id = %s)
        """
        params: tuple = (
            f"%{clean_query}%",
            f"%{clean_query}%",
            visible_project_ids,
            IDENTITY_ENTITY_ID,
        )
        if active_topics:
            entity_query += " AND entity.topic = ANY(%s)"
            params = (*params, active_topics)
        entity_query += """
        GROUP BY entity.entity_id
        ORDER BY entity.last_mentioned_ms DESC NULLS LAST
        LIMIT %s
        """
        params = (*params, limit)

        try:
            entity_rows = await self.client.fetch_all(entity_query, params)
            results: list[Dict] = []
            for row in entity_rows:
                entity_id = int(row["id"])
                connections = await self.get_entity_relationships(
                    entity_id,
                    visible_project_ids=visible_project_ids,
                )
                results.append(
                    {
                        "id": entity_id,
                        "canonical_name": row["canonical_name"],
                        "aliases": row["aliases"] or [],
                        "type": row["type"],
                        "topic": row["topic"],
                        "last_mentioned": row["last_mentioned"],
                        "top_connections": [
                            {
                                "canonical_name": connection["neighbor_name"],
                                "aliases": [],
                                "weight": connection["evidence_count"],
                                "evidence_refs": connection["message_refs"][:evidence_limit],
                                "context": connection["context"],
                            }
                            for connection in connections[:connections_limit]
                        ],
                    }
                )
            return results
        except Exception as exc:
            self._raise_storage_read("search_by_name", exc)

    async def search_similar_entities(
        self,
        entity_id: int,
        *,
        visible_project_ids: List[str],
        limit: int = 50,
    ) -> List[Tuple[int, float]]:
        limit = self._validate_query_limit(limit, "search_similar_entities")
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
        query = """
        SELECT entity_id, 1 - (embedding <=> %s::vector) AS similarity
        FROM entities
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
            self._raise_storage_read("search_similar_entities", e)

    async def search_entities_by_embedding(
        self,
        embedding: List[float],
        *,
        visible_project_ids: List[str],
        limit: int = 10,
        score_threshold: float = 0.8,
    ) -> List[Tuple[int, float]]:
        limit = self._validate_query_limit(
            limit,
            "search_entities_by_embedding",
        )
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
        query = """
        SELECT entity_id, 1 - (embedding <=> %s::vector) AS similarity
        FROM entities
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
            self._raise_storage_read("search_entities_by_embedding", e)

    async def validate_existing_ids(
        self,
        ids: List[int],
        *,
        visible_project_ids: List[str],
    ) -> Set[int]:
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
            self._raise_storage_read("validate_existing_ids", e)

    async def preview_project_entity_cleanup(
        self,
        *,
        user_name: str,
        project_id: str,
        limit: int = 100,
    ) -> List[Dict]:
        """Return project-owned derived entities with deletion decision evidence."""

        project_id = require_scope_value(
            project_id,
            "project_id",
            "preview_project_entity_cleanup",
        )
        if not user_name or not user_name.strip():
            raise ValueError("preview_project_entity_cleanup requires user_name")
        limit = self._validate_query_limit(limit, "preview_project_entity_cleanup")
        query = """
        SELECT
            e.entity_id,
            e.canonical_name,
            e.type,
            e.topic,
            COALESCE(array_agg(DISTINCT alias.alias)
                FILTER (WHERE alias.alias IS NOT NULL), ARRAY[]::TEXT[]) AS aliases,
            e.last_mentioned_ms,
            COUNT(DISTINCT mer.message_id) AS message_reference_count,
            COUNT(DISTINCT relationship.relationship_id) AS relationship_count,
            COUNT(DISTINCT episode_entity.episode_id) AS episode_reference_count
        FROM public.entities e
        LEFT JOIN public.entity_aliases alias
            ON alias.entity_id = e.entity_id
        LEFT JOIN public.message_entity_refs mer
            ON mer.entity_id = e.entity_id
        LEFT JOIN public.relationships relationship
            ON relationship.project_id = e.project_id
           AND (
                relationship.entity_a_id = e.entity_id
                OR relationship.entity_b_id = e.entity_id
           )
        LEFT JOIN public.episode_entities episode_entity
            ON episode_entity.project_id = e.project_id
           AND episode_entity.entity_id = e.entity_id
        WHERE e.user_name = %s
          AND e.project_id = %s
          AND e.entity_id <> %s
        GROUP BY e.entity_id, e.canonical_name, e.type, e.topic, e.last_mentioned_ms
        ORDER BY e.last_mentioned_ms NULLS FIRST, e.entity_id
        LIMIT %s
        """
        try:
            rows = await self.client.fetch_all(
                query,
                (user_name, project_id, IDENTITY_ENTITY_ID, limit),
            )
            return [dict(row) for row in rows]
        except Exception as e:
            self._raise_storage_read("preview_project_entity_cleanup", e)

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
            self._raise_storage_read("get_entity_count_by_type", e)

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
            self._raise_storage_read("get_entity_count_by_topic", e)

    async def get_top_connected_entities(
        self, *, visible_project_ids: List[str], limit: int = 10
    ) -> List[Dict]:
        limit = self._validate_query_limit(limit, "get_top_connected_entities")
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
            self._raise_storage_read("get_top_connected_entities", e)

    async def get_related_entities_by_name(
        self,
        entity_names: List[str],
        *,
        visible_project_ids: List[str],
        active_topics: Optional[List[str]] = None,
        limit: int = 25,
    ) -> List[Dict]:
        """Return relationship identities and their canonical observations."""

        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_related_entities_by_name",
        )
        limit = self._validate_query_limit(limit, "get_related_entities_by_name")
        if not entity_names:
            return []

        query = """
        SELECT
            source.canonical_name AS source,
            target.canonical_name AS target,
            relationship.relationship_id,
            relationship.relationship_type,
            relationship."symmetric",
            COUNT(observation.observation_id)::INTEGER AS observation_count,
            COUNT(DISTINCT observation.message_id)::INTEGER AS evidence_message_count,
            COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'user_name', observation.user_name,
                        'session_id', observation.session_id,
                        'message_id', observation.message_id
                    )
                    ORDER BY observation.observed_at_ms DESC,
                             observation.observation_id DESC
                ) FILTER (WHERE observation.observation_id IS NOT NULL),
                '[]'::jsonb
            ) AS evidence_refs,
            COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'observation_id', observation.observation_id,
                        'observed_relationship_label', observation.observed_relationship_label,
                        'canonical_relationship_type', observation.canonical_relationship_type,
                        'observed_at_ms', observation.observed_at_ms,
                        'confidence', observation.confidence,
                        'context', left(COALESCE(observation.context, ''), 600)
                    )
                    ORDER BY observation.observed_at_ms DESC,
                             observation.observation_id DESC
                ) FILTER (WHERE observation.observation_id IS NOT NULL),
                '[]'::jsonb
            ) AS observation_refs,
            MIN(observation.observed_at_ms) AS first_observed,
            MAX(observation.observed_at_ms) AS last_observed
        FROM entities source
        JOIN relationships relationship
          ON relationship.entity_a_id = source.entity_id
          OR relationship.entity_b_id = source.entity_id
        JOIN entities target
          ON target.entity_id = CASE
              WHEN relationship.entity_a_id = source.entity_id
              THEN relationship.entity_b_id
              ELSE relationship.entity_a_id
          END
        LEFT JOIN relationship_observations observation
          ON observation.relationship_id = relationship.relationship_id
         AND observation.project_id = relationship.project_id
        WHERE source.canonical_name = ANY(%s)
          AND (source.project_id = ANY(%s) OR source.entity_id = %s)
          AND (target.project_id = ANY(%s) OR target.entity_id = %s)
          AND relationship.project_id = ANY(%s)
        """
        params: tuple = (
            entity_names,
            visible_project_ids,
            IDENTITY_ENTITY_ID,
            visible_project_ids,
            IDENTITY_ENTITY_ID,
            visible_project_ids,
        )
        if active_topics is not None:
            query += " AND target.topic = ANY(%s)"
            params = (*params, active_topics)
        query += """
        GROUP BY
            source.canonical_name,
            target.canonical_name,
            relationship.relationship_id,
            relationship.relationship_type,
            relationship."symmetric"
        ORDER BY observation_count DESC, last_observed DESC
        LIMIT %s
        """
        params = (*params, limit)

        try:
            rows = await self.client.fetch_all(query, params)
            return [
                {
                    "source": row["source"],
                    "target": row["target"],
                    "relationship_id": row["relationship_id"],
                    "relationship_type": row["relationship_type"],
                    "symmetric": bool(row["symmetric"]),
                    "relationship_semantics": "observed_evidence",
                    "connection_strength": float(row["observation_count"] or 0),
                    "evidence_refs": row["evidence_refs"] or [],
                    "observation_refs": row["observation_refs"] or [],
                    "evidence_message_count": int(
                        row["evidence_message_count"] or 0
                    ),
                    "observation_count": int(row["observation_count"] or 0),
                    "first_observed": row["first_observed"],
                    "last_observed": row["last_observed"],
                }
                for row in rows
            ]
        except Exception as exc:
            self._raise_storage_read("get_related_entities_by_name", exc)

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
            COUNT(ref.observation_id) AS evidence_count,
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
            (array_agg(ref.context ORDER BY ref.observed_at_ms DESC)
                FILTER (WHERE ref.context IS NOT NULL))[1] AS context,
            MAX(ref.confidence) AS confidence
        FROM relationships r
        JOIN entities neighbor
          ON neighbor.entity_id = CASE
              WHEN r.entity_a_id = %s THEN r.entity_b_id
              ELSE r.entity_a_id
          END
        LEFT JOIN relationship_observations ref
          ON ref.relationship_id = r.relationship_id
         AND ref.project_id = r.project_id
        WHERE %s IN (r.entity_a_id, r.entity_b_id)
          AND r.project_id = ANY(%s)
          AND (neighbor.project_id = ANY(%s) OR neighbor.entity_id = %s)
        GROUP BY
            r.relationship_id,
            r.entity_a_id,
            r.entity_b_id,
            neighbor.canonical_name
        ORDER BY MAX(ref.observed_at_ms) DESC NULLS LAST
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
                    "evidence_count": int(r["evidence_count"] or 0),
                    "message_refs": r["message_refs"] or [],
                    "context": self._clean_string(r["context"]),
                    "confidence": float(r["confidence"] or 1.0),
                }
                for r in res
            ]
        except Exception as e:
            self._raise_storage_read("get_entity_relationships", e)

    async def get_recently_active_entities(
        self,
        *,
        visible_project_ids: List[str],
        days: int = 7,
        limit: int = 10,
    ) -> List[Dict]:
        days = self._validate_activity_days(days)
        limit = self._validate_query_limit(limit, "get_recently_active_entities")
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
            self._raise_storage_read("get_recently_active_entities", e)

    async def get_notable_entities(
        self, *, visible_project_ids: List[str], limit: int = 10
    ) -> List[Dict]:
        limit = self._validate_query_limit(limit, "get_notable_entities")
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
            self._raise_storage_read("get_notable_entities", e)
