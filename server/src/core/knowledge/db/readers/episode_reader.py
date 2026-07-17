import json
import math
from typing import Dict, List

from common.schema.primitives import (
    EntityEpisode,
    Episode,
    MessageEpisode,
    RelationshipEpisode,
)
from common.scoping import IDENTITY_ENTITY_ID, require_scope_value
from infrastructure.postgres_client import PostgresClient


class EpisodeReader:
    """Reads scoped episodic-memory aggregates and their canonical evidence."""

    def __init__(self, client: PostgresClient) -> None:
        self.client = client

    async def get_episode(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> Episode | None:
        """Load one episode and every persisted attachment."""

        scope = self._require_scope(user_name, project_id, session_id, "get_episode")
        episode_id = require_scope_value(episode_id, "episode_id", "get_episode")
        row = await self.client.fetch_one(
            self._episode_query("e.episode_id = %s"),
            (episode_id, *scope),
        )
        if row is None:
            return None
        return await self._hydrate_episode(row)

    async def get_episodes_for_entity(
        self,
        entity_id: int,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        limit: int = 10,
    ) -> List[Episode]:
        """Return all matching episodes, prioritizing focus memberships."""

        if entity_id <= 0:
            raise ValueError("get_episodes_for_entity requires a positive entity_id")
        if limit <= 0:
            return []
        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "get_episodes_for_entity",
        )
        query = self._episode_query(
            "ee.entity_id = %s",
            joins="JOIN episode_entities ee ON ee.episode_id = e.episode_id",
            ordering=(
                "ee.is_focus_entity DESC, ee.prominence_weight DESC, "
                "e.importance DESC, e.updated_at DESC"
            ),
            limit=True,
        )
        rows = await self.client.fetch_all(query, (entity_id, *scope, limit))
        return [await self._hydrate_episode(row) for row in rows]

    async def get_episodes_for_entities(
        self,
        entity_ids: List[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        limit: int = 10,
    ) -> List[Episode]:
        """Return prior episodes ranked by overlap with a source entity set."""

        normalized_entity_ids = sorted(
            {int(entity_id) for entity_id in entity_ids}
        )
        if not normalized_entity_ids or limit <= 0:
            return []
        if any(entity_id <= 0 for entity_id in normalized_entity_ids):
            raise ValueError(
                "get_episodes_for_entities requires positive entity IDs"
            )
        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "get_episodes_for_entities",
        )
        rows = await self.client.fetch_all(
            """
            SELECT
                e.episode_id,
                e.project_id,
                e.session_id,
                e.summary,
                e.new_developments,
                e.updates,
                e.unresolved,
                e.importance,
                e.source_message_count,
                e.first_message_at,
                e.last_message_at,
                e.generator_metadata,
                e.created_at,
                e.updated_at,
                COUNT(DISTINCT ee.entity_id) AS entity_overlap
            FROM episodes e
            JOIN sessions s
              ON s.session_id = e.session_id
             AND s.project_id = e.project_id
            JOIN episode_entities ee ON ee.episode_id = e.episode_id
            WHERE ee.entity_id = ANY(%s)
              AND s.user_name = %s
              AND e.project_id = %s
              AND e.session_id = %s
            GROUP BY e.episode_id
            ORDER BY entity_overlap DESC, e.updated_at DESC
            LIMIT %s
            """,
            (normalized_entity_ids, *scope, limit),
        )
        return [await self._hydrate_episode(row) for row in rows]

    async def get_merge_evidence_for_entities(
        self,
        entity_ids: List[int],
        *,
        project_id: str,
        evidence_limit: int = 4,
        source_message_limit: int = 2,
    ) -> Dict[int, List[Dict]]:
        """Return bounded message and episode evidence for merge classification.

        This intentionally returns canonical messages and episodic summaries,
        never derived atomic records. The resolver has already scoped the candidate IDs to
        the active project before calling this reader.
        """

        normalized_entity_ids = sorted({int(entity_id) for entity_id in entity_ids})
        if not normalized_entity_ids or evidence_limit <= 0:
            return {}
        if any(entity_id <= 0 for entity_id in normalized_entity_ids):
            raise ValueError(
                "get_merge_evidence_for_entities requires positive entity IDs"
            )
        project_id = require_scope_value(
            project_id,
            "project_id",
            "get_merge_evidence_for_entities",
        )

        evidence_by_entity = {
            entity_id: [] for entity_id in normalized_entity_ids
        }
        message_rows = await self.client.fetch_all(
            """
            WITH ranked_messages AS (
                SELECT
                    mer.entity_id,
                    m.message_id,
                    m.session_id,
                    m.role,
                    m.content,
                    m.timestamp_ms,
                    ROW_NUMBER() OVER (
                        PARTITION BY mer.entity_id
                        ORDER BY m.timestamp_ms DESC NULLS LAST, m.message_id DESC
                    ) AS evidence_rank
                FROM message_entity_refs mer
                JOIN messages m ON m.message_id = mer.message_id
                WHERE mer.entity_id = ANY(%s)
                  AND m.project_id = %s
            )
            SELECT *
            FROM ranked_messages
            WHERE evidence_rank <= %s
            ORDER BY entity_id, evidence_rank
            """,
            (normalized_entity_ids, project_id, evidence_limit),
        )
        for row in message_rows:
            entity_id = int(row["entity_id"])
            evidence_by_entity[entity_id].append(
                {
                    "kind": "message",
                    "message_id": int(row["message_id"]),
                    "session_id": str(row["session_id"]),
                    "text": str(row.get("content") or ""),
                    "role": row.get("role"),
                    "timestamp_ms": row.get("timestamp_ms"),
                }
            )

        episode_rows = await self.client.fetch_all(
            """
            WITH ranked_episodes AS (
                SELECT
                    ee.entity_id,
                    e.episode_id,
                    e.session_id,
                    e.summary,
                    e.importance,
                    ee.is_focus_entity,
                    ee.prominence_weight,
                    ROW_NUMBER() OVER (
                        PARTITION BY ee.entity_id
                        ORDER BY
                            ee.is_focus_entity DESC,
                            ee.prominence_weight DESC,
                            e.importance DESC,
                            e.updated_at DESC
                    ) AS evidence_rank
                FROM episode_entities ee
                JOIN episodes e ON e.episode_id = ee.episode_id
                WHERE ee.entity_id = ANY(%s)
                  AND e.project_id = %s
            )
            SELECT *
            FROM ranked_episodes
            WHERE evidence_rank <= %s
            ORDER BY entity_id, evidence_rank
            """,
            (normalized_entity_ids, project_id, evidence_limit),
        )
        episode_ids = sorted({str(row["episode_id"]) for row in episode_rows})
        source_messages_by_episode: Dict[str, List[Dict]] = {
            episode_id: [] for episode_id in episode_ids
        }
        if episode_ids and source_message_limit > 0:
            source_rows = await self.client.fetch_all(
                """
                WITH ranked_sources AS (
                    SELECT
                        em.episode_id,
                        m.message_id,
                        m.session_id,
                        m.role,
                        m.content,
                        m.timestamp_ms,
                        em.influence_weight,
                        ROW_NUMBER() OVER (
                            PARTITION BY em.episode_id
                            ORDER BY em.influence_weight DESC, em.message_position
                        ) AS source_rank
                    FROM episode_messages em
                    JOIN episodes e ON e.episode_id = em.episode_id
                    JOIN messages m ON m.message_id = em.message_id
                    WHERE em.episode_id = ANY(%s)
                      AND e.project_id = %s
                      AND m.project_id = %s
                )
                SELECT *
                FROM ranked_sources
                WHERE source_rank <= %s
                ORDER BY episode_id, source_rank
                """,
                (episode_ids, project_id, project_id, source_message_limit),
            )
            for row in source_rows:
                source_messages_by_episode[str(row["episode_id"])].append(
                    {
                        "kind": "episode_message",
                        "episode_id": str(row["episode_id"]),
                        "message_id": int(row["message_id"]),
                        "session_id": str(row["session_id"]),
                        "text": str(row.get("content") or ""),
                        "role": row.get("role"),
                        "timestamp_ms": row.get("timestamp_ms"),
                        "influence_weight": float(row["influence_weight"]),
                    }
                )

        for row in episode_rows:
            entity_id = int(row["entity_id"])
            episode_id = str(row["episode_id"])
            evidence_by_entity[entity_id].append(
                {
                    "kind": "episode",
                    "episode_id": episode_id,
                    "session_id": str(row["session_id"]),
                    "text": str(row.get("summary") or ""),
                    "importance": float(row["importance"]),
                    "is_focus_entity": bool(row["is_focus_entity"]),
                    "prominence_weight": float(row["prominence_weight"]),
                }
            )
            evidence_by_entity[entity_id].extend(
                source_messages_by_episode.get(episode_id, [])
            )
        return evidence_by_entity

    async def search_episodes(
        self,
        query: str,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        limit: int = 10,
    ) -> List[Episode]:
        """Search scoped episode summaries and structured narrative fields."""

        normalized_query = query.strip()
        if not normalized_query or limit <= 0:
            return []
        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "search_episodes",
        )
        document = (
            "to_tsvector('simple', concat_ws(' ', e.summary, "
            "e.new_developments::text, e.updates::text, e.unresolved::text))"
        )
        rows = await self.client.fetch_all(
            f"""
            SELECT
                e.episode_id,
                e.project_id,
                e.session_id,
                e.summary,
                e.new_developments,
                e.updates,
                e.unresolved,
                e.importance,
                e.source_message_count,
                e.first_message_at,
                e.last_message_at,
                e.generator_metadata,
                e.created_at,
                e.updated_at
            FROM episodes e
            JOIN sessions s
              ON s.session_id = e.session_id
             AND s.project_id = e.project_id
            WHERE s.user_name = %s
              AND e.project_id = %s
              AND e.session_id = %s
              AND {document} @@ websearch_to_tsquery('simple', %s)
            ORDER BY
                ts_rank_cd({document}, websearch_to_tsquery('simple', %s)) DESC,
                e.importance DESC,
                e.updated_at DESC
            LIMIT %s
            """,
            (*scope, normalized_query, normalized_query, limit),
        )
        return [await self._hydrate_episode(row) for row in rows]

    async def search_episodes_by_embedding(
        self,
        embedding: List[float],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        limit: int = 10,
        score_threshold: float = 0.35,
    ) -> List[tuple[Episode, float]]:
        """Return scoped episodes ranked by cosine similarity to a query vector."""

        if limit <= 0:
            return []
        if not 0.0 <= score_threshold <= 1.0:
            raise ValueError("episode score_threshold must be between 0 and 1")
        normalized_embedding = self._normalize_embedding(embedding)
        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "search_episodes_by_embedding",
        )
        vector = json.dumps(normalized_embedding)
        rows = await self.client.fetch_all(
            """
            SELECT
                e.episode_id,
                e.project_id,
                e.session_id,
                e.summary,
                e.new_developments,
                e.updates,
                e.unresolved,
                e.importance,
                e.source_message_count,
                e.first_message_at,
                e.last_message_at,
                e.embedding,
                e.generator_metadata,
                e.created_at,
                e.updated_at,
                1 - (e.embedding <=> %s::vector) AS similarity
            FROM episodes e
            JOIN sessions s
              ON s.session_id = e.session_id
             AND s.project_id = e.project_id
            WHERE s.user_name = %s
              AND e.project_id = %s
              AND e.session_id = %s
              AND e.embedding IS NOT NULL
              AND 1 - (e.embedding <=> %s::vector) >= %s
            ORDER BY e.embedding <=> %s::vector ASC
            LIMIT %s
            """,
            (vector, *scope, vector, score_threshold, vector, limit),
        )
        return [
            (await self._hydrate_episode(row), float(row["similarity"]))
            for row in rows
        ]

    async def get_recent_episodes(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        limit: int = 1,
    ) -> List[Episode]:
        """Return the most recently updated episodes in one conversation."""

        if limit <= 0:
            return []
        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "get_recent_episodes",
        )
        rows = await self.client.fetch_all(
            self._episode_query("TRUE", limit=True),
            (*scope, limit),
        )
        return [await self._hydrate_episode(row) for row in rows]

    async def get_episode_source_messages(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> List[Dict]:
        """Expand an episode into its ordered canonical message evidence."""

        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "get_episode_source_messages",
        )
        episode_id = require_scope_value(
            episode_id,
            "episode_id",
            "get_episode_source_messages",
        )
        query = """
        SELECT
            m.message_id,
            m.role,
            m.content,
            m.timestamp_ms,
            em.influence_weight,
            em.influence_reason,
            em.message_position,
            em.attached_at
        FROM episodes e
        JOIN sessions s
          ON s.session_id = e.session_id
         AND s.project_id = e.project_id
        JOIN episode_messages em ON em.episode_id = e.episode_id
        JOIN messages m ON m.message_id = em.message_id
        WHERE e.episode_id = %s
          AND s.user_name = %s
          AND e.project_id = %s
          AND e.session_id = %s
          AND m.user_name = %s
          AND m.project_id = %s
          AND m.session_id = %s
        ORDER BY em.message_position
        """
        return await self.client.fetch_all(
            query,
            (episode_id, *scope, *scope),
        )

    async def get_episode_graph_context(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> Dict[str, List[EntityEpisode] | List[RelationshipEpisode]] | None:
        """Load the complete entity and relationship context for one episode."""

        episode = await self.get_episode(
            episode_id,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        if episode is None:
            return None
        return {
            "entities": episode.entities,
            "relationships": episode.relationships,
        }

    async def get_last_evaluated_message_id(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> int:
        """Return zero for a valid conversation with no episode checkpoint yet."""

        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "get_last_evaluated_message_id",
        )
        row = await self.client.fetch_one(
            """
            SELECT COALESCE(ec.last_evaluated_message_id, 0) AS message_id
            FROM sessions s
            LEFT JOIN episode_processing_checkpoints ec
              ON ec.project_id = s.project_id
             AND ec.session_id = s.session_id
            WHERE s.user_name = %s
              AND s.project_id = %s
              AND s.session_id = %s
            """,
            scope,
        )
        if row is None:
            raise ValueError("Episode checkpoint requires an existing session")
        return int(row["message_id"])

    async def get_next_episode_window(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        after_message_id: int,
        message_count: int,
    ) -> List[Dict]:
        """Load the next fully ingested window in chronological message order."""

        if after_message_id < 0:
            raise ValueError("after_message_id must not be negative")
        if message_count <= 0:
            raise ValueError("message_count must be positive")
        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "get_next_episode_window",
        )
        rows = await self.client.fetch_all(
            """
            SELECT
                m.message_id,
                m.role,
                m.content,
                m.timestamp_ms,
                (elm.message_id IS NOT NULL) AS is_episode_eligible
            FROM messages m
            LEFT JOIN episode_eligible_messages elm
              ON elm.message_id = m.message_id
            WHERE m.user_name = %s
              AND m.project_id = %s
              AND m.session_id = %s
              AND m.message_id > %s
            ORDER BY m.timestamp_ms ASC NULLS LAST, m.message_id
            LIMIT %s
            """,
            (*scope, after_message_id, message_count),
        )
        if len(rows) < message_count or any(
            not row["is_episode_eligible"] for row in rows
        ):
            return []
        return [
            {
                key: value
                for key, value in row.items()
                if key != "is_episode_eligible"
            }
            for row in rows
        ]

    async def get_relationship_ids_for_messages(
        self,
        message_ids: List[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> Dict[int, List[str]]:
        """Return canonical relationship evidence attached to source messages."""

        normalized_message_ids = sorted(
            {int(message_id) for message_id in message_ids}
        )
        if not normalized_message_ids:
            return {}
        if any(message_id <= 0 for message_id in normalized_message_ids):
            raise ValueError(
                "get_relationship_ids_for_messages requires positive message IDs"
            )
        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "get_relationship_ids_for_messages",
        )
        rows = await self.client.fetch_all(
            """
            SELECT rer.message_id, rer.relationship_id
            FROM relationship_evidence_refs rer
            JOIN relationships r ON r.relationship_id = rer.relationship_id
            JOIN messages m ON m.message_id = rer.message_id
            WHERE rer.message_id = ANY(%s)
              AND rer.user_name = %s
              AND rer.session_id = %s
              AND r.project_id = %s
              AND m.user_name = %s
              AND m.project_id = %s
              AND m.session_id = %s
            ORDER BY rer.message_id, rer.relationship_id
            """,
            (normalized_message_ids, scope[0], scope[2], scope[1], *scope),
        )
        relationships_by_message = {
            message_id: [] for message_id in normalized_message_ids
        }
        for row in rows:
            relationships_by_message[int(row["message_id"])].append(
                str(row["relationship_id"])
            )
        return relationships_by_message

    async def get_episode_generation_catalog(
        self,
        message_ids: List[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> tuple[List[Dict], List[Dict]]:
        """Return the resolved entity and relationship catalogs for source messages."""

        normalized_message_ids = sorted({int(message_id) for message_id in message_ids})
        if not normalized_message_ids:
            return [], []
        if any(message_id <= 0 for message_id in normalized_message_ids):
            raise ValueError(
                "get_episode_generation_catalog requires positive message IDs"
            )
        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "get_episode_generation_catalog",
        )
        entity_rows = await self.client.fetch_all(
            """
            SELECT
                e.entity_id,
                e.canonical_name,
                e.type,
                COALESCE(
                    array_agg(DISTINCT ea.alias)
                        FILTER (WHERE ea.alias IS NOT NULL),
                    ARRAY[]::text[]
                ) AS aliases
            FROM message_entity_refs mer
            JOIN messages m ON m.message_id = mer.message_id
            JOIN entities e ON e.entity_id = mer.entity_id
            LEFT JOIN entity_aliases ea ON ea.entity_id = e.entity_id
            WHERE mer.message_id = ANY(%s)
              AND m.user_name = %s
              AND m.project_id = %s
              AND m.session_id = %s
              AND (e.project_id = %s OR e.entity_id = %s)
            GROUP BY e.entity_id, e.canonical_name, e.type
            ORDER BY e.entity_id
            """,
            (normalized_message_ids, *scope, project_id, IDENTITY_ENTITY_ID),
        )
        relationship_rows = await self.client.fetch_all(
            """
            SELECT
                r.relationship_id,
                r.entity_a_id,
                entity_a.canonical_name AS entity_a_name,
                entity_a.type AS entity_a_type,
                r.entity_b_id,
                entity_b.canonical_name AS entity_b_name,
                entity_b.type AS entity_b_type,
                r.relationship_type,
                r.confidence,
                r.context,
                array_agg(DISTINCT rer.message_id ORDER BY rer.message_id)
                    AS evidence_message_ids
            FROM relationship_evidence_refs rer
            JOIN relationships r ON r.relationship_id = rer.relationship_id
            JOIN messages m ON m.message_id = rer.message_id
            JOIN entities entity_a ON entity_a.entity_id = r.entity_a_id
            JOIN entities entity_b ON entity_b.entity_id = r.entity_b_id
            WHERE rer.message_id = ANY(%s)
              AND rer.user_name = %s
              AND rer.session_id = %s
              AND m.user_name = %s
              AND m.project_id = %s
              AND m.session_id = %s
              AND r.project_id = %s
            GROUP BY
                r.relationship_id,
                r.entity_a_id,
                entity_a.canonical_name,
                entity_a.type,
                r.entity_b_id,
                entity_b.canonical_name,
                entity_b.type,
                r.relationship_type,
                r.confidence,
                r.context
            ORDER BY r.relationship_id
            """,
            (
                normalized_message_ids,
                scope[0],
                scope[2],
                *scope,
                project_id,
            ),
        )
        return (
            [
                {
                    "entity_id": int(row["entity_id"]),
                    "canonical_name": str(row["canonical_name"]),
                    "type": row.get("type"),
                    "aliases": list(row.get("aliases") or []),
                }
                for row in entity_rows
            ],
            [
                {
                    "relationship_id": str(row["relationship_id"]),
                    "entity_a": {
                        "entity_id": int(row["entity_a_id"]),
                        "canonical_name": str(row["entity_a_name"]),
                        "type": row.get("entity_a_type"),
                    },
                    "entity_b": {
                        "entity_id": int(row["entity_b_id"]),
                        "canonical_name": str(row["entity_b_name"]),
                        "type": row.get("entity_b_type"),
                    },
                    "relationship_type": row.get("relationship_type"),
                    "confidence": float(row["confidence"]),
                    "context": row.get("context"),
                    "evidence_message_ids": [
                        int(message_id)
                        for message_id in row.get("evidence_message_ids") or []
                    ],
                }
                for row in relationship_rows
            ],
        )

    @staticmethod
    def _require_scope(
        user_name: str,
        project_id: str,
        session_id: str,
        operation: str,
    ) -> tuple[str, str, str]:
        return (
            require_scope_value(user_name, "user_name", operation),
            require_scope_value(project_id, "project_id", operation),
            require_scope_value(session_id, "session_id", operation),
        )

    @staticmethod
    def _episode_query(
        predicate: str,
        *,
        joins: str = "",
        ordering: str = "e.updated_at DESC",
        limit: bool = False,
    ) -> str:
        query = f"""
        SELECT
            e.episode_id,
            e.project_id,
            e.session_id,
            e.summary,
            e.new_developments,
            e.updates,
            e.unresolved,
            e.importance,
            e.source_message_count,
            e.first_message_at,
            e.last_message_at,
            e.embedding,
            e.generator_metadata,
            e.created_at,
            e.updated_at
        FROM episodes e
        JOIN sessions s
          ON s.session_id = e.session_id
         AND s.project_id = e.project_id
        {joins}
        WHERE {predicate}
          AND s.user_name = %s
          AND e.project_id = %s
          AND e.session_id = %s
        ORDER BY {ordering}
        """
        if limit:
            query += " LIMIT %s"
        return query

    async def _hydrate_episode(self, row: Dict) -> Episode:
        episode_id = str(row["episode_id"])
        messages = await self._load_messages(episode_id)
        entities = await self._load_entities(episode_id)
        relationships = await self._load_relationships(episode_id)
        return Episode(
            episode_id=episode_id,
            project_id=str(row["project_id"]),
            session_id=str(row["session_id"]),
            summary=str(row["summary"]),
            new_developments=self._json_list(row.get("new_developments")),
            updates=self._json_list(row.get("updates")),
            unresolved=self._json_list(row.get("unresolved")),
            importance=float(row["importance"]),
            source_message_count=int(row.get("source_message_count") or 0),
            first_message_at=row.get("first_message_at"),
            last_message_at=row.get("last_message_at"),
            embedding=self._vector_list(row.get("embedding")),
            messages=messages,
            entities=entities,
            relationships=relationships,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            generator_metadata=self._json_dict(row.get("generator_metadata")),
        )

    async def _load_messages(self, episode_id: str) -> List[MessageEpisode]:
        rows = await self.client.fetch_all(
            """
            SELECT
                message_id,
                influence_weight,
                influence_reason,
                message_position,
                attached_at
            FROM episode_messages
            WHERE episode_id = %s
            ORDER BY message_position
            """,
            (episode_id,),
        )
        return [
            MessageEpisode(
                message_id=int(row["message_id"]),
                influence_weight=float(row["influence_weight"]),
                influence_reason=row.get("influence_reason"),
                message_position=int(row["message_position"]),
                attached_at=row.get("attached_at"),
            )
            for row in rows
        ]

    async def _load_entities(self, episode_id: str) -> List[EntityEpisode]:
        rows = await self.client.fetch_all(
            """
            SELECT
                entity_id,
                prominence_weight,
                role,
                is_focus_entity,
                source_message_count,
                first_seen_at,
                last_seen_at
            FROM episode_entities
            WHERE episode_id = %s
            ORDER BY is_focus_entity DESC, prominence_weight DESC, entity_id
            """,
            (episode_id,),
        )
        return [
            EntityEpisode(
                entity_id=int(row["entity_id"]),
                prominence_weight=float(row["prominence_weight"]),
                role=row.get("role"),
                is_focus_entity=bool(row["is_focus_entity"]),
                source_message_count=int(row["source_message_count"]),
                first_seen_at=row.get("first_seen_at"),
                last_seen_at=row.get("last_seen_at"),
            )
            for row in rows
        ]

    async def _load_relationships(self, episode_id: str) -> List[RelationshipEpisode]:
        rows = await self.client.fetch_all(
            """
            SELECT
                relationship_id,
                prominence_weight,
                is_central_relationship,
                source_message_count
            FROM episode_relationships
            WHERE episode_id = %s
            ORDER BY is_central_relationship DESC, prominence_weight DESC,
                relationship_id
            """,
            (episode_id,),
        )
        return [
            RelationshipEpisode(
                relationship_id=str(row["relationship_id"]),
                prominence_weight=float(row["prominence_weight"]),
                is_central_relationship=bool(row["is_central_relationship"]),
                source_message_count=int(row["source_message_count"]),
            )
            for row in rows
        ]

    @staticmethod
    def _json_list(value) -> List[str]:
        if isinstance(value, str):
            value = json.loads(value)
        return list(value or [])

    @staticmethod
    def _json_dict(value) -> Dict:
        if isinstance(value, str):
            value = json.loads(value)
        return dict(value or {})

    @staticmethod
    def _vector_list(value) -> List[float] | None:
        if value is None:
            return None
        if isinstance(value, str):
            value = json.loads(value)
        return [float(item) for item in value]

    @staticmethod
    def _normalize_embedding(embedding: List[float]) -> List[float]:
        normalized = [float(value) for value in embedding]
        if len(normalized) != 1024:
            raise ValueError("episode embedding must contain exactly 1024 dimensions")
        if not all(math.isfinite(value) for value in normalized):
            raise ValueError("episode embedding must contain only finite values")
        return normalized
