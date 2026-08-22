import json
import math
from typing import Dict, List, Optional

from common.schema.episode.models import (
    EPISODE_EMBEDDING_DIMENSION,
    EntityEpisode,
    Episode,
    EpisodeCheckpoint,
    EpisodeVersion,
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
            joins=(
                "JOIN episode_entities ee ON ee.episode_id = e.episode_id "
                "AND ee.project_id = e.project_id"
            ),
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

        normalized_entity_ids = sorted({int(entity_id) for entity_id in entity_ids})
        if not normalized_entity_ids or limit <= 0:
            return []
        if any(entity_id <= 0 for entity_id in normalized_entity_ids):
            raise ValueError("get_episodes_for_entities requires positive entity IDs")
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
                e.summary,
                e.new_developments,
                e.updates,
                e.unresolved,
                e.importance,
                e.source_message_count,
                e.first_message_at,
                e.last_message_at,
                e.generator_metadata,
                e.version_history,
                e.created_at,
                e.updated_at,
                COUNT(DISTINCT ee.entity_id) AS entity_overlap
            FROM episodes e
            JOIN episode_entities ee
              ON ee.episode_id = e.episode_id
             AND ee.project_id = e.project_id
            WHERE ee.entity_id = ANY(%s)
              AND EXISTS (
                  SELECT 1 FROM episode_messages em
                  JOIN sessions s ON s.session_id = em.session_id
                     AND s.project_id = em.project_id
                  WHERE em.episode_id = e.episode_id
                    AND em.project_id = e.project_id
                    AND s.user_name = %s
                    AND em.project_id = %s
                    AND em.session_id = %s
              )
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

        evidence_by_entity = {entity_id: [] for entity_id in normalized_entity_ids}
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
                JOIN episodes e
                  ON e.episode_id = ee.episode_id
                 AND e.project_id = ee.project_id
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
                    JOIN episodes e
                      ON e.episode_id = em.episode_id
                     AND e.project_id = em.project_id
                    JOIN messages m
                      ON m.message_id = em.message_id
                     AND m.project_id = em.project_id
                     AND m.session_id = em.session_id
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
        rows = await self.client.fetch_all(
            """
            WITH query_terms AS (
                SELECT websearch_to_tsquery('simple', %s) AS terms
            )
            SELECT
                e.episode_id,
                e.project_id,
                e.summary,
                e.new_developments,
                e.updates,
                e.unresolved,
                e.importance,
                e.source_message_count,
                e.first_message_at,
                e.last_message_at,
                e.generator_metadata,
                e.version_history,
                e.created_at,
                e.updated_at
            FROM episodes e
            CROSS JOIN query_terms q
            WHERE EXISTS (
                SELECT 1 FROM episode_messages em
                JOIN sessions s ON s.session_id = em.session_id AND s.project_id = em.project_id
                WHERE em.episode_id = e.episode_id AND em.project_id = e.project_id
                  AND s.user_name = %s AND em.project_id = %s AND em.session_id = %s
            )
              AND e.search_tsvector @@ q.terms
            ORDER BY
                ts_rank_cd(e.search_tsvector, q.terms) DESC,
                e.importance DESC,
                e.updated_at DESC
            LIMIT %s
            """,
            (normalized_query, *scope, limit),
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
                e.version_history,
                e.created_at,
                e.updated_at,
                1 - (e.embedding <=> %s::vector) AS similarity
            FROM episodes e
            WHERE EXISTS (
                SELECT 1 FROM episode_messages em
                JOIN sessions s ON s.session_id = em.session_id AND s.project_id = em.project_id
                WHERE em.episode_id = e.episode_id AND em.project_id = e.project_id
                  AND s.user_name = %s AND em.project_id = %s AND em.session_id = %s
            )
              AND e.embedding IS NOT NULL
              AND 1 - (e.embedding <=> %s::vector) >= %s
            ORDER BY e.embedding <=> %s::vector ASC
            LIMIT %s
            """,
            (vector, *scope, vector, score_threshold, vector, limit),
        )
        return [
            (await self._hydrate_episode(row), float(row["similarity"])) for row in rows
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
        JOIN episode_messages em
          ON em.episode_id = e.episode_id
         AND em.project_id = e.project_id
        JOIN sessions s
          ON s.session_id = em.session_id
         AND s.project_id = em.project_id
        JOIN messages m
          ON m.message_id = em.message_id
         AND m.project_id = em.project_id
         AND m.session_id = em.session_id
        WHERE e.episode_id = %s
          AND s.user_name = %s
          AND e.project_id = %s
          AND em.session_id = %s
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

    async def get_episode_checkpoint(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> EpisodeCheckpoint:
        """Return the chronological cursor for a valid conversation."""

        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "get_episode_checkpoint",
        )
        row = await self.client.fetch_one(
            """
            SELECT
                COALESCE(ec.last_evaluated_message_id, 0) AS message_id,
                ec.last_evaluated_timestamp_ms
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
        return EpisodeCheckpoint(
            last_evaluated_message_id=int(row["message_id"]),
            last_evaluated_timestamp_ms=row["last_evaluated_timestamp_ms"],
        )

    async def get_next_episode_window(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        checkpoint: EpisodeCheckpoint,
        message_count: int,
    ) -> List[Dict]:
        """Load the next fully ingested window in chronological message order."""

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
                m.episode_type,
                m.episode_eligible AS is_episode_eligible
            FROM messages m
            WHERE m.user_name = %s
              AND m.project_id = %s
              AND m.session_id = %s
              AND (
                    (%s = 0 AND %s::BIGINT IS NULL)
                 OR (
                        %s::BIGINT IS NOT NULL
                    AND (
                           m.timestamp_ms > %s
                        OR (m.timestamp_ms = %s AND m.message_id > %s)
                        OR m.timestamp_ms IS NULL
                    )
                 )
                 OR (
                        %s::BIGINT IS NULL
                    AND %s > 0
                    AND m.timestamp_ms IS NULL
                    AND m.message_id > %s
                 )
              )
            ORDER BY m.timestamp_ms ASC NULLS LAST, m.message_id
            LIMIT %s
            """,
            (
                *scope,
                checkpoint.last_evaluated_message_id,
                checkpoint.last_evaluated_timestamp_ms,
                checkpoint.last_evaluated_timestamp_ms,
                checkpoint.last_evaluated_timestamp_ms,
                checkpoint.last_evaluated_timestamp_ms,
                checkpoint.last_evaluated_message_id,
                checkpoint.last_evaluated_timestamp_ms,
                checkpoint.last_evaluated_message_id,
                checkpoint.last_evaluated_message_id,
                message_count,
            ),
        )
        if len(rows) < message_count or any(
            not row["is_episode_eligible"] for row in rows
        ):
            return []
        return [
            {key: value for key, value in row.items() if key != "is_episode_eligible"}
            for row in rows
        ]

    async def get_next_project_episode_window(
        self,
        *,
        user_name: str,
        project_id: str,
        message_count: int,
    ) -> List[Dict]:
        """Merge each session's next ready stream into one project window.

        A session stops at its first non-ready message, preserving its own
        chronology.  Other sessions are intentionally independent, so a draft
        or failed claim in one cannot starve project memory from another.
        """

        if message_count <= 0:
            raise ValueError("message_count must be positive")
        rows = await self.client.fetch_all(
            """
            SELECT
                m.message_id, m.session_id, m.role, m.content, m.timestamp_ms,
                m.user_msg_id, m.lifecycle_state, m.ingestion_state,
                m.episode_eligible,
                s.episode_participation_enabled,
                s.episode_participation_after_message_id,
                parent.ingestion_state AS parent_ingestion_state,
                parent.lifecycle_state AS parent_lifecycle_state,
                COALESCE(ec.last_evaluated_message_id, 0) AS checkpoint_message_id,
                ec.last_evaluated_timestamp_ms AS checkpoint_timestamp_ms
            FROM messages m
            JOIN sessions s
              ON s.session_id = m.session_id AND s.project_id = m.project_id
            LEFT JOIN episode_processing_checkpoints ec
              ON ec.project_id = m.project_id AND ec.session_id = m.session_id
            LEFT JOIN messages parent
              ON parent.message_id = m.user_msg_id
             AND parent.project_id = m.project_id
             AND parent.session_id = m.session_id
            WHERE m.user_name = %s
              AND m.project_id = %s
              AND s.status <> 'deleted'
              AND s.episode_participation_enabled = TRUE
              AND m.message_id > s.episode_participation_after_message_id
              AND (
                    (COALESCE(ec.last_evaluated_message_id, 0) = 0
                     AND ec.last_evaluated_timestamp_ms IS NULL)
                 OR (
                    ec.last_evaluated_timestamp_ms IS NOT NULL AND (
                        m.timestamp_ms > ec.last_evaluated_timestamp_ms
                        OR (m.timestamp_ms = ec.last_evaluated_timestamp_ms
                            AND m.message_id > ec.last_evaluated_message_id)
                        OR m.timestamp_ms IS NULL
                    ))
                 OR (ec.last_evaluated_timestamp_ms IS NULL
                     AND COALESCE(ec.last_evaluated_message_id, 0) > 0
                     AND m.timestamp_ms IS NULL
                     AND m.message_id > ec.last_evaluated_message_id)
              )
            ORDER BY m.session_id, m.timestamp_ms ASC NULLS LAST, m.message_id
            """,
            (user_name, project_id),
        )
        streams: Dict[str, List[Dict]] = {}
        for row in rows:
            ready = (
                row["role"] == "user"
                and row["lifecycle_state"] == "sealed"
                and row["ingestion_state"] == "processed"
                and row["episode_eligible"]
            ) or (
                row["role"] == "assistant"
                and row["parent_lifecycle_state"] == "sealed"
                and row["parent_ingestion_state"] == "processed"
            )
            session_id = str(row["session_id"])
            if not ready:
                # The first blocked record holds only this session's stream.
                streams.setdefault(session_id, [])
                streams[session_id].append({"_blocked": True})
                continue
            if any(item.get("_blocked") for item in streams.get(session_id, [])):
                continue
            streams.setdefault(session_id, []).append(dict(row))

        bundles: List[List[Dict]] = []
        for stream in streams.values():
            index = 0
            while index < len(stream) and not stream[index].get("_blocked"):
                bundle = [stream[index]]
                user_id = int(stream[index]["message_id"])
                index += 1
                while (
                    index < len(stream)
                    and not stream[index].get("_blocked")
                    and stream[index].get("role") == "assistant"
                    and stream[index].get("user_msg_id") == user_id
                ):
                    bundle.append(stream[index])
                    index += 1
                bundles.append(bundle)
        bundles.sort(
            key=lambda bundle: (
                bundle[0].get("timestamp_ms") is None,
                bundle[0].get("timestamp_ms") or 0,
                bundle[0]["message_id"],
            )
        )
        selected: List[Dict] = []
        for bundle in bundles:
            if len(selected) >= message_count:
                break
            selected.extend(bundle)
        if len(selected) < message_count:
            return []
        return [
            {
                key: value
                for key, value in row.items()
                if key not in {"lifecycle_state", "ingestion_state", "episode_eligible", "parent_ingestion_state", "parent_lifecycle_state", "checkpoint_message_id", "checkpoint_timestamp_ms", "episode_participation_enabled", "episode_participation_after_message_id"}
            }
            for row in selected
        ]

    async def has_ready_project_episode_window(
        self,
        *,
        user_name: str,
        project_id: str,
        message_count: int,
    ) -> bool:
        """Return whether a project has one full unblocked episode window.

        This intentionally reads no message bodies.  It mirrors the
        project-window readiness rules while stopping after enough durable
        evidence has been counted.
        """

        if message_count <= 0:
            raise ValueError("message_count must be positive")
        row = await self.client.fetch_one(
            """
            WITH candidate_messages AS (
                SELECT
                    m.message_id,
                    m.session_id,
                    m.role,
                    m.timestamp_ms,
                    m.user_msg_id,
                    m.lifecycle_state,
                    m.ingestion_state,
                    m.episode_eligible,
                    parent.ingestion_state AS parent_ingestion_state,
                    parent.lifecycle_state AS parent_lifecycle_state,
                    COALESCE(ec.last_evaluated_message_id, 0) AS checkpoint_message_id,
                    ec.last_evaluated_timestamp_ms AS checkpoint_timestamp_ms
                FROM messages m
                JOIN sessions s
                  ON s.session_id = m.session_id AND s.project_id = m.project_id
                LEFT JOIN episode_processing_checkpoints ec
                  ON ec.project_id = m.project_id AND ec.session_id = m.session_id
                LEFT JOIN messages parent
                  ON parent.message_id = m.user_msg_id
                 AND parent.project_id = m.project_id
                 AND parent.session_id = m.session_id
                WHERE m.user_name = %s
                  AND m.project_id = %s
                  AND s.status <> 'deleted'
                  AND s.episode_participation_enabled = TRUE
                  AND m.message_id > s.episode_participation_after_message_id
                  AND (
                        (COALESCE(ec.last_evaluated_message_id, 0) = 0
                         AND ec.last_evaluated_timestamp_ms IS NULL)
                     OR (
                        ec.last_evaluated_timestamp_ms IS NOT NULL AND (
                            m.timestamp_ms > ec.last_evaluated_timestamp_ms
                            OR (m.timestamp_ms = ec.last_evaluated_timestamp_ms
                                AND m.message_id > ec.last_evaluated_message_id)
                            OR m.timestamp_ms IS NULL
                        )
                     )
                     OR (ec.last_evaluated_timestamp_ms IS NULL
                         AND COALESCE(ec.last_evaluated_message_id, 0) > 0
                         AND m.timestamp_ms IS NULL
                         AND m.message_id > ec.last_evaluated_message_id)
                  )
            ),
            readiness AS (
                SELECT
                    *,
                    (
                        (role = 'user'
                         AND lifecycle_state = 'sealed'
                         AND ingestion_state = 'processed'
                         AND episode_eligible)
                        OR (role = 'assistant'
                            AND parent_lifecycle_state = 'sealed'
                            AND parent_ingestion_state = 'processed')
                    ) AS is_ready
                FROM candidate_messages
            ),
            ordered_streams AS (
                SELECT
                    *,
                    SUM(CASE WHEN is_ready THEN 0 ELSE 1 END) OVER (
                        PARTITION BY session_id
                        ORDER BY timestamp_ms ASC NULLS LAST, message_id
                    ) AS blocked_count
                FROM readiness
            )
            SELECT count(*) AS ready_count
            FROM (
                SELECT 1
                FROM ordered_streams
                WHERE is_ready AND blocked_count = 0
                LIMIT %s
            ) AS bounded_ready
            """,
            (user_name, project_id, message_count),
        )
        return int(row["ready_count"] if row else 0) >= message_count

    async def get_project_episode(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        visible_project_ids: Optional[List[str]] = None,
    ) -> Episode | None:
        row = await self.client.fetch_one(
            """
            SELECT e.* FROM episodes e
            JOIN projects p ON p.project_id = e.project_id
            WHERE e.episode_id = %s AND e.project_id = ANY(%s) AND p.user_name = %s
            """,
            (episode_id, visible_project_ids or [project_id], user_name),
        )
        return await self._hydrate_episode(row) if row else None

    async def get_recent_project_episodes(
        self,
        *,
        user_name: str,
        project_id: str,
        limit: int,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[Episode]:
        rows = await self.client.fetch_all(
            """
            SELECT e.* FROM episodes e JOIN projects p ON p.project_id = e.project_id
            WHERE e.project_id = ANY(%s) AND p.user_name = %s
            ORDER BY e.updated_at DESC LIMIT %s
            """,
            (visible_project_ids or [project_id], user_name, limit),
        )
        return [await self._hydrate_episode(row) for row in rows]

    async def search_project_episodes(
        self,
        query: str,
        *,
        user_name: str,
        project_id: str,
        limit: int,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[Episode]:
        rows = await self.client.fetch_all(
            """
            WITH terms AS (SELECT websearch_to_tsquery('simple', %s) AS query)
            SELECT e.* FROM episodes e
            JOIN projects p ON p.project_id = e.project_id CROSS JOIN terms
            WHERE e.project_id = ANY(%s) AND p.user_name = %s
              AND e.search_tsvector @@ terms.query
            ORDER BY ts_rank_cd(e.search_tsvector, terms.query) DESC,
                     e.importance DESC, e.updated_at DESC LIMIT %s
            """,
            (query, visible_project_ids or [project_id], user_name, limit),
        )
        return [await self._hydrate_episode(row) for row in rows]

    async def search_project_episodes_by_embedding(
        self,
        embedding: List[float],
        *,
        user_name: str,
        project_id: str,
        limit: int,
        score_threshold: float = 0.35,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[tuple[Episode, float]]:
        vector = json.dumps(self._normalize_embedding(embedding))
        rows = await self.client.fetch_all(
            """
            SELECT e.*, 1 - (e.embedding <=> %s::vector) AS similarity
            FROM episodes e JOIN projects p ON p.project_id = e.project_id
            WHERE e.project_id = ANY(%s) AND p.user_name = %s AND e.embedding IS NOT NULL
              AND 1 - (e.embedding <=> %s::vector) >= %s
            ORDER BY e.embedding <=> %s::vector ASC LIMIT %s
            """,
            (
                vector,
                visible_project_ids or [project_id],
                user_name,
                vector,
                score_threshold,
                vector,
                limit,
            ),
        )
        return [(await self._hydrate_episode(row), float(row["similarity"])) for row in rows]

    async def get_project_episodes_for_entities(
        self,
        entity_ids: List[int],
        *,
        user_name: str,
        project_id: str,
        limit: int,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[Episode]:
        if not entity_ids:
            return []
        rows = await self.client.fetch_all(
            """
            SELECT e.*, COUNT(DISTINCT ee.entity_id) AS entity_overlap
            FROM episodes e
            JOIN projects p ON p.project_id = e.project_id
            JOIN episode_entities ee ON ee.episode_id = e.episode_id AND ee.project_id = e.project_id
            WHERE e.project_id = ANY(%s) AND p.user_name = %s AND ee.entity_id = ANY(%s)
              AND e.user_modified = FALSE
            GROUP BY e.episode_id
            ORDER BY entity_overlap DESC, e.updated_at DESC LIMIT %s
            """,
            (visible_project_ids or [project_id], user_name, entity_ids, limit),
        )
        return [await self._hydrate_episode(row) for row in rows]

    async def get_project_episode_source_messages(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[Dict]:
        return await self.client.fetch_all(
            """
            SELECT m.message_id, m.session_id, m.role, m.content, m.timestamp_ms,
                   em.influence_weight, em.influence_reason, em.message_position,
                   em.attached_at
            FROM episodes e
            JOIN projects p ON p.project_id = e.project_id
            JOIN episode_messages em ON em.episode_id = e.episode_id AND em.project_id = e.project_id
            JOIN messages m ON m.message_id = em.message_id AND m.project_id = em.project_id
                           AND m.session_id = em.session_id
            WHERE e.episode_id = %s AND e.project_id = ANY(%s) AND p.user_name = %s
            ORDER BY em.message_position
            """,
            (episode_id, visible_project_ids or [project_id], user_name),
        )

    async def get_relationship_ids_for_messages(
        self,
        message_ids: List[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> Dict[int, List[str]]:
        """Return canonical relationship evidence attached to source messages."""

        normalized_message_ids = sorted({int(message_id) for message_id in message_ids})
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
            FROM relationship_observations rer
            JOIN relationships r
              ON r.relationship_id = rer.relationship_id
             AND r.project_id = rer.project_id
            JOIN messages m
              ON m.user_name = rer.user_name
             AND m.session_id = rer.session_id
             AND m.message_id = rer.message_id
             AND m.project_id = rer.project_id
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
                MAX(rer.confidence) AS confidence,
                (array_agg(rer.context ORDER BY rer.observed_at_ms DESC)
                    FILTER (WHERE rer.context IS NOT NULL))[1] AS context,
                array_agg(DISTINCT rer.message_id ORDER BY rer.message_id)
                    AS evidence_message_ids
            FROM relationship_observations rer
            JOIN relationships r
              ON r.relationship_id = rer.relationship_id
             AND r.project_id = rer.project_id
            JOIN messages m
              ON m.user_name = rer.user_name
             AND m.session_id = rer.session_id
             AND m.message_id = rer.message_id
             AND m.project_id = rer.project_id
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
                r.relationship_type
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
            e.version_history,
            e.created_at,
            e.updated_at
        FROM episodes e
        {joins}
        WHERE {predicate}
          AND EXISTS (
              SELECT 1
              FROM episode_messages em
              JOIN sessions s
                ON s.session_id = em.session_id
               AND s.project_id = em.project_id
              WHERE em.episode_id = e.episode_id
                AND em.project_id = e.project_id
                AND s.user_name = %s
                AND e.project_id = %s
                AND em.session_id = %s
          )
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
            version_history=self._episode_version_list(row.get("version_history")),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            generator_metadata=self._json_dict(row.get("generator_metadata")),
            user_modified=bool(row.get("user_modified", False)),
        )

    async def _load_messages(self, episode_id: str) -> List[MessageEpisode]:
        rows = await self.client.fetch_all(
            """
            SELECT
                message_id,
                session_id,
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
                session_id=str(row["session_id"]),
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
    def _episode_version_list(value) -> List[EpisodeVersion]:
        return [
            EpisodeVersion.model_validate(item)
            for item in EpisodeReader._json_list(value)
        ]

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
        if len(normalized) != EPISODE_EMBEDDING_DIMENSION:
            raise ValueError(
                "episode embedding must contain exactly "
                f"{EPISODE_EMBEDDING_DIMENSION} dimensions"
            )
        if not all(math.isfinite(value) for value in normalized):
            raise ValueError("episode embedding must contain only finite values")
        return normalized
