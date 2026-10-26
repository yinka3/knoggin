import json
from datetime import datetime, timezone
from functools import wraps
from typing import Dict, List

from loguru import logger
from psycopg import Error as PsycopgError

from common.exceptions import EpisodeEditConflictError, StorageWriteError
from common.schema.episode.models import Episode
from common.scoping import require_scope_value
from infrastructure.postgres_client import PostgresClient


def _storage_write(operation: str):
    """Translate infrastructure failures without hiding contract violations."""

    def decorate(method):
        @wraps(method)
        async def wrapped(self, *args, **kwargs):
            try:
                return await method(self, *args, **kwargs)
            except (StorageWriteError, TypeError, ValueError):
                raise
            except PsycopgError as exc:
                self._raise_storage_write(operation, exc)

        return wrapped

    return decorate


class EpisodeWriter:
    """Persists an episode with all graph context derived from source messages."""

    def __init__(self, client: PostgresClient) -> None:
        self.client = client

    @staticmethod
    def _raise_storage_write(operation: str, exc: Exception) -> None:
        logger.error("Storage write failed for {}: {}", operation, exc)
        raise StorageWriteError(
            operation,
            details={"error_type": type(exc).__name__},
        ) from exc

    @_storage_write("write_project_semantic_window_episodes")
    async def write_project_semantic_window_episodes(
        self,
        *,
        window_id: str,
        episodes: List[Episode],
        window_messages: List[Dict],
        user_name: str,
        project_id: str,
    ) -> bool:
        """Record a semantic result without legacy cursors or graph enrichment.

        ``False`` means a prior transaction already recorded the result. An
        empty episode list is a durable successful zero-result, not a missing
        evaluation.
        """

        user_name = require_scope_value(
            user_name, "user_name", "write_project_semantic_window_episodes"
        )
        project_id = require_scope_value(
            project_id, "project_id", "write_project_semantic_window_episodes"
        )
        if any(episode.project_id != project_id for episode in episodes):
            raise ValueError("Semantic episode result must match the window project")
        if len({episode.episode_id for episode in episodes}) != len(episodes):
            raise ValueError("Semantic episode result cannot repeat episode identities")

        async with self.client.transaction() as cur:
            await cur.execute(
                """
                SELECT origin, episode_result_recorded
                FROM public.project_semantic_windows
                WHERE window_id = %s
                  AND user_name = %s
                  AND project_id = %s
                  AND stage = 'claimed'
                FOR UPDATE
                """,
                (window_id, user_name, project_id),
            )
            window = await cur.fetchone()
            if window is None:
                raise ValueError("Semantic episode result requires a claimed window")
            if bool(window["episode_result_recorded"]):
                return False

            await cur.execute(
                """
                SELECT message_id, session_id
                FROM public.project_semantic_window_messages
                WHERE window_id = %s AND project_id = %s
                ORDER BY ordinal
                """,
                (window_id, project_id),
            )
            expected_membership = [
                (int(row["message_id"]), str(row["session_id"]))
                for row in await cur.fetchall()
            ]
            supplied_membership = [
                (int(message["message_id"]), str(message["session_id"]))
                for message in window_messages
            ]
            if supplied_membership != expected_membership:
                raise ValueError("Semantic episode result must use frozen window membership")
            if window["origin"] == "conversation" and not expected_membership:
                raise ValueError("Conversation semantic windows require source messages")
            if window["origin"] == "human_edit" and (expected_membership or episodes):
                raise ValueError("Human-edit semantic windows have no episode result")

            window_sources = set(expected_membership)
            for ordinal, episode in enumerate(episodes):
                episode_sources = {
                    (message.message_id, message.session_id)
                    for message in episode.messages
                }
                if not episode_sources:
                    raise ValueError("Episode result must include semantic-window evidence")
                if not episode_sources.issubset(window_sources):
                    raise ValueError(
                        "Episode source evidence must come from the semantic window"
                    )
                await self._write_semantic_episode(
                    cur,
                    episode,
                    user_name=user_name,
                )
                await cur.execute(
                    """
                    INSERT INTO public.project_semantic_window_episodes (
                        window_id, project_id, episode_id, ordinal
                    ) VALUES (%s, %s, %s, %s)
                    """,
                    (window_id, project_id, episode.episode_id, ordinal),
                )
            await cur.execute(
                """
                UPDATE public.project_semantic_windows
                SET episode_result_recorded = TRUE,
                    attempt_count = 0,
                    last_failure_stage = NULL,
                    last_failure_code = NULL,
                    last_failure_at_ms = NULL,
                    last_error_summary = NULL,
                    next_retry_at_ms = NULL,
                    updated_at = NOW()
                WHERE window_id = %s AND project_id = %s
                """,
                (window_id, project_id),
            )
        return True

    @_storage_write("enrich_project_semantic_window_episodes")
    async def enrich_project_semantic_window_episodes(
        self,
        *,
        window_id: str,
        user_name: str,
        project_id: str,
    ) -> dict[str, int]:
        """Attach Context-committed graph evidence to this window's Episodes.

        Episode narration is intentionally written before Knowledge. This
        idempotent post-Knowledge pass joins only the frozen Episode messages to
        active Context block supports, so a retry cannot manufacture duplicate
        episode links or revive retired relationship evidence.
        """

        user_name = require_scope_value(
            user_name, "user_name", "enrich_project_semantic_window_episodes"
        )
        project_id = require_scope_value(
            project_id, "project_id", "enrich_project_semantic_window_episodes"
        )
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                SELECT context_revision_id
                FROM public.project_semantic_windows
                WHERE window_id = %s
                  AND user_name = %s
                  AND project_id = %s
                  AND stage IN ('knowledge_committed', 'completed')
                FOR UPDATE
                """,
                (window_id, user_name, project_id),
            )
            window = await cur.fetchone()
            if window is None:
                raise ValueError("Episode enrichment requires a Knowledge-committed window")
            revision_id = window["context_revision_id"]
            if revision_id is None:
                raise RuntimeError("Knowledge-committed window has no Context revision")

            await cur.execute(
                """
                WITH links AS (
                    SELECT episode.episode_id,
                           block_entity.entity_id,
                           count(DISTINCT support.message_id)::integer AS source_count,
                           min(message.timestamp_ms) AS first_timestamp_ms,
                           max(message.timestamp_ms) AS last_timestamp_ms,
                           bool_and(
                               block_entity.occurrence_provenance_complete
                               AND EXISTS (
                                   SELECT 1
                                   FROM public.context_block_entity_occurrences AS occurrence
                                   WHERE occurrence.block_id = block_entity.block_id
                                     AND occurrence.project_id = block_entity.project_id
                                     AND coalesce(occurrence.corrected_entity_id,
                                                  occurrence.resolved_entity_id)
                                         = block_entity.entity_id
                               )
                           ) AS provenance_complete
                    FROM public.project_semantic_window_episodes AS membership
                    JOIN public.episodes AS episode
                      ON episode.episode_id = membership.episode_id
                     AND episode.project_id = membership.project_id
                    JOIN public.episode_messages AS episode_message
                      ON episode_message.episode_id = episode.episode_id
                     AND episode_message.project_id = episode.project_id
                    JOIN public.messages AS message
                      ON message.message_id = episode_message.message_id
                     AND message.project_id = episode_message.project_id
                     AND message.session_id = episode_message.session_id
                    JOIN public.project_context_block_supports AS support
                      ON support.project_id = message.project_id
                     AND support.message_id = message.message_id
                     AND support.session_id = message.session_id
                    JOIN public.project_context_revision_blocks AS revision_block
                      ON revision_block.revision_id = %s
                     AND revision_block.project_id = support.project_id
                     AND revision_block.block_id = support.block_id
                    JOIN public.context_block_entities AS block_entity
                      ON block_entity.project_id = support.project_id
                     AND block_entity.block_id = support.block_id
                    WHERE membership.window_id = %s
                      AND membership.project_id = %s
                    GROUP BY episode.episode_id, block_entity.entity_id
                )
                INSERT INTO public.episode_entities (
                    episode_id, project_id, entity_id, source_message_count,
                    first_seen_at, last_seen_at, occurrence_provenance_complete
                )
                SELECT episode_id,
                       %s,
                       entity_id,
                       source_count,
                       to_timestamp(first_timestamp_ms / 1000.0),
                       to_timestamp(last_timestamp_ms / 1000.0),
                       provenance_complete
                FROM links
                ON CONFLICT (episode_id, entity_id) DO UPDATE
                SET source_message_count = GREATEST(
                        episode_entities.source_message_count,
                        EXCLUDED.source_message_count
                    ),
                    first_seen_at = LEAST(
                        episode_entities.first_seen_at, EXCLUDED.first_seen_at
                    ),
                    last_seen_at = GREATEST(
                        episode_entities.last_seen_at, EXCLUDED.last_seen_at
                    ),
                    occurrence_provenance_complete =
                        episode_entities.occurrence_provenance_complete
                        AND EXCLUDED.occurrence_provenance_complete
                """,
                (revision_id, window_id, project_id, project_id),
            )
            entity_links = cur.rowcount
            await cur.execute(
                """INSERT INTO public.episode_entity_occurrences
                   (episode_id, project_id, window_id, pass_number, occurrence_index,
                    block_id, message_id, resolved_entity_id)
                   SELECT DISTINCT membership.episode_id, membership.project_id,
                          occurrence.window_id, occurrence.pass_number,
                          occurrence.occurrence_index, occurrence.block_id,
                          support.message_id, occurrence.resolved_entity_id
                   FROM public.project_semantic_window_episodes AS membership
                   JOIN public.episode_messages AS episode_message
                     ON episode_message.episode_id = membership.episode_id
                    AND episode_message.project_id = membership.project_id
                   JOIN public.project_context_block_supports AS support
                     ON support.project_id = episode_message.project_id
                    AND support.message_id = episode_message.message_id
                    AND support.session_id = episode_message.session_id
                   JOIN public.project_context_revision_blocks AS revision_block
                     ON revision_block.revision_id = %s
                    AND revision_block.project_id = support.project_id
                    AND revision_block.block_id = support.block_id
                   JOIN public.context_block_entity_occurrences AS occurrence
                     ON occurrence.block_id = support.block_id
                    AND occurrence.project_id = support.project_id
                   JOIN public.context_block_entities AS association
                     ON association.block_id = occurrence.block_id
                    AND association.project_id = occurrence.project_id
                    AND association.entity_id = coalesce(
                        occurrence.corrected_entity_id, occurrence.resolved_entity_id)
                   WHERE membership.window_id = %s AND membership.project_id = %s
                   ON CONFLICT DO NOTHING""",
                (revision_id, window_id, project_id),
            )
            # Rebuild this window's relationship links from active, complete
            # observations. Earlier passes may have linked partial or since-
            # retired evidence; an upsert alone cannot remove those links.
            await cur.execute(
                """
                DELETE FROM public.episode_relationships AS link
                USING public.project_semantic_window_episodes AS membership
                WHERE membership.window_id = %s
                  AND membership.project_id = %s
                  AND link.episode_id = membership.episode_id
                  AND link.project_id = membership.project_id
                """,
                (window_id, project_id),
            )
            await cur.execute(
                """
                WITH matching_support AS (
                    SELECT episode.episode_id,
                           observation.observation_id,
                           observation.relationship_id,
                           support.block_id,
                           support.message_id
                    FROM public.project_semantic_window_episodes AS membership
                    JOIN public.episodes AS episode
                      ON episode.episode_id = membership.episode_id
                     AND episode.project_id = membership.project_id
                    JOIN public.episode_messages AS episode_message
                      ON episode_message.episode_id = episode.episode_id
                     AND episode_message.project_id = episode.project_id
                    JOIN public.project_context_block_supports AS support
                      ON support.project_id = episode_message.project_id
                     AND support.message_id = episode_message.message_id
                     AND support.session_id = episode_message.session_id
                    JOIN public.project_context_revision_blocks AS revision_block
                      ON revision_block.revision_id = %s
                     AND revision_block.project_id = support.project_id
                     AND revision_block.block_id = support.block_id
                    JOIN public.relationship_observation_blocks AS observation_block
                      ON observation_block.project_id = support.project_id
                     AND observation_block.block_id = support.block_id
                    JOIN public.relationship_observations AS observation
                      ON observation.observation_id = observation_block.observation_id
                     AND observation.project_id = observation_block.project_id
                     AND observation.retired_at IS NULL
                     AND observation.relationship_id IS NOT NULL
                    WHERE membership.window_id = %s
                      AND membership.project_id = %s
                ),
                complete_observations AS (
                    SELECT episode_id, observation_id, relationship_id
                    FROM matching_support
                    GROUP BY episode_id, observation_id, relationship_id
                    HAVING count(DISTINCT block_id) = (
                        SELECT count(*)
                        FROM public.relationship_observation_blocks AS required_block
                        WHERE required_block.observation_id = matching_support.observation_id
                    )
                ),
                links AS (
                    SELECT complete.episode_id,
                           complete.relationship_id,
                           count(DISTINCT support.message_id)::integer AS source_count
                    FROM complete_observations AS complete
                    JOIN matching_support AS support
                      ON support.episode_id = complete.episode_id
                     AND support.observation_id = complete.observation_id
                    GROUP BY complete.episode_id, complete.relationship_id
                )
                INSERT INTO public.episode_relationships (
                    episode_id, project_id, relationship_id, source_message_count
                )
                SELECT episode_id, %s, relationship_id, source_count
                FROM links
                ON CONFLICT (episode_id, relationship_id) DO UPDATE
                SET source_message_count = GREATEST(
                    episode_relationships.source_message_count,
                    EXCLUDED.source_message_count
                )
                """,
                (revision_id, window_id, project_id, project_id),
            )
            relationship_links = cur.rowcount
        return {"entities": entity_links, "relationships": relationship_links}

    @_storage_write("edit_episode")
    async def edit_episode(
        self,
        *,
        episode_id: str,
        user_name: str,
        project_id: str,
        summary: str,
        new_developments: List[str],
        updates: List[str],
        unresolved: List[str],
        embedding: List[float],
        expected_updated_at: datetime,
    ) -> datetime:
        """Atomically apply a user-owned narrative and vector edit."""

        episode_id = require_scope_value(episode_id, "episode_id", "edit_episode")
        user_name = require_scope_value(user_name, "user_name", "edit_episode")
        project_id = require_scope_value(project_id, "project_id", "edit_episode")
        if not isinstance(summary, str) or not summary.strip():
            raise ValueError("Episode summary must not be blank")
        if not isinstance(expected_updated_at, datetime):
            raise TypeError("expected_updated_at must be a datetime")
        if (
            expected_updated_at.tzinfo is None
            or expected_updated_at.utcoffset() is None
        ):
            raise ValueError("expected_updated_at must be timezone-aware")
        narrative_lists = (new_developments, updates, unresolved)
        if not all(isinstance(values, list) for values in narrative_lists) or not all(
            isinstance(value, str) for values in narrative_lists for value in values
        ):
            raise TypeError("Episode narrative lists must contain only strings")
        validated_embedding = Episode.validate_embedding(embedding)
        if validated_embedding is None:
            raise ValueError("Episode edit requires an embedding")

        normalized_summary = summary.strip()
        normalized_new_developments = [
            value.strip() for value in new_developments if value.strip()
        ]
        normalized_updates = [value.strip() for value in updates if value.strip()]
        normalized_unresolved = [value.strip() for value in unresolved if value.strip()]
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE episodes AS episode
                SET summary = %s,
                    new_developments = %s::jsonb,
                    updates = %s::jsonb,
                    unresolved = %s::jsonb,
                    embedding = %s::vector,
                    user_modified = TRUE,
                    updated_at = NOW()
                WHERE episode.episode_id = %s
                  AND episode.project_id = %s
                  AND episode.updated_at = %s
                  AND EXISTS (
                      SELECT 1
                      FROM projects AS project
                      WHERE project.project_id = episode.project_id
                        AND project.user_name = %s
                        AND project.status = 'active'
                  )
                RETURNING episode.updated_at
                """,
                (
                    normalized_summary,
                    json.dumps(normalized_new_developments),
                    json.dumps(normalized_updates),
                    json.dumps(normalized_unresolved),
                    json.dumps(validated_embedding),
                    episode_id,
                    project_id,
                    expected_updated_at,
                    user_name,
                ),
            )
            edited = await cur.fetchone()
            if edited is None:
                raise EpisodeEditConflictError()
            updated_at = edited.get("updated_at")
            if not isinstance(updated_at, datetime):
                raise RuntimeError("Episode edit did not return its revision timestamp")
        return updated_at

    async def _write_semantic_episode(self, cur, episode: Episode, *, user_name: str) -> None:
        """Persist narrative and canonical sources only; Knowledge owns graph links."""

        await cur.execute(
            "SELECT episode_id FROM episodes WHERE episode_id = %s FOR UPDATE",
            (episode.episode_id,),
        )
        if await cur.fetchone() is not None:
            raise ValueError("Finalized episode identities cannot be reused")
        source_ids = [item.message_id for item in episode.messages]
        expected_sessions = {item.message_id: item.session_id for item in episode.messages}
        await cur.execute(
            """
            SELECT message_id, session_id, timestamp_ms
            FROM messages
            WHERE user_name = %s AND project_id = %s AND message_id = ANY(%s)
            """,
            (user_name, episode.project_id, source_ids),
        )
        rows = await cur.fetchall()
        if {int(row["message_id"]) for row in rows} != set(source_ids) or any(
            str(row["session_id"]) != expected_sessions[int(row["message_id"])]
            for row in rows
        ):
            raise ValueError("Episode source messages must exist in their recorded sessions")
        timestamps = {int(row["message_id"]): row.get("timestamp_ms") for row in rows}
        await self._insert_semantic_episode(cur, episode, timestamps)
        await self._insert_messages(cur, episode)

    @staticmethod
    async def _insert_semantic_episode(
        cur,
        episode: Episode,
        source_message_timestamps: Dict[int, int | None],
    ) -> None:
        timestamps = [
            timestamp
            for timestamp in source_message_timestamps.values()
            if timestamp is not None
        ]
        first_message_at = (
            datetime.fromtimestamp(min(timestamps) / 1000, tz=timezone.utc)
            if timestamps
            else None
        )
        last_message_at = (
            datetime.fromtimestamp(max(timestamps) / 1000, tz=timezone.utc)
            if timestamps
            else None
        )
        await cur.execute(
            """
            INSERT INTO episodes (
                episode_id,
                project_id,
                summary,
                new_developments,
                updates,
                unresolved,
                source_message_count,
                first_message_at,
                last_message_at,
                embedding,
                generator_metadata,
                user_modified,
                created_at,
                updated_at
            )
            VALUES (
                %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb,
                %s, %s, %s, %s::vector, %s::jsonb, %s, %s, %s
            )
            ON CONFLICT (episode_id) DO NOTHING
            RETURNING episode_id
            """,
            (
                episode.episode_id,
                episode.project_id,
                episode.summary,
                json.dumps(episode.new_developments),
                json.dumps(episode.updates),
                json.dumps(episode.unresolved),
                len(source_message_timestamps),
                first_message_at,
                last_message_at,
                (
                    json.dumps(episode.embedding)
                    if episode.embedding is not None
                    else None
                ),
                json.dumps(episode.generator_metadata),
                episode.user_modified,
                episode.created_at,
                episode.updated_at,
            ),
        )
        if await cur.fetchone() is None:
            raise ValueError("Finalized episode identities cannot be reused")

    @staticmethod
    async def _insert_messages(cur, episode: Episode) -> None:
        for message in episode.messages:
            await cur.execute(
                """
                INSERT INTO episode_messages (
                    episode_id,
                    project_id,
                    session_id,
                    message_id,
                    message_position
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    episode.episode_id,
                    episode.project_id,
                    message.session_id,
                    message.message_id,
                    message.message_position,
                ),
            )
