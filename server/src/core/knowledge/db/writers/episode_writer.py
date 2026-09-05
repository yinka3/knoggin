import json
from datetime import datetime, timezone
from functools import wraps
from typing import Dict, List, Set

from loguru import logger
from psycopg import Error as PsycopgError

from common.exceptions import StorageWriteError
from common.schema.episode.models import (
    EntityEpisode,
    Episode,
    EpisodeCheckpoint,
    RelationshipEpisode,
)
from common.scoping import IDENTITY_ENTITY_ID, require_scope_value
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

    @_storage_write("write_project_episode_window")
    async def write_project_episode_window(
        self,
        episodes: List[Episode],
        window_messages: List[Dict],
        *,
        user_name: str,
        project_id: str,
    ) -> bool:
        """Commit a project window and all affected session cursors together."""

        user_name = require_scope_value(user_name, "user_name", "write_project_episode_window")
        project_id = require_scope_value(project_id, "project_id", "write_project_episode_window")
        if not window_messages:
            raise ValueError("Project episode window requires source messages")
        by_session: Dict[str, List[Dict]] = {}
        for message in window_messages:
            session_id = require_scope_value(str(message.get("session_id") or ""), "session_id", "write_project_episode_window")
            by_session.setdefault(session_id, []).append(message)
        if any(episode.project_id != project_id for episode in episodes):
            raise ValueError("Project episode scope must match its checkpoint scope")

        async with self.client.transaction() as cur:
            checkpoints: Dict[str, EpisodeCheckpoint] = {}
            for session_id in sorted(by_session):
                checkpoints[session_id] = await self._lock_checkpoint(
                    cur, user_name=user_name, project_id=project_id, session_id=session_id
                )
            already_checkpointed = []
            for session_id, messages in by_session.items():
                already_checkpointed.append(
                    await self._window_is_checkpointed(
                        cur,
                        [int(message["message_id"]) for message in messages],
                        checkpoint=checkpoints[session_id],
                        user_name=user_name,
                        project_id=project_id,
                        session_id=session_id,
                    )
                )
            if all(already_checkpointed):
                return False
            next_checkpoints: Dict[str, EpisodeCheckpoint] = {}
            for session_id, messages in by_session.items():
                next_checkpoints[session_id] = await self._validate_project_session_window(
                    cur,
                    messages,
                    checkpoint=checkpoints[session_id],
                    user_name=user_name,
                    project_id=project_id,
                    session_id=session_id,
                )
            for episode in episodes:
                await self._write_project_episode(cur, episode, user_name=user_name)
            for session_id, checkpoint in next_checkpoints.items():
                await cur.execute(
                    """
                    UPDATE episode_processing_checkpoints
                    SET last_evaluated_message_id = %s,
                        last_evaluated_timestamp_ms = %s,
                        updated_at = NOW()
                    WHERE project_id = %s AND session_id = %s
                    """,
                    (checkpoint.last_evaluated_message_id, checkpoint.last_evaluated_timestamp_ms, project_id, session_id),
                )
        return True

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

            window_message_ids = {message_id for message_id, _ in expected_membership}
            for ordinal, episode in enumerate(episodes):
                source_message_ids = {message.message_id for message in episode.messages}
                if not source_message_ids.intersection(window_message_ids):
                    raise ValueError("Episode result must include semantic-window evidence")
                action = episode.generator_metadata.get("decision_action")
                if action != "consolidate" and not source_message_ids.issubset(
                    window_message_ids
                ):
                    raise ValueError(
                        "New episode source evidence must come from the semantic window"
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
                FOR KEY SHARE
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
                           max(message.timestamp_ms) AS last_timestamp_ms
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
                    first_seen_at, last_seen_at
                )
                SELECT episode_id,
                       %s,
                       entity_id,
                       source_count,
                       to_timestamp(first_timestamp_ms / 1000.0),
                       to_timestamp(last_timestamp_ms / 1000.0)
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
                    )
                """,
                (revision_id, window_id, project_id, project_id),
            )
            entity_links = cur.rowcount
            await cur.execute(
                """
                WITH links AS (
                    SELECT episode.episode_id,
                           observation.relationship_id,
                           count(DISTINCT support.message_id)::integer AS source_count
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
                    GROUP BY episode.episode_id, observation.relationship_id
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
    ) -> None:
        """Apply a user-owned narrative edit without changing source evidence."""

        if not summary.strip():
            raise ValueError("Episode summary must not be blank")
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE episodes AS episode
                SET summary = %s,
                    new_developments = %s::jsonb,
                    updates = %s::jsonb,
                    unresolved = %s::jsonb,
                    user_modified = TRUE,
                    updated_at = NOW()
                WHERE episode.episode_id = %s
                  AND episode.project_id = %s
                  AND EXISTS (
                      SELECT 1
                      FROM projects AS project
                      WHERE project.project_id = episode.project_id
                        AND project.user_name = %s
                  )
                RETURNING episode_id
                """,
                (
                    summary.strip(),
                    json.dumps(new_developments),
                    json.dumps(updates),
                    json.dumps(unresolved),
                    episode_id,
                    project_id,
                    user_name,
                ),
            )
            if await cur.fetchone() is None:
                raise ValueError("Episode is unavailable for editing")
        return None

    async def _write_project_episode(self, cur, episode: Episode, *, user_name: str) -> None:
        await cur.execute(
            """
            SELECT user_modified FROM episodes
            WHERE episode_id = %s AND project_id = %s FOR UPDATE
            """,
            (episode.episode_id, episode.project_id),
        )
        existing = await cur.fetchone()
        if existing is not None and bool(existing["user_modified"]):
            raise ValueError("User-modified episodes cannot be regenerated automatically")
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
            str(row["session_id"]) != expected_sessions[int(row["message_id"])] for row in rows
        ):
            raise ValueError("Episode source messages must exist in their recorded sessions")
        timestamps = {int(row["message_id"]): row.get("timestamp_ms") for row in rows}
        entities = await self._load_entities_by_message(cur, source_ids, project_id=episode.project_id)
        relationships = await self._load_project_relationships(cur, source_ids, project_id=episode.project_id)
        self._validate_ranked_context(episode.entities, episode.relationships, entities, relationships)
        await self._upsert_episode(
            cur,
            episode,
            timestamps,
        )
        # Reconcile all derived membership rows from the final canonical
        # source set.  Upserts alone would leave stale entity/relationship or
        # source-message rows after a successful consolidation.
        for table in ("episode_messages", "episode_entities", "episode_relationships"):
            await cur.execute(
                f"DELETE FROM {table} WHERE episode_id = %s AND project_id = %s",
                (episode.episode_id, episode.project_id),
            )
        await self._upsert_messages(cur, episode)
        await self._upsert_entities(cur, episode, entities, timestamps)
        await self._upsert_relationships(cur, episode, relationships)

    async def _write_semantic_episode(self, cur, episode: Episode, *, user_name: str) -> None:
        """Persist narrative and canonical sources only; Knowledge owns graph links."""

        await cur.execute(
            """
            SELECT user_modified FROM episodes
            WHERE episode_id = %s AND project_id = %s FOR UPDATE
            """,
            (episode.episode_id, episode.project_id),
        )
        existing = await cur.fetchone()
        if existing is not None and bool(existing["user_modified"]):
            raise ValueError("User-modified episodes cannot be regenerated automatically")
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
        await self._upsert_episode(cur, episode, timestamps)
        for table in ("episode_messages", "episode_entities", "episode_relationships"):
            await cur.execute(
                f"DELETE FROM {table} WHERE episode_id = %s AND project_id = %s",
                (episode.episode_id, episode.project_id),
            )
        await self._upsert_messages(cur, episode)

    @staticmethod
    async def _validate_project_session_window(
        cur, messages: List[Dict], *, checkpoint: EpisodeCheckpoint,
        user_name: str, project_id: str, session_id: str,
    ) -> EpisodeCheckpoint:
        """Reject a stale or partial completed-turn range before any writes."""

        if len(messages) % 2:
            raise ValueError("Project episode windows require complete turns")
        await cur.execute(
            """
            SELECT user_message.message_id AS user_message_id,
                   user_message.timestamp_ms AS user_timestamp_ms,
                   user_message.lifecycle_state AS user_lifecycle_state,
                   user_message.ingestion_state AS user_ingestion_state,
                   assistant_message.message_id AS assistant_message_id,
                   assistant_message.timestamp_ms AS assistant_timestamp_ms,
                   assistant_message.lifecycle_state AS assistant_lifecycle_state,
                   assistant_message.ingestion_state AS assistant_ingestion_state,
                   s.episode_participation_enabled,
                   s.episode_participation_after_message_id
            FROM messages AS user_message
            JOIN sessions AS s
              ON s.session_id = user_message.session_id
             AND s.project_id = user_message.project_id
            LEFT JOIN messages AS assistant_message
              ON assistant_message.user_name = user_message.user_name
             AND assistant_message.project_id = user_message.project_id
             AND assistant_message.session_id = user_message.session_id
             AND assistant_message.user_msg_id = user_message.message_id
             AND assistant_message.role = 'assistant'
            WHERE user_message.user_name = %s
              AND user_message.project_id = %s
              AND user_message.session_id = %s
              AND user_message.role = 'user'
              AND s.status <> 'deleted'
              AND ((%s = 0 AND %s::BIGINT IS NULL)
                OR (%s::BIGINT IS NOT NULL AND (user_message.timestamp_ms > %s
                    OR (user_message.timestamp_ms = %s
                        AND user_message.message_id > %s)
                    OR user_message.timestamp_ms IS NULL))
                OR (%s::BIGINT IS NULL AND %s > 0
                    AND user_message.timestamp_ms IS NULL
                    AND user_message.message_id > %s))
            ORDER BY user_message.timestamp_ms ASC NULLS LAST,
                     user_message.message_id,
                     assistant_message.timestamp_ms ASC NULLS LAST,
                     assistant_message.message_id
            LIMIT %s
            """,
            (
                user_name,
                project_id,
                session_id,
                checkpoint.last_evaluated_message_id,
                checkpoint.last_evaluated_timestamp_ms,
                checkpoint.last_evaluated_timestamp_ms,
                checkpoint.last_evaluated_timestamp_ms,
                checkpoint.last_evaluated_timestamp_ms,
                checkpoint.last_evaluated_message_id,
                checkpoint.last_evaluated_timestamp_ms,
                checkpoint.last_evaluated_message_id,
                checkpoint.last_evaluated_message_id,
                len(messages) // 2,
            ),
        )
        rows = await cur.fetchall()
        expected_ids = [int(item["message_id"]) for item in messages]
        actual_ids = [
            message_id
            for row in rows
            for message_id in (
                int(row["user_message_id"]),
                int(row["assistant_message_id"])
                if row["assistant_message_id"] is not None else -1,
            )
        ]
        if actual_ids != expected_ids:
            raise ValueError("Project episode window is stale or not a next session range")
        for row in rows:
            if (
                not row["episode_participation_enabled"]
                or int(row["user_message_id"])
                <= int(row["episode_participation_after_message_id"])
            ):
                raise ValueError("Project episode window includes an excluded session message")
            ready = (
                row["user_lifecycle_state"] == "sealed"
                and row["user_ingestion_state"] == "processed"
                and row["assistant_message_id"] is not None
                and row["assistant_lifecycle_state"] == "sealed"
                and row["assistant_ingestion_state"] == "excluded"
            )
            if not ready:
                raise ValueError("Project episode window includes a non-ready message")
        last = rows[-1]
        return EpisodeCheckpoint(
            last_evaluated_message_id=int(last["assistant_message_id"]),
            last_evaluated_timestamp_ms=last["assistant_timestamp_ms"],
        )

    @staticmethod
    async def _load_project_relationships(cur, message_ids: List[int], *, project_id: str) -> Dict[int, Set[str]]:
        await cur.execute(
            """
            SELECT rer.message_id, rer.relationship_id
            FROM relationship_observations rer
            JOIN relationships r ON r.relationship_id = rer.relationship_id AND r.project_id = rer.project_id
            WHERE rer.message_id = ANY(%s) AND rer.project_id = %s
            """,
            (message_ids, project_id),
        )
        values = {message_id: set() for message_id in message_ids}
        for row in await cur.fetchall():
            values[int(row["message_id"])].add(str(row["relationship_id"]))
        return values

    @staticmethod
    async def _lock_checkpoint(
        cur,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> EpisodeCheckpoint:
        await cur.execute(
            """
            SELECT session_id
            FROM sessions
            WHERE user_name = %s
              AND project_id = %s
              AND session_id = %s
            FOR UPDATE
            """,
            (user_name, project_id, session_id),
        )
        if await cur.fetchone() is None:
            raise ValueError("Episode checkpoint requires an existing session")
        await cur.execute(
            """
            INSERT INTO episode_processing_checkpoints (project_id, session_id)
            VALUES (%s, %s)
            ON CONFLICT (project_id, session_id) DO NOTHING
            """,
            (project_id, session_id),
        )
        await cur.execute(
            """
            SELECT last_evaluated_message_id, last_evaluated_timestamp_ms
            FROM episode_processing_checkpoints
            WHERE project_id = %s
              AND session_id = %s
            FOR UPDATE
            """,
            (project_id, session_id),
        )
        row = await cur.fetchone()
        if row is None:
            raise RuntimeError("Episode checkpoint could not be locked")
        return EpisodeCheckpoint(
            last_evaluated_message_id=int(row["last_evaluated_message_id"]),
            last_evaluated_timestamp_ms=row["last_evaluated_timestamp_ms"],
        )

    @staticmethod
    async def _window_is_checkpointed(
        cur,
        window_message_ids: List[int],
        *,
        checkpoint: EpisodeCheckpoint,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> bool:
        """Recognize an exact retry without treating a lower ID as stale."""

        if (
            checkpoint.last_evaluated_message_id == 0
            and checkpoint.last_evaluated_timestamp_ms is None
        ):
            return False
        await cur.execute(
            """
            SELECT message_id, timestamp_ms
            FROM messages
            WHERE message_id = ANY(%s)
              AND user_name = %s
              AND project_id = %s
              AND session_id = %s
            """,
            (window_message_ids, user_name, project_id, session_id),
        )
        rows = await cur.fetchall()
        if {int(row["message_id"]) for row in rows} != set(window_message_ids):
            return False
        return all(
            EpisodeWriter._message_at_or_before_checkpoint(row, checkpoint)
            for row in rows
        )

    @staticmethod
    def _message_at_or_before_checkpoint(
        message: Dict, checkpoint: EpisodeCheckpoint
    ) -> bool:
        """Compare message and cursor using the SQL NULLS LAST order."""

        timestamp_ms = message["timestamp_ms"]
        if checkpoint.last_evaluated_timestamp_ms is None:
            return timestamp_ms is None and int(message["message_id"]) <= (
                checkpoint.last_evaluated_message_id
            )
        if timestamp_ms is None:
            return False
        if int(timestamp_ms) != checkpoint.last_evaluated_timestamp_ms:
            return int(timestamp_ms) < checkpoint.last_evaluated_timestamp_ms
        return int(message["message_id"]) <= checkpoint.last_evaluated_message_id

    @staticmethod
    async def _load_entities_by_message(
        cur,
        message_ids: List[int],
        *,
        project_id: str,
    ) -> Dict[int, Set[int]]:
        await cur.execute(
            """
            SELECT mer.message_id, mer.entity_id
            FROM message_entity_refs mer
            JOIN entities e ON e.entity_id = mer.entity_id
            WHERE mer.message_id = ANY(%s)
              AND (e.entity_id = %s OR EXISTS (
                  SELECT 1 FROM project_entity_contexts context
                  WHERE context.entity_id = e.entity_id
                    AND context.project_id = %s
              ))
            """,
            (message_ids, IDENTITY_ENTITY_ID, project_id),
        )
        entities_by_message = {message_id: set() for message_id in message_ids}
        for row in await cur.fetchall():
            entities_by_message[int(row["message_id"])].add(int(row["entity_id"]))
        return entities_by_message

    @staticmethod
    def _validate_ranked_context(
        supplied_entities: List[EntityEpisode],
        supplied_relationships: List[RelationshipEpisode],
        entities_by_message: Dict[int, Set[int]],
        relationships_by_message: Dict[int, Set[str]],
    ) -> None:
        derived_entity_ids = set().union(*entities_by_message.values())
        supplied_entity_ids = {entity.entity_id for entity in supplied_entities}
        if not supplied_entity_ids.issubset(derived_entity_ids):
            raise ValueError("Episode entities must be derived from source messages")

        derived_relationship_ids = set().union(*relationships_by_message.values())
        supplied_relationship_ids = {
            relationship.relationship_id for relationship in supplied_relationships
        }
        if not supplied_relationship_ids.issubset(derived_relationship_ids):
            raise ValueError(
                "Episode relationships must be derived from source messages"
            )

    @staticmethod
    async def _upsert_episode(
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
            ON CONFLICT (episode_id) DO UPDATE
            SET summary = EXCLUDED.summary,
                new_developments = EXCLUDED.new_developments,
                updates = EXCLUDED.updates,
                unresolved = EXCLUDED.unresolved,
                source_message_count = EXCLUDED.source_message_count,
                first_message_at = EXCLUDED.first_message_at,
                last_message_at = EXCLUDED.last_message_at,
                embedding = EXCLUDED.embedding,
                generator_metadata = EXCLUDED.generator_metadata,
                updated_at = EXCLUDED.updated_at
            WHERE episodes.project_id = EXCLUDED.project_id
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
            raise ValueError("Episode ID belongs to a different episode scope")

    @staticmethod
    async def _upsert_messages(cur, episode: Episode) -> None:
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
                ON CONFLICT (episode_id, message_id) DO UPDATE
                SET message_position = EXCLUDED.message_position
                """,
                (
                    episode.episode_id,
                    episode.project_id,
                    message.session_id,
                    message.message_id,
                    message.message_position,
                ),
            )

    @staticmethod
    async def _upsert_entities(
        cur,
        episode: Episode,
        entities_by_message: Dict[int, Set[int]],
        source_message_timestamps: Dict[int, int | None],
    ) -> None:
        entity_ids = set().union(*entities_by_message.values())

        for entity_id in sorted(entity_ids):
            source_message_ids = [
                message_id
                for message_id, message_entities in entities_by_message.items()
                if entity_id in message_entities
            ]
            timestamps = [
                source_message_timestamps[message_id]
                for message_id in source_message_ids
                if source_message_timestamps[message_id] is not None
            ]
            first_seen_at = (
                datetime.fromtimestamp(min(timestamps) / 1000, tz=timezone.utc)
                if timestamps
                else None
            )
            last_seen_at = (
                datetime.fromtimestamp(max(timestamps) / 1000, tz=timezone.utc)
                if timestamps
                else None
            )
            await cur.execute(
                """
                INSERT INTO episode_entities (
                    episode_id,
                    project_id,
                    entity_id,
                    source_message_count,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (episode_id, entity_id) DO UPDATE
                SET source_message_count = EXCLUDED.source_message_count,
                    first_seen_at = EXCLUDED.first_seen_at,
                    last_seen_at = EXCLUDED.last_seen_at
                """,
                (
                    episode.episode_id,
                    episode.project_id,
                    entity_id,
                    len(source_message_ids),
                    first_seen_at,
                    last_seen_at,
                ),
            )

    @staticmethod
    async def _upsert_relationships(
        cur,
        episode: Episode,
        relationships_by_message: Dict[int, Set[str]],
    ) -> None:
        relationship_ids = set().union(*relationships_by_message.values())

        for relationship_id in sorted(relationship_ids):
            source_message_ids = [
                message_id
                for message_id, message_relationships in (
                    relationships_by_message.items()
                )
                if relationship_id in message_relationships
            ]
            await cur.execute(
                """
                INSERT INTO episode_relationships (
                    episode_id,
                    project_id,
                    relationship_id,
                    source_message_count
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (episode_id, relationship_id) DO UPDATE
                SET source_message_count = EXCLUDED.source_message_count
                """,
                (
                    episode.episode_id,
                    episode.project_id,
                    relationship_id,
                    len(source_message_ids),
                ),
            )
