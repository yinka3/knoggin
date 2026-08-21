import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from common.schema.episode.models import (
    EntityEpisode,
    Episode,
    EpisodeCheckpoint,
    RelationshipEpisode,
)
from common.scoping import IDENTITY_ENTITY_ID, require_scope_value
from infrastructure.postgres_client import PostgresClient

# UI may expose prior automated narratives, but source messages stay canonical.
# Keep history bounded so a long-lived project episode cannot grow without limit.
EPISODE_VERSION_HISTORY_LIMIT = 10


class EpisodeWriter:
    """Persists an episode with all graph context derived from source messages."""

    def __init__(self, client: PostgresClient) -> None:
        self.client = client

    async def create_episode(self, episode: Episode, *, user_name: str) -> None:
        """Create or retry an episode without duplicating any attachment rows."""

        user_name = require_scope_value(user_name, "user_name", "create_episode")
        project_id = require_scope_value(
            episode.project_id, "project_id", "create_episode"
        )
        if not episode.messages:
            raise ValueError("create_episode requires at least one source message")

        source_messages = sorted(
            episode.messages,
            key=lambda message: message.message_position,
        )
        source_message_ids = [message.message_id for message in source_messages]
        source_sessions = {message.session_id for message in source_messages}
        if len(source_sessions) != 1:
            raise ValueError("create_episode requires one source session; use project window")

        async with self.client.transaction() as cur:
            await self._write_episode(
                cur,
                episode,
                source_message_ids,
                user_name=user_name,
                project_id=project_id,
                session_id=source_sessions.pop(),
            )

    async def write_episode_window(
        self,
        episode: Optional[Episode],
        window_message_ids: List[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> bool:
        """Atomically persist an episode decision and advance its checkpoint.

        A ``None`` episode represents a validated skip. Returning ``False``
        means a retry found that this window had already been checkpointed.
        """

        user_name = require_scope_value(user_name, "user_name", "write_episode_window")
        project_id = require_scope_value(
            project_id, "project_id", "write_episode_window"
        )
        session_id = require_scope_value(
            session_id, "session_id", "write_episode_window"
        )
        normalized_window_ids = [int(message_id) for message_id in window_message_ids]
        if not normalized_window_ids or any(
            message_id <= 0 for message_id in normalized_window_ids
        ):
            raise ValueError("Episode window requires positive source message IDs")
        if len(normalized_window_ids) != len(set(normalized_window_ids)):
            raise ValueError("Episode window must not contain duplicate message IDs")
        if episode and episode.project_id != project_id:
            raise ValueError("Episode scope must match its checkpoint scope")
        if episode:
            episode_message_ids = {message.message_id for message in episode.messages}
            if not set(normalized_window_ids).issubset(episode_message_ids):
                raise ValueError(
                    "Episode must include every message in its checkpoint window"
                )

        async with self.client.transaction() as cur:
            checkpoint = await self._lock_checkpoint(
                cur,
                user_name=user_name,
                project_id=project_id,
                session_id=session_id,
            )
            if await self._window_is_checkpointed(
                cur,
                normalized_window_ids,
                checkpoint=checkpoint,
                user_name=user_name,
                project_id=project_id,
                session_id=session_id,
            ):
                return False
            next_checkpoint = await self._validate_next_eligible_window(
                cur,
                normalized_window_ids,
                checkpoint=checkpoint,
                user_name=user_name,
                project_id=project_id,
                session_id=session_id,
            )
            if episode:
                source_message_ids = [
                    message.message_id
                    for message in sorted(
                        episode.messages,
                        key=lambda message: message.message_position,
                    )
                ]
                await self._write_episode(
                    cur,
                    episode,
                    source_message_ids,
                    user_name=user_name,
                    project_id=project_id,
                    session_id=session_id,
                )
            await cur.execute(
                """
                UPDATE episode_processing_checkpoints
                SET last_evaluated_message_id = %s,
                    last_evaluated_timestamp_ms = %s,
                    updated_at = NOW()
                WHERE project_id = %s
                  AND session_id = %s
                """,
                (
                    next_checkpoint.last_evaluated_message_id,
                    next_checkpoint.last_evaluated_timestamp_ms,
                    project_id,
                    session_id,
                ),
            )
        return True

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

    async def _write_project_episode(self, cur, episode: Episode, *, user_name: str) -> None:
        if episode.generator_metadata.get("effective_action") == "consolidate":
            await cur.execute(
                """
                SELECT user_modified FROM episodes
                WHERE episode_id = %s AND project_id = %s FOR UPDATE
                """,
                (episode.episode_id, episode.project_id),
            )
            existing = await cur.fetchone()
            if existing is not None and bool(existing["user_modified"]):
                raise ValueError("User-modified episodes cannot be consolidated automatically")
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
        await self._upsert_messages(cur, episode)
        await self._upsert_entities(cur, episode, entities, timestamps)
        await self._upsert_relationships(cur, episode, relationships)

    @staticmethod
    async def _validate_project_session_window(
        cur, messages: List[Dict], *, checkpoint: EpisodeCheckpoint,
        user_name: str, project_id: str, session_id: str,
    ) -> EpisodeCheckpoint:
        """Reject a stale or non-contiguous source stream before any writes."""

        await cur.execute(
            """
            SELECT m.message_id, m.timestamp_ms, m.role, m.lifecycle_state,
                   m.ingestion_state, m.episode_eligible, m.user_msg_id,
                   s.episode_participation_enabled,
                   s.episode_participation_after_message_id,
                   parent.lifecycle_state AS parent_lifecycle_state,
                   parent.ingestion_state AS parent_ingestion_state
            FROM messages m
            JOIN sessions s
              ON s.session_id = m.session_id AND s.project_id = m.project_id
            LEFT JOIN messages parent
              ON parent.message_id = m.user_msg_id
             AND parent.project_id = m.project_id AND parent.session_id = m.session_id
            WHERE m.user_name = %s AND m.project_id = %s AND m.session_id = %s
              AND s.status <> 'deleted'
              AND ((%s = 0 AND %s::BIGINT IS NULL)
                OR (%s::BIGINT IS NOT NULL AND (m.timestamp_ms > %s
                    OR (m.timestamp_ms = %s AND m.message_id > %s) OR m.timestamp_ms IS NULL))
                OR (%s::BIGINT IS NULL AND %s > 0 AND m.timestamp_ms IS NULL
                    AND m.message_id > %s))
            ORDER BY m.timestamp_ms ASC NULLS LAST, m.message_id
            LIMIT %s
            """,
            (user_name, project_id, session_id, checkpoint.last_evaluated_message_id,
             checkpoint.last_evaluated_timestamp_ms, checkpoint.last_evaluated_timestamp_ms,
             checkpoint.last_evaluated_timestamp_ms, checkpoint.last_evaluated_timestamp_ms,
             checkpoint.last_evaluated_message_id, checkpoint.last_evaluated_timestamp_ms,
             checkpoint.last_evaluated_message_id, checkpoint.last_evaluated_message_id, len(messages)),
        )
        rows = await cur.fetchall()
        expected_ids = [int(item["message_id"]) for item in messages]
        if [int(row["message_id"]) for row in rows] != expected_ids:
            raise ValueError("Project episode window is stale or not a next session range")
        for row in rows:
            if (
                not row["episode_participation_enabled"]
                or int(row["message_id"])
                <= int(row["episode_participation_after_message_id"])
            ):
                raise ValueError("Project episode window includes an excluded session message")
            ready = (row["role"] == "user" and row["lifecycle_state"] == "sealed" and row["ingestion_state"] == "processed" and row["episode_eligible"]) or (row["role"] == "assistant" and row["parent_lifecycle_state"] == "sealed" and row["parent_ingestion_state"] == "processed")
            if not ready:
                raise ValueError("Project episode window includes a non-ready message")
        last = rows[-1]
        return EpisodeCheckpoint(last_evaluated_message_id=int(last["message_id"]), last_evaluated_timestamp_ms=last["timestamp_ms"])

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

    async def _write_episode(
        self,
        cur,
        episode: Episode,
        source_message_ids: List[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> None:
        source_message_timestamps = await self._validate_source_messages(
            cur,
            source_message_ids,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        entities_by_message = await self._load_entities_by_message(
            cur,
            source_message_ids,
            project_id=project_id,
        )
        relationships_by_message = await self._load_relationships_by_message(
            cur,
            source_message_ids,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        self._validate_ranked_context(
            episode.entities,
            episode.relationships,
            entities_by_message,
            relationships_by_message,
        )
        await self._upsert_episode(
            cur,
            episode,
            source_message_timestamps,
        )
        await self._upsert_messages(cur, episode)
        await self._upsert_entities(
            cur,
            episode,
            entities_by_message,
            source_message_timestamps,
        )
        await self._upsert_relationships(cur, episode, relationships_by_message)

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
    async def _validate_next_eligible_window(
        cur,
        window_message_ids: List[int],
        *,
        checkpoint: EpisodeCheckpoint,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> EpisodeCheckpoint:
        await cur.execute(
            """
            SELECT message_id, timestamp_ms
            FROM messages
            WHERE user_name = %s
              AND project_id = %s
              AND session_id = %s
              AND (
                    (%s = 0 AND %s::BIGINT IS NULL)
                 OR (
                        %s::BIGINT IS NOT NULL
                    AND (
                           timestamp_ms > %s
                        OR (timestamp_ms = %s AND message_id > %s)
                        OR timestamp_ms IS NULL
                    )
                 )
                 OR (
                        %s::BIGINT IS NULL
                    AND %s > 0
                    AND timestamp_ms IS NULL
                    AND message_id > %s
                 )
              )
            ORDER BY timestamp_ms ASC NULLS LAST, message_id
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
                len(window_message_ids),
            ),
        )
        next_messages = await cur.fetchall()
        next_message_ids = [int(row["message_id"]) for row in next_messages]
        if next_message_ids != window_message_ids:
            raise ValueError(
                "Episode window is not the next chronological message range"
            )

        await cur.execute(
            """
            SELECT m.message_id
            FROM messages m
            WHERE m.message_id = ANY(%s)
              AND m.user_name = %s
              AND m.project_id = %s
              AND m.session_id = %s
              AND m.episode_eligible = TRUE
            """,
            (window_message_ids, user_name, project_id, session_id),
        )
        eligible_message_ids = {int(row["message_id"]) for row in await cur.fetchall()}
        if eligible_message_ids != set(window_message_ids):
            raise ValueError("Episode window includes messages that are not eligible")
        final_message = next_messages[-1]
        return EpisodeCheckpoint(
            last_evaluated_message_id=int(final_message["message_id"]),
            last_evaluated_timestamp_ms=final_message["timestamp_ms"],
        )

    @staticmethod
    async def _validate_source_messages(
        cur,
        message_ids: List[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> Dict[int, int | None]:
        await cur.execute(
            """
            SELECT message_id, timestamp_ms
            FROM messages
            WHERE message_id = ANY(%s)
              AND user_name = %s
              AND project_id = %s
              AND session_id = %s
            """,
            (message_ids, user_name, project_id, session_id),
        )
        message_timestamps = {
            int(row["message_id"]): row.get("timestamp_ms")
            for row in await cur.fetchall()
        }
        if set(message_timestamps) != set(message_ids):
            raise ValueError("Episode source messages must exist in the episode scope")
        return {
            message_id: int(timestamp) if timestamp is not None else None
            for message_id, timestamp in message_timestamps.items()
        }

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
              AND (e.project_id = %s OR e.entity_id = %s)
            """,
            (message_ids, project_id, IDENTITY_ENTITY_ID),
        )
        entities_by_message = {message_id: set() for message_id in message_ids}
        for row in await cur.fetchall():
            entities_by_message[int(row["message_id"])].add(int(row["entity_id"]))
        return entities_by_message

    @staticmethod
    async def _load_relationships_by_message(
        cur,
        message_ids: List[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> Dict[int, Set[str]]:
        await cur.execute(
            """
            SELECT rer.message_id, rer.relationship_id
            FROM relationship_observations rer
            JOIN relationships r
              ON r.relationship_id = rer.relationship_id
             AND r.project_id = rer.project_id
            WHERE rer.message_id = ANY(%s)
              AND rer.user_name = %s
              AND rer.session_id = %s
              AND r.project_id = %s
            """,
            (message_ids, user_name, session_id, project_id),
        )
        relationships_by_message = {message_id: set() for message_id in message_ids}
        for row in await cur.fetchall():
            relationships_by_message[int(row["message_id"])].add(
                str(row["relationship_id"])
            )
        return relationships_by_message

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
        version_history = [
            version.model_dump(mode="json") for version in episode.version_history
        ]
        if episode.generator_metadata.get("effective_action") == "consolidate":
            version_history = await EpisodeWriter._snapshot_before_consolidation(
                cur,
                episode,
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
                importance,
                source_message_count,
                first_message_at,
                last_message_at,
                embedding,
                generator_metadata,
                version_history,
                user_modified,
                created_at,
                updated_at
            )
            VALUES (
                %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb,
                %s, %s, %s, %s, %s::vector, %s::jsonb, %s::jsonb, %s, %s, %s
            )
            ON CONFLICT (episode_id) DO UPDATE
            SET summary = EXCLUDED.summary,
                new_developments = EXCLUDED.new_developments,
                updates = EXCLUDED.updates,
                unresolved = EXCLUDED.unresolved,
                importance = EXCLUDED.importance,
                source_message_count = EXCLUDED.source_message_count,
                first_message_at = EXCLUDED.first_message_at,
                last_message_at = EXCLUDED.last_message_at,
                embedding = EXCLUDED.embedding,
                generator_metadata = EXCLUDED.generator_metadata,
                version_history = CASE
                    WHEN EXCLUDED.generator_metadata->>'effective_action'
                        = 'consolidate'
                    THEN EXCLUDED.version_history
                    ELSE episodes.version_history
                END,
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
                episode.importance,
                len(source_message_timestamps),
                first_message_at,
                last_message_at,
                (
                    json.dumps(episode.embedding)
                    if episode.embedding is not None
                    else None
                ),
                json.dumps(episode.generator_metadata),
                json.dumps(version_history),
                episode.user_modified,
                episode.created_at,
                episode.updated_at,
            ),
        )
        if await cur.fetchone() is None:
            raise ValueError("Episode ID belongs to a different episode scope")

    @staticmethod
    async def _snapshot_before_consolidation(cur, episode: Episode) -> List[Dict]:
        """Retain bounded automated narrative snapshots before replacement."""

        await cur.execute(
            """
            SELECT
                summary,
                new_developments,
                updates,
                unresolved,
                importance,
                first_message_at,
                last_message_at,
                generator_metadata,
                version_history
            FROM episodes
            WHERE episode_id = %s
              AND project_id = %s
            FOR UPDATE
            """,
            (episode.episode_id, episode.project_id),
        )
        existing = await cur.fetchone()
        if existing is None:
            return [
                version.model_dump(mode="json") for version in episode.version_history
            ]

        await cur.execute(
            """
            SELECT message_id
            FROM episode_messages
            WHERE episode_id = %s
            ORDER BY message_position
            """,
            (episode.episode_id,),
        )
        source_message_ids = [int(row["message_id"]) for row in await cur.fetchall()]
        if not source_message_ids:
            source_message_ids = [
                message.message_id
                for message in sorted(
                    episode.messages,
                    key=lambda message: message.message_position,
                )
            ]

        history = EpisodeWriter._json_list(existing.get("version_history"))
        next_version = (
            max(
                (int(item.get("version") or 0) for item in history),
                default=0,
            )
            + 1
        )
        history.append(
            {
                "version": next_version,
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "summary": str(existing["summary"]),
                "new_developments": EpisodeWriter._json_list(
                    existing.get("new_developments")
                ),
                "updates": EpisodeWriter._json_list(existing.get("updates")),
                "unresolved": EpisodeWriter._json_list(existing.get("unresolved")),
                "importance": float(existing["importance"]),
                "first_message_at": EpisodeWriter._isoformat(
                    existing.get("first_message_at")
                ),
                "last_message_at": EpisodeWriter._isoformat(
                    existing.get("last_message_at")
                ),
                "source_message_ids": source_message_ids,
                "generator_metadata": EpisodeWriter._json_dict(
                    existing.get("generator_metadata")
                ),
            }
        )
        return history[-EPISODE_VERSION_HISTORY_LIMIT:]

    @staticmethod
    def _json_list(value) -> List:
        if isinstance(value, str):
            value = json.loads(value)
        return list(value or [])

    @staticmethod
    def _json_dict(value) -> Dict:
        if isinstance(value, str):
            value = json.loads(value)
        return dict(value or {})

    @staticmethod
    def _isoformat(value):
        return value.isoformat() if hasattr(value, "isoformat") else value

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
                    influence_weight,
                    influence_reason,
                    message_position
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (episode_id, message_id) DO UPDATE
                SET influence_weight = EXCLUDED.influence_weight,
                    influence_reason = EXCLUDED.influence_reason,
                    message_position = EXCLUDED.message_position
                """,
                (
                    episode.episode_id,
                    episode.project_id,
                    message.session_id,
                    message.message_id,
                    message.influence_weight,
                    message.influence_reason,
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
        messages_by_id = {message.message_id: message for message in episode.messages}
        supplied_entities = {entity.entity_id: entity for entity in episode.entities}
        entity_ids = set().union(*entities_by_message.values())

        for entity_id in sorted(entity_ids):
            source_message_ids = [
                message_id
                for message_id, message_entities in entities_by_message.items()
                if entity_id in message_entities
            ]
            baseline_prominence = sum(
                messages_by_id[message_id].influence_weight
                for message_id in source_message_ids
            )
            ranked = supplied_entities.get(entity_id)
            prominence_weight = max(
                baseline_prominence,
                ranked.prominence_weight if ranked else 0.0,
            )
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
                    prominence_weight,
                    role,
                    is_focus_entity,
                    source_message_count,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (episode_id, entity_id) DO UPDATE
                SET prominence_weight = EXCLUDED.prominence_weight,
                    role = EXCLUDED.role,
                    is_focus_entity = EXCLUDED.is_focus_entity,
                    source_message_count = EXCLUDED.source_message_count,
                    first_seen_at = EXCLUDED.first_seen_at,
                    last_seen_at = EXCLUDED.last_seen_at
                """,
                (
                    episode.episode_id,
                    episode.project_id,
                    entity_id,
                    prominence_weight,
                    ranked.role if ranked else None,
                    ranked.is_focus_entity if ranked else False,
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
        messages_by_id = {message.message_id: message for message in episode.messages}
        supplied_relationships = {
            relationship.relationship_id: relationship
            for relationship in episode.relationships
        }
        relationship_ids = set().union(*relationships_by_message.values())

        for relationship_id in sorted(relationship_ids):
            source_message_ids = [
                message_id
                for message_id, message_relationships in (
                    relationships_by_message.items()
                )
                if relationship_id in message_relationships
            ]
            baseline_prominence = sum(
                messages_by_id[message_id].influence_weight
                for message_id in source_message_ids
            )
            ranked = supplied_relationships.get(relationship_id)
            prominence_weight = max(
                baseline_prominence,
                ranked.prominence_weight if ranked else 0.0,
            )
            await cur.execute(
                """
                INSERT INTO episode_relationships (
                    episode_id,
                    project_id,
                    relationship_id,
                    prominence_weight,
                    is_central_relationship,
                    source_message_count
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (episode_id, relationship_id) DO UPDATE
                SET prominence_weight = EXCLUDED.prominence_weight,
                    is_central_relationship = EXCLUDED.is_central_relationship,
                    source_message_count = EXCLUDED.source_message_count
                """,
                (
                    episode.episode_id,
                    episode.project_id,
                    relationship_id,
                    prominence_weight,
                    ranked.is_central_relationship if ranked else False,
                    len(source_message_ids),
                ),
            )
