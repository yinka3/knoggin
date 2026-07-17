import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from common.schema.primitives import Episode, EntityEpisode, RelationshipEpisode
from common.scoping import IDENTITY_ENTITY_ID, require_scope_value
from infrastructure.postgres_client import PostgresClient


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
        session_id = require_scope_value(
            episode.session_id, "session_id", "create_episode"
        )
        if not episode.messages:
            raise ValueError("create_episode requires at least one source message")

        source_messages = sorted(
            episode.messages,
            key=lambda message: message.message_position,
        )
        source_message_ids = [message.message_id for message in source_messages]

        async with self.client.transaction() as cur:
            await self._write_episode(
                cur,
                episode,
                source_message_ids,
                user_name=user_name,
                project_id=project_id,
                session_id=session_id,
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

        user_name = require_scope_value(
            user_name, "user_name", "write_episode_window"
        )
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
        if episode and (
            episode.project_id != project_id or episode.session_id != session_id
        ):
            raise ValueError("Episode scope must match its checkpoint scope")
        if episode:
            episode_message_ids = {
                message.message_id for message in episode.messages
            }
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
            if max(normalized_window_ids) <= checkpoint:
                return False
            await self._validate_next_eligible_window(
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
                    updated_at = NOW()
                WHERE project_id = %s
                  AND session_id = %s
                """,
                (max(normalized_window_ids), project_id, session_id),
            )
        return True

    async def advance_checkpoint(
        self,
        last_evaluated_message_id: int,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> int:
        """Advance one conversation checkpoint without allowing it to regress."""

        if last_evaluated_message_id < 0:
            raise ValueError("Episode checkpoint message ID must not be negative")
        user_name = require_scope_value(
            user_name, "user_name", "advance_episode_checkpoint"
        )
        project_id = require_scope_value(
            project_id, "project_id", "advance_episode_checkpoint"
        )
        session_id = require_scope_value(
            session_id, "session_id", "advance_episode_checkpoint"
        )
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                INSERT INTO episode_processing_checkpoints (
                    project_id,
                    session_id,
                    last_evaluated_message_id
                )
                SELECT s.project_id, s.session_id, %s
                FROM sessions s
                WHERE s.user_name = %s
                  AND s.project_id = %s
                  AND s.session_id = %s
                ON CONFLICT (project_id, session_id) DO UPDATE
                SET last_evaluated_message_id = GREATEST(
                        episode_processing_checkpoints.last_evaluated_message_id,
                        EXCLUDED.last_evaluated_message_id
                    ),
                    updated_at = NOW()
                RETURNING last_evaluated_message_id
                """,
                (last_evaluated_message_id, user_name, project_id, session_id),
            )
            row = await cur.fetchone()
        if row is None:
            raise ValueError("Episode checkpoint requires an existing session")
        return int(row["last_evaluated_message_id"])

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
        await self._upsert_episode(cur, episode, source_message_timestamps)
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
    ) -> int:
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
            SELECT last_evaluated_message_id
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
        return int(row["last_evaluated_message_id"])

    @staticmethod
    async def _validate_next_eligible_window(
        cur,
        window_message_ids: List[int],
        *,
        checkpoint: int,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> None:
        await cur.execute(
            """
            SELECT message_id
            FROM messages
            WHERE user_name = %s
              AND project_id = %s
              AND session_id = %s
              AND message_id > %s
            ORDER BY timestamp_ms ASC NULLS LAST, message_id
            LIMIT %s
            """,
            (user_name, project_id, session_id, checkpoint, len(window_message_ids)),
        )
        next_message_ids = [
            int(row["message_id"]) for row in await cur.fetchall()
        ]
        if next_message_ids != window_message_ids:
            raise ValueError("Episode window is not the next chronological message range")

        await cur.execute(
            """
            SELECT m.message_id
            FROM messages m
            JOIN episode_eligible_messages elm ON elm.message_id = m.message_id
            WHERE m.message_id = ANY(%s)
              AND m.user_name = %s
              AND m.project_id = %s
              AND m.session_id = %s
            """,
            (window_message_ids, user_name, project_id, session_id),
        )
        eligible_message_ids = {
            int(row["message_id"]) for row in await cur.fetchall()
        }
        if eligible_message_ids != set(window_message_ids):
            raise ValueError("Episode window includes messages that are not eligible")

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
            FROM relationship_evidence_refs rer
            JOIN relationships r ON r.relationship_id = rer.relationship_id
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
        await cur.execute(
            """
            INSERT INTO episodes (
                episode_id,
                project_id,
                session_id,
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
                created_at,
                updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb,
                %s, %s, %s, %s, %s::vector, %s::jsonb, %s, %s
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
                updated_at = EXCLUDED.updated_at
            WHERE episodes.project_id = EXCLUDED.project_id
              AND episodes.session_id = EXCLUDED.session_id
            RETURNING episode_id
            """,
            (
                episode.episode_id,
                episode.project_id,
                episode.session_id,
                episode.summary,
                json.dumps(episode.new_developments),
                json.dumps(episode.updates),
                json.dumps(episode.unresolved),
                episode.importance,
                len(source_message_timestamps),
                first_message_at,
                last_message_at,
                json.dumps(episode.embedding) if episode.embedding is not None else None,
                json.dumps(episode.generator_metadata),
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
                    message_id,
                    influence_weight,
                    influence_reason,
                    message_position
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (episode_id, message_id) DO UPDATE
                SET influence_weight = EXCLUDED.influence_weight,
                    influence_reason = EXCLUDED.influence_reason,
                    message_position = EXCLUDED.message_position
                """,
                (
                    episode.episode_id,
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
                    entity_id,
                    prominence_weight,
                    role,
                    is_focus_entity,
                    source_message_count,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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
                for message_id, message_relationships
                in relationships_by_message.items()
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
                    relationship_id,
                    prominence_weight,
                    is_central_relationship,
                    source_message_count
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (episode_id, relationship_id) DO UPDATE
                SET prominence_weight = EXCLUDED.prominence_weight,
                    is_central_relationship = EXCLUDED.is_central_relationship,
                    source_message_count = EXCLUDED.source_message_count
                """,
                (
                    episode.episode_id,
                    relationship_id,
                    prominence_weight,
                    ranked.is_central_relationship if ranked else False,
                    len(source_message_ids),
                ),
            )
