import json
import math
from typing import Dict, List, Optional

from common.schema.episode.models import (
    EPISODE_EMBEDDING_DIMENSION,
    EntityEpisode,
    Episode,
    EpisodeCard,
    MessageEpisode,
    RelationshipEpisode,
)
from common.scoping import require_scope_value, require_visible_project_ids
from infrastructure.postgres_client import PostgresClient


class EpisodeReader:
    """Reads scoped episodic-memory aggregates and their canonical evidence."""

    def __init__(self, client: PostgresClient) -> None:
        self.client = client


    async def get_project_episode(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        visible_project_ids: Optional[List[str]] = None,
    ) -> Episode | None:
        episode_id = require_scope_value(
            episode_id, "episode_id", "get_project_episode"
        )
        user_name, visible_project_ids = self._project_scope(
            user_name,
            project_id,
            visible_project_ids,
            "get_project_episode",
        )
        row = await self.client.fetch_one(
            """
            SELECT e.* FROM episodes e
            JOIN projects p ON p.project_id = e.project_id
            WHERE e.episode_id = %s AND e.project_id = ANY(%s) AND p.user_name = %s
            """,
            (episode_id, visible_project_ids, user_name),
        )
        return await self._hydrate_episode(row) if row else None

    async def get_recent_project_episodes(
        self,
        *,
        user_name: str,
        project_id: str,
        limit: int,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[EpisodeCard]:
        if limit <= 0:
            return []
        user_name, visible_project_ids = self._project_scope(
            user_name,
            project_id,
            visible_project_ids,
            "get_recent_project_episodes",
        )
        rows = await self.client.fetch_all(
            """
            SELECT e.* FROM episodes e JOIN projects p ON p.project_id = e.project_id
            WHERE e.project_id = ANY(%s) AND p.user_name = %s
            ORDER BY e.last_message_at DESC NULLS LAST, e.episode_id DESC LIMIT %s
            """,
            (visible_project_ids, user_name, limit),
        )
        return [await self._hydrate_episode_card(row) for row in rows]

    async def search_project_episodes(
        self,
        query: str,
        *,
        user_name: str,
        project_id: str,
        limit: int,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[EpisodeCard]:
        query = query.strip()
        if not query or limit <= 0:
            return []
        user_name, visible_project_ids = self._project_scope(
            user_name,
            project_id,
            visible_project_ids,
            "search_project_episodes",
        )
        rows = await self.client.fetch_all(
            """
            WITH terms AS (SELECT websearch_to_tsquery('simple', %s) AS query)
            SELECT e.* FROM episodes e
            JOIN projects p ON p.project_id = e.project_id CROSS JOIN terms
            WHERE e.project_id = ANY(%s) AND p.user_name = %s
              AND e.search_tsvector @@ terms.query
            ORDER BY ts_rank_cd(e.search_tsvector, terms.query) DESC,
                     e.last_message_at DESC NULLS LAST, e.episode_id DESC LIMIT %s
            """,
            (query, visible_project_ids, user_name, limit),
        )
        return [await self._hydrate_episode_card(row) for row in rows]

    async def search_project_episodes_by_embedding(
        self,
        embedding: List[float],
        *,
        user_name: str,
        project_id: str,
        limit: int,
        score_threshold: float = 0.35,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[tuple[EpisodeCard, float]]:
        if limit <= 0:
            return []
        if not 0.0 <= score_threshold <= 1.0:
            raise ValueError("episode score_threshold must be between 0 and 1")
        user_name, visible_project_ids = self._project_scope(
            user_name,
            project_id,
            visible_project_ids,
            "search_project_episodes_by_embedding",
        )
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
                visible_project_ids,
                user_name,
                vector,
                score_threshold,
                vector,
                limit,
            ),
        )
        return [
            (await self._hydrate_episode_card(row), float(row["similarity"]))
            for row in rows
        ]

    async def get_project_episodes_for_entities(
        self,
        entity_ids: List[int],
        *,
        user_name: str,
        project_id: str,
        limit: int,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[EpisodeCard]:
        if not entity_ids or limit <= 0:
            return []
        user_name, visible_project_ids = self._project_scope(
            user_name,
            project_id,
            visible_project_ids,
            "get_project_episodes_for_entities",
        )
        rows = await self.client.fetch_all(
            """
            SELECT e.*, COUNT(DISTINCT ee.entity_id) AS entity_overlap
            FROM episodes e
            JOIN projects p ON p.project_id = e.project_id
            JOIN episode_entities ee ON ee.episode_id = e.episode_id AND ee.project_id = e.project_id
            WHERE e.project_id = ANY(%s) AND p.user_name = %s AND ee.entity_id = ANY(%s)
            GROUP BY e.episode_id
            ORDER BY entity_overlap DESC, e.last_message_at DESC NULLS LAST,
                     e.episode_id DESC LIMIT %s
            """,
            (visible_project_ids, user_name, entity_ids, limit),
        )
        return [await self._hydrate_episode_card(row) for row in rows]

    async def get_project_episode_source_messages(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[Dict]:
        episode_id = require_scope_value(
            episode_id,
            "episode_id",
            "get_project_episode_source_messages",
        )
        user_name, visible_project_ids = self._project_scope(
            user_name,
            project_id,
            visible_project_ids,
            "get_project_episode_source_messages",
        )
        return await self.client.fetch_all(
            """
            SELECT m.message_id, m.session_id, m.role, m.content, m.timestamp_ms,
                   em.message_position,
                   em.attached_at
            FROM episodes e
            JOIN projects p ON p.project_id = e.project_id
            JOIN episode_messages em ON em.episode_id = e.episode_id AND em.project_id = e.project_id
            JOIN messages m ON m.message_id = em.message_id AND m.project_id = em.project_id
                           AND m.session_id = em.session_id
            WHERE e.episode_id = %s AND e.project_id = ANY(%s) AND p.user_name = %s
            ORDER BY em.message_position
            """,
            (episode_id, visible_project_ids, user_name),
        )

    @staticmethod
    def _project_scope(
        user_name: str,
        project_id: str,
        visible_project_ids: Optional[List[str]],
        operation: str,
    ) -> tuple[str, List[str]]:
        user_name = require_scope_value(user_name, "user_name", operation)
        project_id = require_scope_value(project_id, "project_id", operation)
        visible_project_ids = require_visible_project_ids(
            visible_project_ids if visible_project_ids is not None else [project_id],
            operation,
        )
        if project_id not in visible_project_ids:
            raise ValueError("visible_project_ids must include project_id")
        return user_name, visible_project_ids

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
            user_modified=bool(row.get("user_modified", False)),
        )

    async def _hydrate_episode_card(self, row: Dict) -> EpisodeCard:
        """Hydrate discovery metadata without loading source-message rows."""

        episode_id = str(row["episode_id"])
        return EpisodeCard(
            episode_id=episode_id,
            project_id=str(row["project_id"]),
            summary=str(row["summary"]),
            new_developments=self._json_list(row.get("new_developments")),
            updates=self._json_list(row.get("updates")),
            unresolved=self._json_list(row.get("unresolved")),
            source_message_count=int(row.get("source_message_count") or 0),
            first_message_at=row.get("first_message_at"),
            last_message_at=row.get("last_message_at"),
            entities=await self._load_entities(episode_id),
            relationships=await self._load_relationships(episode_id),
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
                source_message_count,
                first_seen_at,
                last_seen_at
            FROM episode_entities
            WHERE episode_id = %s
            ORDER BY entity_id
            """,
            (episode_id,),
        )
        return [
            EntityEpisode(
                entity_id=int(row["entity_id"]),
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
                source_message_count
            FROM episode_relationships
            WHERE episode_id = %s
            ORDER BY relationship_id
            """,
            (episode_id,),
        )
        return [
            RelationshipEpisode(
                relationship_id=str(row["relationship_id"]),
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
        if len(normalized) != EPISODE_EMBEDDING_DIMENSION:
            raise ValueError(
                "episode embedding must contain exactly "
                f"{EPISODE_EMBEDDING_DIMENSION} dimensions"
            )
        if not all(math.isfinite(value) for value in normalized):
            raise ValueError("episode embedding must contain only finite values")
        return normalized
