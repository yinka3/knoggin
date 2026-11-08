import json
import math
from datetime import datetime
from typing import Dict, List, Optional

from common.schema.episode.models import (
    EPISODE_EMBEDDING_DIMENSION,
    EntityEpisode,
    Episode,
    EpisodeCard,
    EpisodeEvent,
    MessageEpisode,
    RelationshipEpisode,
)
from common.scoping import require_scope_value, require_visible_project_ids
from infrastructure.postgres_client import PostgresClient


# Event text as searched: what happened plus the wording about when.
_EVENT_TSVECTOR = "to_tsvector('english', ev.what || ' ' || coalesce(ev.date_text, ''))"


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
        after: Optional[datetime] = None,
        before: Optional[datetime] = None,
        offset: int = 0,
    ) -> List[EpisodeCard]:
        if limit <= 0:
            return []
        user_name, visible_project_ids = self._project_scope(
            user_name,
            project_id,
            visible_project_ids,
            "get_recent_project_episodes",
        )
        date_sql, date_params = self._date_range(after, before)
        rows = await self.client.fetch_all(
            f"""
            SELECT e.* FROM episodes e JOIN projects p ON p.project_id = e.project_id
            WHERE e.project_id = ANY(%s) AND p.user_name = %s {date_sql}
            ORDER BY e.last_message_at DESC NULLS LAST, e.episode_id DESC LIMIT %s OFFSET %s
            """,
            (visible_project_ids, user_name, *date_params, limit, offset),
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
        after: Optional[datetime] = None,
        before: Optional[datetime] = None,
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
        date_sql, date_params = self._date_range(after, before)
        rows = await self.client.fetch_all(
            f"""
            -- Any important word may match; ranking favors Episodes with more.
            WITH terms AS (
                SELECT replace(plainto_tsquery('english', %s)::text, '&', '|')::tsquery AS query
            )
            SELECT e.* FROM episodes e
            JOIN projects p ON p.project_id = e.project_id CROSS JOIN terms
            WHERE e.project_id = ANY(%s) AND p.user_name = %s
              AND e.search_tsvector @@ terms.query {date_sql}
            ORDER BY ts_rank_cd(e.search_tsvector, terms.query) DESC,
                     e.last_message_at DESC NULLS LAST, e.episode_id DESC LIMIT %s
            """,
            (query, visible_project_ids, user_name, *date_params, limit),
        )
        return [await self._hydrate_episode_card(row) for row in rows]

    async def search_project_episodes_by_events(
        self,
        query: str,
        *,
        user_name: str,
        project_id: str,
        limit: int,
        visible_project_ids: Optional[List[str]] = None,
        after: Optional[datetime] = None,
        before: Optional[datetime] = None,
    ) -> List[EpisodeCard]:
        """Episodes whose event lines match, best-matching event first.

        An event keeps a passing mention the Episode summary may bury.
        """

        query = query.strip()
        if not query or limit <= 0:
            return []
        user_name, visible_project_ids = self._project_scope(
            user_name, project_id, visible_project_ids, "search_project_episodes_by_events"
        )
        date_sql, date_params = self._date_range(after, before)
        rows = await self.client.fetch_all(
            f"""
            WITH terms AS (
                SELECT replace(plainto_tsquery('english', %s)::text, '&', '|')::tsquery AS query
            ),
            hits AS (
                SELECT ev.episode_id, max(ts_rank_cd({_EVENT_TSVECTOR}, terms.query)) AS rank
                FROM episode_events ev
                JOIN episodes e ON e.episode_id = ev.episode_id
                JOIN projects p ON p.project_id = e.project_id CROSS JOIN terms
                WHERE e.project_id = ANY(%s) AND p.user_name = %s
                  AND {_EVENT_TSVECTOR} @@ terms.query {date_sql}
                GROUP BY ev.episode_id
            )
            SELECT e.* FROM hits JOIN episodes e ON e.episode_id = hits.episode_id
            ORDER BY hits.rank DESC, e.last_message_at DESC NULLS LAST, e.episode_id DESC
            LIMIT %s
            """,
            (query, visible_project_ids, user_name, *date_params, limit),
        )
        return [await self._hydrate_episode_card(row) for row in rows]

    async def list_project_events(
        self,
        *,
        user_name: str,
        project_id: str,
        limit: int,
        offset: int = 0,
        visible_project_ids: Optional[List[str]] = None,
        after: Optional[datetime] = None,
        before: Optional[datetime] = None,
        kind: Optional[str] = None,
        query: Optional[str] = None,
    ) -> List[Dict]:
        """List event mentions by when they happened.

        With a date range, an event whose range only partly overlaps it is
        returned as "overlaps"; undated events are left out. A kind filter
        also returns events labeled other.
        """

        if limit <= 0:
            return []
        user_name, visible_project_ids = self._project_scope(
            user_name, project_id, visible_project_ids, "list_project_events"
        )
        start = after.date() if after is not None else None
        end = before.date() if before is not None else None
        where, params = "", []
        # With a query, the best-matching mentions come first; otherwise by date.
        order = "ev.start_date NULLS LAST, e.first_message_at NULLS LAST"
        order_params: list = []
        if start is not None:
            where += " AND ev.end_date >= %s"
            params.append(start)
        if end is not None:
            where += " AND ev.start_date <= %s"
            params.append(end)
        if kind:
            # A kind is the model's guess; events it labeled other may still fit.
            where += " AND ev.kind IN (%s, 'other')"
            params.append(kind.strip().lower())
        if query and query.strip():
            # Any important word may match, as in Episode keyword search.
            where += (
                f" AND {_EVENT_TSVECTOR}"
                " @@ replace(plainto_tsquery('english', %s)::text, '&', '|')::tsquery"
            )
            params.append(query)
            order = (
                f"ts_rank_cd({_EVENT_TSVECTOR},"
                " replace(plainto_tsquery('english', %s)::text, '&', '|')::tsquery) DESC, "
                + order
            )
            order_params.append(query)
        rows = await self.client.fetch_all(
            f"""
            SELECT ev.episode_id, ev.what, ev.kind, ev.start_date, ev.end_date,
                   ev.precision, ev.date_text, ev.source_message_ids,
                   CASE WHEN (%s::date IS NULL OR ev.start_date >= %s::date)
                         AND (%s::date IS NULL OR ev.end_date <= %s::date)
                        THEN 'inside' ELSE 'overlaps' END AS in_range
            FROM episode_events ev
            JOIN episodes e ON e.episode_id = ev.episode_id
            JOIN projects p ON p.project_id = e.project_id
            WHERE e.project_id = ANY(%s) AND p.user_name = %s {where}
            ORDER BY {order}, ev.episode_id, ev.ordinal
            LIMIT %s OFFSET %s
            """,
            (
                start, start, end, end, visible_project_ids, user_name, *params,
                *order_params, limit, offset,
            ),
        )
        return [
            {**row, "source_message_ids": [int(i) for i in row["source_message_ids"]]}
            for row in rows
        ]

    async def search_project_episodes_by_embedding(
        self,
        embedding: List[float],
        *,
        user_name: str,
        project_id: str,
        limit: int,
        score_threshold: float,
        visible_project_ids: Optional[List[str]] = None,
        after: Optional[datetime] = None,
        before: Optional[datetime] = None,
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
        date_sql, date_params = self._date_range(after, before)
        rows = await self.client.fetch_all(
            f"""
            SELECT e.*, 1 - (e.embedding <=> %s::vector) AS similarity
            FROM episodes e JOIN projects p ON p.project_id = e.project_id
            WHERE e.project_id = ANY(%s) AND p.user_name = %s AND e.embedding IS NOT NULL
              AND 1 - (e.embedding <=> %s::vector) >= %s {date_sql}
            ORDER BY e.embedding <=> %s::vector ASC LIMIT %s
            """,
            (
                vector,
                visible_project_ids,
                user_name,
                vector,
                score_threshold,
                *date_params,
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
        after: Optional[datetime] = None,
        before: Optional[datetime] = None,
    ) -> List[EpisodeCard]:
        if not entity_ids or limit <= 0:
            return []
        user_name, visible_project_ids = self._project_scope(
            user_name,
            project_id,
            visible_project_ids,
            "get_project_episodes_for_entities",
        )
        date_sql, date_params = self._date_range(after, before)
        rows = await self.client.fetch_all(
            f"""
            SELECT e.*, COUNT(DISTINCT ee.entity_id) AS entity_overlap
            FROM episodes e
            JOIN projects p ON p.project_id = e.project_id
            JOIN episode_entities ee ON ee.episode_id = e.episode_id AND ee.project_id = e.project_id
            WHERE e.project_id = ANY(%s) AND p.user_name = %s AND ee.entity_id = ANY(%s) {date_sql}
            GROUP BY e.episode_id
            ORDER BY entity_overlap DESC, e.last_message_at DESC NULLS LAST,
                     e.episode_id DESC LIMIT %s
            """,
            (visible_project_ids, user_name, entity_ids, *date_params, limit),
        )
        return [await self._hydrate_episode_card(row) for row in rows]

    async def get_project_episode_source_messages(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        visible_project_ids: Optional[List[str]] = None,
        limit: int | None = None,
        offset: int = 0,
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
        if limit is not None and not 1 <= limit <= 101:
            raise ValueError("episode source message limit must be between 1 and 101")
        if offset < 0:
            raise ValueError("episode source message offset must not be negative")
        limit_clause = "LIMIT %s" if limit is not None else ""
        offset_clause = "OFFSET %s" if offset else ""
        return await self.client.fetch_all(
            f"""
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
            {limit_clause}
            {offset_clause}
            """,
            (episode_id, visible_project_ids, user_name)
            + ((limit,) if limit is not None else ())
            + ((offset,) if offset else ()),
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
            events=await self._load_events(episode_id),
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
                link.entity_id,
                entity.canonical_name,
                link.source_message_count,
                link.first_seen_at,
                link.last_seen_at
            FROM episode_entities link
            JOIN entities entity ON entity.entity_id = link.entity_id
            WHERE link.episode_id = %s
            ORDER BY link.entity_id
            """,
            (episode_id,),
        )
        return [
            EntityEpisode(
                entity_id=int(row["entity_id"]),
                name=row["canonical_name"],
                source_message_count=int(row["source_message_count"]),
                first_seen_at=row.get("first_seen_at"),
                last_seen_at=row.get("last_seen_at"),
            )
            for row in rows
        ]

    async def _load_events(self, episode_id: str) -> List[EpisodeEvent]:
        rows = await self.client.fetch_all(
            """
            SELECT what, kind, start_date, end_date, precision, date_text,
                   source_message_ids
            FROM episode_events WHERE episode_id = %s ORDER BY ordinal
            """,
            (episode_id,),
        )
        return [
            EpisodeEvent(
                **{**row, "source_message_ids": [int(i) for i in row["source_message_ids"]]}
            )
            for row in rows
        ]

    async def _load_relationships(self, episode_id: str) -> List[RelationshipEpisode]:
        rows = await self.client.fetch_all(
            """
            SELECT
                link.relationship_id,
                source.canonical_name AS source,
                relationship.relationship_type,
                target.canonical_name AS target,
                link.source_message_count
            FROM episode_relationships link
            JOIN relationships relationship
              ON relationship.relationship_id = link.relationship_id
            JOIN entities source ON source.entity_id = relationship.entity_a_id
            JOIN entities target ON target.entity_id = relationship.entity_b_id
            WHERE link.episode_id = %s
            ORDER BY link.relationship_id
            """,
            (episode_id,),
        )
        return [
            RelationshipEpisode(
                relationship_id=str(row["relationship_id"]),
                source=row["source"],
                relationship_type=row["relationship_type"],
                target=row["target"],
                source_message_count=int(row["source_message_count"]),
            )
            for row in rows
        ]

    @staticmethod
    def _date_range(
        after: Optional[datetime], before: Optional[datetime]
    ) -> tuple[str, list[datetime]]:
        """Match Episodes whose time span overlaps the range."""

        sql, params = "", []
        if after is not None:
            sql += " AND e.last_message_at >= %s"
            params.append(after)
        if before is not None:
            sql += " AND e.first_message_at <= %s"
            params.append(before)
        return sql, params

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
