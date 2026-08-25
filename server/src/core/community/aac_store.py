"""Durable PostgreSQL persistence for autonomous AAC discussions."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from loguru import logger
from psycopg import Error as PsycopgError

from common.exceptions import StorageReadError, StorageWriteError
from infrastructure.postgres_client import PostgresClient

_DISCUSSION_TERMINAL_STATUSES = frozenset(
    {"completed", "stopped", "interrupted", "failed"}
)
_TIMELINE_KINDS = frozenset({"agent_message", "system_event"})
_INSIGHT_VISIBILITIES = frozenset({"shared", "private"})
_VOTES = frozenset({"up", "down"})


class AACStore:
    """Own the user-level AAC transcript, Insight, and vote tables."""

    def __init__(self, client: PostgresClient) -> None:
        self._client = client

    @staticmethod
    def _text(value: object, field: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field} must be a nonblank string")
        return value.strip()

    @staticmethod
    def _limit(value: int, *, maximum: int = 100) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
            raise ValueError(f"limit must be an integer between 1 and {maximum}")
        return value

    @staticmethod
    def _nonnegative(value: object, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{field} must be a non-negative integer")
        return value

    async def _write(self, operation: str, query: str, params: dict[str, Any]) -> int:
        try:
            return await self._client.execute(query, params)
        except StorageWriteError:
            raise
        except PsycopgError as exc:
            logger.error("AAC storage write failed for {}: {}", operation, exc)
            raise StorageWriteError(
                operation,
                details={"error_type": type(exc).__name__},
            ) from exc

    async def _read(
        self,
        operation: str,
        query: str,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        try:
            return await self._client.fetch_all(query, params)
        except StorageReadError:
            raise
        except PsycopgError as exc:
            logger.error("AAC storage read failed for {}: {}", operation, exc)
            raise StorageReadError(
                operation,
                details={"error_type": type(exc).__name__},
            ) from exc

    async def create_discussion(
        self,
        *,
        discussion_id: str,
        user_name: str,
        topic: str,
        token_budget: int,
    ) -> None:
        """Create one active, application-owned AAC discussion."""

        discussion_id = self._text(discussion_id, "discussion_id")
        user_name = self._text(user_name, "user_name")
        topic = self._text(topic, "topic")
        token_budget = self._nonnegative(token_budget, "token_budget")
        await self._write(
            "create_aac_discussion",
            """
            INSERT INTO public.aac_discussions (
                discussion_id, user_name, topic, status, token_budget, tokens_used
            ) VALUES (
                %(discussion_id)s, %(user_name)s, %(topic)s, 'active',
                %(token_budget)s, 0
            )
            """,
            {
                "discussion_id": discussion_id,
                "user_name": user_name,
                "topic": topic,
                "token_budget": token_budget,
            },
        )

    async def append_timeline(
        self,
        *,
        discussion_id: str,
        user_name: str,
        kind: str,
        content: str,
        agent_id: str | None = None,
    ) -> str:
        """Append a scoped transcript message or lightweight system event."""

        discussion_id = self._text(discussion_id, "discussion_id")
        user_name = self._text(user_name, "user_name")
        if kind not in _TIMELINE_KINDS:
            raise ValueError(f"Unsupported AAC timeline kind: {kind}")
        content = self._text(content, "content")
        if agent_id is not None:
            agent_id = self._text(agent_id, "agent_id")
        timeline_id = str(uuid4())
        inserted = await self._write(
            "append_aac_timeline",
            """
            INSERT INTO public.aac_timeline (
                timeline_id, discussion_id, kind, agent_id, content
            )
            SELECT
                %(timeline_id)s, discussion_id, %(kind)s, %(agent_id)s, %(content)s
            FROM public.aac_discussions
            WHERE discussion_id = %(discussion_id)s AND user_name = %(user_name)s
            """,
            {
                "timeline_id": timeline_id,
                "discussion_id": discussion_id,
                "user_name": user_name,
                "kind": kind,
                "agent_id": agent_id,
                "content": content,
            },
        )
        if inserted == 0:
            raise ValueError("AAC discussion is not available to this user")
        return timeline_id

    async def finish_discussion(
        self,
        *,
        discussion_id: str,
        user_name: str,
        status: str,
        tokens_used: int,
    ) -> None:
        """Persist a terminal status and final approximate token total."""

        if status not in _DISCUSSION_TERMINAL_STATUSES:
            raise ValueError(f"Unsupported AAC terminal status: {status}")
        updated = await self._write(
            "finish_aac_discussion",
            """
            UPDATE public.aac_discussions
            SET status = %(status)s,
                tokens_used = GREATEST(tokens_used, %(tokens_used)s),
                ended_at = NOW()
            WHERE discussion_id = %(discussion_id)s AND user_name = %(user_name)s
              AND status = 'active'
            """,
            {
                "discussion_id": self._text(discussion_id, "discussion_id"),
                "user_name": self._text(user_name, "user_name"),
                "status": status,
                "tokens_used": self._nonnegative(tokens_used, "tokens_used"),
            },
        )
        if updated == 0:
            raise ValueError("AAC discussion is not active for this user")

    async def interrupt_active_discussions(self, *, user_name: str) -> int:
        """Mark stale active work interrupted during application startup."""

        return await self._write(
            "interrupt_active_aac_discussions",
            """
            UPDATE public.aac_discussions
            SET status = 'interrupted', ended_at = NOW()
            WHERE user_name = %(user_name)s AND status = 'active'
            """,
            {"user_name": self._text(user_name, "user_name")},
        )

    async def create_insight(
        self,
        *,
        user_name: str,
        author_agent_id: str,
        content: str,
        visibility: str = "shared",
        discussion_id: str | None = None,
    ) -> str:
        """Save an advisory AAC Insight independent of transcript retention."""

        if visibility not in _INSIGHT_VISIBILITIES:
            raise ValueError(f"Unsupported AAC Insight visibility: {visibility}")
        if discussion_id is not None:
            discussion_id = self._text(discussion_id, "discussion_id")
        insight_id = str(uuid4())
        inserted = await self._write(
            "create_aac_insight",
            """
            INSERT INTO public.aac_insights (
                insight_id, user_name, discussion_id, author_agent_id, visibility,
                content
            )
            SELECT
                %(insight_id)s, %(user_name)s, %(discussion_id)s,
                %(author_agent_id)s, %(visibility)s, %(content)s
            WHERE %(discussion_id)s IS NULL OR EXISTS (
                SELECT 1
                FROM public.aac_discussions
                WHERE discussion_id = %(discussion_id)s
                  AND user_name = %(user_name)s
            )
            """,
            {
                "insight_id": insight_id,
                "user_name": self._text(user_name, "user_name"),
                "discussion_id": discussion_id,
                "author_agent_id": self._text(author_agent_id, "author_agent_id"),
                "visibility": visibility,
                "content": self._text(content, "content"),
            },
        )
        if inserted == 0:
            raise ValueError("AAC discussion is not available to this user")
        return insight_id

    async def search_insights(
        self,
        *,
        user_name: str,
        viewer_agent_id: str,
        query: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Return shared Insights and the requesting agent's private Insights."""

        query = query.strip() if isinstance(query, str) else ""
        return await self._read(
            "search_aac_insights",
            """
            SELECT insight_id, discussion_id, author_agent_id, visibility, content,
                   created_at
            FROM public.aac_insights
            WHERE user_name = %(user_name)s
              AND (visibility = 'shared' OR author_agent_id = %(viewer_agent_id)s)
              AND (%(query)s = '' OR content ILIKE '%%' || %(query)s || '%%')
            ORDER BY created_at DESC, insight_id DESC
            LIMIT %(limit)s
            """,
            {
                "user_name": self._text(user_name, "user_name"),
                "viewer_agent_id": self._text(viewer_agent_id, "viewer_agent_id"),
                "query": query,
                "limit": self._limit(limit),
            },
        )

    async def cast_insight_vote(
        self,
        *,
        insight_id: str,
        user_name: str,
        voter_agent_id: str,
        vote: str,
        reason: str,
    ) -> None:
        """Create or replace one other-agent vote on a shared Insight."""

        if vote not in _VOTES:
            raise ValueError(f"Unsupported AAC Insight vote: {vote}")
        reason = self._text(reason, "reason")
        if len(reason) > 500:
            raise ValueError("AAC Insight vote reason must be 500 characters or fewer")
        updated = await self._write(
            "cast_aac_insight_vote",
            """
            INSERT INTO public.aac_insight_votes (
                insight_id, voter_agent_id, vote, reason
            )
            SELECT %(insight_id)s, %(voter_agent_id)s, %(vote)s, %(reason)s
            FROM public.aac_insights
            WHERE insight_id = %(insight_id)s
              AND user_name = %(user_name)s
              AND visibility = 'shared'
              AND author_agent_id <> %(voter_agent_id)s
            ON CONFLICT (insight_id, voter_agent_id) DO UPDATE
            SET vote = EXCLUDED.vote,
                reason = EXCLUDED.reason,
                updated_at = NOW()
            """,
            {
                "insight_id": self._text(insight_id, "insight_id"),
                "user_name": self._text(user_name, "user_name"),
                "voter_agent_id": self._text(voter_agent_id, "voter_agent_id"),
                "vote": vote,
                "reason": reason,
            },
        )
        if updated == 0:
            raise ValueError("Only another agent may vote on a shared AAC Insight")

    async def remove_insight_vote(
        self,
        *,
        insight_id: str,
        user_name: str,
        voter_agent_id: str,
    ) -> bool:
        """Remove the current agent's own vote, if present."""

        deleted = await self._write(
            "remove_aac_insight_vote",
            """
            DELETE FROM public.aac_insight_votes AS vote
            USING public.aac_insights AS insight
            WHERE vote.insight_id = insight.insight_id
              AND vote.insight_id = %(insight_id)s
              AND insight.user_name = %(user_name)s
              AND vote.voter_agent_id = %(voter_agent_id)s
            """,
            {
                "insight_id": self._text(insight_id, "insight_id"),
                "user_name": self._text(user_name, "user_name"),
                "voter_agent_id": self._text(voter_agent_id, "voter_agent_id"),
            },
        )
        return bool(deleted)

    async def list_insight_votes(
        self,
        *,
        insight_id: str,
        user_name: str,
    ) -> list[dict[str, Any]]:
        """Return advisory vote details for a user-owned Insight."""

        return await self._read(
            "list_aac_insight_votes",
            """
            SELECT vote.voter_agent_id, vote.vote, vote.reason, vote.created_at,
                   vote.updated_at
            FROM public.aac_insight_votes AS vote
            JOIN public.aac_insights AS insight
              ON insight.insight_id = vote.insight_id
            WHERE vote.insight_id = %(insight_id)s
              AND insight.user_name = %(user_name)s
            ORDER BY vote.updated_at DESC, vote.voter_agent_id
            """,
            {
                "insight_id": self._text(insight_id, "insight_id"),
                "user_name": self._text(user_name, "user_name"),
            },
        )
