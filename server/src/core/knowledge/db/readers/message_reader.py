"""Canonical message read queries."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from loguru import logger

from common.exceptions import StorageReadError
from common.scoping import require_scope_value, require_visible_project_ids
from infrastructure.postgres_client import PostgresClient


@dataclass(frozen=True, slots=True)
class UserAgentExchange:
    """Canonical terminal state needed to replay one accepted agent request."""

    user_message_id: int
    exchange_state: str
    exchange_outcome: str | None
    assistant_message_id: int | None
    assistant_content: str | None
    assistant_metadata: dict[str, Any]
    source_ref_ids: tuple[str, ...]


class MessageReader:
    """Read canonical messages, including lexical full-text search."""

    def __init__(self, client: PostgresClient):
        self.client = client

    @staticmethod
    def _raise_storage_read(operation: str, exc: Exception) -> None:
        logger.error("Storage read failed for {}: {}", operation, exc)
        raise StorageReadError(
            operation,
            details={"error_type": type(exc).__name__},
        ) from exc

    @staticmethod
    def _sanitize_fts_query(query: str) -> str:
        tokens = re.findall(r"\w+", query or "")
        return " | ".join(tokens)

    @staticmethod
    def _clean_string(value: Any) -> Any:
        if isinstance(value, str):
            return value.strip('"')
        return value

    def _parse_message_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "user_name": self._clean_string(row["user_name"]),
            "session_id": self._clean_string(row["session_id"]),
            "role": self._clean_string(row["role"]),
            "content": self._clean_string(row["content"]),
            "timestamp": row["timestamp"],
        }

    async def get_visible_session_ids(
        self,
        *,
        user_name: str,
        visible_project_ids: List[str],
    ) -> List[str]:
        """Return open sessions visible to one scoped message search."""

        user_name = require_scope_value(
            user_name,
            "user_name",
            "get_visible_session_ids",
        )
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_visible_session_ids",
        )
        try:
            rows = await self.client.fetch_all(
                """
                SELECT session_id
                FROM public.sessions
                WHERE user_name = %s
                  AND project_id = ANY(%s)
                  AND status = 'open'
                """,
                (user_name, visible_project_ids),
            )
        except Exception as exc:
            self._raise_storage_read("get_visible_session_ids", exc)
        return sorted({str(row["session_id"]) for row in rows})

    async def search_fts(
        self,
        query: str,
        *,
        user_name: str,
        session_ids: list[str],
        visible_project_ids: list[str],
        limit: int = 50,
    ) -> list[tuple[int, float, str]]:
        """Return canonical messages matching a sanitized lexical query."""

        user_name = require_scope_value(user_name, "user_name", "search_fts")
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "search_fts",
        )
        sanitized = self._sanitize_fts_query(query)
        if not sanitized or not session_ids:
            return []

        sql = """
        SELECT
            m.message_id,
            m.session_id,
            ts_rank(m.search_tsvector, to_tsquery('english', %s)) AS score
        FROM messages m
        JOIN sessions s
          ON s.session_id = m.session_id
         AND s.project_id = m.project_id
         AND s.user_name = m.user_name
        WHERE m.search_tsvector @@ to_tsquery('english', %s)
          AND m.user_name = %s
          AND m.session_id = ANY(%s)
          AND m.project_id = ANY(%s)
          AND m.lifecycle_state = 'sealed'
          AND s.status = 'open'
        ORDER BY score DESC
        LIMIT %s
        """
        try:
            rows = await self.client.fetch_all(
                sql,
                (
                    sanitized,
                    sanitized,
                    user_name,
                    session_ids,
                    visible_project_ids,
                    limit,
                ),
            )
            return [
                (int(row["message_id"]), float(row["score"]), row["session_id"])
                for row in rows
            ]
        except Exception as exc:
            self._raise_storage_read("search_fts", exc)

    async def get_user_agent_exchange(
        self,
        user_message_id: int,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> UserAgentExchange | None:
        """Read one accepted user exchange and its canonical assistant result."""

        if not isinstance(user_message_id, int) or isinstance(user_message_id, bool):
            raise ValueError("user_message_id must be an integer")
        if user_message_id <= 0:
            raise ValueError("user_message_id must be positive")
        user_name = require_scope_value(
            user_name, "user_name", "get_user_agent_exchange"
        )
        project_id = require_scope_value(
            project_id, "project_id", "get_user_agent_exchange"
        )
        session_id = require_scope_value(
            session_id, "session_id", "get_user_agent_exchange"
        )
        try:
            row = await self.client.fetch_one(
                """
                SELECT user_message.message_id AS user_message_id,
                       user_message.exchange_state,
                       user_message.exchange_outcome,
                       assistant_message.message_id AS assistant_message_id,
                       assistant_message.content AS assistant_content,
                       assistant_message.metadata AS assistant_metadata,
                       COALESCE(
                           (
                               SELECT array_agg(
                                   reference.source_ref_id::text
                                   ORDER BY reference.created_at ASC,
                                            reference.result_position ASC,
                                            reference.source_ref_id ASC
                               )
                               FROM public.message_source_refs AS reference
                               WHERE reference.project_id = assistant_message.project_id
                                 AND reference.session_id = assistant_message.session_id
                                 AND reference.message_id = assistant_message.message_id
                           ),
                           ARRAY[]::text[]
                       ) AS source_ref_ids
                FROM public.messages AS user_message
                LEFT JOIN public.messages AS assistant_message
                  ON assistant_message.user_name = user_message.user_name
                 AND assistant_message.project_id = user_message.project_id
                 AND assistant_message.session_id = user_message.session_id
                 AND assistant_message.user_msg_id = user_message.message_id
                 AND assistant_message.role = 'assistant'
                WHERE user_message.user_name = %s
                  AND user_message.project_id = %s
                  AND user_message.session_id = %s
                  AND user_message.message_id = %s
                  AND user_message.role = 'user'
                """,
                (user_name, project_id, session_id, user_message_id),
            )
        except Exception as exc:
            self._raise_storage_read("get_user_agent_exchange", exc)
        if row is None:
            return None
        metadata = row.get("assistant_metadata") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (TypeError, ValueError):
                metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        source_ref_ids = row.get("source_ref_ids") or []
        return UserAgentExchange(
            user_message_id=int(row["user_message_id"]),
            exchange_state=str(row["exchange_state"]),
            exchange_outcome=(
                None
                if row.get("exchange_outcome") is None
                else str(row["exchange_outcome"])
            ),
            assistant_message_id=(
                None
                if row.get("assistant_message_id") is None
                else int(row["assistant_message_id"])
            ),
            assistant_content=(
                None
                if row.get("assistant_content") is None
                else str(row["assistant_content"])
            ),
            assistant_metadata=metadata,
            source_ref_ids=tuple(str(value) for value in source_ref_ids),
        )

    async def get_message_text(
        self,
        message_id: int,
        *,
        user_name: str,
        session_id: str,
        visible_project_ids: List[str],
    ) -> str:
        """Return one scoped canonical message body or normal absence."""

        user_name = require_scope_value(user_name, "user_name", "get_message_text")
        session_id = require_scope_value(session_id, "session_id", "get_message_text")
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_message_text",
        )
        try:
            row = await self.client.fetch_one(
                """
                SELECT content
                FROM messages
                WHERE user_name = %s
                  AND session_id = %s
                  AND message_id = %s
                  AND project_id = ANY(%s)
                """,
                (user_name, session_id, message_id, visible_project_ids),
            )
        except Exception as exc:
            self._raise_storage_read("get_message_text", exc)
        if not row:
            return ""
        return self._clean_string(row["content"])

    async def get_messages_by_ids(
        self,
        ids: List[int],
        *,
        user_name: str,
        session_ids: List[str],
        visible_project_ids: List[str],
        discoverable_only: bool = False,
    ) -> List[Dict[str, Any]]:
        """Return scoped canonical messages, with discovery filtering when asked."""

        user_name = require_scope_value(
            user_name,
            "user_name",
            "get_messages_by_ids",
        )
        if not session_ids:
            raise ValueError("get_messages_by_ids requires session_ids scope")
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_messages_by_ids",
        )
        if not ids:
            return []

        discovery_clause = (
            """
          AND lifecycle_state = 'sealed'
          AND EXISTS (
              SELECT 1
              FROM sessions
              WHERE sessions.session_id = messages.session_id
                AND sessions.project_id = messages.project_id
                AND sessions.user_name = messages.user_name
                AND sessions.status = 'open'
          )
            """
            if discoverable_only
            else ""
        )
        query = f"""
        SELECT
            message_id AS id,
            user_name,
            session_id,
            role,
            content,
            timestamp_ms AS timestamp
        FROM messages
        WHERE message_id = ANY(%s)
          AND user_name = %s
          AND session_id = ANY(%s)
          AND project_id = ANY(%s)
          {discovery_clause}
        ORDER BY message_id ASC
        """
        try:
            rows = await self.client.fetch_all(
                query,
                (
                    ids,
                    user_name,
                    session_ids,
                    visible_project_ids,
                ),
            )
        except Exception as exc:
            self._raise_storage_read("get_messages_by_ids", exc)
        return [self._parse_message_row(row) for row in rows]

    async def get_recent_project_messages(
        self,
        user_name: str,
        project_id: str,
        limit: int,
        before_message_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Return a project-owned chronological message window."""

        user_name = require_scope_value(
            user_name,
            "user_name",
            "get_recent_project_messages",
        )
        project_id = require_scope_value(
            project_id,
            "project_id",
            "get_recent_project_messages",
        )
        if limit <= 0:
            return []

        before_clause = "AND message_id < %s" if before_message_id is not None else ""
        query = f"""
        SELECT
            message_id AS id,
            user_name,
            session_id,
            role,
            content,
            timestamp_ms AS timestamp
        FROM messages
        WHERE user_name = %s
        AND project_id = %s
        {before_clause}
        ORDER BY message_id DESC
        LIMIT %s
        """
        query_params = (
            (user_name, project_id, before_message_id)
            if before_message_id is not None
            else (user_name, project_id)
        )
        try:
            rows = await self.client.fetch_all(query, (*query_params, limit))
        except Exception as exc:
            self._raise_storage_read("get_recent_project_messages", exc)
        return [self._parse_message_row(row) for row in reversed(rows)]

    async def get_surrounding_messages(
        self,
        message_id: int,
        *,
        user_name: str,
        session_id: str,
        visible_project_ids: List[str],
        forward: int = 3,
        target_total: int = 10,
        discoverable_only: bool = False,
    ) -> List[Dict[str, Any]]:
        """Return a bounded chronological window around one scoped message."""

        user_name = require_scope_value(
            user_name,
            "user_name",
            "get_surrounding_messages",
        )
        session_id = require_scope_value(
            session_id,
            "session_id",
            "get_surrounding_messages",
        )
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_surrounding_messages",
        )
        back_limit = max(0, target_total - forward - 1)
        discovery_clause = (
            """
              AND lifecycle_state = 'sealed'
              AND EXISTS (
                  SELECT 1
                  FROM sessions
                  WHERE sessions.session_id = messages.session_id
                    AND sessions.project_id = messages.project_id
                    AND sessions.user_name = messages.user_name
                    AND sessions.status = 'open'
              )
            """
            if discoverable_only
            else ""
        )
        try:
            target_rows = await self.get_messages_by_ids(
                [message_id],
                user_name=user_name,
                session_ids=[session_id],
                visible_project_ids=visible_project_ids,
                discoverable_only=discoverable_only,
            )
            if not target_rows:
                return []
            target = target_rows[0]
            target_ts = target["timestamp"]
            back_query = f"""
            SELECT
                message_id AS id,
                user_name,
                session_id,
                role,
                content,
                timestamp_ms AS timestamp
            FROM messages
            WHERE (
                    timestamp_ms < %s
                 OR (timestamp_ms = %s AND message_id < %s)
                 OR (%s::BIGINT IS NULL AND timestamp_ms IS NOT NULL)
                 OR (
                        %s::BIGINT IS NULL
                    AND timestamp_ms IS NULL
                    AND message_id < %s
                 )
              )
              AND user_name = %s
              AND session_id = %s
              AND project_id = ANY(%s)
              {discovery_clause}
            ORDER BY timestamp_ms DESC NULLS FIRST, message_id DESC
            LIMIT %s
            """
            forward_query = f"""
            SELECT
                message_id AS id,
                user_name,
                session_id,
                role,
                content,
                timestamp_ms AS timestamp
            FROM messages
            WHERE (
                    timestamp_ms > %s
                 OR (timestamp_ms = %s AND message_id > %s)
                 OR (%s::BIGINT IS NOT NULL AND timestamp_ms IS NULL)
                 OR (
                        %s::BIGINT IS NULL
                    AND timestamp_ms IS NULL
                    AND message_id > %s
                 )
              )
              AND user_name = %s
              AND session_id = %s
              AND project_id = ANY(%s)
              {discovery_clause}
            ORDER BY timestamp_ms ASC NULLS LAST, message_id ASC
            LIMIT %s
            """
            params_prefix = (
                target_ts,
                target_ts,
                message_id,
                target_ts,
                target_ts,
                message_id,
                user_name,
                session_id,
                visible_project_ids,
            )
            previous_rows = await self.client.fetch_all(
                back_query,
                (*params_prefix, back_limit),
            )
            following_rows = await self.client.fetch_all(
                forward_query,
                (*params_prefix, forward),
            )
        except Exception as exc:
            self._raise_storage_read("get_surrounding_messages", exc)
        return [
            *[self._parse_message_row(row) for row in reversed(previous_rows)],
            target,
            *[self._parse_message_row(row) for row in following_rows],
        ]
