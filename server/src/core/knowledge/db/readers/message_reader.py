"""Canonical message read queries."""

from __future__ import annotations

import re

from loguru import logger

from common.exceptions import StorageReadError
from common.scoping import require_scope_value, require_visible_project_ids
from infrastructure.postgres_client import PostgresClient


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
            message_id,
            session_id,
            ts_rank(search_tsvector, to_tsquery('english', %s)) AS score
        FROM messages
        WHERE search_tsvector @@ to_tsquery('english', %s)
          AND user_name = %s
          AND session_id = ANY(%s)
          AND project_id = ANY(%s)
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
