"""Canonical message-row persistence."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from functools import wraps
from typing import Dict, List

from loguru import logger
from psycopg import Error as PsycopgError

from common.exceptions import StorageWriteError
from common.utils.time_utils import get_now_ms
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


class MessageWriter:
    """Own canonical message insertion; lifecycle stays with MessageLifecycleWriter."""

    def __init__(self, client: PostgresClient):
        self.client = client

    @staticmethod
    def _raise_storage_write(operation: str, exc: Exception) -> None:
        logger.error("Storage write failed for {}: {}", operation, exc)
        raise StorageWriteError(
            operation,
            details={"error_type": type(exc).__name__},
        ) from exc

    @staticmethod
    def _timestamp_ms(value) -> int:
        if value is None or value == "":
            return get_now_ms()
        if isinstance(value, datetime):
            return int(value.timestamp() * 1000)
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return int(parsed.timestamp() * 1000)
        return int(value)

    @asynccontextmanager
    async def _cursor(self, cur):
        if cur is not None:
            yield cur
            return
        async with self.client.transaction() as transaction_cursor:
            yield transaction_cursor

    @_storage_write("save_message_logs")
    async def save_message_logs(self, messages: List[Dict], *, cur=None) -> bool:
        if not messages:
            return True
        async with self._cursor(cur) as cursor:
            for message in messages:
                missing = [
                    key
                    for key in ("user_name", "session_id", "project_id")
                    if not message.get(key)
                ]
                if missing:
                    raise ValueError(
                        f"Message {message.get('id')} missing required scope fields: {missing}"
                    )
                timestamp = self._timestamp_ms(message.get("timestamp"))
                await cursor.execute(
                    """
                    INSERT INTO messages (
                        user_name, session_id, message_id, project_id, role, content,
                        user_msg_id, metadata, timestamp_ms, lifecycle_state,
                        editable_until_ms, sealed_at_ms, selected_revision,
                        replaces_message_id, superseded_at_ms, ingestion_state,
                        ingestion_not_before_ms, ingestion_claim_id,
                        ingestion_claimed_at_ms, episode_eligible, episode_type
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (user_name, session_id, message_id)
                    DO UPDATE SET message_id = EXCLUDED.message_id,
                        user_msg_id = EXCLUDED.user_msg_id,
                        metadata = EXCLUDED.metadata
                    WHERE messages.project_id = EXCLUDED.project_id
                      AND messages.role = EXCLUDED.role
                      AND messages.content = EXCLUDED.content
                      AND messages.timestamp_ms = EXCLUDED.timestamp_ms
                    RETURNING message_id
                    """,
                    (
                        message["user_name"], message["session_id"], message["id"],
                        message["project_id"], message["role"], message["content"],
                        message.get("user_msg_id") or (
                            message["id"] if message["role"] == "user" else None
                        ),
                        json.dumps(message.get("metadata") or {}), timestamp,
                        message.get("lifecycle_state", "sealed"),
                        message.get("editable_until_ms"), message.get("sealed_at_ms"),
                        int(message.get("selected_revision", 1)),
                        message.get("replaces_message_id"), message.get("superseded_at_ms"),
                        message.get("ingestion_state", "excluded"),
                        message.get("ingestion_not_before_ms"),
                        message.get("ingestion_claim_id"),
                        message.get("ingestion_claimed_at_ms"),
                        bool(message.get("episode_eligible", False)),
                        message.get("episode_type"),
                    ),
                )
                if not await cursor.fetchone():
                    raise RuntimeError(
                        "Canonical message ID collision for "
                        f"{message['user_name']}/{message['session_id']}/{message['id']}"
                    )
        return True
