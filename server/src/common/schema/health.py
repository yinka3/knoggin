"""Typed, bounded health contracts shared by runtime health surfaces.

The component coordinators expose rich internal snapshots for diagnostics. This
module provides the small public contract that those snapshots can be projected
into without leaking connection strings, document/message content, exception
objects, or identifiers belonging to another project.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import datetime
from enum import StrEnum
from itertools import islice
from typing import Any

from pydantic import Field, field_validator

from common.schema.config import ConfigModel
from common.utils.time_utils import get_now


class HealthStatus(StrEnum):
    """Severity of a component's current health."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"


class HealthActivity(StrEnum):
    """Whether a component is currently doing work independent of severity."""

    IDLE = "idle"
    BUSY = "busy"
    DELAYED = "delayed"


_SENSITIVE_KEY_NAMES = frozenset(
    {
        "active_projects",
        "agent_id",
        "authorization",
        "content",
        "credentials",
        "dsn",
        "document_id",
        "document_ids",
        "environment",
        "env",
        "error",
        "errors",
        "exception",
        "exceptions",
        "log",
        "logs",
        "message",
        "messages",
        "password",
        "project",
        "projects",
        "prompt",
        "project_id",
        "project_ids",
        "queued_by_project",
        "raw",
        "raw_error",
        "ready_projects",
        "secret",
        "secrets",
        "session_id",
        "source_id",
        "source_ids",
        "token",
        "tokens",
        "traceback",
        "url",
    }
)
_SENSITIVE_KEY_PARTS = (
    "api_key",
    "apikey",
    "connection_string",
    "credential",
    "password",
    "secret",
    "token",
    "traceback",
    "url",
)
_MAX_DETAIL_ITEMS = 100
_MAX_DETAIL_DEPTH = 4
_MAX_DETAIL_STRING_LENGTH = 500
_SENSITIVE_STRING_MARKERS = (
    "postgres://",
    "postgresql://",
    "amqp://",
    "mongodb://",
    "bearer ",
    "api_key=",
    "apikey=",
    "password=",
    "secret=",
    "token=",
)


def _is_sensitive_key(key: str) -> bool:
    normalized = key.casefold().replace("-", "_")
    if normalized in _SENSITIVE_KEY_NAMES:
        return True
    if normalized.endswith(("_id", "_ids")):
        return True
    if normalized.endswith(("_dsn", "_uri")):
        return True
    return any(part in normalized for part in _SENSITIVE_KEY_PARTS)


def _sanitize_value(value: Any, *, depth: int) -> Any:
    """Return a JSON-safe value while keeping snapshots bounded."""

    if value is None or isinstance(value, (str, int, bool)):
        if isinstance(value, str):
            return _sanitize_string(value)
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, BaseException):
        return None
    if depth >= _MAX_DETAIL_DEPTH:
        return "[truncated]"
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for raw_key, raw_value in islice(value.items(), _MAX_DETAIL_ITEMS):
            if not isinstance(raw_key, str) or _is_sensitive_key(raw_key):
                continue
            result[raw_key] = _sanitize_value(raw_value, depth=depth + 1)
        return result
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [
            _sanitize_value(item, depth=depth + 1)
            for item in islice(value, _MAX_DETAIL_ITEMS)
        ]
    if isinstance(value, (set, frozenset)):
        return [
            _sanitize_value(item, depth=depth + 1)
            for item in islice(value, _MAX_DETAIL_ITEMS)
        ]
    # Never call repr() on an arbitrary object: it can contain secrets and is
    # not guaranteed to be JSON serializable.
    return None


def _sanitize_string(value: str) -> str:
    """Bound strings and replace obvious credential-bearing payloads."""

    bounded = value[:_MAX_DETAIL_STRING_LENGTH]
    normalized = bounded.casefold()
    if any(marker in normalized for marker in _SENSITIVE_STRING_MARKERS):
        return "[redacted]"
    return bounded


def sanitize_health_details(details: Mapping[str, Any]) -> dict[str, Any]:
    """Project arbitrary component details into a bounded public shape."""

    sanitized = _sanitize_value(details, depth=0)
    return sanitized if isinstance(sanitized, dict) else {}


class HealthSnapshot(ConfigModel):
    """Common JSON-safe health envelope for one runtime component."""

    status: HealthStatus = HealthStatus.HEALTHY
    activity: HealthActivity = HealthActivity.IDLE
    summary: str = Field(min_length=1, max_length=500)
    details: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    checked_at: datetime = Field(default_factory=get_now)

    @field_validator("summary")
    @classmethod
    def _normalize_summary(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("summary must not be blank")
        return normalized

    @field_validator("details", mode="before")
    @classmethod
    def _sanitize_details(cls, value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if not isinstance(value, Mapping):
            raise ValueError("details must be a mapping")
        return sanitize_health_details(value)

    @field_validator("warnings", mode="before")
    @classmethod
    def _normalize_warnings(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str) or not isinstance(value, Sequence):
            raise ValueError("warnings must be a sequence of strings")
        return [
            _sanitize_string(item.strip())
            for item in islice(value, 32)
            if isinstance(item, str) and item.strip()
        ]
