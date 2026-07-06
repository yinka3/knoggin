from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Dict, Optional

MAX_TEXT_LENGTH = 200
MAX_LIST_ITEMS = 25

CONTENT_KEYS = frozenset(
    {
        "prompt",
        "content",
        "message",
        "messages",
        "conversation",
        "conversation_text",
        "generated",
        "answer",
        "response",
        "document",
        "chunk",
        "session_text",
        "batch_result",
    }
)

APPROVED_EVENTS = frozenset(
    {
        ("pipeline", "dlq_enqueued"),
        ("pipeline", "dlq_write_failed"),
        ("pipeline", "graph_write_failed"),
        ("pipeline", "buffer_invalid_entries"),
        ("pipeline", "drain_complete"),
        ("job", "dlq_parked"),
        ("job", "dlq_retry_success"),
        ("job", "dlq_retry_failed"),
        ("job", "dlq_reprocess_success"),
        ("job", "dlq_graph_write_success"),
        ("job", "dirty_entities_marked"),
        ("job", "dirty_entities_cleared"),
        ("job", "merge_queue_marked"),
        ("job", "merge_queue_removed"),
        ("job", "invalidation_failures"),
        ("job", "facts_write_failed"),
        ("job", "maintenance_deferred"),
        ("job", "profile_refinement_failed"),
        ("job", "profiles_refined"),
        ("job", "user_profile_refined"),
        ("job", "failed"),
        ("job", "timeout"),
        ("entities", "entity_merged"),
    }
)

SAFE_FIELDS = frozenset(
    {
        "user",
        "user_name",
        "project_id",
        "session_id",
        "name",
        "job",
        "stage",
        "attempt",
        "max_attempts",
        "reason",
        "error",
        "error_type",
        "redis_key",
        "dlq_key",
        "dlq_id",
        "park_key",
        "source_buffer_key",
        "buffer_key",
        "dirty_key",
        "merge_key",
        "message_id",
        "message_ids",
        "msg_id",
        "msg_ids",
        "entity_id",
        "entity_ids",
        "primary_id",
        "duplicate_id",
        "audit_id",
        "proposal_id",
        "failed_fact_ids",
        "entity_count",
        "fact_count",
        "facts_created",
        "facts_invalidated",
        "msg_count",
        "message_count",
        "dlq_count",
        "count",
        "cleared_count",
        "marked_count",
        "partial_flush",
        "status",
    }
)


@dataclass(frozen=True)
class CoordinationEventRecord:
    fields: Dict[str, Any]


def normalize_coordination_event(
    *,
    ts: str,
    scope_id: str,
    component: str,
    event: str,
    data: Optional[Dict[str, Any]],
    verbose_only: bool = False,
) -> Optional[CoordinationEventRecord]:
    if verbose_only or (component, event) not in APPROVED_EVENTS:
        return None

    payload = data or {}
    fields: Dict[str, Any] = {
        "ts": ts,
        "label": "RECOVERY",
        "retention": "recovery",
        "component": component,
        "event": f"{component}.{event}",
    }

    user = payload.get("user") or payload.get("user_name")
    if user:
        fields["user"] = user

    project_id = payload.get("project_id") or scope_id
    if project_id:
        fields["project_id"] = project_id

    for key, value in payload.items():
        if key in CONTENT_KEYS or key not in SAFE_FIELDS:
            continue
        normalized_key = _normalize_field_name(key)
        if normalized_key in fields:
            continue
        safe_value = _safe_value(value)
        if safe_value is not None:
            fields[normalized_key] = safe_value

    return CoordinationEventRecord(fields=fields)


def _normalize_field_name(key: str) -> str:
    if key == "name":
        return "job"
    if key == "user_name":
        return "user"
    if key == "msg_ids":
        return "message_ids"
    if key == "msg_id":
        return "message_id"
    return key


def _safe_value(value: Any) -> Optional[Any]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, (list, tuple, set)):
        return _safe_list(value)
    if isinstance(value, dict):
        return None
    text = str(value).replace("\n", " ").replace("\r", " ").strip()
    if not text:
        return None
    if len(text) > MAX_TEXT_LENGTH:
        return f"{text[:MAX_TEXT_LENGTH]}..."
    return text


def _safe_list(values: Iterable[Any]) -> str:
    parts = []
    for value in list(values)[:MAX_LIST_ITEMS]:
        safe_value = _safe_value(value)
        if safe_value is not None:
            parts.append(str(safe_value))
    return ",".join(parts)
