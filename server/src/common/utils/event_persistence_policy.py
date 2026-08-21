from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Dict, Optional

from common.schema.events import InternalEvent

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
        ("pipeline", "graph_write_failed"),
        ("pipeline", "buffer_invalid_entries"),
        ("pipeline", "drain_complete"),
        ("pipeline", "local_reference_resolution_failed"),
        ("job", "merge_queue_marked"),
        ("job", "merge_queue_removed"),
        ("job", "maintenance_deferred"),
        ("job", "episode_processed"),
        ("job", "episode_processing_failed"),
        ("job", "episode_validation_failed"),
        ("job", "local_reference_resolution_failed"),
        ("job", "episodes_write_failed"),
        ("job", "failed"),
        ("job", "timeout"),
        ("agent", "episode_retrieval_completed"),
        ("agent", "episode_source_messages_expanded"),
        ("agent", "local_reference_resolution_failed"),
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
        "pipeline",
        "reference_type",
        "error",
        "error_type",
        "redis_key",
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
        "entity_count",
        "entity_link_count",
        "msg_count",
        "message_count",
        "source_message_count",
        "episode_source_message_count",
        "episode_count",
        "focus_episode_count",
        "relationship_link_count",
        "returned_evidence_count",
        "expanded_source_message_count",
        "count",
        "cleared_count",
        "marked_count",
        "partial_flush",
        "status",
        "action",
        "episode_id",
        "failed_episode_ids",
        "strategy",
        "processing_latency_ms",
        "retrieval_latency_ms",
        "source_message_expansion_latency_ms",
        "consolidation_limit_hit",
        "episode_at_max_size",
        "invalid_identifier",
        "focus_entity_retrieval",
        "used_raw_message_fallback",
    }
)

LOCAL_REFERENCE_FAILURE_FIELDS = frozenset(
    {
        "pipeline",
        "reference_type",
        "reason",
        "stage",
    }
)


def normalize_coordination_event(
    internal_event: InternalEvent,
) -> Optional[Dict[str, Any]]:
    """Return scrubbed coordination fields for an internal event, if retained."""

    ts = internal_event.ts
    scope_id = internal_event.scope_id
    component = internal_event.component
    event = internal_event.event
    data = internal_event.data
    verbose_only = internal_event.verbose_only
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

    allowed_fields = (
        LOCAL_REFERENCE_FAILURE_FIELDS
        if event == "local_reference_resolution_failed"
        else SAFE_FIELDS
    )
    for key, value in payload.items():
        if key in CONTENT_KEYS or key not in allowed_fields:
            continue
        normalized_key = _normalize_field_name(key)
        if normalized_key in fields:
            continue
        safe_value = _safe_value(value)
        if safe_value is not None:
            fields[normalized_key] = safe_value

    return fields


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
