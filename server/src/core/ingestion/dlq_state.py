import hashlib
import json
from typing import Any, Dict, Iterable

DLQ_STATUS_QUEUED = "queued"
DLQ_STATUS_PROCESSING = "processing"
DLQ_STATUS_PARKED = "parked"
DLQ_STATUS_COMPLETED = "completed"

TERMINAL_DLQ_STATUSES = frozenset({DLQ_STATUS_PARKED, DLQ_STATUS_COMPLETED})


def compute_dlq_id(entry: Dict[str, Any]) -> str:
    """Deterministic ID for one logical DLQ work item."""
    identity = {
        "user_name": entry.get("user_name"),
        "project_id": entry.get("project_id"),
        "session_id": entry.get("session_id"),
        "stage": entry.get("stage", "processing"),
        "message_ids": sorted(_clean_ids(_message_ids(entry))),
        "entity_ids": sorted(_clean_ids(_entity_ids(entry))),
    }
    raw = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def ensure_dlq_id(entry: Dict[str, Any]) -> str:
    dlq_id = str(entry.get("dlq_id") or "").strip()
    if not dlq_id:
        dlq_id = compute_dlq_id(entry)
        entry["dlq_id"] = dlq_id
    return dlq_id


def serialize_dlq_entry(entry: Dict[str, Any]) -> str:
    ensure_dlq_id(entry)
    return json.dumps(entry, sort_keys=True, default=str)


def _message_ids(entry: Dict[str, Any]) -> Iterable[Any]:
    for message in entry.get("messages") or []:
        if isinstance(message, dict):
            yield message.get("id")


def _entity_ids(entry: Dict[str, Any]) -> Iterable[Any]:
    batch_result = entry.get("batch_result")
    if not isinstance(batch_result, dict):
        return
    resolution = batch_result.get("resolution")
    if not isinstance(resolution, dict):
        return
    for key in ("entity_ids", "new_ids", "alias_ids"):
        value = resolution.get(key)
        if isinstance(value, (list, tuple, set)):
            yield from value


def _clean_ids(values: Iterable[Any]) -> list[str]:
    clean = []
    for value in values:
        if value is None:
            continue
        clean.append(str(value))
    return clean
