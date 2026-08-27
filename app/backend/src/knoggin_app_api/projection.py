"""UI-specific JSON and SSE projections of the SDK facade."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from knoggin import SessionHandle

from .runs import RunEvent, RunSnapshot


def project_response(project: dict[str, Any] | None) -> dict[str, Any]:
    """Project engine project data into the browser-facing response shape."""

    project = project or {}
    return {
        "id": str(project.get("id", "")),
        "name": str(project.get("name", "")),
        "description": project.get("description"),
        "status": str(project.get("status", "active")),
        "sessionCount": int(project.get("session_count", 0)),
        "createdAt": timestamp(project.get("created_at")),
        "updatedAt": timestamp(project.get("updated_at")),
    }


def session_response(session: SessionHandle) -> dict[str, Any]:
    return {
        "id": session.session_id,
        "projectId": session.project_id,
        "model": session.model,
    }


def run_response(run: RunSnapshot) -> dict[str, Any]:
    result = completed_data(run.result) if run.result is not None else None
    return {
        "id": run.run_id,
        "sessionId": run.session_id,
        "status": run.status,
        "createdAt": run.created_at.isoformat(),
        "completedAt": run.completed_at.isoformat() if run.completed_at else None,
        "assistantMessageId": result.get("assistantMessageId") if result else None,
        "result": result,
    }


def event_response(event: RunEvent) -> dict[str, Any] | None:
    """Return one UI event, omitting engine details unsafe for a browser."""

    data = event.data
    event_type: str
    public_data: dict[str, Any]
    if event.event == "run_started":
        event_type, public_data = "run.started", {}
    elif event.event == "token":
        event_type, public_data = (
            "message.delta",
            {"text": str(data.get("content", ""))},
        )
    elif event.event == "thinking":
        return None
    elif event.event == "tool_start":
        event_type, public_data = (
            "tool.started",
            {
                "callId": str(data.get("call_id", "")),
                "tool": str(data.get("tool", "")),
                "arguments": allowlisted_arguments(data.get("args")),
            },
        )
    elif event.event in {"tool_end", "tool_error"}:
        event_type, public_data = (
            "tool.completed",
            {
                "callId": str(data.get("call_id", "")),
                "tool": str(data.get("tool", "")),
                "status": "failed" if event.event == "tool_error" else "completed",
            },
        )
    elif event.event == "response":
        event_type, public_data = "run.completed", completed_data(data)
    elif event.event == "clarification":
        event_type, public_data = (
            "run.awaiting_input",
            {"question": str(data.get("question", ""))},
        )
    elif event.event in {"error", "run_failed"}:
        event_type, public_data = (
            "run.failed",
            {"code": str(data.get("code", "RUN_FAILED"))},
        )
    elif event.event == "run_cancelled":
        event_type, public_data = "run.cancelled", {}
    else:
        return None
    return {
        "type": event_type,
        "version": "v1",
        "runId": event.run_id,
        "sequence": event.sequence,
        "timestamp": event.timestamp.isoformat(),
        "data": public_data,
    }


def completed_data(data: dict[str, Any]) -> dict[str, Any]:
    sources = data.get("sources_consulted", [])
    if not isinstance(sources, list):
        sources = []
    source_ref_ids = data.get("source_ref_ids", [])
    if not isinstance(source_ref_ids, list):
        source_ref_ids = []

    citations = []
    for index, source in enumerate(sources):
        if not isinstance(source, dict):
            continue
        citation = citation_response(source)
        if index < len(source_ref_ids) and isinstance(source_ref_ids[index], str):
            citation["id"] = source_ref_ids[index]
        citations.append(citation)

    usage = data.get("usage")
    return {
        "content": str(data.get("content", "")),
        "citations": citations,
        "usage": usage if isinstance(usage, dict) else None,
        "assistantMessageId": data.get("assistant_message_id"),
        "sourceRefIds": [item for item in source_ref_ids if isinstance(item, str)],
    }


def citation_response(source: dict[str, Any]) -> dict[str, Any]:
    metadata = source.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    kind = str(source.get("source_kind", "unknown"))
    label = (
        text(metadata.get("document_name"))
        or text(metadata.get("title"))
        or ("Pasted text" if kind == "user_pasted_text" else kind)
    )
    citation: dict[str, Any] = {
        "kind": kind,
        "label": label,
        "excerpt": str(source.get("excerpt", "")),
        "locator": source.get("locator"),
    }
    for public_name, engine_name in (
        ("id", "source_ref_id"),
        ("documentId", "document_id"),
        ("url", "canonical_url"),
    ):
        value = text(source.get(engine_name))
        if value:
            citation[public_name] = value
    return citation


def allowlisted_arguments(value: Any) -> dict[str, str | int | float | bool]:
    if not isinstance(value, dict):
        return {}
    allowed = {
        "query",
        "search_query",
        "name",
        "entity_name",
        "entity_type",
        "document_id",
        "relative_path",
        "page",
        "limit",
        "start_line",
        "end_line",
    }
    result: dict[str, str | int | float | bool] = {}
    for key, item in value.items():
        if key not in allowed or isinstance(item, (dict, list, tuple)):
            continue
        if isinstance(item, (str, int, float, bool)):
            result[key] = item[:200] if isinstance(item, str) else item
    return result


def text(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def timestamp(value: Any) -> str | None:
    """Serialize an engine timestamp only at the browser HTTP boundary."""

    if isinstance(value, datetime):
        return value.isoformat()
    return text(value)
