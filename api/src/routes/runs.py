"""Run admission and safe SSE translation."""

import json
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from uuid import uuid4

from pydantic import ValidationError
from starlette.responses import StreamingResponse
from starlette.routing import Route

from common.exceptions import (
    IdempotencyConflictError,
    RequestInProgressError,
    SessionBusyError,
)
from common.schema.primitives import Message
from common.schema.document import DocumentSelection, create_document_focus
from common.schema.source.references import SourceConsulted
from common.utils.streams import ClosingAsyncIterator

from contracts.sessions import StartRun
from routes.project_settings import _error, _json


class OwnedStreamResponse(StreamingResponse):
    def __init__(self, stream: ClosingAsyncIterator):
        super().__init__(stream, media_type="text/event-stream", headers={
            "Cache-Control": "no-cache", "X-Accel-Buffering": "no",
        })
        self.owned_stream = stream

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self.owned_stream.aclose()


def _frame(event: str, data: dict) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data, separators=(',', ':'))}\n\n".encode()


async def _events(raw: AsyncIterator, run_id: str, *, runtime=None, session=None):
    sequence = 0
    yield _frame("run_started", {"run_id": run_id, "sequence": sequence})
    sequence += 1
    terminal = False
    try:
        async for item in raw:
            if terminal:
                continue
            name = item.get("event") if isinstance(item, dict) else None
            data = item.get("data", {}) if isinstance(item, dict) else {}
            if not isinstance(data, dict):
                data = {}
            if name == "token":
                event, public = "message_delta", {"content": str(data.get("content", ""))}
            elif name in {"tool_start", "tool_end", "tool_error"}:
                event = "tool"
                public = {"name": str(data.get("tool", "")), "state": name}
                if name == "tool_error":
                    if data.get("code") in {"tool_failed", "workspace_conflict"}:
                        public["code"] = data["code"]
                    public["retryable"] = bool(data.get("retryable", False))
            elif name == "response":
                message_id = data.get("assistant_message_id")
                sources = []
                if runtime is not None and session is not None and isinstance(message_id, int) and message_id > 0:
                    values = await runtime.resources.knowledge_store.get_message_source_refs(
                        message_id, user_name=session.user_name,
                        project_id=session.project_id, session_id=session.session_id,
                    )
                    sources = [SourceConsulted.model_validate(value).model_dump(mode="json") for value in values]
                for source in sources:
                    yield _frame("source_added", {"run_id": run_id, "sequence": sequence, "source": source})
                    sequence += 1
                if data.get("usage") is not None:
                    yield _frame("usage_updated", {"run_id": run_id, "sequence": sequence, "usage": data["usage"]})
                    sequence += 1
                artifact = None
                if runtime is not None and session is not None and isinstance(message_id, int) and message_id > 0:
                    value = await runtime.resources.knowledge_store.get_message_artifact(
                        message_id, user_name=session.user_name,
                        project_id=session.project_id, session_id=session.session_id,
                    )
                    if value is not None:
                        payload = value.model_dump() if hasattr(value, "model_dump") else dict(value)
                        fields = ("artifact_id", "project_id", "session_id", "originating_message_id",
                                  "kind", "title", "status", "current_revision", "created_at", "updated_at")
                        artifact = {
                            field: str(payload[field]) if field == "artifact_id"
                            else payload[field].isoformat() if field in {"created_at", "updated_at"}
                            else payload[field]
                            for field in fields
                        }
                event, public = "run_completed", {
                    "content": str(data.get("content", "")),
                    "assistant_message_id": message_id,
                    "usage": data.get("usage"),
                    "research_mode": data.get("research_mode"),
                    "source_ref_ids": data.get("source_ref_ids", []),
                    "artifact": artifact,
                }
                terminal = True
            elif name == "clarification":
                event, public = "run_failed", {
                    "code": "clarification_required", "message": "The agent needs clarification."
                }
                terminal = True
            elif name == "error":
                event, public = "run_failed", {
                    "code": data.get("code") if data.get("code") in {"llm_budget_exhausted", "workspace_conflict"} else "run_failed",
                    "message": "The response could not be completed.",
                }
                terminal = True
            else:
                continue
            yield _frame(event, {"run_id": run_id, "sequence": sequence, **public})
            sequence += 1
    except Exception:
        if not terminal:
            yield _frame("run_failed", {
                "run_id": run_id, "sequence": sequence, "code": "run_failed",
                "message": "The response could not be completed.",
            })
            terminal = True
    if not terminal:
        yield _frame("run_failed", {
            "run_id": run_id, "sequence": sequence, "code": "run_failed",
            "message": "The response did not produce a final answer.",
        })


async def start_run(request):
    runtime = request.app.state.runtime
    session_id = request.path_params["session_id"]
    try:
        body = StartRun.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_run", "Invalid run request", 422)
    metadata = (await runtime.sessions.list_sessions()).get(session_id)
    if metadata is None:
        return _error("session_not_found", "Session not found", 404)
    project = await runtime.projects.get_project(metadata["project_id"])
    if project is None or project["status"] != "active":
        return _error("project_unavailable", "Project is unavailable", 409)
    try:
        session = await runtime.sessions.get_or_resume_session(session_id)
        focus = await _request_focus(session, body)
        raw = await session.open_agent_run_stream(
            Message(content=body.query), model=body.model, agent_id=body.agent_id,
            enabled_tools=body.enabled_tools, document_focus=focus,
            idempotency_key=body.idempotency_key,
            research_mode=body.research_mode,
        )
    except SessionBusyError:
        return _error("session_busy", "This session already has an active run", 409)
    except IdempotencyConflictError:
        return _error("idempotency_conflict", "This request key has different content", 409)
    except RequestInProgressError:
        return _error("request_in_progress", "This request is already in progress", 409)
    except ValueError:
        return _error("invalid_run", "Invalid run request", 422)
    run_id = str(uuid4())
    return OwnedStreamResponse(ClosingAsyncIterator(
        _events(raw, run_id, runtime=runtime, session=session), raw,
    ))


async def _request_focus(session, body):
    requested = body.document_focus
    if requested is None:
        return None
    service = session.document_service
    if service is None:
        raise RuntimeError("Session document service is unavailable")
    target = await service.resolve_focus_target(
        document_id=requested.document_id if requested.target_type == "document" else None,
        path_prefix=requested.path_prefix if requested.target_type == "subtree" else None,
    )
    if requested.target_type == "document" and requested.selection is not None:
        resolved = await service.resolve_document_selection(
            document_id=requested.document_id, selection=requested.selection,
        )
        target["selection"] = DocumentSelection.model_validate(resolved)
    return create_document_focus(
        mode="request", behavior=requested.behavior,
        created_at=datetime.now(timezone.utc), **target,
    )


routes = [
    Route("/v1/sessions/{session_id}/runs", start_run, methods=["POST"]),
]
