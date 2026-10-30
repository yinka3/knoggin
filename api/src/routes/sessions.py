"""Durable session metadata routes."""

from pydantic import ValidationError
from starlette.responses import JSONResponse
from starlette.routing import Route

from common.exceptions import SessionBusyError
from common.schema.source.references import SourceConsulted
from contracts.sessions import (
    CreateSession,
    ImportExchanges,
    SessionPreferences,
    focus_input,
)
from routes.project_settings import _error, _json


def _public(row):
    from datetime import datetime

    return {
        key: value.isoformat() if isinstance(value, datetime) else value
        for key, value in row.items()
        if key in {"session_id", "project_id", "status", "model", "agent_id",
                   "enabled_tools", "created_at", "last_active_at"}
    }


def _public_history_message(row):
    """Return stable conversation fields without stored request bookkeeping."""

    metadata = row.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    public_metadata = {}

    usage = metadata.get("usage")
    if isinstance(usage, dict):
        public_metadata["usage"] = {
            key: usage[key]
            for key in (
                "prompt_tokens", "completion_tokens", "total_tokens", "approximate"
            )
            if key in usage
        }
    if metadata.get("research_mode") in {"normal", "research", "deep_research"}:
        public_metadata["research_mode"] = metadata["research_mode"]
    if metadata.get("fallback") is True:
        public_metadata["fallback"] = True
    if metadata.get("origin") == "import":
        public_metadata["origin"] = "import"

    annotations = metadata.get("response_annotations")
    if isinstance(annotations, list):
        public_metadata["response_annotations"] = [
            {key: item[key] for key in ("message_id", "start", "end", "quote")}
            for item in annotations if isinstance(item, dict)
            and all(key in item for key in ("message_id", "start", "end", "quote"))
        ][:5]

    terminal_error = metadata.get("terminal_error")
    if isinstance(terminal_error, dict):
        public_metadata["terminal_error"] = {
            key: terminal_error[key]
            for key in ("code", "retryable")
            if key in terminal_error
        }

    steer = metadata.get("run_steer")
    if isinstance(steer, dict):
        public_metadata["run_steer"] = {
            key: steer[key]
            for key in ("steer_id", "instruction", "state", "response_annotations")
            if key in steer
        }

    return {
        key: value.isoformat() if hasattr(value, "isoformat") else value
        for key, value in row.items()
        if key in {"message_id", "role", "content", "timestamp", "user_msg_id"}
    } | {"metadata": public_metadata}


async def _metadata(runtime, session_id):
    return (await runtime.sessions.list_sessions()).get(session_id)


async def list_sessions(request):
    rows = await request.app.state.runtime.sessions.list_sessions()
    project_id = request.query_params.get("project_id")
    return JSONResponse([
        _public(row) for row in rows.values()
        if project_id is None or row["project_id"] == project_id
    ])


async def create_session(request):
    try:
        body = CreateSession.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_session", "Invalid session", 422)
    project = await request.app.state.runtime.projects.get_project(body.project_id)
    if project is None or project["status"] == "deleted":
        return _error("project_not_found", "Project not found", 404)
    if project["status"] != "active":
        return _error("project_archived", "Archived project cannot be changed", 409)
    try:
        session = await request.app.state.runtime.sessions.create_session(
            **body.model_dump()
        )
    except ValueError:
        return _error("invalid_session", "Invalid session", 422)
    row = await _metadata(request.app.state.runtime, session.session_id)
    return JSONResponse(_public(row), status_code=201)


async def get_session(request):
    row = await _metadata(request.app.state.runtime, request.path_params["session_id"])
    if row is None:
        return _error("session_not_found", "Session not found", 404)
    return JSONResponse(_public(row))


async def update_session(request):
    session_id = request.path_params["session_id"]
    if await _metadata(request.app.state.runtime, session_id) is None:
        return _error("session_not_found", "Session not found", 404)
    try:
        body = SessionPreferences.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_session", "Invalid session preferences", 422)
    if not body.model_fields_set:
        return _error("invalid_session", "Invalid session preferences", 422)
    try:
        await request.app.state.runtime.sessions.update_session(
            session_id, body.model_dump(exclude_unset=True)
        )
    except ValueError:
        return _error("invalid_session", "Invalid session preferences", 422)
    return JSONResponse(_public(await _metadata(request.app.state.runtime, session_id)))


async def delete_session(request):
    session_id = request.path_params["session_id"]
    if await _metadata(request.app.state.runtime, session_id) is None:
        return _error("session_not_found", "Session not found", 404)
    await request.app.state.runtime.sessions.delete_session(session_id)
    return JSONResponse({"session_id": session_id, "deleted": True})


async def history(request):
    session_id = request.path_params["session_id"]
    if await _metadata(request.app.state.runtime, session_id) is None:
        return _error("session_not_found", "Session not found", 404)
    try:
        limit = int(request.query_params.get("limit", "100"))
    except ValueError:
        return _error("invalid_limit", "Invalid history limit", 422)
    if not 1 <= limit <= 1000:
        return _error("invalid_limit", "Invalid history limit", 422)
    rows = await request.app.state.runtime.sessions.get_session_history_readonly(
        session_id, limit=limit
    )
    return JSONResponse([_public_history_message(row) for row in rows])


async def import_exchanges(request):
    """Add recorded chat turns at their original times; they become memory."""
    runtime = request.app.state.runtime
    session_id = request.path_params["session_id"]
    payload = await _json(request)
    if isinstance(payload, dict) and isinstance(payload.get("exchanges"), list):
        maximum = runtime.config_manager.config.developer_settings.ingestion.max_import_exchanges
        if len(payload["exchanges"]) > maximum:
            return _error("batch_too_large", "Import has too many exchanges", 413)
    try:
        body = ImportExchanges.model_validate(payload)
    except ValidationError:
        return _error("invalid_import", "Invalid import", 422)
    metadata = await _metadata(runtime, session_id)
    if metadata is None:
        return _error("session_not_found", "Session not found", 404)
    project = await runtime.projects.get_project(metadata["project_id"])
    if project is None or project["status"] != "active":
        return _error("project_unavailable", "Project is unavailable", 409)
    session = await runtime.sessions.get_or_resume_session(session_id)
    if session is None:
        return _error("session_not_found", "Session not found", 404)
    try:
        results = await session.import_exchanges(body.exchanges)
    except SessionBusyError:
        return _error("session_busy", "This session has an active run", 409)
    return JSONResponse({"exchanges": results})


async def message_sources(request):
    """Reload the public source cards for one completed assistant message."""
    runtime = request.app.state.runtime
    session_id = request.path_params["session_id"]
    session = await _metadata(runtime, session_id)
    if session is None:
        return _error("session_not_found", "Session not found", 404)
    try:
        message_id = int(request.path_params["message_id"])
    except ValueError:
        return _error("message_not_found", "Message not found", 404)
    if message_id <= 0:
        return _error("message_not_found", "Message not found", 404)
    messages = await runtime.resources.knowledge_store.get_messages_by_ids(
        [message_id], user_name=runtime.sessions.user_name,
        session_ids=[session_id], visible_project_ids=[session["project_id"]],
    )
    if not messages or messages[0]["role"] != "assistant":
        return _error("message_not_found", "Message not found", 404)
    values = await runtime.resources.knowledge_store.get_message_source_refs(
        message_id, user_name=runtime.sessions.user_name,
        project_id=session["project_id"], session_id=session_id,
    )
    return JSONResponse({
        "message_id": message_id,
        "sources": [
            SourceConsulted.model_validate(value).model_dump(mode="json")
            for value in values
        ],
    })


async def get_focus(request):
    try:
        focus = await request.app.state.runtime.sessions.get_document_focus(
            request.path_params["session_id"]
        )
    except FileNotFoundError:
        return _error("session_not_found", "Session not found", 404)
    return JSONResponse(focus)


async def set_focus(request):
    try:
        body = focus_input.validate_python(await _json(request))
    except ValidationError:
        return _error("invalid_focus", "Invalid document focus", 422)
    target = body.model_dump()
    try:
        focus = await request.app.state.runtime.sessions.set_document_focus(
            request.path_params["session_id"],
            document_id=target.get("document_id"),
            path_prefix=target.get("path_prefix"),
            behavior=target["behavior"],
        )
    except FileNotFoundError:
        return _error("session_not_found", "Session or focus target not found", 404)
    return JSONResponse(focus)


async def clear_focus(request):
    try:
        await request.app.state.runtime.sessions.clear_document_focus(
            request.path_params["session_id"]
        )
    except FileNotFoundError:
        return _error("session_not_found", "Session not found", 404)
    return JSONResponse({"cleared": True})


routes = [
    Route("/v1/sessions", list_sessions, methods=["GET"]),
    Route("/v1/sessions", create_session, methods=["POST"]),
    Route("/v1/sessions/{session_id}", get_session, methods=["GET"]),
    Route("/v1/sessions/{session_id}", update_session, methods=["PATCH"]),
    Route("/v1/sessions/{session_id}", delete_session, methods=["DELETE"]),
    Route("/v1/sessions/{session_id}/history", history, methods=["GET"]),
    Route("/v1/sessions/{session_id}/imports", import_exchanges, methods=["POST"]),
    Route("/v1/sessions/{session_id}/messages/{message_id}/sources", message_sources, methods=["GET"]),
    Route("/v1/sessions/{session_id}/document-focus", get_focus, methods=["GET"]),
    Route("/v1/sessions/{session_id}/document-focus", set_focus, methods=["PUT"]),
    Route("/v1/sessions/{session_id}/document-focus", clear_focus, methods=["DELETE"]),
]
