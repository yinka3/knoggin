"""Durable session metadata routes."""

from pydantic import ValidationError
from starlette.responses import JSONResponse
from starlette.routing import Route

from contracts.sessions import CreateSession, SessionPreferences, focus_input
from routes.project_settings import _error, _json


def _public(row):
    from datetime import datetime

    return {
        key: value.isoformat() if isinstance(value, datetime) else value
        for key, value in row.items()
        if key in {"session_id", "project_id", "status", "model", "agent_id",
                   "enabled_tools", "created_at", "last_active_at"}
    }


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
    return JSONResponse([
        {key: value.isoformat() if hasattr(value, "isoformat") else value
         for key, value in row.items()}
        for row in rows
    ])


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
    Route("/v1/sessions/{session_id}/document-focus", get_focus, methods=["GET"]),
    Route("/v1/sessions/{session_id}/document-focus", set_focus, methods=["PUT"]),
    Route("/v1/sessions/{session_id}/document-focus", clear_focus, methods=["DELETE"]),
]
