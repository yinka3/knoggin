"""User-facing project participation and blocked-work controls."""

from pydantic import ValidationError
from starlette.responses import JSONResponse
from starlette.routing import Route
from uuid import UUID

from contracts.project_control import SemanticParticipationSelection
from routes.project_settings import _error, _json, _project_error


async def participation(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=False):
        return error
    rows = await runtime.projects.list_session_semantic_participation(project_id)
    return JSONResponse({"sessions": rows})


async def select_participation(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    try:
        body = SemanticParticipationSelection.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_participation", "Invalid session selection", 422)
    try:
        rows = await runtime.projects.set_session_semantic_participation(
            project_id, body.session_ids
        )
    except ValueError:
        return await _project_error(runtime, project_id, write=True) or _error(
            "invalid_participation", "Selection includes an unavailable session", 422
        )
    return JSONResponse({"sessions": rows, "effect": "future_messages_only"})


async def blocked_windows(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=False):
        return error
    try:
        limit = int(request.query_params.get("limit", "50"))
    except ValueError:
        return _error("invalid_limit", "Invalid window limit", 422)
    if not 1 <= limit <= 100:
        return _error("invalid_limit", "Invalid window limit", 422)
    rows = await runtime.projects.maintenance_service.inspect_semantic_window_blockages(
        project_id, limit=limit
    )
    return JSONResponse({"windows": [row.model_dump(mode="json") for row in rows]})


async def retry_window(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    try:
        UUID(request.path_params["window_id"])
    except ValueError:
        return _error("invalid_window_id", "Invalid window ID", 422)
    try:
        window = await runtime.projects.maintenance_service.retry_semantic_window(
            project_id, request.path_params["window_id"]
        )
    except ValueError:
        return _error("window_not_retryable", "Window is not available for retry", 409)
    return JSONResponse({
        "window_id": str(window.window_id),
        "stage": window.stage,
        "attempt_count": window.attempt_count,
        "retry_accepted": True,
    })


async def save_memory_now(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    if not await runtime.projects.maintenance_service.flush_semantic_memory(project_id):
        return _error(
            "project_not_running",
            "Project is not running; its conversation is saved after it starts",
            409,
        )
    return JSONResponse({"flush_requested": True}, status_code=202)


async def control_overview(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=False):
        return error
    project = await runtime.projects.get_project(project_id)
    sessions = await runtime.projects.list_session_semantic_participation(project_id)
    windows = await runtime.projects.maintenance_service.inspect_semantic_window_blockages(
        project_id, limit=21
    )
    reviews = await runtime.projects.maintenance_service.list_maintenance_reviews(project_id)
    health = await runtime.health_service.get_ingestion_health(
        user_name=runtime.sessions.user_name, project_id=project_id
    )
    return JSONResponse({
        "project_id": project_id,
        "status": project["status"],
        "sessions": sessions,
        "blocked_windows": [row.model_dump(mode="json") for row in windows[:20]],
        "more_blocked_windows": len(windows) > 20,
        "open_review_count": sum(review.status == "open" for review in reviews),
        "ingestion_health": {
            "status": health.status,
            "summary": health.summary,
            "warnings": health.warnings,
        },
    })


routes = [
    Route("/v1/projects/{project_id}/control", control_overview, methods=["GET"]),
    Route("/v1/projects/{project_id}/semantic-participation", participation, methods=["GET"]),
    Route("/v1/projects/{project_id}/semantic-participation", select_participation, methods=["PUT"]),
    Route("/v1/projects/{project_id}/maintenance/semantic-windows/blocked", blocked_windows, methods=["GET"]),
    Route("/v1/projects/{project_id}/maintenance/semantic-windows/{window_id}/retry", retry_window, methods=["POST"]),
    Route("/v1/projects/{project_id}/memory/save", save_memory_now, methods=["POST"]),
]
