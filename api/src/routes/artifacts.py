"""Read-only project artifact routes."""

from starlette.responses import JSONResponse
from starlette.routing import Route

from routes.project_settings import _error, _project_error


async def _scope(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    return await _project_error(runtime, project_id, write=False)


async def list_artifacts(request):
    if error := await _scope(request):
        return error
    try:
        limit = int(request.query_params.get("limit", "50"))
    except ValueError:
        return _error("invalid_limit", "Invalid artifact limit", 422)
    if not 1 <= limit <= 500:
        return _error("invalid_limit", "Invalid artifact limit", 422)
    runtime = request.app.state.runtime
    rows = await runtime.resources.knowledge_store.list_project_artifacts(
        user_name=runtime.sessions.user_name,
        project_id=request.path_params["project_id"],
        session_id=request.query_params.get("session_id"), limit=limit,
    )
    return JSONResponse({"artifacts": [row.model_dump(mode="json") for row in rows]})


async def get_artifact(request):
    if error := await _scope(request):
        return error
    runtime = request.app.state.runtime
    row = await runtime.resources.knowledge_store.get_project_artifact(
        request.path_params["artifact_id"],
        user_name=runtime.sessions.user_name,
        project_id=request.path_params["project_id"],
        session_id=request.query_params.get("session_id"),
    )
    if row is None:
        return _error("artifact_not_found", "Artifact not found", 404)
    return JSONResponse(row.model_dump(mode="json"))


async def get_revision(request):
    if error := await _scope(request):
        return error
    try:
        revision = int(request.path_params["revision"])
    except ValueError:
        return _error("invalid_revision", "Invalid artifact revision", 422)
    if revision < 1:
        return _error("invalid_revision", "Invalid artifact revision", 422)
    runtime = request.app.state.runtime
    row = await runtime.resources.knowledge_store.get_project_artifact_revision(
        request.path_params["artifact_id"], revision,
        user_name=runtime.sessions.user_name,
        project_id=request.path_params["project_id"],
        session_id=request.query_params.get("session_id"),
    )
    if row is None:
        return _error("artifact_not_found", "Artifact revision not found", 404)
    return JSONResponse(row.model_dump(mode="json"))


routes = [
    Route("/v1/projects/{project_id}/artifacts", list_artifacts, methods=["GET"]),
    Route("/v1/projects/{project_id}/artifacts/{artifact_id}", get_artifact, methods=["GET"]),
    Route("/v1/projects/{project_id}/artifacts/{artifact_id}/revisions/{revision}", get_revision, methods=["GET"]),
]
