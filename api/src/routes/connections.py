"""Saved logins (user level) and the project sandbox reset.

Login values go in and never come back out: responses only say whether each
service is set and where from (environment or saved).
"""

from pydantic import BaseModel, ConfigDict, ValidationError
from starlette.responses import JSONResponse
from starlette.routing import Route

from common import credential_store
from routes.project_settings import _error, _json, _project_error


class SaveLogin(BaseModel):
    model_config = ConfigDict(extra="forbid")

    values: dict[str, str]


def _status() -> dict:
    return {"connections": credential_store.status()}


async def list_connections(_request):
    return JSONResponse(_status())


async def save_connection(request):
    service = request.path_params["service"]
    if service not in credential_store.SERVICES:
        return _error("connection_not_found", "Unknown service", 404)
    try:
        body = SaveLogin.model_validate(await _json(request))
        credential_store.save(service, body.values)
    except (ValidationError, ValueError):
        return _error("invalid_connection", "Invalid login values", 422)
    if service == "openrouter":
        # The model client keeps its key; give it the new one now.
        runtime = request.app.state.runtime
        runtime.resources.llm_service.update_settings(runtime.config_manager.config.llm)
    return JSONResponse(_status())


async def delete_connection(request):
    service = request.path_params["service"]
    if service not in credential_store.SERVICES:
        return _error("connection_not_found", "Unknown service", 404)
    if not credential_store.remove(service):
        return _error("connection_not_found", "No saved login for this service", 404)
    if service == "openrouter":
        runtime = request.app.state.runtime
        runtime.resources.llm_service.update_settings(runtime.config_manager.config.llm)
    return JSONResponse(_status())


async def reset_sandbox(request):
    """Throw away the project's command container so the next command starts
    fresh. The sandbox home (installed packages, downloads) is kept."""

    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    sandbox = getattr(runtime, "sandbox", None)
    if sandbox is None:
        return _error("sandbox_unavailable", "Sandbox is unavailable", 503)
    await sandbox.reset(project_id)
    return JSONResponse({"reset": True})


routes = [
    Route("/v1/connections", list_connections, methods=["GET"]),
    Route("/v1/connections/{service}", save_connection, methods=["PUT"]),
    Route("/v1/connections/{service}", delete_connection, methods=["DELETE"]),
    Route("/v1/projects/{project_id}/sandbox/reset", reset_sandbox, methods=["POST"]),
]
