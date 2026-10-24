"""Project domain and folder-scan configuration routes."""

from pydantic import ValidationError
from starlette.responses import JSONResponse
from starlette.routing import Route

from contracts.project_settings import (
    DomainActivationRequest,
    FolderScanSettingsRequest,
)
from common.conf.domain_config import DomainConfigError
from core.project.domain_config_store import (
    DomainConfigConflict,
    DomainConfigSectionRemovalConflict,
)


def _error(code: str, message: str, status: int, **details) -> JSONResponse:
    return JSONResponse(
        {"error": {"code": code, "message": message, **details}},
        status_code=status,
    )


async def _json(request):
    try:
        return await request.json()
    except (ValueError, UnicodeDecodeError):
        return None


async def _project_error(
    runtime, project_id: str, *, write: bool
) -> JSONResponse | None:
    project = await runtime.projects.get_project(project_id)
    if project is None or project["status"] == "deleted":
        return _error("project_not_found", "Project not found", 404)
    if project["status"] == "archived" and write:
        return _error("project_archived", "Archived project cannot be changed", 409)
    if project["status"] not in {"active", "archived"}:
        return _error("project_unavailable", "Project is unavailable", 409)
    return None


async def get_domain(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=False):
        return error
    try:
        config = await runtime.projects.get_domain_config(project_id)
    except ValueError:
        return await _project_error(runtime, project_id, write=False) or _error(
            "domain_unavailable", "Project domain is unavailable", 409
        )
    return JSONResponse(config.to_dict())


async def validate_domain(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=False):
        return error
    candidate = await _json(request)
    return JSONResponse(runtime.projects.validate_domain_config(candidate).to_dict())


async def preview_domain(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=False):
        return error
    candidate = await _json(request)
    try:
        preview = await runtime.projects.preview_domain_config(project_id, candidate)
    except (DomainConfigError, TypeError):
        return _error("invalid_domain", "Invalid domain configuration", 422)
    except ValueError:
        return await _project_error(runtime, project_id, write=False) or _error(
            "invalid_domain", "Invalid domain configuration", 422
        )
    return JSONResponse(preview.to_dict())


async def activate_domain(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    try:
        body = DomainActivationRequest.model_validate(await _json(request))
    except ValidationError:
        return _error(
            "invalid_domain_request", "Invalid domain activation request", 422
        )
    try:
        result = await runtime.projects.activate_domain_config(
            project_id, body.config, expected_version=body.expected_version
        )
    except DomainConfigConflict as exc:
        return _error(
            "domain_version_conflict",
            "Domain configuration changed",
            409,
            expected_version=exc.expected_version,
            actual_version=exc.actual_version,
        )
    except DomainConfigSectionRemovalConflict:
        return _error("domain_section_conflict", "Domain section is still in use", 409)
    except (DomainConfigError, TypeError):
        return _error("invalid_domain", "Invalid domain configuration", 422)
    except ValueError:
        return await _project_error(runtime, project_id, write=True) or _error(
            "invalid_domain", "Invalid domain configuration", 422
        )
    return JSONResponse(
        {
            "previous_version": result.previous_version,
            "config": result.config.to_dict(),
        }
    )


async def get_scan_settings(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    try:
        settings = await runtime.projects.get_document_scan_settings(project_id)
    except ValueError:
        return await _project_error(runtime, project_id, write=True) or _error(
            "project_unavailable", "Project is unavailable", 409
        )
    return JSONResponse(settings.model_dump(mode="json"))


async def put_scan_settings(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    try:
        settings = FolderScanSettingsRequest.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_scan_settings", "Invalid folder scan settings", 422)
    try:
        saved = await runtime.projects.save_document_scan_settings(project_id, settings)
    except ValueError:
        return await _project_error(runtime, project_id, write=True) or _error(
            "project_unavailable", "Project is unavailable", 409
        )
    return JSONResponse(saved.model_dump(mode="json"))


async def reset_scan_settings(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    try:
        defaults = await runtime.projects.reset_document_scan_settings(project_id)
    except ValueError:
        return await _project_error(runtime, project_id, write=True) or _error(
            "project_unavailable", "Project is unavailable", 409
        )
    return JSONResponse(defaults.model_dump(mode="json"))


routes = [
    Route("/v1/projects/{project_id}/domain", get_domain, methods=["GET"]),
    Route(
        "/v1/projects/{project_id}/domain/validate", validate_domain, methods=["POST"]
    ),
    Route("/v1/projects/{project_id}/domain/preview", preview_domain, methods=["POST"]),
    Route(
        "/v1/projects/{project_id}/domain/activate", activate_domain, methods=["POST"]
    ),
    Route(
        "/v1/projects/{project_id}/scan-settings", get_scan_settings, methods=["GET"]
    ),
    Route(
        "/v1/projects/{project_id}/scan-settings", put_scan_settings, methods=["PUT"]
    ),
    Route(
        "/v1/projects/{project_id}/scan-settings",
        reset_scan_settings,
        methods=["DELETE"],
    ),
]
