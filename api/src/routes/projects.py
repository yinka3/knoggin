"""Project metadata and lifecycle routes."""

from pydantic import ValidationError
from starlette.responses import JSONResponse
from starlette.routing import Route

from common.conf.domain_config import DomainConfig
from common.exceptions import WorkspaceConflictError
from core.project.project_files import workspace_name
from project_lease import project_documents

from routes.project_settings import _error, _json


DEFAULT_DOMAIN = DomainConfig.from_mapping(
    {
        "version": 0,
        "topics": {"Identity": {}, "General": {}},
        "entity_types": {
            "Identity": {"topic": "Identity", "labels": ["person"]},
            "Concept": {"topic": "General", "labels": ["concept"]},
        },
    }
)


def _public(row):
    from contracts.projects import ProjectView

    return ProjectView.model_validate(row).model_dump(mode="json")


async def list_projects(request):
    rows = await request.app.state.runtime.projects.list_projects()
    return JSONResponse([_public(row) for row in rows])


async def get_project(request):
    row = await request.app.state.runtime.projects.get_project(
        request.path_params["project_id"]
    )
    if row is None or row["status"] == "deleted":
        return _error("project_not_found", "Project not found", 404)
    return JSONResponse(_public(row))


async def create_project(request):
    from contracts.projects import CreateProject

    try:
        body = CreateProject.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_project", "Invalid project", 422)
    try:
        row = await request.app.state.runtime.projects.create_project(
            name=body.name, description=body.description,
            domain_config=DEFAULT_DOMAIN,
        )
    except ValueError:
        return _error("invalid_project", "Invalid project", 422)
    return JSONResponse(_public(row), status_code=201)


async def update_project(request):
    from contracts.projects import UpdateProject

    project_id = request.path_params["project_id"]
    if error := await _editable(request, project_id):
        return error
    try:
        body = UpdateProject.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_project", "Invalid project", 422)
    if not body.model_fields_set:
        return _error("invalid_project", "Invalid project", 422)
    try:
        row = await request.app.state.runtime.projects.update_project(
            project_id, **body.model_dump(exclude_unset=True)
        )
    except ValueError:
        return _error("invalid_project", "Invalid project", 422)
    return JSONResponse(_public(row))


async def _editable(request, project_id):
    row = await request.app.state.runtime.projects.get_project(project_id)
    if row is None or row["status"] == "deleted":
        return _error("project_not_found", "Project not found", 404)
    if row["status"] != "active":
        return _error("project_archived", "Archived project cannot be changed", 409)
    return None


async def archive_project(request):
    project_id = request.path_params["project_id"]
    if error := await _editable(request, project_id):
        return error
    try:
        row = await request.app.state.runtime.projects.archive_project(project_id)
    except WorkspaceConflictError:
        return _error("project_busy", "Project has active sessions", 409)
    return JSONResponse(_public(row))


async def reactivate_project(request):
    project_id = request.path_params["project_id"]
    project = await request.app.state.runtime.projects.get_project(project_id)
    if project is None or project["status"] == "deleted":
        return _error("project_not_found", "Project not found", 404)
    row = await request.app.state.runtime.projects.reactivate_project(project_id)
    return JSONResponse(_public(row))


async def delete_project(request):
    project_id = request.path_params["project_id"]
    # The owner also retries pending file cleanup for an already-deleted row.
    try:
        row = await request.app.state.runtime.projects.delete_project(project_id)
    except WorkspaceConflictError:
        return _error("project_busy", "Project has active sessions", 409)
    if row is None:
        return _error("project_not_found", "Project not found", 404)
    return JSONResponse(
        {"project_id": project_id, "deleted": True,
         "file_cleanup_status": row["file_cleanup_status"]}
    )


async def add_workspace(request):
    from contracts.projects import AddWorkspace

    project_id = request.path_params["project_id"]
    if error := await _editable(request, project_id):
        return error
    try:
        name = workspace_name(AddWorkspace.model_validate(await _json(request)).name)
    except (ValidationError, ValueError):
        return _error("invalid_workspace", "Invalid workspace name", 422)
    projects = request.app.state.runtime.projects
    try:
        workspaces = await projects.add_workspace(project_id, name)
    except ValueError:
        return _error(
            "workspace_unavailable",
            "Workspace exists or the project has reached its workspace limit", 409,
        )
    try:
        async with project_documents(request.app.state.runtime, project_id) as service:
            await service.create_project_folder(name)
    except (OSError, ValueError, WorkspaceConflictError):
        # A file already has that name; the folder is the workspace.
        await projects.remove_workspace(project_id, name)
        return _error("invalid_workspace", "A project file already uses that name", 409)
    return JSONResponse({"workspaces": workspaces}, status_code=201)


async def remove_workspace(request):
    project_id = request.path_params["project_id"]
    if error := await _editable(request, project_id):
        return error
    try:
        workspaces = await request.app.state.runtime.projects.remove_workspace(
            project_id, request.path_params["name"]
        )
    except WorkspaceConflictError:
        return _error("workspace_not_empty", "Workspace still has documents", 409)
    if workspaces is None:
        return _error("workspace_not_found", "Workspace not found", 404)
    return JSONResponse({"workspaces": workspaces})


routes = [
    Route("/v1/projects", list_projects, methods=["GET"]),
    Route("/v1/projects", create_project, methods=["POST"]),
    Route("/v1/projects/{project_id}", get_project, methods=["GET"]),
    Route("/v1/projects/{project_id}", update_project, methods=["PATCH"]),
    Route("/v1/projects/{project_id}/archive", archive_project, methods=["POST"]),
    Route("/v1/projects/{project_id}/reactivate", reactivate_project, methods=["POST"]),
    Route("/v1/projects/{project_id}", delete_project, methods=["DELETE"]),
    Route("/v1/projects/{project_id}/workspaces", add_workspace, methods=["POST"]),
    Route(
        "/v1/projects/{project_id}/workspaces/{name}", remove_workspace,
        methods=["DELETE"],
    ),
]
