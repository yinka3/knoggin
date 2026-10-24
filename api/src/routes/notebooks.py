"""Project-owned notebook CRUD; specialized views are separate routes."""

from uuid import UUID

from pydantic import ValidationError
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from common.schema.project_notebook import NotebookReplace, NotebookWrite, ProjectNotebook
from contracts.notebooks import DeleteNotebook
from core.project.notebook_store import NotebookConflict, NotebookReferenceUnavailable
from routes.project_settings import _error, _json, _project_error


def _public(row):
    return ProjectNotebook.model_validate(row).model_dump(mode="json", exclude={"user_name"})


def _notebook_id(request):
    try:
        return str(UUID(request.path_params["notebook_id"]))
    except ValueError:
        return None


async def list_notebooks(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=False):
        return error
    rows = await runtime.projects.notebooks.list(project_id)
    return JSONResponse({"notebooks": [_public(row) for row in rows]})


async def create_notebook(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    try:
        body = NotebookWrite.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_notebook", "Invalid notebook", 422)
    try:
        row = await runtime.projects.notebooks.create(project_id, body)
    except NotebookReferenceUnavailable:
        return _error("source_unavailable", "Notebook source is unavailable", 422)
    except NotebookConflict:
        return _error("project_unavailable", "Project is unavailable", 409)
    return JSONResponse(_public(row), status_code=201)


async def get_notebook(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=False):
        return error
    notebook_id = _notebook_id(request)
    if notebook_id is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    row = await runtime.projects.notebooks.get(project_id, notebook_id)
    if row is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    return JSONResponse(_public(row))


async def source_desk(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=False):
        return error
    notebook_id = _notebook_id(request)
    if notebook_id is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    try:
        result = await runtime.projects.notebooks.source_desk(project_id, notebook_id)
    except NotebookConflict:
        return _error("wrong_notebook_view", "Notebook uses another view", 409)
    if result is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    return JSONResponse({"notebook": _public(result["notebook"]), "items": result["items"]})


async def replace_notebook(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    notebook_id = _notebook_id(request)
    if notebook_id is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    try:
        body = NotebookReplace.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_notebook", "Invalid notebook", 422)
    try:
        row = await runtime.projects.notebooks.replace(project_id, notebook_id, body)
    except NotebookReferenceUnavailable:
        return _error("source_unavailable", "Notebook source is unavailable", 422)
    except NotebookConflict:
        return _error("notebook_conflict", "Notebook changed", 409)
    if row is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    return JSONResponse(_public(row))


async def delete_notebook(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    notebook_id = _notebook_id(request)
    if notebook_id is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    try:
        body = DeleteNotebook.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_notebook", "Invalid notebook revision", 422)
    try:
        deleted = await runtime.projects.notebooks.delete(
            project_id, notebook_id, body.expected_revision
        )
    except NotebookConflict:
        return _error("notebook_conflict", "Notebook changed", 409)
    if not deleted:
        return _error("notebook_not_found", "Notebook not found", 404)
    return Response(status_code=204)


routes = [
    Route("/v1/projects/{project_id}/notebooks", list_notebooks, methods=["GET"]),
    Route("/v1/projects/{project_id}/notebooks", create_notebook, methods=["POST"]),
    Route("/v1/projects/{project_id}/notebooks/{notebook_id}", get_notebook, methods=["GET"]),
    Route("/v1/projects/{project_id}/notebooks/{notebook_id}/source-desk", source_desk, methods=["GET"]),
    Route("/v1/projects/{project_id}/notebooks/{notebook_id}", replace_notebook, methods=["PUT"]),
    Route("/v1/projects/{project_id}/notebooks/{notebook_id}", delete_notebook, methods=["DELETE"]),
]
