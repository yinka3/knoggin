"""Controlled edits to user-owned project files."""

from pydantic import ValidationError
from starlette.responses import JSONResponse
from starlette.routing import Route

from common.exceptions import WorkspaceConflictError
from contracts.workspace import (
    FileAppend, FileCreate, FileDelete, FileMove, FileReplace, FolderCreate,
)
from project_lease import project_documents, readable_project_documents
from routes.project_settings import _error, _json, _project_error


async def _project(request, *, write=False):
    return await _project_error(
        request.app.state.runtime, request.path_params["project_id"], write=write
    )


async def _body(request, model):
    try:
        return model.model_validate(await _json(request))
    except ValidationError:
        return None


def _operation_error(exc):
    if isinstance(exc, WorkspaceConflictError):
        return _error("workspace_conflict", "Project file has changed", 409)
    if isinstance(exc, FileExistsError):
        return _error("file_exists", "Project path already exists", 409)
    if isinstance(exc, FileNotFoundError):
        return _error("file_not_found", "Project file not found", 404)
    return _error("invalid_project_file", "Invalid project file or path", 422)


async def list_files(request):
    if error := await _project(request):
        return error
    try:
        limit = int(request.query_params.get("limit", "100"))
    except ValueError:
        return _error("invalid_limit", "Invalid file limit", 422)
    if not 1 <= limit <= 100:
        return _error("invalid_limit", "Invalid file limit", 422)
    async with readable_project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            items = await service.list_project_files(
                path_prefix=request.query_params.get("path_prefix"), limit=limit
            )
        except ValueError as exc:
            return _operation_error(exc)
    return JSONResponse({"items": items})


async def read_file(request):
    if error := await _project(request):
        return error
    path = request.query_params.get("path")
    if not path:
        return _error("invalid_project_file", "Project file path is required", 422)
    try:
        start_line = int(request.query_params.get("start_line", "1"))
        end_line = request.query_params.get("end_line")
        end_line = int(end_line) if end_line is not None else None
    except ValueError:
        return _error("invalid_project_file", "Invalid line range", 422)
    async with readable_project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            row = await service.read_project_file(
                path, start_line=start_line, end_line=end_line
            )
        except (ValueError, FileNotFoundError, WorkspaceConflictError) as exc:
            return _operation_error(exc)
    return JSONResponse(row)


async def create_file(request):
    if error := await _project(request, write=True):
        return error
    body = await _body(request, FileCreate)
    if body is None:
        return _error("invalid_project_file", "Invalid project file", 422)
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            row = await service.create_project_file(body.path, body.content)
        except (ValueError, FileExistsError, FileNotFoundError, WorkspaceConflictError) as exc:
            return _operation_error(exc)
    return JSONResponse(row, status_code=201)


async def replace_file(request):
    if error := await _project(request, write=True):
        return error
    body = await _body(request, FileReplace)
    if body is None:
        return _error("invalid_project_file", "Invalid project file", 422)
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            row = await service.update_project_file(
                body.path, body.content,
                expected_content_hash=body.expected_content_hash,
            )
        except (ValueError, FileExistsError, FileNotFoundError, WorkspaceConflictError) as exc:
            return _operation_error(exc)
    return JSONResponse(row)


async def append_file(request):
    if error := await _project(request, write=True):
        return error
    body = await _body(request, FileAppend)
    if body is None:
        return _error("invalid_project_file", "Invalid project file", 422)
    path = request.query_params.get("path")
    if not path:
        return _error("invalid_project_file", "Project file path is required", 422)
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            row = await service.append_project_file(
                path, body.content, expected_content_hash=body.expected_content_hash
            )
        except (ValueError, FileNotFoundError, WorkspaceConflictError) as exc:
            return _operation_error(exc)
    return JSONResponse(row)


async def move_file(request):
    if error := await _project(request, write=True):
        return error
    body = await _body(request, FileMove)
    if body is None:
        return _error("invalid_project_file", "Invalid project file move", 422)
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            row = await service.move_project_file(
                body.source_path, body.destination_path,
                expected_content_hash=body.expected_content_hash,
            )
        except (ValueError, FileExistsError, FileNotFoundError, WorkspaceConflictError) as exc:
            return _operation_error(exc)
    return JSONResponse(row)


async def delete_file(request):
    if error := await _project(request, write=True):
        return error
    body = await _body(request, FileDelete)
    path = request.query_params.get("path")
    if body is None or not path:
        return _error("invalid_project_file", "Invalid project file deletion", 422)
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            row = await service.delete_project_file(
                path, expected_content_hash=body.expected_content_hash
            )
        except (ValueError, FileNotFoundError, WorkspaceConflictError) as exc:
            return _operation_error(exc)
    return JSONResponse(row)


async def create_folder(request):
    if error := await _project(request, write=True):
        return error
    body = await _body(request, FolderCreate)
    if body is None:
        return _error("invalid_project_file", "Invalid project folder", 422)
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            row = await service.create_project_folder(body.path)
        except (ValueError, FileExistsError, FileNotFoundError, WorkspaceConflictError) as exc:
            return _operation_error(exc)
    return JSONResponse(row, status_code=201)


routes = [
    Route("/v1/projects/{project_id}/workspace/files", list_files, methods=["GET"]),
    Route("/v1/projects/{project_id}/workspace/file", read_file, methods=["GET"]),
    Route("/v1/projects/{project_id}/workspace/file", create_file, methods=["POST"]),
    Route("/v1/projects/{project_id}/workspace/file", replace_file, methods=["PUT"]),
    Route("/v1/projects/{project_id}/workspace/file", delete_file, methods=["DELETE"]),
    Route("/v1/projects/{project_id}/workspace/file/append", append_file, methods=["POST"]),
    Route("/v1/projects/{project_id}/workspace/file/move", move_file, methods=["POST"]),
    Route("/v1/projects/{project_id}/workspace/folders", create_folder, methods=["POST"]),
]
