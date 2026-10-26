"""User-authored Context blocks through the versioned Context owner."""

from uuid import UUID

from pydantic import ValidationError
from starlette.responses import JSONResponse
from starlette.routing import Route

from contracts.context_notes import NoteCreate, NoteDelete, NoteReplace
from core.knowledge.context.models import (
    ContextProjectionConflictError, ContextRevisionConflictError,
)
from project_lease import project_runtime
from routes.project_settings import _error, _json, _project_error


async def _notes(runtime, project_id):
    store = runtime.resources.knowledge_store
    user = runtime.sessions.user_name
    revision = await store.get_current_project_context_revision(
        user_name=user, project_id=project_id
    )
    snapshot = await store.get_project_context_snapshot(
        str(revision.revision_id), user_name=user, project_id=project_id
    ) if revision else None
    return {
        "revision_id": str(revision.revision_id) if revision else None,
        "notes": [{
            "block_id": str(block.block_id),
            "section_key": block.section_key,
            "markdown": block.markdown,
        } for block in snapshot.blocks
            if block.assertion_kind == "human_asserted"] if snapshot else [],
    }


async def list_notes(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=False):
        return error
    return JSONResponse(await _notes(runtime, project_id))


async def _change(request, *, operation, model):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    try:
        body = model.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_user_note", "Invalid Context user note", 422)
    block_id = None
    if operation != "add":
        try:
            block_id = UUID(request.path_params["block_id"])
        except ValueError:
            return _error("note_not_found", "User note not found", 404)
    async with project_runtime(runtime, project_id) as project:
        try:
            await project.project_semantic_processor.change_user_note(
                user_name=runtime.sessions.user_name,
                project_id=project_id,
                expected_revision_id=body.expected_revision_id,
                operation=operation,
                block_id=block_id,
                section_key=getattr(body, "section_key", None),
                markdown=getattr(body, "markdown", None),
            )
        except ContextRevisionConflictError:
            return _error("context_conflict", "Context has changed", 409)
        except ContextProjectionConflictError:
            return _error("context_projection_conflict", "Context file has unimported changes", 409)
        except FileNotFoundError:
            return _error("note_not_found", "User note not found", 404)
        except ValueError:
            return _error("invalid_user_note", "Invalid Context user note", 422)
        project.signal_semantic_work()
    return JSONResponse(await _notes(runtime, project_id),
                        status_code=201 if operation == "add" else 200)


async def create_note(request):
    return await _change(request, operation="add", model=NoteCreate)


async def replace_note(request):
    return await _change(request, operation="replace", model=NoteReplace)


async def delete_note(request):
    return await _change(request, operation="delete", model=NoteDelete)


routes = [
    Route("/v1/projects/{project_id}/context/user-notes", list_notes, methods=["GET"]),
    Route("/v1/projects/{project_id}/context/user-notes", create_note, methods=["POST"]),
    Route("/v1/projects/{project_id}/context/user-notes/{block_id}", replace_note, methods=["PUT"]),
    Route("/v1/projects/{project_id}/context/user-notes/{block_id}", delete_note, methods=["DELETE"]),
]
