"""Project-owned notebook CRUD; specialized views are separate routes."""

from uuid import UUID

from pydantic import ValidationError
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from common.schema.project_notebook import NotebookReplace, NotebookWrite, ProjectNotebook
from common.schema.evidence_board import BoardConfirmation, BoardWrite
from contracts.notebooks import (
    AcceptBoardClaim,
    DeleteNotebook,
    EvaluateBoardClaim,
    LookupRuledOut,
    ProposeBoardClaim,
    SaveFinding,
)
from core.project.evidence_board_service import BoardEvaluationUnavailable
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
    return JSONResponse({"notebooks": [
        {**_public(row), "board_state": None} if row["view"] == "evidence_board"
        else _public(row)
        for row in rows
    ]})


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
    if row["view"] == "evidence_board":
        row = await runtime.projects.notebooks.evidence_board(project_id, notebook_id)
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


async def timeline(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=False):
        return error
    notebook_id = _notebook_id(request)
    if notebook_id is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    try:
        result = await runtime.projects.notebooks.timeline(project_id, notebook_id)
    except NotebookConflict:
        return _error("wrong_notebook_view", "Notebook uses another view", 409)
    if result is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    return JSONResponse({"notebook": _public(result["notebook"]), "items": result["items"]})


async def evidence_board(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=False):
        return error
    notebook_id = _notebook_id(request)
    if notebook_id is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    try:
        row = await runtime.projects.notebooks.evidence_board(project_id, notebook_id)
    except NotebookConflict:
        return _error("wrong_notebook_view", "Notebook uses another view", 409)
    if row is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    return JSONResponse(_public(row))


async def lookup_ruled_out(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=False):
        return error
    try:
        params = LookupRuledOut.model_validate(dict(request.query_params))
        if not params.query.strip():
            raise ValueError("empty query")
    except (ValidationError, ValueError):
        return _error("invalid_lookup", "Invalid lookup query", 422)
    matches = await runtime.projects.notebooks.lookup_ruled_out_hypotheses(
        project_id, params.query, limit=params.limit,
    )
    return JSONResponse({"matches": matches})


async def replace_evidence_board(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    notebook_id = _notebook_id(request)
    if notebook_id is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    try:
        body = BoardWrite.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_board", "Invalid evidence board", 422)
    try:
        row = await runtime.projects.notebooks.replace_board(project_id, notebook_id, body)
    except NotebookReferenceUnavailable:
        return _error("source_unavailable", "Board citation is unavailable", 422)
    except NotebookConflict:
        return _error("notebook_conflict", "Notebook changed", 409)
    if row is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    return JSONResponse(_public(row))


async def evaluate_board_claim(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    notebook_id = _notebook_id(request)
    try:
        question_id = str(UUID(request.path_params["question_id"]))
        claim_id = str(UUID(request.path_params["claim_id"]))
        body = EvaluateBoardClaim.model_validate(await _json(request))
    except (ValidationError, ValueError):
        return _error("invalid_evaluation", "Invalid board evaluation", 422)
    if notebook_id is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    try:
        row = await runtime.projects.evidence_boards.evaluate(
            project_id, notebook_id, expected_revision=body.expected_revision,
            question_id=question_id, claim_id=claim_id,
        )
    except NotebookConflict:
        return _error("notebook_conflict", "Board or evidence changed", 409)
    except BoardEvaluationUnavailable:
        return _error("evaluation_unavailable", "JEV could not evaluate this claim", 503)
    return JSONResponse(_public(row))


async def propose_board_claim(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    notebook_id = _notebook_id(request)
    try:
        question_id = str(UUID(request.path_params["question_id"]))
        body = ProposeBoardClaim.model_validate(await _json(request))
    except (ValidationError, ValueError):
        return _error("invalid_proposal", "Invalid board proposal", 422)
    if notebook_id is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    try:
        row = await runtime.projects.notebooks.propose_board_claim(
            project_id, notebook_id, question_id=question_id,
            **body.model_dump(),
        )
    except NotebookReferenceUnavailable:
        return _error("source_unavailable", "Answer or citation is unavailable", 422)
    except NotebookConflict:
        return _error("notebook_conflict", "Board changed", 409)
    return JSONResponse(_public(row), status_code=201)


async def accept_board_claim(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    notebook_id = _notebook_id(request)
    try:
        question_id = str(UUID(request.path_params["question_id"]))
        claim_id = str(UUID(request.path_params["claim_id"]))
        body = AcceptBoardClaim.model_validate(await _json(request))
    except (ValidationError, ValueError):
        return _error("invalid_acceptance", "Invalid board acceptance", 422)
    if notebook_id is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    try:
        row = await runtime.projects.notebooks.accept_board_claim(
            project_id, notebook_id, question_id=question_id, claim_id=claim_id,
            expected_revision=body.expected_revision,
        )
    except NotebookConflict:
        return _error("notebook_conflict", "Board changed", 409)
    return JSONResponse(_public(row))


async def confirm_board_claim(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    notebook_id = _notebook_id(request)
    try:
        question_id = str(UUID(request.path_params["question_id"]))
        claim_id = str(UUID(request.path_params["claim_id"]))
        body = BoardConfirmation.model_validate(await _json(request))
    except (ValidationError, ValueError):
        return _error("invalid_confirmation", "Invalid board confirmation", 422)
    if notebook_id is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    try:
        row = await runtime.projects.notebooks.confirm_board_judgment(
            project_id, notebook_id, question_id=question_id,
            claim_id=claim_id, confirmation=body,
        )
        row = await runtime.projects.notebooks.evidence_board(
            project_id, row["notebook_id"]
        )
    except NotebookConflict:
        return _error("notebook_conflict", "Board or evidence changed", 409)
    return JSONResponse(_public(row))


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


async def save_finding(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    notebook_id = _notebook_id(request)
    if notebook_id is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    try:
        body = SaveFinding.model_validate(await _json(request))
        finding = body.entry()
    except (ValidationError, ValueError):
        return _error("invalid_finding", "Invalid saved finding", 422)
    try:
        row = await runtime.projects.notebooks.save_finding(
            project_id, notebook_id,
            expected_revision=body.expected_revision, finding=finding,
        )
    except NotebookReferenceUnavailable:
        return _error("source_unavailable", "Answer or source is unavailable", 422)
    except NotebookConflict:
        return _error("notebook_conflict", "Notebook changed", 409)
    if row is None:
        return _error("notebook_not_found", "Notebook not found", 404)
    return JSONResponse(_public(row), status_code=201)


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
    Route("/v1/projects/{project_id}/ruled-out-hypotheses", lookup_ruled_out, methods=["GET"]),
    Route("/v1/projects/{project_id}/notebooks/{notebook_id}", get_notebook, methods=["GET"]),
    Route("/v1/projects/{project_id}/notebooks/{notebook_id}/source-desk", source_desk, methods=["GET"]),
    Route("/v1/projects/{project_id}/notebooks/{notebook_id}/timeline", timeline, methods=["GET"]),
    Route("/v1/projects/{project_id}/notebooks/{notebook_id}/evidence-board", evidence_board, methods=["GET"]),
    Route("/v1/projects/{project_id}/notebooks/{notebook_id}/evidence-board", replace_evidence_board, methods=["PUT"]),
    Route("/v1/projects/{project_id}/notebooks/{notebook_id}/evidence-board/questions/{question_id}/proposals", propose_board_claim, methods=["POST"]),
    Route("/v1/projects/{project_id}/notebooks/{notebook_id}/evidence-board/questions/{question_id}/claims/{claim_id}/accept", accept_board_claim, methods=["POST"]),
    Route("/v1/projects/{project_id}/notebooks/{notebook_id}/evidence-board/questions/{question_id}/claims/{claim_id}/evaluate", evaluate_board_claim, methods=["POST"]),
    Route("/v1/projects/{project_id}/notebooks/{notebook_id}/evidence-board/questions/{question_id}/claims/{claim_id}/confirm", confirm_board_claim, methods=["POST"]),
    Route("/v1/projects/{project_id}/notebooks/{notebook_id}/findings", save_finding, methods=["POST"]),
    Route("/v1/projects/{project_id}/notebooks/{notebook_id}", replace_notebook, methods=["PUT"]),
    Route("/v1/projects/{project_id}/notebooks/{notebook_id}", delete_notebook, methods=["DELETE"]),
]
