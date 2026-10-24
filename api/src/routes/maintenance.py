"""Knowledge maintenance review and rollback routes."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ValidationError
from starlette.responses import JSONResponse
from starlette.routing import Route

from contracts.maintenance import AdvisoryDecision, Decision, RollbackSelection
from routes.project_settings import _error, _json, _project_error


def _wire(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return {key: _wire(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_wire(item) for item in value]
    return value


def _review(review):
    return {
        "review_id": review.review_id,
        "scope": review.scope,
        "project_id": review.project_id,
        "kind": review.kind,
        "reasoning": review.reasoning,
        "proposed_plan": _wire(review.proposed_plan),
        "expected_state": _wire(review.expected_state),
        "status": review.status,
        "created_at": _wire(review.created_at),
        "resolved_at": _wire(review.resolved_at),
    }


def _detail(detail):
    return {
        "review": _review(detail.review),
        "stored_snapshot": _wire(detail.stored_snapshot),
        "current_evidence": _wire(detail.current_evidence),
        "unavailable_pointers": _wire(detail.unavailable_pointers),
        "evidence_state": detail.evidence_state,
    }


async def _scope(request, *, write=False):
    return await _project_error(
        request.app.state.runtime, request.path_params["project_id"], write=write
    )


async def global_reviews(request):
    reviews = await request.app.state.runtime.projects.list_global_maintenance_reviews()
    return JSONResponse({"reviews": [_review(review) for review in reviews]})


async def global_decision(request):
    try:
        body = Decision.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_decision", "Invalid maintenance decision", 422)
    projects = request.app.state.runtime.projects
    review_id = request.path_params["review_id"]
    if body.action == "apply":
        result = await projects.apply_global_entity_merge_review(
            review_id, expected_state=body.expected_state
        )
        return JSONResponse(_wire(result))
    review = await projects.dismiss_global_maintenance_review(
        review_id, expected_state=body.expected_state, reason=body.reason
    )
    return JSONResponse({"review": _review(review)})


async def project_reviews(request):
    if error := await _scope(request):
        return error
    reviews = await request.app.state.runtime.projects.maintenance_service.list_maintenance_reviews(
        request.path_params["project_id"]
    )
    return JSONResponse({"reviews": [_review(review) for review in reviews]})


async def project_review(request):
    if error := await _scope(request):
        return error
    detail = await request.app.state.runtime.projects.maintenance_service.get_maintenance_review_detail(
        request.path_params["project_id"], request.path_params["review_id"]
    )
    return JSONResponse(_detail(detail))


async def project_preview(request):
    if error := await _scope(request):
        return error
    detail, impact = await request.app.state.runtime.projects.maintenance_service.preview_maintenance_review(
        request.path_params["project_id"], request.path_params["review_id"]
    )
    return JSONResponse({"detail": _detail(detail), "impact": _wire(impact)})


async def project_decision(request):
    if error := await _scope(request, write=True):
        return error
    try:
        body = Decision.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_decision", "Invalid maintenance decision", 422)
    review = await request.app.state.runtime.projects.maintenance_service.transition_maintenance_review(
        request.path_params["project_id"], request.path_params["review_id"],
        status="applied" if body.action == "apply" else "dismissed",
        expected_state=body.expected_state, reason=body.reason,
    )
    return JSONResponse({"review": _review(review)})


async def advisory_decision(request):
    if error := await _scope(request, write=True):
        return error
    try:
        body = AdvisoryDecision.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_decision", "Invalid advisory decision", 422)
    runtime = request.app.state.runtime
    decision = await runtime.projects.maintenance_service.apply_relationship_advisory_action(
        request.path_params["project_id"], request.path_params["review_id"],
        body.action, relationship_type=body.relationship_type, note=body.note,
        decided_by=runtime.sessions.user_name,
    )
    return JSONResponse({"decision": decision.to_dict()})


async def rollback_preview(request):
    result = await request.app.state.runtime.projects.preview_global_entity_merge_rollback(
        request.path_params["merge_id"]
    )
    return JSONResponse(_wire(result))


async def rollback(request):
    try:
        body = RollbackSelection.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_rollback", "Invalid rollback selection", 422)
    result = await request.app.state.runtime.projects.rollback_global_entity_merge(
        request.path_params["merge_id"],
        approved_mutation_ids=body.approved_mutation_ids,
    )
    return JSONResponse(_wire(result))


routes = [
    Route("/v1/maintenance/reviews", global_reviews, methods=["GET"]),
    Route("/v1/maintenance/reviews/{review_id}/decision", global_decision, methods=["POST"]),
    Route("/v1/maintenance/merges/{merge_id}/rollback/preview", rollback_preview, methods=["GET"]),
    Route("/v1/maintenance/merges/{merge_id}/rollback", rollback, methods=["POST"]),
    Route("/v1/projects/{project_id}/maintenance/reviews", project_reviews, methods=["GET"]),
    Route("/v1/projects/{project_id}/maintenance/reviews/{review_id}", project_review, methods=["GET"]),
    Route("/v1/projects/{project_id}/maintenance/reviews/{review_id}/preview", project_preview, methods=["GET"]),
    Route("/v1/projects/{project_id}/maintenance/reviews/{review_id}/decision", project_decision, methods=["POST"]),
    Route("/v1/projects/{project_id}/maintenance/reviews/{review_id}/advisory", advisory_decision, methods=["POST"]),
]
