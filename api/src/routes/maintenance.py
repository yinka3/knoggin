"""Knowledge maintenance review and rollback routes."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ValidationError
from starlette.responses import JSONResponse
from starlette.routing import Route

from contracts.maintenance import (
    AdvisoryDecision,
    ApplyJevIdentityDecision,
    Decision,
    FlagJevContextDecision,
    FlagJevIdentityDecision,
    MergeCandidates,
    MergeProposal,
    ReopenJevContextCorrection,
    ReopenJevIdentityCorrection,
    RollbackSelection,
)
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


async def blocked_duplicates(request):
    service = request.app.state.runtime.projects.entity_maintenance_service
    return JSONResponse({"duplicates": _wire(await service.list_blocked_duplicates())})


async def dismiss_duplicate(request):
    try:
        ids = int(request.path_params["entity_id"]), int(request.path_params["other_entity_id"])
    except ValueError:
        return _error("duplicate_not_found", "Duplicate pair not found", 404)
    service = request.app.state.runtime.projects.entity_maintenance_service
    if not await service.dismiss_duplicate(*ids):
        return _error("duplicate_not_found", "Duplicate pair not found", 404)
    return JSONResponse({"dismissed": True})


async def global_merge_preview(request):
    try:
        body = MergeCandidates.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_merge", "Invalid merge candidates", 422)
    result = await request.app.state.runtime.projects.entity_maintenance_service.preview_merge(
        survivor_entity_id=body.survivor_entity_id,
        retired_entity_id=body.retired_entity_id,
        context_choices=body.context_choices,
    )
    return JSONResponse(_wire({
        "plan": result["plan"], "affected_project_ids": result["affected_project_ids"],
        "context_conflicts": result["context_conflicts"], "ready": result["ready"],
        "state_hash": result["state_hash"],
    }))


async def global_merge_proposal(request):
    try:
        body = MergeProposal.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_merge", "Invalid merge proposal", 422)
    review = await request.app.state.runtime.projects.entity_maintenance_service.propose(
        survivor_entity_id=body.survivor_entity_id,
        retired_entity_id=body.retired_entity_id,
        context_choices=body.context_choices,
        reasoning=body.reasoning,
        evidence_refs=body.evidence_refs,
        expected_state_hash=body.expected_state_hash,
    )
    return JSONResponse({"review": _review(review)}, status_code=201)


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


async def flag_jev_context_decision(request):
    if error := await _scope(request, write=True):
        return error
    try:
        body = FlagJevContextDecision.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_correction", "Invalid JEV correction", 422)
    review = await request.app.state.runtime.projects.maintenance_service.flag_jev_entity_context_decision(
        request.path_params["project_id"], capability=body.capability,
        window_id=str(body.window_id), pass_number=body.pass_number,
        occurrence_key=body.occurrence_key, entity_id=body.entity_id,
        entity_type=body.entity_type, topic=body.topic, reasoning=body.reasoning,
    )
    return JSONResponse({"review": _review(review)}, status_code=201)


async def flag_jev_identity_decision(request):
    if error := await _scope(request, write=True):
        return error
    try:
        body = FlagJevIdentityDecision.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_correction", "Invalid identity correction", 422)
    review = await request.app.state.runtime.projects.maintenance_service.flag_jev_identity_decision(
        request.path_params["project_id"], window_id=str(body.window_id),
        pass_number=body.pass_number, occurrence_key=body.occurrence_key,
        source_entity_id=body.source_entity_id, target_entity_id=body.target_entity_id,
        new_entity_name=body.new_entity_name, entity_type=body.entity_type,
        topic=body.topic, reasoning=body.reasoning,
    )
    return JSONResponse({"review": _review(review)}, status_code=201)


async def apply_jev_identity_decision(request):
    if error := await _scope(request, write=True):
        return error
    try:
        body = ApplyJevIdentityDecision.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_correction", "Invalid identity correction approval", 422)
    result = await request.app.state.runtime.projects.maintenance_service.apply_jev_identity_review(
        request.path_params["project_id"], request.path_params["review_id"],
        expected_state=body.expected_state,
    )
    return JSONResponse({
        "review": _review(result["review"]),
        "target_entity_id": result["target_entity_id"],
    })


async def reopen_jev_identity_correction(request):
    if error := await _scope(request, write=True):
        return error
    try:
        body = ReopenJevIdentityCorrection.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_reopen", "Invalid identity correction reopen request", 422)
    result = await request.app.state.runtime.projects.maintenance_service.reopen_jev_identity_correction(
        request.path_params["project_id"], request.path_params["review_id"],
        expected_target_entity_id=body.expected_target_entity_id,
    )
    return JSONResponse(result)


async def reopen_jev_context_correction(request):
    if error := await _scope(request, write=True):
        return error
    try:
        entity_id = int(request.path_params["entity_id"])
        if entity_id <= 0:
            raise ValueError("invalid entity ID")
        body = ReopenJevContextCorrection.model_validate(await _json(request))
    except (ValidationError, ValueError):
        return _error("invalid_reopen", "Invalid correction reopen request", 422)
    result = await request.app.state.runtime.projects.maintenance_service.reopen_jev_entity_context_correction(
        request.path_params["project_id"], entity_id,
        expected_review_id=body.expected_review_id,
    )
    return JSONResponse(result)


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
    Route("/v1/maintenance/duplicates", blocked_duplicates, methods=["GET"]),
    Route(
        "/v1/maintenance/duplicates/{entity_id}/{other_entity_id}/dismiss",
        dismiss_duplicate, methods=["POST"],
    ),
    Route("/v1/maintenance/merges/preview", global_merge_preview, methods=["POST"]),
    Route("/v1/maintenance/merges", global_merge_proposal, methods=["POST"]),
    Route("/v1/maintenance/reviews/{review_id}/decision", global_decision, methods=["POST"]),
    Route("/v1/maintenance/merges/{merge_id}/rollback/preview", rollback_preview, methods=["GET"]),
    Route("/v1/maintenance/merges/{merge_id}/rollback", rollback, methods=["POST"]),
    Route("/v1/projects/{project_id}/maintenance/reviews", project_reviews, methods=["GET"]),
    Route("/v1/projects/{project_id}/maintenance/jev-context-corrections", flag_jev_context_decision, methods=["POST"]),
    Route("/v1/projects/{project_id}/maintenance/jev-identity-corrections", flag_jev_identity_decision, methods=["POST"]),
    Route("/v1/projects/{project_id}/maintenance/jev-identity-corrections/{review_id}/apply", apply_jev_identity_decision, methods=["POST"]),
    Route("/v1/projects/{project_id}/maintenance/jev-identity-corrections/{review_id}/reopen", reopen_jev_identity_correction, methods=["POST"]),
    Route("/v1/projects/{project_id}/maintenance/jev-context-corrections/{entity_id}/reopen", reopen_jev_context_correction, methods=["POST"]),
    Route("/v1/projects/{project_id}/maintenance/reviews/{review_id}", project_review, methods=["GET"]),
    Route("/v1/projects/{project_id}/maintenance/reviews/{review_id}/preview", project_preview, methods=["GET"]),
    Route("/v1/projects/{project_id}/maintenance/reviews/{review_id}/decision", project_decision, methods=["POST"]),
    Route("/v1/projects/{project_id}/maintenance/reviews/{review_id}/advisory", advisory_decision, methods=["POST"]),
]
