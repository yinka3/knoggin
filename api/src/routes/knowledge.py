"""Bounded, project-scoped reads for the knowledge explorer."""

from uuid import UUID

from starlette.responses import JSONResponse
from starlette.routing import Route

from common.schema.evidence import EvidenceTraversalLimits
from routes.project_settings import _error, _project_error


def _positive(request, name, *, default=None, maximum=100):
    raw = request.query_params.get(name)
    if raw is None and default is None:
        return None
    try:
        value = int(raw) if raw is not None else default
    except ValueError:
        return None
    return value if 1 <= value <= maximum else None


def _entity_id(request, name="entity_id"):
    try:
        value = int(request.path_params[name])
    except ValueError:
        return None
    return value if value > 0 else None


def _fields(row, names):
    return {name: row[name] for name in names if name in row}


def _message(row):
    return _fields(row, ("id", "session_id", "role", "content", "timestamp", "score"))


def _entity(row):
    return {
        **_fields(row, ("id", "entity_id", "canonical_name", "aliases")),
        "contexts": [
            _fields(context, ("project_id", "entity_type", "topic", "last_mentioned_ms"))
            for context in row.get("contexts", [])
        ],
    }


def _evidence_ref(row):
    return _fields(row, ("kind", "project_id", "session_id", "message_id", "observation_id"))


async def _scope(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    error = await _project_error(runtime, project_id, write=False)
    return runtime, project_id, error


async def messages(request):
    runtime, project_id, error = await _scope(request)
    if error:
        return error
    limit = _positive(request, "limit", default=20, maximum=100)
    if limit is None:
        return _error("invalid_limit", "Invalid message limit", 422)
    query = request.query_params.get("query")
    store = runtime.resources.knowledge_store
    user = runtime.sessions.user_name
    if query is None:
        before = request.query_params.get("before_message_id")
        if before is not None:
            try:
                before = int(before)
            except ValueError:
                before = None
            if before is None or before <= 0:
                return _error("invalid_cursor", "Invalid message cursor", 422)
        rows = await store.get_recent_project_messages(
            user, project_id, limit, before_message_id=before
        )
    else:
        if not query.strip():
            return _error("invalid_query", "Invalid message query", 422)
        session_ids = await store.get_visible_session_ids(
            user_name=user, visible_project_ids=[project_id]
        )
        if not session_ids:
            rows = []
        else:
            hits = await store.search_messages_fts(
                query.strip(), user_name=user, session_ids=session_ids,
                visible_project_ids=[project_id], limit=limit,
            )
            rows = await store.get_messages_by_ids(
                [message_id for message_id, _score, _session in hits],
                user_name=user, session_ids=session_ids,
                visible_project_ids=[project_id],
            )
            scores = {(message_id, session): score for message_id, score, session in hits}
            rows = [{**row, "score": scores.get((row["id"], row["session_id"]))}
                    for row in rows]
            positions = {(message_id, session): index
                         for index, (message_id, _score, session) in enumerate(hits)}
            rows.sort(key=lambda row: positions.get((row["id"], row["session_id"]), len(hits)))
    return JSONResponse({"items": [_message(row) for row in rows]})


async def entities(request):
    runtime, project_id, error = await _scope(request)
    if error:
        return error
    query = request.query_params.get("query", "").strip()
    limit = _positive(request, "limit", default=20, maximum=50)
    if not query or limit is None:
        return _error("invalid_query", "Provide a query and valid limit", 422)
    rows = await runtime.resources.knowledge_store.search_entity(
        query, visible_project_ids=[project_id], limit=limit,
    )
    return JSONResponse({"items": [_entity(row) for row in rows]})


async def _visible_entity(request):
    runtime, project_id, error = await _scope(request)
    if error:
        return runtime, project_id, None, error
    entity_id = _entity_id(request)
    if entity_id is None:
        return runtime, project_id, None, _error("entity_not_found", "Entity not found", 404)
    row = await runtime.resources.knowledge_store.get_entity_by_id(
        entity_id, visible_project_ids=[project_id]
    )
    if row is None:
        return runtime, project_id, None, _error("entity_not_found", "Entity not found", 404)
    return runtime, project_id, row, None


async def entity_detail(request):
    _runtime, _project_id, row, error = await _visible_entity(request)
    return error or JSONResponse(_entity(row))


async def entity_connections(request):
    runtime, project_id, row, error = await _visible_entity(request)
    if error:
        return error
    limit = _positive(request, "limit", default=40, maximum=100)
    if limit is None:
        return _error("invalid_limit", "Invalid connection limit", 422)
    relationships = await runtime.resources.knowledge_store.get_related_entities(
        [row["id"]], visible_project_ids=[project_id], limit=limit,
    )
    return JSONResponse({"items": [{
        **_fields(item, (
            "project_id", "source_entity_id", "target_entity_id", "source", "target",
            "relationship_id", "relationship_type", "symmetric", "observation_count",
            "evidence_message_count", "first_observed", "last_observed",
        )),
        "evidence_refs": [_evidence_ref(ref) for ref in item.get("evidence_refs", [])[:8]],
        "evidence_refs_truncated": len(item.get("evidence_refs", [])) > 8,
        "observation_refs": [_fields(ref, ("observation_id", "observed_at_ms"))
                             for ref in item.get("observation_refs", [])[:8]],
        "observation_refs_truncated": len(item.get("observation_refs", [])) > 8,
    } for item in relationships]})


async def entity_activity(request):
    runtime, project_id, row, error = await _visible_entity(request)
    if error:
        return error
    hours = _positive(request, "hours", default=24, maximum=24 * 365)
    if hours is None:
        return _error("invalid_hours", "Invalid activity window", 422)
    items = await runtime.resources.knowledge_store.get_recent_activity(
        row["id"], visible_project_ids=[project_id], hours=hours,
    )
    return JSONResponse({"items": [{
        **_fields(item, ("entity_id", "entity", "project_id", "time")),
        "evidence_refs": [_evidence_ref(ref) for ref in item.get("evidence_refs", [])[:8]],
        "evidence_refs_truncated": len(item.get("evidence_refs", [])) > 8,
        "observation_refs": [_fields(ref, ("relationship_id", "observation_id", "observed_at_ms"))
                             for ref in item.get("observation_refs", [])[:8]],
        "observation_refs_truncated": len(item.get("observation_refs", [])) > 8,
    } for item in items]})


async def entity_path(request):
    runtime, project_id, source, error = await _visible_entity(request)
    if error:
        return error
    target_id = _entity_id(request, "target_id")
    target = await runtime.resources.knowledge_store.get_entity_by_id(
        target_id, visible_project_ids=[project_id]
    ) if target_id is not None else None
    if target is None:
        return _error("entity_not_found", "Entity not found", 404)
    steps = await runtime.resources.knowledge_store.find_path(
        source["id"], target["id"], visible_project_ids=[project_id], max_depth=4,
    )
    return JSONResponse({"steps": [{
        **_fields(step, (
            "step", "entity_a_id", "entity_b_id", "entity_a", "entity_b",
            "relationship_id", "project_id", "source_entity_id", "target_entity_id",
            "source", "target", "relationship_type", "symmetric",
        )),
        "evidence_refs": [_evidence_ref(ref) for ref in step.get("evidence_refs", [])],
    } for step in steps]})


async def observation_evidence(request):
    runtime, project_id, error = await _scope(request)
    if error:
        return error
    observation_id = _entity_id(request, "observation_id")
    if observation_id is None:
        return _error("observation_not_found", "Observation not found", 404)
    bundle = await runtime.resources.knowledge_store.get_visible_relationship_observation_evidence(
        observation_id, user_name=runtime.sessions.user_name,
        visible_project_ids=[project_id],
        limits=EvidenceTraversalLimits(
            max_observations=1, max_context_blocks=4, max_leaf_evidence=8,
            max_edges=16,
        ),
    )
    if not bundle.nodes:
        return _error("observation_not_found", "Observation not found", 404)
    return JSONResponse(bundle.model_dump(mode="json"))


def _jev_explanation(item, *, advanced=False):
    observation = item["decision"]
    record = observation["record"]
    capability = item["capability"]
    baseline_field, suggestion_field, operational_field = {
        "identity": ("baseline_entity_id", "suggested_entity_id", "operational_entity_id"),
        "extraction": ("proposed_entity_type", "suggested_entity_type", "accepted_entity_type"),
        "classification": ("baseline_topic", "suggested_topic", "operational_topic"),
    }[capability]
    baseline = observation.get(baseline_field)
    suggestion = observation.get(suggestion_field)
    operational = observation.get(operational_field)
    result = {
        "capability": capability,
        "baseline": baseline if baseline is not None else record["baseline_outcome"],
        "suggestion": suggestion,
        "operational_result": operational,
        "operational_result_recorded": operational is not None,
        "acceptance_status": record["acceptance_status"],
        "evidence_block_ids": record["evidence_block_ids"],
    }
    if capability == "extraction":
        result["candidate_name"] = observation.get("candidate_name")
    if advanced:
        response = (record["result"].get("response") or {})
        result["advanced"] = {
            "window_id": str(item["window_id"]),
            "pass_number": item["pass_number"],
            "occurrence_key": item["occurrence_key"],
            "model": record["pinned_model"],
            "question_version": record["question_version"],
            "acceptance_policy_version": record["acceptance_policy_version"],
            "option_mapping": record["option_mapping"],
            "result_outcome": record["result"]["outcome"],
            "answers": {
                key: _fields(answer, ("type", "choice", "confidence", "probabilities", "noul"))
                for key, answer in response.get("answers", {}).items()
            },
        }
    return result


async def block_jev_decisions(request):
    runtime, project_id, error = await _scope(request)
    if error:
        return error
    try:
        block_id = UUID(request.path_params["block_id"])
    except ValueError:
        return _error("block_not_found", "Context block not found", 404)
    limit = _positive(request, "limit", default=50, maximum=100)
    if limit is None or request.query_params.get("advanced", "false") not in {"true", "false"}:
        return _error("invalid_query", "Invalid decision query", 422)
    advanced = request.query_params.get("advanced") == "true"
    bundle = await runtime.resources.knowledge_store.list_jev_decisions_for_block(
        str(block_id), user_name=runtime.sessions.user_name,
        project_id=project_id, limit=limit,
    )
    if bundle is None:
        return _error("block_not_found", "Context block not found", 404)
    return JSONResponse({
        "block_id": bundle["block_id"], "evidence_excerpt": bundle["excerpt"],
        "items": [_jev_explanation(item, advanced=advanced) for item in bundle["decisions"]],
    })


async def entity_jev_decisions(request):
    runtime, project_id, entity, error = await _visible_entity(request)
    if error:
        return error
    limit = _positive(request, "limit", default=50, maximum=100)
    if limit is None or request.query_params.get("advanced", "false") not in {"true", "false"}:
        return _error("invalid_query", "Invalid decision query", 422)
    rows = await runtime.resources.knowledge_store.list_jev_decisions_for_entity(
        entity["id"], user_name=runtime.sessions.user_name,
        project_id=project_id, limit=limit,
    )
    if rows is None:
        return _error("entity_not_found", "Entity not found", 404)
    advanced = request.query_params.get("advanced") == "true"
    return JSONResponse({
        "entity_id": entity["id"],
        "items": [_jev_explanation(item, advanced=advanced) for item in rows],
    })


routes = [
    Route("/v1/projects/{project_id}/knowledge/messages", messages, methods=["GET"]),
    Route("/v1/projects/{project_id}/knowledge/entities", entities, methods=["GET"]),
    Route("/v1/projects/{project_id}/knowledge/entities/{entity_id}", entity_detail, methods=["GET"]),
    Route("/v1/projects/{project_id}/knowledge/entities/{entity_id}/jev-decisions", entity_jev_decisions, methods=["GET"]),
    Route("/v1/projects/{project_id}/knowledge/entities/{entity_id}/connections", entity_connections, methods=["GET"]),
    Route("/v1/projects/{project_id}/knowledge/entities/{entity_id}/activity", entity_activity, methods=["GET"]),
    Route("/v1/projects/{project_id}/knowledge/entities/{entity_id}/path/{target_id}", entity_path, methods=["GET"]),
    Route("/v1/projects/{project_id}/knowledge/observations/{observation_id}/evidence", observation_evidence, methods=["GET"]),
    Route("/v1/projects/{project_id}/knowledge/context-blocks/{block_id}/jev-decisions", block_jev_decisions, methods=["GET"]),
]
