"""Community discussion controls and read-only history."""

from datetime import datetime

from starlette.responses import JSONResponse
from starlette.routing import Route

from routes.project_settings import _error


def _limit(request, default: int, maximum: int):
    try:
        value = int(request.query_params.get("limit", str(default)))
    except ValueError:
        return None
    return value if 1 <= value <= maximum else None


def _public(row, fields):
    return {
        key: value.isoformat() if isinstance(value, datetime) else value
        for key, value in row.items() if key in fields
    }


async def trigger(request):
    result = await request.app.state.runtime.aac_runtime.trigger_discussion()
    return JSONResponse({
        "outcome": result.outcome, "reason": result.reason,
        "discussion_id": result.discussion_id,
    })


async def stop(request):
    stopped = await request.app.state.runtime.aac_runtime.request_stop()
    return JSONResponse({"stop_requested": stopped})


async def discussions(request):
    limit = _limit(request, 20, 100)
    if limit is None:
        return _error("invalid_limit", "Invalid discussion limit", 422)
    rows = await request.app.state.runtime.aac_runtime.list_discussions(limit=limit)
    fields = {"discussion_id", "topic", "status", "end_reason", "token_budget",
              "tokens_used", "started_at", "ended_at"}
    return JSONResponse([_public(row, fields) for row in rows])


async def timeline(request):
    limit = _limit(request, 100, 500)
    if limit is None:
        return _error("invalid_limit", "Invalid timeline limit", 422)
    try:
        after = int(request.query_params.get("after_sequence", "0"))
    except ValueError:
        return _error("invalid_sequence", "Invalid timeline sequence", 422)
    if after < 0:
        return _error("invalid_sequence", "Invalid timeline sequence", 422)
    rows = await request.app.state.runtime.aac_runtime.list_timeline(
        request.path_params["discussion_id"], limit=limit, after_sequence=after
    )
    fields = {"timeline_id", "kind", "agent_id", "content", "created_at", "event_sequence"}
    return JSONResponse([_public(row, fields) for row in rows])


async def insights(request):
    limit = _limit(request, 20, 100)
    if limit is None:
        return _error("invalid_limit", "Invalid insight limit", 422)
    rows = await request.app.state.runtime.aac_runtime.list_insights(
        query=request.query_params.get("query"), limit=limit
    )
    fields = {"insight_id", "discussion_id", "author_agent_id", "visibility",
              "content", "created_at"}
    return JSONResponse([_public(row, fields) for row in rows])


async def votes(request):
    rows = await request.app.state.runtime.aac_runtime.list_insight_votes(
        request.path_params["insight_id"]
    )
    fields = {"voter_agent_id", "vote", "reason", "created_at", "updated_at"}
    return JSONResponse([_public(row, fields) for row in rows])


routes = [
    Route("/v1/community/discussions", discussions, methods=["GET"]),
    Route("/v1/community/discussions/trigger", trigger, methods=["POST"]),
    Route("/v1/community/discussions/stop", stop, methods=["POST"]),
    Route("/v1/community/discussions/{discussion_id}/timeline", timeline, methods=["GET"]),
    Route("/v1/community/insights", insights, methods=["GET"]),
    Route("/v1/community/insights/{insight_id}/votes", votes, methods=["GET"]),
]
