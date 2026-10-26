"""Project episode reads and curated narrative edits."""

import asyncio

from pydantic import ValidationError
from starlette.responses import JSONResponse
from starlette.routing import Route

from common.exceptions import EpisodeEditConflictError
from common.utils.lifecycle import settle_owned_task

from contracts.episodes import EditEpisode
from project_lease import project_runtime
from routes.project_settings import _error, _json, _project_error


async def _episode(runtime, project_id, episode_id):
    episode = await runtime.resources.knowledge_store.get_project_episode(
        episode_id, user_name=runtime.sessions.user_name,
        project_id=project_id, visible_project_ids=[project_id],
    )
    if episode is None:
        return None
    if episode.project_id != project_id or episode.episode_id != episode_id:
        return None
    return episode


def _public(episode):
    fields = (
        "episode_id", "project_id", "summary", "new_developments", "updates",
        "unresolved", "user_modified", "created_at", "updated_at",
    )
    return {field: (
        getattr(episode, field).isoformat()
        if field in {"created_at", "updated_at"}
        else getattr(episode, field)
    ) for field in fields}


def _limit(request, *, default=20, maximum=100):
    try:
        value = int(request.query_params.get("limit", str(default)))
    except ValueError:
        return None
    return value if 1 <= value <= maximum else None


async def list_episodes(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=False):
        return error
    limit = _limit(request)
    if limit is None:
        return _error("invalid_limit", "Invalid episode limit", 422)
    query = request.query_params.get("query")
    store = runtime.resources.knowledge_store
    scope = {
        "user_name": runtime.sessions.user_name,
        "project_id": project_id,
        "visible_project_ids": [project_id],
        "limit": limit,
    }
    if query is None:
        cards = await store.get_recent_project_episodes(**scope)
    elif query.strip():
        cards = await store.search_project_episodes(query.strip(), **scope)
    else:
        return _error("invalid_query", "Invalid episode query", 422)
    return JSONResponse({"items": [_public(card) for card in cards]})


async def episode_sources(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=False):
        return error
    limit = _limit(request, default=100, maximum=100)
    if limit is None:
        return _error("invalid_limit", "Invalid episode source limit", 422)
    try:
        offset = int(request.query_params.get("offset", "0"))
    except ValueError:
        return _error("invalid_offset", "Invalid episode source offset", 422)
    if offset < 0:
        return _error("invalid_offset", "Invalid episode source offset", 422)
    episode_id = request.path_params["episode_id"]
    if await _episode(runtime, project_id, episode_id) is None:
        return _error("episode_not_found", "Episode not found", 404)
    rows = await runtime.resources.knowledge_store.get_project_episode_source_messages(
        episode_id, user_name=runtime.sessions.user_name,
        project_id=project_id, visible_project_ids=[project_id],
        limit=limit + 1, offset=offset,
    )
    return JSONResponse({"episode_id": episode_id, "has_more": len(rows) > limit,
                         "next_offset": offset + limit if len(rows) > limit else None,
                         "messages": [{
        key: row[key].isoformat() if hasattr(row[key], "isoformat") else row[key]
        for key in ("message_id", "session_id", "role", "content", "timestamp_ms",
                    "message_position", "attached_at")
        if key in row
    } for row in rows[:limit]]})


async def get_episode(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=False):
        return error
    episode = await _episode(runtime, project_id, request.path_params["episode_id"])
    if episode is None:
        return _error("episode_not_found", "Episode not found", 404)
    return JSONResponse(_public(episode))


async def edit_episode(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    if error := await _project_error(runtime, project_id, write=True):
        return error
    try:
        body = EditEpisode.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_episode", "Invalid episode narrative", 422)
    maximum = runtime.config_manager.config.developer_settings.jobs.episode.max_narrative_chars
    if sum(map(len, (body.summary, *body.new_developments, *body.updates, *body.unresolved))) > maximum:
        return _error("invalid_episode", "Episode narrative is too long", 422)
    async with project_runtime(runtime, project_id):
        episode = await _episode(runtime, project_id, request.path_params["episode_id"])
        if episode is None:
            return _error("episode_not_found", "Episode not found", 404)
        if episode.updated_at != body.expected_updated_at:
            return _error("episode_conflict", "Episode has changed", 409)
        worker = asyncio.create_task(runtime.resources.knowledge_store.edit_episode(
            episode_id=episode.episode_id, user_name=runtime.sessions.user_name,
            project_id=project_id, summary=body.summary,
            new_developments=body.new_developments, updates=body.updates,
            unresolved=body.unresolved, expected_updated_at=body.expected_updated_at,
        ))
        try:
            updated_at = await asyncio.shield(worker)
        except asyncio.CancelledError:
            try:
                await settle_owned_task(worker)
            except Exception:
                pass
            raise
        except EpisodeEditConflictError:
            return _error("episode_conflict", "Episode has changed", 409)
    return JSONResponse({"episode_id": episode.episode_id, "project_id": project_id,
                         "user_modified": True, "updated_at": updated_at.isoformat()})


routes = [
    Route("/v1/projects/{project_id}/episodes", list_episodes, methods=["GET"]),
    Route("/v1/projects/{project_id}/episodes/{episode_id}", get_episode, methods=["GET"]),
    Route("/v1/projects/{project_id}/episodes/{episode_id}", edit_episode, methods=["PUT"]),
    Route("/v1/projects/{project_id}/episodes/{episode_id}/sources", episode_sources, methods=["GET"]),
]
