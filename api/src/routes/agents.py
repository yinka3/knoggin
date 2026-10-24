"""Agent preferences and AAC participation routes."""

from pydantic import BaseModel, ConfigDict, ValidationError
from starlette.responses import JSONResponse
from starlette.routing import Route

from contracts.agents import CreateAgent, UpdateAgent
from routes.project_settings import _error, _json


class AACParticipation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool


def _public(agent):
    return {
        "id": agent.id,
        "name": agent.name,
        "persona": agent.persona.to_dict(),
        "model": agent.model,
        "temperature": agent.temperature,
        "enabled_tools": agent.enabled_tools,
        "aac_enabled": agent.aac_enabled,
        "is_default": agent.is_default,
        "created_at": agent.created_at.isoformat(),
        "last_turn_at": agent.last_turn_at.isoformat() if agent.last_turn_at else None,
    }


async def list_agents(request):
    agents = await request.app.state.runtime.agent_manager.list_agents()
    return JSONResponse([_public(agent) for agent in agents])


async def get_agent(request):
    agent = await request.app.state.runtime.agent_manager.get_agent(
        request.path_params["agent_id"]
    )
    if agent is None:
        return _error("agent_not_found", "Agent not found", 404)
    return JSONResponse(_public(agent))


async def create_agent(request):
    try:
        body = CreateAgent.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_agent", "Invalid agent", 422)
    try:
        agent = await request.app.state.runtime.agent_manager.create_agent(
            name=body.name, persona=body.persona.model_dump(), model=body.model,
            temperature=body.temperature, enabled_tools=body.enabled_tools,
        )
    except ValueError:
        return _error("invalid_agent", "Invalid agent", 422)
    return JSONResponse(_public(agent), status_code=201)


async def update_agent(request):
    agent_id = request.path_params["agent_id"]
    if await request.app.state.runtime.agent_manager.get_agent(agent_id) is None:
        return _error("agent_not_found", "Agent not found", 404)
    try:
        body = UpdateAgent.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_agent", "Invalid agent settings", 422)
    if not body.model_fields_set:
        return _error("invalid_agent", "Invalid agent settings", 422)
    try:
        agent = await request.app.state.runtime.agent_manager.update_agent(
            agent_id, **body.model_dump(exclude_unset=True)
        )
    except ValueError:
        return _error("invalid_agent", "Invalid agent settings", 422)
    return JSONResponse(_public(agent))


async def set_default_agent(request):
    agent_id = request.path_params["agent_id"]
    if not await request.app.state.runtime.agent_manager.set_default_agent(agent_id):
        return _error("agent_not_found", "Agent not found", 404)
    return await get_agent(request)


async def delete_agent(request):
    agent_id = request.path_params["agent_id"]
    agent = await request.app.state.runtime.agent_manager.get_agent(agent_id)
    if agent is None:
        return _error("agent_not_found", "Agent not found", 404)
    if not await request.app.state.runtime.agent_manager.delete_agent(agent_id):
        return _error("default_agent", "Default agent cannot be deleted", 409)
    return JSONResponse({"agent_id": agent_id, "deleted": True})


async def set_aac_participation(request):
    agent_id = request.path_params["agent_id"]
    try:
        body = AACParticipation.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_participation", "Invalid participation choice", 422)
    agent = await request.app.state.runtime.aac_runtime.set_participation(
        agent_id, body.enabled
    )
    if agent is None:
        return _error("agent_not_found", "Agent not found", 404)
    return JSONResponse({"agent_id": agent_id, "enabled": agent.aac_enabled})


routes = [
    Route("/v1/agents", list_agents, methods=["GET"]),
    Route("/v1/agents", create_agent, methods=["POST"]),
    Route("/v1/agents/{agent_id}", get_agent, methods=["GET"]),
    Route("/v1/agents/{agent_id}", update_agent, methods=["PATCH"]),
    Route("/v1/agents/{agent_id}", delete_agent, methods=["DELETE"]),
    Route("/v1/agents/{agent_id}/default", set_default_agent, methods=["POST"]),
    Route("/v1/agents/{agent_id}/aac-participation", set_aac_participation, methods=["PUT"]),
]
