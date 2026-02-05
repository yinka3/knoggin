from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from api.state import AppState

router = APIRouter()


def get_app_state(request: Request) -> AppState:
    return request.app.state.app_state


class CreateAgentRequest(BaseModel):
    name: str
    persona: str
    model: Optional[str] = None


class UpdateAgentRequest(BaseModel):
    name: Optional[str] = None
    persona: Optional[str] = None
    model: Optional[str] = None


@router.get("/")
async def list_agents(state: AppState = Depends(get_app_state)):
    agents = await state.list_agents()
    return {
        "agents": [a.to_dict() for a in agents]
    }


@router.post("/")
async def create_agent(
    body: CreateAgentRequest,
    state: AppState = Depends(get_app_state)
):
    existing = await state.get_agent_by_name(body.name)
    if existing:
        raise HTTPException(status_code=400, detail="Agent with this name already exists")
    
    agent = await state.create_agent(
        name=body.name,
        persona=body.persona,
        model=body.model
    )
    
    return agent.to_dict()


@router.get("/by-name/{name}")
async def get_agent_by_name(
    name: str,
    state: AppState = Depends(get_app_state)
):
    agent = await state.get_agent_by_name(name)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent.to_dict()


@router.get("/{agent_id}")
async def get_agent(
    agent_id: str,
    state: AppState = Depends(get_app_state)
):
    agent = await state.get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent.to_dict()


@router.patch("/{agent_id}")
async def update_agent(
    agent_id: str,
    body: UpdateAgentRequest,
    state: AppState = Depends(get_app_state)
):
    if body.name:
        existing = await state.get_agent_by_name(body.name)
        if existing and existing.id != agent_id:
            raise HTTPException(status_code=400, detail="Agent with this name already exists")
    
    agent = await state.update_agent(
        agent_id=agent_id,
        name=body.name,
        persona=body.persona,
        model=body.model
    )
    
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    
    return agent.to_dict()


@router.delete("/{agent_id}")
async def delete_agent(
    agent_id: str,
    state: AppState = Depends(get_app_state)
):
    agent = await state.get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    
    if agent.is_default:
        raise HTTPException(status_code=400, detail="Cannot delete default agent")
    
    success = await state.delete_agent(agent_id)
    return {"success": success}


@router.post("/{agent_id}/set-default")
async def set_default_agent(
    agent_id: str,
    state: AppState = Depends(get_app_state)
):
    success = await state.set_default_agent(agent_id)
    if not success:
        raise HTTPException(status_code=404, detail="Agent not found")
    return {"success": True}