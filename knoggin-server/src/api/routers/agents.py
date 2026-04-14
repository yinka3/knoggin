import json
import uuid
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.deps import get_app_state
from api.state import AppState
from common.infra.redis import RedisKeys
from datetime import datetime, timezone

router = APIRouter()


class CreateAgentRequest(BaseModel):
    name: str
    persona: str
    instructions: Optional[str] = None
    model: Optional[str] = None
    temperature: Optional[float] = 0.7
    enabled_tools: Optional[list[str]] = None


class UpdateAgentRequest(BaseModel):
    name: Optional[str] = None
    persona: Optional[str] = None
    instructions: Optional[str] = None
    model: Optional[str] = None
    temperature: Optional[float] = None
    enabled_tools: Optional[list[str]] = None


class AgentMemoryEntry(BaseModel):
    content: str


@router.get("/defaults")
async def get_agent_defaults(state: AppState = Depends(get_app_state)):
    return {
        "default_persona": "Warm and direct. Match their energy. No corporate filler.",
        "default_instructions": ""
    }


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
        instructions=body.instructions,
        model=body.model,
        temperature=body.temperature,
        enabled_tools=body.enabled_tools
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
        instructions=body.instructions,
        model=body.model,
        temperature=body.temperature,
        enabled_tools=body.enabled_tools
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

@router.get("/memory/{session_id}")
async def get_session_memory(
    session_id: str,
    state: AppState = Depends(get_app_state)
):
    context = await state.get_or_resume_session(session_id)
    if not context:
        raise HTTPException(status_code=404, detail="Session not found")
    
    topics = list(context.topic_config.raw.keys()) + ["General"]
    blocks = {}
    
    for topic in topics:
        key = RedisKeys.agent_memory(context.user_name, session_id, topic)
        raw = await context.redis_client.hgetall(key)
        if raw:
            entries = []
            for mem_id, payload in raw.items():
                try:
                    data = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                entries.append({
                    "id": mem_id,
                    "content": data.get("content", ""),
                    "topic": data.get("topic", topic),
                    "created_at": data.get("created_at", ""),
                })
            entries.sort(key=lambda x: x["created_at"])
            blocks[topic] = entries
    
    total = sum(len(v) for v in blocks.values())
    return {"blocks": blocks, "total": total}

@router.get("/{agent_id}/memory")
async def get_agent_memory(
    agent_id: str,
    state: AppState = Depends(get_app_state)
):
    agent = await state.get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
        
    categories = ["rules", "preferences", "icks"]
    blocks = {}
    
    for category in categories:
        key = RedisKeys.agent_working_memory(agent_id, category)
        raw = await state.resources.redis.hgetall(key)
        
        entries = []
        if raw:
            for mem_id, payload in raw.items():
                try:
                    data = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                data["id"] = mem_id
                entries.append(data)
            
            entries.sort(key=lambda x: x.get("created_at", ""))
            
        blocks[category] = entries
        
    return blocks

@router.post("/{agent_id}/memory/{category}")
async def add_agent_memory(
    agent_id: str,
    category: str,
    body: AgentMemoryEntry,
    state: AppState = Depends(get_app_state)
):
    agent = await state.get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
        
    if category not in ["rules", "preferences", "icks"]:
        raise HTTPException(status_code=400, detail="Invalid memory category")
        
    key = RedisKeys.agent_working_memory(agent_id, category)
    
    mem_id = f"mem_{str(uuid.uuid4().hex)[:8]}"
    payload = json.dumps({
        "content": body.content,
        "created_at": datetime.now(timezone.utc).isoformat()
    })
    
    await state.resources.redis.hset(key, mem_id, payload)
    
    return {
        "id": mem_id,
        "content": body.content,
        "category": category
    }

@router.delete("/{agent_id}/memory/{category}/{memory_id}")
async def delete_agent_memory(
    agent_id: str,
    category: str,
    memory_id: str,
    state: AppState = Depends(get_app_state)
):
    agent = await state.get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
        
    if category not in ["rules", "preferences", "icks"]:
        raise HTTPException(status_code=400, detail="Invalid memory category")
        
    key = RedisKeys.agent_working_memory(agent_id, category)
    
    deleted = await state.resources.redis.hdel(key, memory_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Memory entry not found")
        
    return {"success": True}