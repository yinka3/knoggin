from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.deps import AgentID, SessionID, get_agent_manager, get_memory_manager, get_working_memory_manager
from common.schema.api import (
    AgentDetail,
    AgentListResponse,
    GenericSuccess,
    SessionMemoryResponse,
    WorkingMemoryItem,
    WorkingMemoryResponse,
)
from knoggin.agent.services.agent_manager import AgentManager
from knoggin.knowledge.services.memory_service import MemoryManager

router = APIRouter()


class CreateAgentRequest(BaseModel):
    name: str
    persona: str
    instructions: str = ""
    model: str = None
    temperature: float = 0.7
    enabled_tools: list[str] = None


class UpdateAgentRequest(BaseModel):
    name: str = None
    persona: str = None
    instructions: str = None
    model: str = None
    temperature: float = None
    enabled_tools: list[str] = None


class AgentMemoryEntry(BaseModel):
    content: str


@router.get("/defaults")
async def get_agent_defaults():
    return {
        "default_persona": "Warm and direct. Match their energy. No corporate filler.",
        "default_instructions": "",
    }


@router.get("/", response_model=AgentListResponse)
async def list_agents(agent_manager: AgentManager = Depends(get_agent_manager)):
    agents = await agent_manager.list_agents()
    return AgentListResponse(agents=[a.to_dict() for a in agents])


@router.post("/", response_model=AgentDetail)
async def create_agent(
    body: CreateAgentRequest, 
    agent_manager: AgentManager = Depends(get_agent_manager)
):
    existing = await agent_manager.get_agent_by_name(body.name)
    if existing:
        raise HTTPException(status_code=400, detail="Agent with this name already exists")

    agent = await agent_manager.create_agent(
        name=body.name,
        persona=body.persona,
        instructions=body.instructions,
        model=body.model,
        temperature=body.temperature,
        enabled_tools=body.enabled_tools,
    )
    return AgentDetail(**agent.to_dict())


@router.get("/by-name/{name}", response_model=AgentDetail)
async def get_agent_by_name(
    name: str, 
    agent_manager: AgentManager = Depends(get_agent_manager)
):
    agent = await agent_manager.get_agent_by_name(name)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return AgentDetail(**agent.to_dict())


@router.get("/{agent_id}", response_model=AgentDetail)
async def get_agent(
    agent_id: AgentID, 
    agent_manager: AgentManager = Depends(get_agent_manager)
):
    agent = await agent_manager.get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return AgentDetail(**agent.to_dict())


@router.patch("/{agent_id}", response_model=AgentDetail)
async def update_agent(
    agent_id: AgentID, 
    body: UpdateAgentRequest, 
    agent_manager: AgentManager = Depends(get_agent_manager)
):
    if body.name:
        existing = await agent_manager.get_agent_by_name(body.name)
        if existing and existing.id != agent_id:
            raise HTTPException(status_code=400, detail="Agent with this name already exists")

    agent = await agent_manager.update_agent(
        agent_id=agent_id,
        name=body.name,
        persona=body.persona,
        instructions=body.instructions,
        model=body.model,
        temperature=body.temperature,
        enabled_tools=body.enabled_tools,
    )
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return AgentDetail(**agent.to_dict())


@router.delete("/{agent_id}", response_model=GenericSuccess)
async def delete_agent(
    agent_id: AgentID, 
    agent_manager: AgentManager = Depends(get_agent_manager)
):
    agent = await agent_manager.get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    if agent.is_default:
        raise HTTPException(status_code=400, detail="Cannot delete default agent")

    success = await agent_manager.delete_agent(agent_id)
    return GenericSuccess(success=success, message="Agent deleted successfully")


@router.post("/{agent_id}/set-default", response_model=GenericSuccess)
async def set_default_agent(
    agent_id: AgentID, 
    agent_manager: AgentManager = Depends(get_agent_manager)
):
    success = await agent_manager.set_default_agent(agent_id)
    if not success:
        raise HTTPException(status_code=404, detail="Agent not found")
    return GenericSuccess(success=True, message="Default agent updated")


@router.get("/memory/{session_id}", response_model=SessionMemoryResponse)
async def get_session_memory(session_id: SessionID, memory_mgr: MemoryManager = Depends(get_memory_manager)):
    """Read agent-saved memory notes for a session."""
    result = await memory_mgr.get_memory_blocks()
    return SessionMemoryResponse(memories=result.blocks, total=result.total)


@router.get("/{agent_id}/memory", response_model=WorkingMemoryResponse)
async def get_agent_memory(agent_id: AgentID, memory_mgr: MemoryManager = Depends(get_working_memory_manager)):
    """Read agent-level working memory (rules, preferences, icks)."""
    result = await memory_mgr.list_working_memory()
    
    formatted = {}
    for cat, entries in result.blocks.items():
        formatted[cat] = [
            WorkingMemoryItem(id=e.id, content=e.content, created_at=e.created_at)
            for e in entries
        ]
    return WorkingMemoryResponse(**formatted)


@router.post("/{agent_id}/memory/{category}", response_model=WorkingMemoryItem)
async def add_agent_memory(
    agent_id: AgentID,
    category: str,
    body: AgentMemoryEntry,
    memory_mgr: MemoryManager = Depends(get_working_memory_manager),
):
    """Add an entry to agent-level working memory."""
    result = await memory_mgr.add_working_memory(category=category, content=body.content)
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)

    return WorkingMemoryItem(
        id=result.memory_id, 
        content=result.content, 
        created_at=datetime.now(timezone.utc).isoformat() # We don't have it in result yet, but good for schema
    )


@router.delete("/{agent_id}/memory/{category}/{memory_id}", response_model=GenericSuccess)
async def delete_agent_memory(
    agent_id: AgentID,
    category: str,
    memory_id: str,
    memory_mgr: MemoryManager = Depends(get_working_memory_manager),
):
    """Remove an entry from agent-level working memory."""
    result = await memory_mgr.remove_working_memory(category=category, memory_id=memory_id)
    if not result.success:
        raise HTTPException(status_code=404, detail=result.error)

    return GenericSuccess(success=True)
