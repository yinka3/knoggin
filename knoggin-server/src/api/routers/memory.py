from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.deps import get_app_state
from api.state import AppState
from common.services.memory_manager import MemoryManager
from common.config.topics_config import TopicConfig

router = APIRouter()

class AddMemoryRequest(BaseModel):
    content: str
    topic: Optional[str] = "General"

@router.post("/{session_id}")
async def add_memory(
    session_id: str,
    body: AddMemoryRequest,
    state: AppState = Depends(get_app_state)
):
    context = await state.get_or_resume_session(session_id)
    if not context:
        raise HTTPException(status_code=404, detail="Session not found")
        
    sessions = await state.list_sessions()
    session_meta = sessions.get(session_id, {})
    agent_id = session_meta.get("agent_id") or await state.get_default_agent_id()

    topic_config = await TopicConfig.load(
        state.resources.redis,
        state.user_name,
        session_id
    )

    memory_mgr = MemoryManager(
        redis=state.resources.redis,
        user_name=state.user_name,
        session_id=session_id,
        agent_id=agent_id,
        topic_config=topic_config
    )
    
    result = await memory_mgr.save_memory(content=body.content, topic=body.topic)
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return {
        "memory_id": result.memory_id,
        "topic": result.topic,
        "content": result.content
    }

@router.delete("/{session_id}/{memory_id}")
async def delete_memory(
    session_id: str,
    memory_id: str,
    state: AppState = Depends(get_app_state)
):
    context = await state.get_or_resume_session(session_id)
    if not context:
        raise HTTPException(status_code=404, detail="Session not found")
        
    sessions = await state.list_sessions()
    session_meta = sessions.get(session_id, {})
    agent_id = session_meta.get("agent_id") or await state.get_default_agent_id()

    topic_config = await TopicConfig.load(
        state.resources.redis,
        state.user_name,
        session_id
    )

    memory_mgr = MemoryManager(
        redis=state.resources.redis,
        user_name=state.user_name,
        session_id=session_id,
        agent_id=agent_id,
        topic_config=topic_config
    )
    
    result = await memory_mgr.forget_memory(memory_id)
    if not result.success:
        raise HTTPException(status_code=404, detail=result.error)
        
    return {
        "success": True,
        "memory_id": result.memory_id,
        "topic": result.topic
    }
