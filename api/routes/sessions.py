from datetime import datetime, timezone
import json
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from api.deps import get_app_state
from api.state import AppState
from shared.redisclient import RedisKeys
from shared.schema.tool_schema import ALL_TOOL_NAMES
router = APIRouter()



class CreateSessionRequest(BaseModel):
    topics_config: Optional[dict] = None
    model: Optional[str] = None
    agent_id: Optional[str] = None
    enabled_tools: Optional[List[str]] = None

class UpdateSessionRequest(BaseModel):
    model: Optional[str] = None
    agent_id: Optional[str] = None
    enabled_tools: Optional[List[str]] = None

@router.get("/")
async def list_sessions(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    state: AppState = Depends(get_app_state)
):
    sessions = await state.list_sessions()

    sorted_sessions = sorted(
        sessions.items(),
        key=lambda x: x[1].get("last_active", ""),
        reverse=True
    )
    paginated = sorted_sessions[offset:offset + limit]

    result = []
    for session_id, metadata in paginated:
        result.append({
            "session_id": session_id,
            "created_at": metadata.get("created_at"),
            "last_active": metadata.get("last_active"),
            "is_active": session_id in state.active_sessions
        })
    
    return {
        "sessions": result,
        "total": len(sessions),
        "limit": limit,
        "offset": offset
    }

@router.post("/")
async def create_session(
    body: CreateSessionRequest = CreateSessionRequest(),
    state: AppState = Depends(get_app_state)
):
    
    agent_id = None
    if body and body.agent_id:
        agent = await state.get_agent(body.agent_id)
        if not agent:
            raise HTTPException(status_code=400, detail="Agent not found")
        agent_id = body.agent_id
    else:
        agent_id = await state.get_default_agent_id()

    context = await state.create_session(
        body.topics_config if body else None,
        model=body.model if body else None,
        agent_id=agent_id,
        enabled_tools=body.enabled_tools if body else None
    )

    await state.resources.redis.delete(f"topic_gen_count:{state.user_name}")

    if body and body.enabled_tools is not None:
        raw = await state.resources.redis.hget(RedisKeys.sessions(state.user_name), context.session_id)
        metadata = json.loads(raw)
        metadata["enabled_tools"] = body.enabled_tools
        await state.resources.redis.hset(
            RedisKeys.sessions(state.user_name),
            context.session_id,
            json.dumps(metadata)
        )
    
    raw = await state.resources.redis.hget(RedisKeys.sessions(state.user_name), context.session_id)
    metadata = json.loads(raw)
    
    return {
        "session_id": context.session_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "model": body.model if body else None,
        "agent_id": agent_id
    }

@router.get("/{session_id}")
async def get_session(
    session_id: str,
    state: AppState = Depends(get_app_state)
):
    sessions = await state.list_sessions()
    
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    metadata = sessions[session_id]
    
    return {
        "session_id": session_id,
        "created_at": metadata.get("created_at"),
        "last_active": metadata.get("last_active"),
        "topics_config": metadata.get("topics_config"),
        "model": metadata.get("model"),
        "agent_id": metadata.get("agent_id"),
        "enabled_tools": metadata.get("enabled_tools"),
        "is_active": session_id in state.active_sessions
    }

@router.patch("/{session_id}")
async def update_session(
    session_id: str,
    body: UpdateSessionRequest,
    state: AppState = Depends(get_app_state)
):
    sessions = await state.list_sessions()
    
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    metadata = sessions[session_id]
    
    if body.model is not None:
        metadata["model"] = body.model
    
    if body.agent_id is not None:
        agent = await state.get_agent(body.agent_id)
        if not agent:
            raise HTTPException(status_code=400, detail="Agent not found")
        metadata["agent_id"] = body.agent_id
    
    if body.enabled_tools is not None:
        
        invalid = set(body.enabled_tools) - set(ALL_TOOL_NAMES)
        if invalid:
            raise HTTPException(status_code=400, detail=f"Invalid tool names: {invalid}")
        metadata["enabled_tools"] = body.enabled_tools
    
    metadata["last_active"] = datetime.now(timezone.utc).isoformat()
    
    await state.resources.redis.hset(
        RedisKeys.sessions(state.user_name),
        session_id,
        json.dumps(metadata)
    )

    if session_id in state.active_sessions:
        if body.model is not None:
            state.active_sessions[session_id].model = body.model
    
    return {"success": True, "model": metadata.get("model"), "agent_id": metadata.get("agent_id"), "enabled_tools": metadata.get("enabled_tools")}

@router.delete("/{session_id}")
async def delete_session(
    session_id: str,
    force: bool = Query(False),
    state: AppState = Depends(get_app_state)
):
    sessions = await state.list_sessions()
    
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    if session_id in state.active_sessions:
        if not force:
            raise HTTPException(
                status_code=409, 
                detail="Session is active. Use ?force=true to close and delete."
            )
        await state.close_session(session_id)
    
    deleted_count = await state.delete_session_data(session_id)
    await state.resources.redis.hdel(RedisKeys.sessions(state.user_name), session_id)
    
    return {"success": True, "keys_deleted": deleted_count}
