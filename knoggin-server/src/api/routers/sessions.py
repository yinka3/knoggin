from datetime import datetime, timezone
import json
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from api.deps import get_app_state
from api.state import AppState
from common.infra.redis import RedisKeys
from common.config.topics_config import TopicConfig
from common.schema.tool_schema import ALL_TOOL_NAMES
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
    title: Optional[str] = None

class ImportSessionRequest(BaseModel):
    messages: List[dict]
    enabled_tools: Optional[List[str]] = None
    title: Optional[str] = None

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
            "title": metadata.get("title"),
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

@router.get("/export/all")
async def export_all_sessions(
    state: AppState = Depends(get_app_state)
):
    """Export all sessions' conversation histories as JSON."""
    sessions = await state.list_sessions()
    
    all_exports = []
    for session_id, metadata in sessions.items():
        try:
            if session_id in state.active_sessions:
                history = await state.active_sessions[session_id].get_conversation_context(num_turns=1000)
                messages = [
                    {"role": turn["role"], "content": turn["content"], "timestamp": turn["timestamp"]}
                    for turn in history
                ]
            else:
                messages = await state.get_session_history_readonly(session_id)
            
            all_exports.append({
                "session_id": session_id,
                "title": metadata.get("title", "Untitled"),
                "created_at": metadata.get("created_at"),
                "last_active": metadata.get("last_active"),
                "agent_id": metadata.get("agent_id"),
                "messages": messages
            })
        except Exception:
            continue
    
    export_data = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "total_sessions": len(all_exports),
        "sessions": all_exports
    }
    
    return JSONResponse(
        content=export_data,
        headers={
            "Content-Disposition": 'attachment; filename="knoggin_all_sessions.json"'
        }
    )

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
        "title": metadata.get("title"),
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
    
    if body.title is not None:
        metadata["title"] = body.title
    
    metadata["last_active"] = datetime.now(timezone.utc).isoformat()
    
    await state.resources.redis.hset(
        RedisKeys.sessions(state.user_name),
        session_id,
        json.dumps(metadata)
    )

    if session_id in state.active_sessions:
        if body.model is not None:
            state.active_sessions[session_id].model = body.model
    
    return {"success": True, "model": metadata.get("model"), "agent_id": metadata.get("agent_id"), "enabled_tools": metadata.get("enabled_tools"), "title": metadata.get("title")}

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


@router.get("/{session_id}/memory")
async def get_session_memory(
    session_id: str,
    state: AppState = Depends(get_app_state)
):
    """Read agent-saved memory notes for a session."""
    sessions = await state.list_sessions()
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    topic_config = await TopicConfig.load(
        state.resources.redis,
        state.user_name,
        session_id
    )
    
    all_topics = list(topic_config.raw.keys())
    memories = {}
    total = 0
    
    for topic in all_topics:
        memory_key = RedisKeys.agent_memory(state.user_name, session_id, topic)
        raw = await state.resources.redis.hgetall(memory_key)
        
        if not raw:
            continue
        
        entries = []
        for mem_id, payload in raw.items():
            data = json.loads(payload)
            entries.append({
                "id": mem_id,
                "content": data.get("content", ""),
                "created_at": data.get("created_at", ""),
                "source_session": data.get("source_session", "")
            })
        entries.sort(key=lambda x: x["created_at"])
        memories[topic] = entries
        total += len(entries)
    
    return {"memories": memories, "total": total}


@router.get("/{session_id}/export")
async def export_session(
    session_id: str,
    state: AppState = Depends(get_app_state)
):
    """Export a single session's conversation history as JSON."""
    context = await state.get_or_resume_session(session_id)
    if not context:
        raise HTTPException(status_code=404, detail="Session not found")
    
    sessions = await state.list_sessions()
    metadata = sessions.get(session_id, {})
    history = await context.get_conversation_context(num_turns=1000)
    
    export_data = {
        "session_id": session_id,
        "title": metadata.get("title", "Untitled"),
        "created_at": metadata.get("created_at"),
        "last_active": metadata.get("last_active"),
        "agent_id": metadata.get("agent_id"),
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "messages": [
            {
                "role": turn["role"],
                "content": turn["content"],
                "timestamp": turn["timestamp"]
            }
            for turn in history
        ]
    }
    
    return JSONResponse(
        content=export_data,
        headers={
            "Content-Disposition": f'attachment; filename="knoggin_session_{session_id[:8]}.json"'
        }
    )

@router.post("/{session_id}/import")
async def import_session(
    session_id: str,
    body: ImportSessionRequest,
    state: AppState = Depends(get_app_state)
):
    """Import a session's conversation history."""
    context = await state.get_or_resume_session(session_id)
    if not context:
        raise HTTPException(status_code=404, detail="Session not found")
    
    imported = 0
    for msg in body.messages:
        role = msg.get("role")
        content = msg.get("content")
        timestamp_str = msg.get("timestamp")
        
        if not role or not content:
            continue
            
        # Parse timestamp if available
        timestamp = datetime.now(timezone.utc)
        if timestamp_str:
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass
                
        if role == "user":
            from common.schema.dtypes import MessageData
            from datetime import datetime
            m = MessageData(message=content, timestamp=timestamp)
            await context.add(m)
            imported += 1
        elif role == "assistant":
            await context.add_assistant_turn(content=content, timestamp=timestamp)
            imported += 1
            
    return {"status": "success", "imported_messages": imported}
