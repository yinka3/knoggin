from datetime import datetime, timezone
import json
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from api.state import AppState

router = APIRouter()

def get_app_state(request: Request) -> AppState:
    return request.app.state.app_state


class CreateSessionRequest(BaseModel):
    topics_config: Optional[dict] = None
    model: Optional[str] = None

class UpdateSessionRequest(BaseModel):
    model: Optional[str] = None

@router.get("/")
async def list_sessions(
    limit: int = 20,
    offset: int = 0,
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
    body: CreateSessionRequest = None,
    state: AppState = Depends(get_app_state)
):

    context = await state.create_session(
        body.topics_config if body else None,
        model=body.model if body else None
    )
    
    raw = await state.resources.redis.hget(f"sessions:{state.user_name}", context.session_id)
    metadata = json.loads(raw)
    
    return {
        "session_id": context.session_id,
        "created_at": metadata["created_at"],
        "model": metadata.get("model")
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
    
    metadata["last_active"] = datetime.now(timezone.utc).isoformat()
    
    await state.resources.redis.hset(
        f"sessions:{state.user_name}",
        session_id,
        json.dumps(metadata)
    )
    
    if session_id in state.active_sessions:
        state.active_sessions[session_id].model = body.model
    
    return {"success": True, "model": body.model}

@router.delete("/{session_id}")
async def delete_session(
    session_id: str,
    state: AppState = Depends(get_app_state)
):
    sessions = await state.list_sessions()
    
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    if session_id in state.active_sessions:
        await state.close_session(session_id)
    
    await state.resources.redis.hdel(f"sessions:{state.user_name}", session_id)
    return {"success": True}
