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


@router.get("/")
async def list_sessions(state: AppState = Depends(get_app_state)):
    sessions = await state.list_sessions()

    result = []
    for session_id, metadata in sessions.items():
        result.append({
            "session_id": session_id,
            "created_at": metadata.get("created_at"),
            "last_active": metadata.get("last_active"),
            "is_active": session_id in state.active_sessions
        })
    
    return {"sessions": result}

@router.post("/")
async def create_session(
    body: CreateSessionRequest = None,
    state: AppState = Depends(get_app_state)
):
    topics_config = body.topics_config if body else None
    
    context = await state.create_session(topics_config)
    
    raw = await state.resources.redis.hget(f"sessions:{state.user_name}", context.session_id)
    metadata = json.loads(raw)
    return {
        "session_id": context.session_id,
        "created_at": metadata["created_at"]
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
        "is_active": session_id in state.active_sessions
    }


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
    
    return {"success": True}
