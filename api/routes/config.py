from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from typing import Optional
from api.state import AppState
from shared.config import get_default_config, load_config, save_config

router = APIRouter()

def get_app_state(request: Request) -> AppState:
    return request.app.state.app_state

class ConfigUpdate(BaseModel):
    user_name: Optional[str] = None
    user_summary: Optional[str] = None
    reasoning_model: Optional[str] = None
    agent_model: Optional[str] = None
    default_topics: Optional[dict] = None
    agent_name: Optional[str] = None
    system_prompt: Optional[str] = None
    openrouter_api_key: Optional[str] = None
    direct_provider: Optional[str] = None
    direct_api_key: Optional[str] = None

@router.get("/")
async def get_config():
    config = load_config()
    if not config:
        return get_default_config()  # Return defaults instead of 404
    return config

@router.patch("/")
async def update_config(
    body: ConfigUpdate,
    state: AppState = Depends(get_app_state)
):
    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
    
    success = save_config(updates)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to save config")
    
    # Propagate model changes to active sessions
    if body.reasoning_model or body.agent_model:
        for context in state.active_sessions.values():
            context.update_models(
                reasoning_model=body.reasoning_model,
                agent_model=body.agent_model
            )
    
    return load_config()