from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from shared.config import get_default_config, load_config, save_config

router = APIRouter()

class ConfigUpdate(BaseModel):
    user_name: Optional[str] = None
    user_summary: Optional[str] = None
    reasoning_model: Optional[str] = None
    agent_model: Optional[str] = None
    default_topics: Optional[dict] = None

@router.get("/")
async def get_config():
    config = load_config()
    if not config:
        return get_default_config()  # Return defaults instead of 404
    return config

@router.patch("/")
async def update_config(body: ConfigUpdate):
    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
    
    success = save_config(updates)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to save config")
    
    return load_config()