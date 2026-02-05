from typing import Optional, List, Dict
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from api.state import AppState
from shared.topics_config import TopicConfig

router = APIRouter()

def get_app_state(request: Request) -> AppState:
    return request.app.state.app_state


class CreateTopicRequest(BaseModel):
    name: str
    labels: List[str] = []
    hierarchy: Dict = {}
    aliases: List[str] = []
    label_aliases: Dict[str, str] = {}
    active: bool = True


class UpdateTopicRequest(BaseModel):
    labels: Optional[List[str]] = None
    hierarchy: Optional[Dict] = None
    aliases: Optional[List[str]] = None
    label_aliases: Optional[Dict[str, str]] = None
    active: Optional[bool] = None


async def get_topic_config(session_id: str, state: AppState) -> TopicConfig:
    """Load TopicConfig for a session."""
    sessions = await state.list_sessions()
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    topic_config = await TopicConfig.load(
        state.resources.redis,
        state.user_name,
        session_id
    )
    return topic_config

@router.get("/{session_id}")
async def list_topics(
    session_id: str,
    state: AppState = Depends(get_app_state)
):
    topic_config = await get_topic_config(session_id, state)
    
    return {
        "topics": topic_config.raw,
        "active_topics": topic_config.active_topics
    }


@router.post("/{session_id}")
async def create_topic(
    session_id: str,
    body: CreateTopicRequest,
    state: AppState = Depends(get_app_state)
):
    topic_config = await get_topic_config(session_id, state)
    
    if body.name in topic_config.raw:
        raise HTTPException(status_code=400, detail="Topic already exists")
    
    topic_config.add_topic(body.name, {
        "labels": body.labels,
        "hierarchy": body.hierarchy,
        "aliases": body.aliases,
        "label_aliases": body.label_aliases,
        "active": body.active
    })
    
    await topic_config.save(state.resources.redis, state.user_name, session_id)
    
    # Update active session if loaded
    if session_id in state.active_sessions:
        await state.active_sessions[session_id].update_topics_config(topic_config.raw)
    
    return {
        "success": True,
        "topic": body.name
    }


@router.get("/{session_id}/{topic_name}")
async def get_topic(
    session_id: str,
    topic_name: str,
    state: AppState = Depends(get_app_state)
):
    topic_config = await get_topic_config(session_id, state)
    
    if topic_name not in topic_config.raw:
        raise HTTPException(status_code=404, detail="Topic not found")
    
    return {
        "name": topic_name,
        "config": topic_config.raw[topic_name],
        "is_active": topic_config.is_active(topic_name)
    }


@router.patch("/{session_id}/{topic_name}")
async def update_topic(
    session_id: str,
    topic_name: str,
    body: UpdateTopicRequest,
    state: AppState = Depends(get_app_state)
):
    topic_config = await get_topic_config(session_id, state)
    
    if topic_name not in topic_config.raw:
        raise HTTPException(status_code=404, detail="Topic not found")
    
    current = topic_config.raw[topic_name]
    
    if body.labels is not None:
        current["labels"] = body.labels
    if body.hierarchy is not None:
        current["hierarchy"] = body.hierarchy
    if body.aliases is not None:
        current["aliases"] = body.aliases
    if body.label_aliases is not None:
        current["label_aliases"] = body.label_aliases
    if body.active is not None:
        topic_config.toggle_active(topic_name, body.active)
    
    await topic_config.save(state.resources.redis, state.user_name, session_id)
    
    # Update active session if loaded
    if session_id in state.active_sessions:
        await state.active_sessions[session_id].update_topics_config(topic_config.raw)
    
    return {"success": True}


@router.delete("/{session_id}/{topic_name}")
async def delete_topic(
    session_id: str,
    topic_name: str,
    confirm: bool = False,
    state: AppState = Depends(get_app_state)
):
    topic_config = await get_topic_config(session_id, state)
    
    if topic_name not in topic_config.raw:
        raise HTTPException(status_code=404, detail="Topic not found")
    
    if topic_name == "General":
        raise HTTPException(status_code=400, detail="Cannot delete General topic")
    
    if not confirm:
        return {
            "warning": "Deleting a topic is not recommended. Consider setting active=false instead via PATCH. To proceed, add ?confirm=true",
            "recommendation": f"PATCH /topics/{session_id}/{topic_name} with {{'active': false}}"
        }
    
    del topic_config.raw[topic_name]
    topic_config._clear_cache()
    
    await topic_config.save(state.resources.redis, state.user_name, session_id)
    
    # Update active session if loaded
    if session_id in state.active_sessions:
        await state.active_sessions[session_id].update_topics_config(topic_config.raw)
    
    return {"success": True, "deleted": topic_name}