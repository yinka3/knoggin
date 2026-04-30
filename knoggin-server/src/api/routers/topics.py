import asyncio
from typing import Optional, List, Dict
from fastapi import APIRouter, Depends, HTTPException
from loguru import logger
from pydantic import BaseModel

from api.deps import get_app_state
from api.state import AppState
from common.config.topics_config import TopicConfig
from services.topic_manager import generate_topics

router = APIRouter()


class CreateTopicRequest(BaseModel):
    name: str
    labels: List[str] = []
    hierarchy: Dict = {}
    aliases: List[str] = []
    active: bool = True
    hot: bool = False


class UpdateTopicRequest(BaseModel):
    labels: Optional[List[str]] = None
    hierarchy: Optional[Dict] = None
    aliases: Optional[List[str]] = None
    active: Optional[bool] = None
    hot: Optional[bool] = None

class GenerateFromDescriptionRequest(BaseModel):
    description: str

async def get_topic_config(session_id: str, state: AppState) -> TopicConfig:
    """Load TopicConfig for a session."""
    sessions = await state.session_manager.list_sessions()
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    topic_config = await TopicConfig.load(
        state.resources.redis,
        state.user_name,
        session_id
    )
    return topic_config

@router.post("/generate")
async def generate_topics_from_description(
    body: GenerateFromDescriptionRequest,
    state: AppState = Depends(get_app_state)
):
    logger.info(f"Topic generation started for user: {state.user_name}")
    if not body.description.strip():
        raise HTTPException(status_code=400, detail="Description is empty")

    limit_key = f"topic_gen_count:{state.user_name}"
    count = await state.resources.redis.get(limit_key)
    count = int(count) if count else 0
    logger.info(f"Topic generation count: {count}")

    if count >= 3:
        raise HTTPException(status_code=429, detail="Generation limit reached (3 attempts). Create the session or start a new configuration.")

    logger.info("Calling LLM for topic generation...")

    try:
        merged = await asyncio.wait_for(
            generate_topics(state.resources.llm_service, body.description.strip()),
            timeout=30.0
        )
        logger.info("Topic generation complete.")
    except asyncio.TimeoutError:
        logger.error("Topic generation timed out after 30s")
        raise HTTPException(status_code=504, detail="Topic generation timed out. Please try again.")
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Topic generation from description failed: {e}")
        raise HTTPException(status_code=500, detail="Topic generation failed due to an internal error.")

    # Increment counter
    await state.resources.redis.incr(limit_key)

    return {"topics": merged, "attempts_remaining": 2 - count}


@router.get("/{session_id}")
async def list_topics(
    session_id: str,
    state: AppState = Depends(get_app_state)
):
    topic_config = await get_topic_config(session_id, state)
    
    return {
        "topics": topic_config.raw,
        "active_topics": topic_config.active_topics,
        "hot_topics": topic_config.hot_topics
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
        "active": body.active,
        "hot": body.hot
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
    if body.active is not None:
        topic_config.toggle_active(topic_name, body.active)
    if body.hot is not None:
        current["hot"] = body.hot
        topic_config._clear_cache()
    
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
    
    if topic_name == "General" and not confirm:
        return {
            "warning": "Deleting General will remove the fallback topic. Entities currently tagged 'General' will keep their data but won't be categorized. Consider disabling instead.",
            "recommendation": f"PATCH /topics/{session_id}/{topic_name} with {{'active': false}}",
            "confirm": "Add ?confirm=true to proceed"
        }
    
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
