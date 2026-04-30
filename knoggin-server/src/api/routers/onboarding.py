import asyncio
from datetime import datetime, timezone
from typing import List
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from loguru import logger

from api.deps import get_app_state
from api.state import AppState
from core.session.onboarding import run_setup
from common.config.base import load_config, async_save_config, get_default_config, get_config
from common.config.topics_config import TopicConfig
from services.topic_manager import generate_topics as generate_topics_from_text

router = APIRouter()


class QAPair(BaseModel):
    question: str
    answer: str

class GenerateTopicsRequest(BaseModel):
    responses: List[QAPair]

class SaveTopicsRequest(BaseModel):
    topics: dict

class ExtractRequest(BaseModel):
    responses: List[QAPair]

ONBOARDING_QUESTIONS = {
    "guided": [
        {
            "id": 1,
            "question": "What are you currently working on? Projects, work, hobbies — anything you'd want to remember and connect."
        },
        {
            "id": 2,
            "question": "Who are the key people in your world right now? Teammates, mentors, collaborators, clients — anyone important."
        },
        {
            "id": 3,
            "question": "What are your main priorities or goals right now?"
        },
    ],
    "structured": [
        {
            "id": 1,
            "question": "What are you currently working on? Projects, work, hobbies — anything you'd want to remember and connect."
        },
        {
            "id": 2,
            "question": "Who are the key people in your world right now? Teammates, mentors, collaborators, clients — anyone important."
        },
        {
            "id": 3,
            "question": "What tools, platforms, or technologies do you use regularly?"
        },
        {
            "id": 4,
            "question": "What are your main priorities or goals right now?"
        },
        {
            "id": 5,
            "question": "Are there any specific domains you'd like to track? For example: investments, research, clients, coursework, health."
        },
        {
            "id": 6,
            "question": "Anything else important that doesn't fit the above? Relationships, ongoing decisions, things you don't want to forget."
        },
    ]
}


@router.post("/generate-topics")
async def generate_topics(
    body: GenerateTopicsRequest,
    state: AppState = Depends(get_app_state)
):
    if not body.responses:
        raise HTTPException(status_code=400, detail="No responses provided")

    text_block = "\n\n".join(
        f"Q: {r.question}\nA: {r.answer}"
        for r in body.responses
        if r.answer.strip()
    )

    if not text_block:
        raise HTTPException(status_code=400, detail="All answers are empty")

    try:
        config = get_config()
        user_name = config.user_name or "User"
        merged = await asyncio.wait_for(
            generate_topics_from_text(state.resources.llm_service, text_block, user_name),
            timeout=30.0
        )
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Topic generation timed out. Please try again.")
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"topics": merged}


@router.post("/save")
async def save_topics(
    body: SaveTopicsRequest,
    state: AppState = Depends(get_app_state)
):
    if not body.topics:
        raise HTTPException(status_code=400, detail="No topics provided")

    config = load_config() or get_default_config()
    config["default_topics"] = body.topics

    success = await async_save_config(config)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to save config")

    for session_id, context in state.active_sessions.items():
        await context.update_topics_config(body.topics)

    return {"success": True, "topic_count": len(body.topics)}

@router.post("/extract")
async def extract(
    body: ExtractRequest,
    state: AppState = Depends(get_app_state)
):
    if not body.responses:
        raise HTTPException(status_code=400, detail="No responses provided")

    config = load_config() or get_default_config()
    if config.get("configured_at"):
        raise HTTPException(status_code=400, detail="Onboarding already completed.")
    
    topics_raw = config.get("default_topics")
    if not topics_raw:
        raise HTTPException(
            status_code=400,
            detail="No topic config found. Call /onboarding/save first."
        )

    topic_config = TopicConfig(topics_raw)

    user_name = config.get("user_name", "")
    if not user_name:
        raise HTTPException(
            status_code=400,
            detail="User name not configured. Set it in settings first."
        )

    try:
        result = await run_setup(
            resources=state.resources,
            topic_config=topic_config,
            user_name=user_name,
            responses=[r.model_dump() for r in body.responses]
        )
    except Exception as e:
        logger.error(f"Onboarding extraction failed: {e}")
        raise HTTPException(status_code=500, detail=f"Extraction failed: {str(e)}")

    config["configured_at"] = datetime.now(timezone.utc).isoformat()
    await async_save_config(config)

    return result

@router.get("/questions/{path}")
async def get_questions(path: str):
    if path not in ONBOARDING_QUESTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid path: '{path}'. Use 'guided' or 'structured'."
        )

    return {
        "path": path,
        "questions": ONBOARDING_QUESTIONS[path]
    }

@router.get("/status")
async def onboarding_status(state: AppState = Depends(get_app_state)):
    config = load_config() or get_default_config()

    return {
        "completed": config.get("configured_at") is not None,
        "configured_at": config.get("configured_at"),
        "user_name": config.get("user_name", ""),
        "has_topics": bool(config.get("default_topics"))
    }




