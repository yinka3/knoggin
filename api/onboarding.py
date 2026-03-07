import asyncio
from datetime import datetime, timezone
from typing import List
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from loguru import logger

from api.deps import get_app_state
from api.state import AppState
from main.setup import run_setup
from shared.config.base import load_config, save_config, get_default_config
from shared.config.topics import TopicConfig, ONBOARDING_QUESTIONS
from shared.services.topics import generate_topics as generate_topics_from_text

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
        merged = await asyncio.wait_for(
            generate_topics_from_text(state.resources.llm_service, text_block),
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

    loop = asyncio.get_running_loop()
    success = await loop.run_in_executor(None, save_config, config)
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
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, save_config, config)

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