from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from api.state import AppState
from shared.config import get_default_config, load_config, save_config

router = APIRouter()

def get_app_state(request: Request) -> AppState:
    return request.app.state.app_state


class IngestionSettings(BaseModel):
    batch_size: Optional[int] = Field(None, ge=1, le=100)
    batch_timeout: Optional[float] = Field(None, ge=10.0)
    checkpoint_interval: Optional[int] = Field(None, ge=1)
    session_window: Optional[int] = Field(None, ge=1)

class CleanerSettings(BaseModel):
    interval_hours: Optional[int] = Field(None, ge=1)
    orphan_age_hours: Optional[int] = Field(None, ge=1)
    stale_junk_days: Optional[int] = Field(None, ge=1)

class ProfileSettings(BaseModel):
    msg_window: Optional[int] = Field(None, ge=5)
    volume_threshold: Optional[int] = Field(None, ge=1)
    idle_threshold: Optional[int] = Field(None, ge=10)
    profile_batch_size: Optional[int] = Field(None, ge=1)
    contradiction_sim_low: Optional[float] = Field(None, ge=0.0, le=1.0)
    contradiction_sim_high: Optional[float] = Field(None, ge=0.0, le=1.0)
    contradiction_batch_size: Optional[int] = Field(None, ge=1)

class MergerSettings(BaseModel):
    auto_threshold: Optional[float] = Field(None, ge=0.5, le=1.0)
    hitl_threshold: Optional[float] = Field(None, ge=0.4, le=1.0)
    cosine_threshold: Optional[float] = Field(None, ge=0.1, le=1.0)

class DLQSettings(BaseModel):
    interval_seconds: Optional[int] = Field(None, ge=10)
    batch_size: Optional[int] = Field(None, ge=1)
    max_attempts: Optional[int] = Field(None, ge=1)

class ArchivalSettings(BaseModel):
    retention_days: Optional[int] = Field(None, ge=1)

class JobSettings(BaseModel):
    cleaner: Optional[CleanerSettings] = None
    profile: Optional[ProfileSettings] = None
    merger: Optional[MergerSettings] = None
    dlq: Optional[DLQSettings] = None
    archival: Optional[ArchivalSettings] = None

class AgentLimitSettings(BaseModel):
    agent_history_turns: Optional[int] = Field(None, ge=1)
    max_tool_calls: Optional[int] = Field(None, ge=1)
    max_attempts: Optional[int] = Field(None, ge=1)
    max_consecutive_errors: Optional[int] = Field(None, ge=1)
    max_accumulated_messages: Optional[int] = Field(None, ge=1)
    conversation_context_turns: Optional[int] = Field(None, ge=1)
    tool_limits: Optional[Dict[str, int]] = None

class NLPPipelineSettings(BaseModel):
    gliner_threshold: Optional[float] = Field(None, ge=0.0, le=1.0)
    vp01_min_confidence: Optional[float] = Field(None, ge=0.0, le=1.0)

class SearchSettings(BaseModel):
    vector_limit: Optional[int] = Field(None, ge=1)
    fts_limit: Optional[int] = Field(None, ge=1)
    rerank_candidates: Optional[int] = Field(None, ge=1)
    default_message_limit: Optional[int] = Field(None, ge=1)
    default_entity_limit: Optional[int] = Field(None, ge=1)
    default_activity_hours: Optional[int] = Field(None, ge=1)

class MemorySettings(BaseModel):
    recall_strictness: Optional[float] = Field(None, ge=0.0, le=1.0)
    fuzzy_match_threshold: Optional[int] = Field(None, ge=50, le=100)

class EntityResolutionSettings(BaseModel):
    fuzzy_substring_threshold: Optional[int] = Field(None, ge=50, le=100)
    fuzzy_non_substring_threshold: Optional[int] = Field(None, ge=50, le=100)
    generic_token_freq: Optional[int] = Field(None, ge=1)
    candidate_fuzzy_threshold: Optional[int] = Field(None, ge=50, le=100)
    candidate_vector_threshold: Optional[float] = Field(None, ge=0.0, le=1.0)

class DeveloperSettings(BaseModel):
    ingestion: Optional[IngestionSettings] = None
    jobs: Optional[JobSettings] = None
    limits: Optional[AgentLimitSettings] = None
    memory: Optional[MemorySettings] = None
    entity_resolution: Optional[EntityResolutionSettings] = None
    nlp_pipeline: Optional[NLPPipelineSettings] = None
    search: Optional[SearchSettings] = None

class ConfigUpdate(BaseModel):
    user_name: Optional[str] = None
    user_summary: Optional[str] = None
    reasoning_model: Optional[str] = None
    agent_model: Optional[str] = None
    default_topics: Optional[dict] = None
    developer_settings: Optional[DeveloperSettings] = None



def deep_merge(source: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively merge update dict into source dict.
    This ensures we don't wipe out 'jobs' when updating 'ingestion'.
    """
    for key, value in updates.items():
        if isinstance(value, dict) and key in source and isinstance(source[key], dict):
            deep_merge(source[key], value)
        else:
            source[key] = value
    return source


@router.get("/")
async def get_config():
    config = load_config()
    if not config:
        return get_default_config()
    return config

@router.get("/status")
async def get_config_status():
    config = load_config()
    
    has_api_key = bool(config and config.get("llm", {}).get("api_key"))
    has_user_name = bool(config and config.get("user_name"))
    
    return {
        "configured": has_api_key and has_user_name,
        "has_api_key": has_api_key,
        "has_user_name": has_user_name
    }

@router.patch("/")
async def update_config(
    body: ConfigUpdate,
    state: AppState = Depends(get_app_state)
):
    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")


    current_config = load_config() or get_default_config()

    merged_config = deep_merge(current_config, updates)

    success = save_config(merged_config)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to save config")
    
    active_count = 0
    for _, context in state.active_sessions.items():
        await context.update_runtime_settings(merged_config)
        active_count += 1
    
    return merged_config