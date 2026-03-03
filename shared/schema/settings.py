from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Any


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
    fallback_interval_hours: Optional[float] = Field(None, ge=0.5)

class TopicConfigSettings(BaseModel):
    interval_msgs: Optional[int] = Field(None, ge=5)
    conversation_window: Optional[int] = Field(None, ge=5)

class JobSettings(BaseModel):
    cleaner: Optional[CleanerSettings] = None
    profile: Optional[ProfileSettings] = None
    merger: Optional[MergerSettings] = None
    dlq: Optional[DLQSettings] = None
    archival: Optional[ArchivalSettings] = None
    topic_config: Optional[TopicConfigSettings] = None

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
    ner_prompt: Optional[str] = None
    connection_prompt: Optional[str] = None
    profile_prompt: Optional[str] = None
    merge_prompt: Optional[str] = None
    contradiction_prompt: Optional[str] = None

class SearchSettings(BaseModel):
    vector_limit: Optional[int] = Field(None, ge=1)
    fts_limit: Optional[int] = Field(None, ge=1)
    rerank_candidates: Optional[int] = Field(None, ge=1)
    default_message_limit: Optional[int] = Field(None, ge=1)
    default_entity_limit: Optional[int] = Field(None, ge=1)
    default_activity_hours: Optional[int] = Field(None, ge=1)

class EntityResolutionSettings(BaseModel):
    fuzzy_substring_threshold: Optional[int] = Field(None, ge=50, le=100)
    fuzzy_non_substring_threshold: Optional[int] = Field(None, ge=50, le=100)
    generic_token_freq: Optional[int] = Field(None, ge=1)
    candidate_fuzzy_threshold: Optional[int] = Field(None, ge=50, le=100)
    candidate_vector_threshold: Optional[float] = Field(None, ge=0.0, le=1.0)
    resolution_threshold: Optional[float] = Field(None, ge=0.0, le=1.0)

class LLMSettings(BaseModel):
    api_key: Optional[str] = None
    agent_model: Optional[str] = None

class SearchAPIKeySettings(BaseModel):
    provider: Optional[str] = None
    brave_api_key: Optional[str] = None
    tavily_api_key: Optional[str] = None

class CommunitySettings(BaseModel):
    enabled: bool = Field(False)
    interval_minutes: int = Field(30, ge=1)
    max_turns: int = Field(10, ge=1)
    seeding_agent_id: Optional[str] = None
    agent_pool_ids: List[str] = Field(default_factory=list)

class DeveloperSettings(BaseModel):
    ingestion: Optional[IngestionSettings] = None
    jobs: Optional[JobSettings] = None
    limits: Optional[AgentLimitSettings] = None
    entity_resolution: Optional[EntityResolutionSettings] = None
    nlp_pipeline: Optional[NLPPipelineSettings] = None
    search: Optional[SearchSettings] = None
    community: Optional[CommunitySettings] = None

class DeveloperModePreset(BaseModel):
    id: str
    name: str
    description: str
    settings: DeveloperSettings

class ConfigUpdate(BaseModel):
    user_name: Optional[str] = None
    user_aliases: Optional[List[str]] = None
    user_facts: Optional[List[str]] = None
    llm: Optional[LLMSettings] = None
    search: Optional[SearchAPIKeySettings] = None
    default_topics: Optional[dict] = None
    developer_settings: Optional[DeveloperSettings] = None
    community: Optional[CommunitySettings] = None
    curated_models: Optional[List[dict]] = None

    @field_validator("user_name")
    @classmethod
    def validate_user_name(cls, v):
        if v is not None and not v.strip():
            raise ValueError("user_name cannot be empty or whitespace")
        return v.strip() if v else v


class MCPServerCreate(BaseModel):
    name: str
    command: str = "uvx"
    args: list = []
    env: dict = None
    enabled: bool = True
    allowed_tools: list = None
