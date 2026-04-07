from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Any


from core.prompts import (
    ner_reasoning_prompt,
    get_connection_reasoning_prompt,
    get_profile_extraction_prompt,
    get_merge_judgment_prompt,
    get_contradiction_judgment_prompt
)

class IngestionSettings(BaseModel):
    batch_size: int = Field(8, ge=1, le=100)
    batch_timeout: float = Field(300.0, ge=10.0)

class CleanerSettings(BaseModel):
    enabled: bool = Field(True)
    interval_hours: int = Field(24, ge=1)
    orphan_age_hours: int = Field(24, ge=1)
    stale_junk_days: int = Field(30, ge=1)

class ProfileSettings(BaseModel):
    msg_window: int = Field(30, ge=5)
    volume_threshold: int = Field(15, ge=1)
    idle_threshold: int = Field(90, ge=10)
    profile_batch_size: int = Field(8, ge=1)
    max_facts_context: int = Field(50, ge=1)
    contradiction_sim_low: float = Field(0.70, ge=0.0, le=1.0)
    contradiction_sim_high: float = Field(0.95, ge=0.0, le=1.0)
    contradiction_batch_size: int = Field(4, ge=1)

class MergerSettings(BaseModel):
    enabled: bool = Field(True)
    auto_threshold: float = Field(0.93, ge=0.5, le=1.0)
    hitl_threshold: float = Field(0.65, ge=0.4, le=1.0)
    cosine_threshold: float = Field(0.65, ge=0.1, le=1.0)

class DLQSettings(BaseModel):
    interval_seconds: int = Field(60, ge=10)
    batch_size: int = Field(50, ge=1)
    max_attempts: int = Field(2, ge=1)

class ArchivalSettings(BaseModel):
    enabled: bool = Field(True)
    retention_days: int = Field(14, ge=1)
    fallback_interval_hours: float = Field(24.0, ge=0.5)

class TopicConfigSettings(BaseModel):
    enabled: bool = Field(True)
    interval_msgs: int = Field(40, ge=5)
    conversation_window: int = Field(50, ge=5)

class JobSettings(BaseModel):
    cleaner: CleanerSettings = Field(default_factory=CleanerSettings)
    profile: ProfileSettings = Field(default_factory=ProfileSettings)
    merger: MergerSettings = Field(default_factory=MergerSettings)
    dlq: DLQSettings = Field(default_factory=DLQSettings)
    archival: ArchivalSettings = Field(default_factory=ArchivalSettings)
    topic_config: TopicConfigSettings = Field(default_factory=TopicConfigSettings)

class AgentLimitSettings(BaseModel):
    agent_history_turns: int = Field(7, ge=1)
    max_tool_calls: int = Field(12, ge=1)
    max_attempts: int = Field(15, ge=1)
    max_consecutive_errors: int = Field(3, ge=1)
    max_accumulated_messages: int = Field(30, ge=1)
    conversation_context_turns: int = Field(10, ge=1)
    max_conversation_history: int = Field(10000, ge=1)
    tool_limits: Dict[str, int] = Field(default_factory=lambda: {
        "search_messages": 6,
        "get_connections": 8,
        "search_entity": 8,
        "get_activity": 8,
        "find_path": 8,
        "get_hierarchy": 8,
        "fact_check": 6,
        "save_memory": 4,
        "forget_memory": 4,
        "search_files": 3,
        "web_search": 4,
        "news_search": 4
    })

class NLPPipelineSettings(BaseModel):
    gliner_threshold: float = Field(0.85, ge=0.0, le=1.0)
    vp01_min_confidence: float = Field(0.8, ge=0.0, le=1.0)
    llm_ner: bool = Field(True)
    ner_prompt: str = Field(default_factory=lambda: ner_reasoning_prompt("{user_name}"))
    connection_prompt: str = Field(default_factory=lambda: get_connection_reasoning_prompt("{user_name}"))
    profile_prompt: str = Field(default_factory=lambda: get_profile_extraction_prompt("{user_name}"))
    merge_prompt: str = Field(default_factory=lambda: get_merge_judgment_prompt())
    contradiction_prompt: str = Field(default_factory=lambda: get_contradiction_judgment_prompt())

class SearchSettings(BaseModel):
    vector_limit: int = Field(50, ge=1)
    fts_limit: int = Field(50, ge=1)
    rerank_candidates: int = Field(45, ge=1)
    default_message_limit: int = Field(8, ge=1)
    default_entity_limit: int = Field(5, ge=1)
    default_activity_hours: int = Field(24, ge=1)

class EntityResolutionSettings(BaseModel):
    fuzzy_substring_threshold: int = Field(75, ge=50, le=100)
    fuzzy_non_substring_threshold: int = Field(91, ge=50, le=100)
    generic_token_freq: int = Field(10, ge=1)
    candidate_fuzzy_threshold: int = Field(85, ge=50, le=100)
    candidate_vector_threshold: float = Field(0.85, ge=0.0, le=1.0)
    resolution_threshold: float = Field(0.85, ge=0.0, le=1.0)

class LLMSettings(BaseModel):
    api_key: str = Field("")
    base_url: Optional[str] = None
    agent_model: str = Field("google/gemini-3-flash-preview")
    extraction_model: str = Field("google/gemini-2.5-flash-preview")
    merge_model: str = Field("google/gemini-2.5-pro")

class SearchAPIKeySettings(BaseModel):
    provider: str = Field("auto")
    brave_api_key: str = Field("")
    tavily_api_key: str = Field("")

class CommunitySettings(BaseModel):
    enabled: bool = Field(False)
    interval_minutes: int = Field(30, ge=1)
    max_turns: int = Field(10, ge=1)
    seeding_agent_id: Optional[str] = None
    agent_pool_ids: List[str] = Field(default_factory=list)

class DeveloperSettings(BaseModel):
    ingestion: IngestionSettings = Field(default_factory=IngestionSettings)
    jobs: JobSettings = Field(default_factory=JobSettings)
    limits: AgentLimitSettings = Field(default_factory=AgentLimitSettings)
    entity_resolution: EntityResolutionSettings = Field(default_factory=EntityResolutionSettings)
    nlp_pipeline: NLPPipelineSettings = Field(default_factory=NLPPipelineSettings)
    search: SearchSettings = Field(default_factory=SearchSettings)
    community: CommunitySettings = Field(default_factory=CommunitySettings)

class RootConfig(BaseModel):
    _warning: str = Field(
        "This file is auto-generated. Use the UI to modify settings. Manual edits may be overwritten.",
        alias="_warning"
    )
    user_name: str = Field("")
    user_aliases: List[str] = Field(default_factory=list)
    user_facts: List[str] = Field(default_factory=list)
    configured_at: Optional[str] = None
    curated_models: List[dict] = Field(default_factory=lambda: [
        {
            "id": "anthropic/claude-sonnet-4.5",
            "name": "Claude Sonnet 4.5",
            "input_price": 3.00,
            "output_price": 15.00
        },
        {
            "id": "anthropic/claude-opus-4.5",
            "name": "Claude Opus 4.5",
            "input_price": 5.00,
            "output_price": 25.00
        },
        {
            "id": "x-ai/grok-4.1-fast",
            "name": "Grok 4.1 Fast",
            "input_price": 0.20,
            "output_price": 0.50
        },
        {
            "id": "openai/gpt-5.1",
            "name": "GPT-5.1",
            "input_price": 1.25,
            "output_price": 10.00
        },
        {
            "id": "google/gemini-3-pro-preview",
            "name": "Gemini 3 Pro",
            "input_price": 2.00,
            "output_price": 12.00
        },
        {
            "id": "anthropic/claude-haiku-4.5",
            "name": "Claude Haiku 4.5",
            "input_price": 1.00,
            "output_price": 5.00
        },
        {
            "id": "google/gemini-2.5-flash-lite-preview-09-2025",
            "name": "Gemini 2.5 Flash Lite",
            "input_price": 0.10,
            "output_price": 0.40
        },
        {
            "id": "google/gemini-2.5-flash",
            "name": "Gemini 2.5 Flash",
            "input_price": 0.30,
            "output_price": 2.50
        },
        {
            "id": "deepseek/deepseek-v3.1",
            "name": "DeepSeek V3.1",
            "input_price": 0.60,
            "output_price": 1.70
        },
        {
            "id": "openai/gpt-oss-120b:free",
            "name": "GPT-OSS-120B",
            "input_price": 0,
            "output_price": 0
        }
    ])
    llm: LLMSettings = Field(default_factory=LLMSettings)
    search: SearchAPIKeySettings = Field(default_factory=SearchAPIKeySettings)
    mcp: Dict[str, Any] = Field(default_factory=lambda: {
        "servers": {},
        "tool_timeout": 15.0,
        "max_mcp_calls_per_run": 3
    })
    default_topics: Dict[str, Any] = Field(default_factory=lambda: {
        "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
        "Identity": {"active": True, "labels": ["person"], "hierarchy": {}, "aliases": []}
    })
    developer_settings: DeveloperSettings = Field(default_factory=DeveloperSettings)


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
    args: List[Any] = Field(default_factory=list)
    env: Optional[Dict[str, str]] = None
    enabled: bool = True
    allowed_tools: Optional[List[str]] = None
