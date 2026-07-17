import os
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from common.exceptions import ConfigurationError


class RedisConnectionSettings(BaseModel):
    """Startup-only Redis connection and pool settings."""

    model_config = ConfigDict(frozen=True)

    url: str = "redis://localhost:6379/0"
    max_connections: int = Field(10, ge=1)
    health_check_interval: int = Field(30, ge=0)
    connect_timeout: float = Field(2.0, gt=0)
    startup_attempts: int = Field(3, ge=1)
    startup_backoff_seconds: float = Field(0.25, ge=0)

    @classmethod
    def from_env(cls) -> "RedisConnectionSettings":
        values = {
            "url": os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            "max_connections": os.getenv("REDIS_MAX_CONNECTIONS", "10"),
            "health_check_interval": os.getenv(
                "REDIS_HEALTH_CHECK_INTERVAL",
                "30",
            ),
            "connect_timeout": os.getenv("REDIS_CONNECT_TIMEOUT", "2.0"),
            "startup_attempts": os.getenv("REDIS_STARTUP_ATTEMPTS", "3"),
            "startup_backoff_seconds": os.getenv(
                "REDIS_STARTUP_BACKOFF_SECONDS",
                "0.25",
            ),
        }
        try:
            return cls.model_validate(values)
        except ValidationError as exc:
            errors = [
                {
                    "field": ".".join(str(part) for part in error["loc"]),
                    "message": error["msg"],
                }
                for error in exc.errors(include_url=False)
            ]
            raise ConfigurationError(
                "Invalid Redis connection settings",
                details={"errors": errors},
            ) from exc


class TopicSchema(BaseModel):
    active: bool = Field(True)
    hot: bool = Field(False)
    labels: List[str] = Field(default_factory=list)
    hierarchy: Dict[str, Any] = Field(default_factory=dict)
    aliases: List[str] = Field(default_factory=list)


DEFAULT_SPARSE_CONTEXT_VERBS = [
    "accepted",
    "acknowledged",
    "added",
    "agreed",
    "answered",
    "approved",
    "asked",
    "called",
    "checked",
    "confirmed",
    "did",
    "emailed",
    "forwarded",
    "liked",
    "mentioned",
    "messaged",
    "noted",
    "okayed",
    "pinged",
    "reacted",
    "replied",
    "responded",
    "said",
    "sent",
    "shared",
    "signed",
    "submitted",
    "texted",
    "told",
    "updated",
    "wrote",
    "yes",
]




class IngestionSettings(BaseModel):
    batch_size: int = Field(8, ge=1, le=100)
    batch_debounce_seconds: float = Field(0.75, ge=0.0, le=10.0)
    batch_timeout: float = Field(300.0, ge=10.0)
    checkpoint_interval: int = Field(32, ge=1)
    session_window: int = Field(24, ge=1)


class DocumentIndexingSettings(BaseModel):
    recovery_interval_seconds: int = Field(60, ge=10)
    recovery_batch_size: int = Field(16, ge=1, le=100)


class CleanerSettings(BaseModel):
    enabled: bool = Field(True)
    interval_hours: int = Field(24, ge=1)
    orphan_age_hours: int = Field(24, ge=1)
    stale_junk_days: int = Field(30, ge=1)


class EpisodeSettings(BaseModel):
    """Configuration for bounded episodic-memory generation windows."""

    enabled: bool = Field(True)
    batch_multiple: int = Field(3, ge=1)
    max_message_count: int = Field(72, ge=1)
    max_age_hours: Optional[float] = Field(None, gt=0)
    max_sessions_per_run: int = Field(4, ge=1, le=100)
    prior_episode_candidate_count: int = Field(3, ge=1, le=3)
    retrieval_episode_limit: int = Field(5, ge=1)
    retrieval_source_message_limit: int = Field(5, ge=1)


class DLQSettings(BaseModel):
    interval_seconds: int = Field(60, ge=10)
    batch_size: int = Field(50, ge=1)
    max_attempts: int = Field(2, ge=1)


class MergeRollbackSettings(BaseModel):
    enabled: bool = Field(True)
    retention_hours: float = Field(5.0, ge=0.5)
    fallback_interval_hours: float = Field(1.0, ge=0.25)


class JobSettings(BaseModel):
    cleaner: CleanerSettings = Field(default_factory=CleanerSettings)
    episode: EpisodeSettings = Field(default_factory=EpisodeSettings)
    dlq: DLQSettings = Field(default_factory=DLQSettings)
    merge_rollback: MergeRollbackSettings = Field(
        default_factory=MergeRollbackSettings
    )
    document_indexing: DocumentIndexingSettings = Field(
        default_factory=DocumentIndexingSettings
    )


class TopicEvaluationSettings(BaseModel):
    enabled: bool = Field(True)
    interval_msgs: int = Field(40, ge=1)


class AgentLimitSettings(BaseModel):
    agent_history_turns: int = Field(7, ge=1)
    max_tool_calls: int = Field(12, ge=1)
    max_attempts: int = Field(15, ge=1)
    max_consecutive_errors: int = Field(3, ge=1)
    max_accumulated_messages: int = Field(30, ge=1)
    conversation_context_turns: int = Field(10, ge=1)
    max_conversation_history: int = Field(10000, ge=1)
    tool_limits: Dict[str, int] = Field(
        default_factory=lambda: {
            "search_messages": 6,
            "get_connections": 8,
            "search_entity": 8,
            "get_recent_activity": 8,
            "find_path": 8,
            "get_hierarchy": 8,
            "episode_check": 6,
            "read_episode": 4,
            "read_recent_episodes": 4,
            "read_brain": 4,
            "list_brain_snapshots": 4,
            "read_brain_snapshot": 4,
            "edit_brain": 2,
            "restore_brain_section": 2,
            "list_documents": 4,
            "search_documents": 3,
            "list_folder_uploads": 3,
            "get_folder_upload_summary": 4,
            "list_folder_tree": 4,
            "get_document_info": 4,
            "read_document": 4,
            "web_search": 4,
            "news_search": 4,
            "update_topics": 1,
        }
    )


class TextProcessorSettings(BaseModel):
    gliner_threshold: float = Field(0.85, ge=0.0, le=1.0)
    vp01_min_confidence: float = Field(0.8, ge=0.0, le=1.0)
    llm_ner: bool = Field(False)


class SearchSettings(BaseModel):
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
    common_word_frequency_threshold: float = Field(1e-5, ge=0.0)
    sparse_context_verbs: List[str] = Field(
        default_factory=lambda: list(DEFAULT_SPARSE_CONTEXT_VERBS)
    )


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
    project_ids: List[str] = Field(default_factory=list)


class CoordinationLogSettings(BaseModel):
    enabled: bool = Field(True)
    path: str = Field("logs/coordination.log", min_length=1)
    retention_days: int = Field(14, ge=1)
    rotation_mb: int = Field(10, ge=1)


class LocalReferenceSettings(BaseModel):
    """Temporary rollout control for model-facing local identifier maps."""

    enabled: bool = Field(True)


class DeveloperSettings(BaseModel):
    ingestion: IngestionSettings = Field(default_factory=IngestionSettings)
    jobs: JobSettings = Field(default_factory=JobSettings)
    topic_evaluation: TopicEvaluationSettings = Field(
        default_factory=TopicEvaluationSettings
    )
    limits: AgentLimitSettings = Field(default_factory=AgentLimitSettings)
    entity_resolution: EntityResolutionSettings = Field(
        default_factory=EntityResolutionSettings
    )
    nlp_pipeline: TextProcessorSettings = Field(default_factory=TextProcessorSettings)
    search: SearchSettings = Field(default_factory=SearchSettings)
    community: CommunitySettings = Field(default_factory=CommunitySettings)
    coordination_log: CoordinationLogSettings = Field(
        default_factory=CoordinationLogSettings
    )
    local_references: LocalReferenceSettings = Field(
        default_factory=LocalReferenceSettings
    )


class RootConfig(BaseModel):
    user_name: str = Field("")
    user_aliases: List[str] = Field(default_factory=list)
    configured_at: Optional[str] = None
    curated_models: List[dict] = Field(
        default_factory=lambda: [
            {
                "id": "anthropic/claude-sonnet-4.5",
                "name": "Claude Sonnet 4.5",
                "input_price": 3.00,
                "output_price": 15.00,
            },
            {
                "id": "anthropic/claude-opus-4.5",
                "name": "Claude Opus 4.5",
                "input_price": 5.00,
                "output_price": 25.00,
            },
            {
                "id": "x-ai/grok-4.1-fast",
                "name": "Grok 4.1 Fast",
                "input_price": 0.20,
                "output_price": 0.50,
            },
            {
                "id": "openai/gpt-5.1",
                "name": "GPT-5.1",
                "input_price": 1.25,
                "output_price": 10.00,
            },
            {
                "id": "google/gemini-3-pro-preview",
                "name": "Gemini 3 Pro",
                "input_price": 2.00,
                "output_price": 12.00,
            },
            {
                "id": "anthropic/claude-haiku-4.5",
                "name": "Claude Haiku 4.5",
                "input_price": 1.00,
                "output_price": 5.00,
            },
            {
                "id": "google/gemini-2.5-flash-lite-preview-09-2025",
                "name": "Gemini 2.5 Flash Lite",
                "input_price": 0.10,
                "output_price": 0.40,
            },
            {
                "id": "google/gemini-2.5-flash",
                "name": "Gemini 2.5 Flash",
                "input_price": 0.30,
                "output_price": 2.50,
            },
            {
                "id": "deepseek/deepseek-v3.1",
                "name": "DeepSeek V3.1",
                "input_price": 0.60,
                "output_price": 1.70,
            },
            {
                "id": "openai/gpt-oss-120b:free",
                "name": "GPT-OSS-120B",
                "input_price": 0,
                "output_price": 0,
            },
        ]
    )
    llm: LLMSettings = Field(default_factory=LLMSettings)
    search: SearchAPIKeySettings = Field(default_factory=SearchAPIKeySettings)
    developer_settings: DeveloperSettings = Field(default_factory=DeveloperSettings)
