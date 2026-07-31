import os
from typing import List, Optional

from pydantic import ConfigDict, Field, ValidationError, field_validator

from common.exceptions import ConfigurationError
from common.schema.agent.settings import AgentLimitSettings
from common.schema.config import ConfigModel


class RedisConnectionSettings(ConfigModel):
    """Startup-only Redis connection and pool settings."""

    model_config = ConfigDict(frozen=True, extra="forbid")

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


class TopicSchema(ConfigModel):
    active: bool = Field(True)
    hot: bool = Field(False)
    labels: List[str] = Field(default_factory=list)
    aliases: List[str] = Field(default_factory=list)

    @field_validator("labels", "aliases")
    @classmethod
    def _normalize_unique_topic_terms(cls, values: List[str]) -> List[str]:
        normalized = []
        seen = set()
        for raw_value in values:
            if not isinstance(raw_value, str):
                raise ValueError("topic labels and aliases must be strings")
            value = " ".join(raw_value.split()).casefold()
            if not value:
                raise ValueError("topic labels and aliases must not be blank")
            if value in seen:
                raise ValueError("topic labels and aliases must not contain duplicates")
            seen.add(value)
            normalized.append(value)
        return normalized


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


class IngestionSettings(ConfigModel):
    batch_size: int = Field(8, ge=1, le=100)
    batch_debounce_seconds: float = Field(0.75, ge=0.0, le=10.0)
    batch_timeout: float = Field(300.0, ge=10.0)
    checkpoint_interval: int = Field(32, ge=1)
    session_window: int = Field(24, ge=1)


class DocumentIndexingSettings(ConfigModel):
    recovery_interval_seconds: int = Field(60, ge=10)
    recovery_batch_size: int = Field(16, ge=1, le=100)


class CleanerSettings(ConfigModel):
    enabled: bool = Field(True)
    interval_hours: int = Field(24, ge=1)
    orphan_age_hours: int = Field(24, ge=1)
    stale_junk_days: int = Field(30, ge=1)


class EpisodeSettings(ConfigModel):
    """Configuration for bounded episodic-memory generation windows."""

    enabled: bool = Field(True)
    batch_multiple: int = Field(3, ge=1)
    max_message_count: int = Field(72, ge=1)
    max_age_hours: Optional[float] = Field(None, gt=0)
    max_sessions_per_run: int = Field(4, ge=1, le=100)
    prior_episode_candidate_count: int = Field(3, ge=1, le=3)
    retrieval_episode_limit: int = Field(5, ge=1)


class DLQSettings(ConfigModel):
    interval_seconds: int = Field(60, ge=10)
    batch_size: int = Field(50, ge=1)
    max_attempts: int = Field(2, ge=1)
    completed_state_retention_hours: float = Field(24.0, ge=0.25)


class MergeRollbackSettings(ConfigModel):
    enabled: bool = Field(True)
    retention_hours: float = Field(5.0, ge=0.5)
    fallback_interval_hours: float = Field(1.0, ge=0.25)


class AuditRetentionSettings(ConfigModel):
    """Retention windows for completed, non-canonical operational records."""

    enabled: bool = Field(True)
    interval_hours: float = Field(24.0, ge=0.25)
    candidate_suggestion_days: int = Field(30, ge=1)
    tool_audit_days: int = Field(180, ge=1)
    merge_history_days: int = Field(180, ge=1)


class JobSettings(ConfigModel):
    cleaner: CleanerSettings = Field(default_factory=CleanerSettings)
    episode: EpisodeSettings = Field(default_factory=EpisodeSettings)
    dlq: DLQSettings = Field(default_factory=DLQSettings)
    merge_rollback: MergeRollbackSettings = Field(default_factory=MergeRollbackSettings)
    audit_retention: AuditRetentionSettings = Field(
        default_factory=AuditRetentionSettings
    )
    document_indexing: DocumentIndexingSettings = Field(
        default_factory=DocumentIndexingSettings
    )


class TopicEvaluationSettings(ConfigModel):
    enabled: bool = Field(True)
    interval_msgs: int = Field(40, ge=1)


class TextProcessorSettings(ConfigModel):
    gliner_threshold: float = Field(0.85, ge=0.0, le=1.0)
    vp01_min_confidence: float = Field(0.8, ge=0.0, le=1.0)
    llm_ner: bool = Field(False)


class SearchSettings(ConfigModel):
    fts_limit: int = Field(50, ge=1)
    rerank_candidates: int = Field(25, ge=1)
    default_message_limit: int = Field(8, ge=1)
    default_entity_limit: int = Field(5, ge=1)
    default_activity_hours: int = Field(24, ge=1)


class EntityResolutionSettings(ConfigModel):
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


class LLMSettings(ConfigModel):
    api_key: str = Field("")
    base_url: Optional[str] = None
    agent_model: str = Field("google/gemini-3-flash-preview")
    extraction_model: str = Field("google/gemini-2.5-flash-preview")
    merge_model: str = Field("google/gemini-2.5-pro")


class SearchAPIKeySettings(ConfigModel):
    provider: str = Field("auto")
    brave_api_key: str = Field("")
    tavily_api_key: str = Field("")


class CommunitySettings(ConfigModel):
    enabled: bool = Field(False)
    interval_minutes: int = Field(30, ge=1)
    max_turns: int = Field(10, ge=1)
    seeding_timeout_seconds: int = Field(300, ge=1)
    seeding_agent_id: Optional[str] = None
    agent_pool_ids: List[str] = Field(default_factory=list)
    project_ids: List[str] = Field(default_factory=list)


class CoordinationLogSettings(ConfigModel):
    enabled: bool = Field(True)
    path: str = Field("logs/coordination.log", min_length=1)
    retention_days: int = Field(14, ge=1)
    rotation_mb: int = Field(10, ge=1)


class DeveloperSettings(ConfigModel):
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


class RootConfig(ConfigModel):
    user_name: str = Field("")
    user_aliases: List[str] = Field(default_factory=list)
    configured_at: Optional[str] = None
    llm: LLMSettings = Field(default_factory=LLMSettings)
    search: SearchAPIKeySettings = Field(default_factory=SearchAPIKeySettings)
    developer_settings: DeveloperSettings = Field(default_factory=DeveloperSettings)
