from typing import Dict, List, Optional

from pydantic import Field, model_validator

from common.schema.agent.settings import AgentLimitSettings
from common.schema.config import ConfigModel

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
    message_edit_window_seconds: int = Field(600, ge=1, le=86_400)
    message_lifecycle_poll_seconds: float = Field(15.0, ge=1.0, le=300.0)
    ingestion_max_attempts: int = Field(3, ge=1, le=20)
    session_window: int = Field(24, ge=1)


class DocumentIndexingSettings(ConfigModel):
    recovery_interval_seconds: int = Field(60, ge=10)
    recovery_batch_size: int = Field(16, ge=1, le=100)
    reconciliation_interval_seconds: int = Field(60, ge=10)


class DocumentSettings(ConfigModel):
    project_library_root: str = Field("data/projects", min_length=1)
    rerank_enabled: bool = True
    rerank_candidates: int = Field(15, ge=1, le=50)


class EpisodeSettings(ConfigModel):
    """Configuration for bounded episodic-memory generation windows."""

    enabled: bool = Field(True)
    # A server-owned hard cap across every persisted narrative field.  The
    # prompt uses 90% of this value; persistence validates the full limit.
    max_narrative_chars: int = Field(4000, ge=500, le=20000)
    prior_episode_candidate_count: int = Field(3, ge=1, le=3)


class MergeRollbackSettings(ConfigModel):
    enabled: bool = Field(True)
    retention_hours: float = Field(5.0, ge=0.5)
    fallback_interval_hours: float = Field(1.0, ge=0.25)


class AuditRetentionSettings(ConfigModel):
    """Retention windows for completed, non-canonical operational records."""

    enabled: bool = Field(True)
    interval_hours: float = Field(24.0, ge=0.25)
    tool_audit_days: int = Field(180, ge=1)
    merge_history_days: int = Field(180, ge=1)


class ConflictDiscoverySettings(ConfigModel):
    enabled: bool = Field(True)
    interval_hours: int = Field(48, ge=1)
    max_seed_span_days: int = Field(60, ge=1, le=365)
    max_package_tokens: int = Field(50_000, ge=1_000, le=200_000)


class JobSettings(ConfigModel):
    document_indexing: DocumentIndexingSettings = Field(
        default_factory=DocumentIndexingSettings
    )
    episode: EpisodeSettings = Field(default_factory=EpisodeSettings)
    merge_rollback: MergeRollbackSettings = Field(default_factory=MergeRollbackSettings)
    audit_retention: AuditRetentionSettings = Field(
        default_factory=AuditRetentionSettings
    )
    conflict_discovery: ConflictDiscoverySettings = Field(
        default_factory=ConflictDiscoverySettings
    )
    document_indexing: DocumentIndexingSettings = Field(
        default_factory=DocumentIndexingSettings
    )


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


class LLMModelPricing(ConfigModel):
    """User-supplied provider pricing for one model, in USD per million tokens."""

    input_usd_per_million_tokens: float = Field(ge=0.0)
    output_usd_per_million_tokens: float = Field(ge=0.0)


class LLMSpendingBudgetSettings(ConfigModel):
    """One server-wide external-model spending ceiling, not per subsystem."""

    limit_usd: Optional[float] = Field(None, ge=0.0)
    model_pricing: Dict[str, LLMModelPricing] = Field(default_factory=dict)
    fallback_pricing: Optional[LLMModelPricing] = None
    reservation_output_tokens: int = Field(1_024, ge=0, le=32_768)
    reset_key: str = Field("")

    @model_validator(mode="after")
    def require_pricing_for_a_positive_limit(self):
        if (
            self.limit_usd is not None
            and self.limit_usd > 0
            and not self.model_pricing
            and self.fallback_pricing is None
        ):
            raise ValueError(
                "llm.spending_budget requires model_pricing or fallback_pricing "
                "when limit_usd is positive"
            )
        return self


class LLMSettings(ConfigModel):
    api_key: str = Field("")
    base_url: Optional[str] = None
    agent_model: str = Field("google/gemini-3-flash-preview")
    extraction_model: str = Field("google/gemini-2.5-flash-preview")
    merge_model: str = Field("google/gemini-2.5-pro")
    spending_budget: LLMSpendingBudgetSettings = Field(
        default_factory=LLMSpendingBudgetSettings
    )


class SearchAPIKeySettings(ConfigModel):
    provider: str = Field("auto")
    brave_api_key: str = Field("")
    tavily_api_key: str = Field("")


class CommunitySettings(ConfigModel):
    enabled: bool = Field(False)
    interval_minutes: int = Field(30, ge=1)
    token_budget: int = Field(50_000_000, ge=0)
    seeding_agent_id: Optional[str] = None


class CoordinationLogSettings(ConfigModel):
    enabled: bool = Field(True)
    path: str = Field("logs/coordination.log", min_length=1)
    retention_days: int = Field(14, ge=1)
    rotation_mb: int = Field(10, ge=1)


class DeveloperSettings(ConfigModel):
    ingestion: IngestionSettings = Field(default_factory=IngestionSettings)
    jobs: JobSettings = Field(default_factory=JobSettings)
    limits: AgentLimitSettings = Field(default_factory=AgentLimitSettings)
    entity_resolution: EntityResolutionSettings = Field(
        default_factory=EntityResolutionSettings
    )
    nlp_pipeline: TextProcessorSettings = Field(default_factory=TextProcessorSettings)
    search: SearchSettings = Field(default_factory=SearchSettings)
    documents: DocumentSettings = Field(default_factory=DocumentSettings)
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
