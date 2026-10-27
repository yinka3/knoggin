from typing import Dict, List, Literal, Optional

from pydantic import Field, model_validator

from common.schema.agent.settings import AgentLimitSettings, PromptMemorySettings
from common.schema.config import ConfigModel
from common.schema.jev import JevSettings

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


class SemanticWindowRetrySettings(ConfigModel):
    """Bounded retry policy for durable project semantic windows."""

    max_attempts: int = Field(3, ge=1, le=20)
    initial_backoff_seconds: int = Field(30, ge=1, le=3_600)
    max_backoff_seconds: int = Field(300, ge=1, le=86_400)

    @model_validator(mode="after")
    def validate_backoff_range(self):
        if self.max_backoff_seconds < self.initial_backoff_seconds:
            raise ValueError("max_backoff_seconds must be >= initial_backoff_seconds")
        return self


class IngestionSettings(ConfigModel):
    message_edit_window_seconds: int = Field(600, ge=1, le=86_400)
    # The admission-size ceiling for project-level semantic windows. A smaller
    # extraction model lowers it to window_share_of_model_context of that
    # model's context, because the Context stage sends the whole window, the
    # current Context, and its instructions in one call.
    semantic_window_tokens: int = Field(128_000, ge=1, le=1_000_000)
    window_share_of_model_context: float = Field(0.4, gt=0, le=1)
    # A window smaller than the ceiling closes after this long with no new
    # finished exchange.
    idle_flush_seconds: int = Field(300, ge=1, le=86_400)
    semantic_window_retry: SemanticWindowRetrySettings = Field(
        default_factory=SemanticWindowRetrySettings
    )


class DocumentIndexingSettings(ConfigModel):
    reconciliation_interval_seconds: int = Field(60, ge=10)


class DocumentSettings(ConfigModel):
    project_library_root: str = Field("data/projects", min_length=1)
    rerank_enabled: bool = True
    rerank_candidates: int = Field(15, ge=1, le=50)
    max_workspaces_per_project: int = Field(3, ge=1, le=20)
    max_staged_folder_bytes: int = Field(2 * 1024 * 1024 * 1024, ge=1)
    max_active_folder_intakes: int = Field(2, ge=1)
    folder_intake_idle_seconds: int = Field(2 * 60 * 60, ge=1)
    max_source_batch_items: int = Field(20, ge=1, le=1_000)


class EpisodeSettings(ConfigModel):
    """Configuration for bounded episodic-memory generation windows."""

    enabled: bool = Field(True)
    # Generation batches and the maximum source evidence an Episode may own
    # are intentionally separate controls.
    max_episode_source_messages: int = Field(72, ge=1, le=10_000)
    max_episode_source_tokens: int = Field(12_000, ge=1, le=1_000_000)
    # A server-owned hard cap across every persisted narrative field.  The
    # prompt uses 90% of this value; persistence validates the full limit.
    max_narrative_chars: int = Field(4000, ge=500, le=20000)


class ConflictDiscoverySettings(ConfigModel):
    """Bounded relationship-conflict discovery and review settings."""

    enabled: bool = Field(True)
    mode: Literal["manual", "assisted"] = Field("assisted")
    interval_hours: int = Field(48, ge=1)
    max_seed_span_days: int = Field(60, ge=1, le=365)
    max_package_tokens: int = Field(50_000, ge=1_000, le=200_000)


class DuplicateReviewSettings(ConfigModel):
    # In the background, the merge model checks likely duplicate entities and
    # opens a merge review for each it confirms; the user approves each one.
    # One model call per pair, and an unchanged pair is not checked again.
    enabled: bool = True
    interval_hours: int = Field(5, ge=1)
    max_pairs_per_run: int = Field(10, ge=1, le=100)


class JobSettings(ConfigModel):
    document_indexing: DocumentIndexingSettings = Field(
        default_factory=DocumentIndexingSettings
    )
    episode: EpisodeSettings = Field(default_factory=EpisodeSettings)
    conflict_discovery: ConflictDiscoverySettings = Field(
        default_factory=ConflictDiscoverySettings
    )
    duplicate_review: DuplicateReviewSettings = Field(
        default_factory=DuplicateReviewSettings
    )


class TextProcessorSettings(ConfigModel):
    gliner_threshold: float = Field(0.85, ge=0.0, le=1.0)
    llm_ner_mode: Literal["disabled", "fallback"] = "fallback"


class SearchSettings(ConfigModel):
    fts_limit: int = Field(50, ge=1)
    semantic_message_limit: int = Field(50, ge=1)
    semantic_message_threshold: float = Field(0.25, ge=0.0, le=1.0)
    rerank_candidates: int = Field(25, ge=1)
    default_message_limit: int = Field(8, ge=1)
    default_entity_limit: int = Field(5, ge=1)
    default_activity_hours: int = Field(24, ge=1)


class EntityResolutionSettings(ConfigModel):
    candidate_fuzzy_threshold: int = Field(85, ge=50, le=100)
    resolution_threshold: float = Field(0.85, ge=0.0, le=1.0)
    resolution_margin: float = Field(0.10, ge=0.0, le=1.0)
    common_word_frequency_threshold: float = Field(1e-5, ge=0.0, allow_inf_nan=False)
    sparse_context_verbs: List[str] = Field(
        default_factory=lambda: list(DEFAULT_SPARSE_CONTEXT_VERBS)
    )


class LLMModelPricing(ConfigModel):
    """User-supplied provider pricing for one model, in USD per million tokens."""

    input_usd_per_million_tokens: float = Field(ge=0.0, allow_inf_nan=False)
    output_usd_per_million_tokens: float = Field(ge=0.0, allow_inf_nan=False)


class LLMSpendingBudgetSettings(ConfigModel):
    """One server-wide external-model spending ceiling, not per subsystem."""

    limit_usd: Optional[float] = Field(None, ge=0.0, allow_inf_nan=False)
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
    # The API key is read from OPENROUTER_API_KEY (common.api_keys), never saved.
    base_url: Optional[str] = None
    agent_model: str = Field("google/gemini-3-flash-preview")
    extraction_model: str = Field("google/gemini-2.5-flash")
    merge_model: str = Field("google/gemini-2.5-pro")
    # Long windows and local models can take minutes for one completion.
    request_timeout_seconds: float = Field(300.0, gt=0, le=3_600, allow_inf_nan=False)
    # Thinking control for non-OpenRouter servers (Ollama, vLLM), which can
    # only switch it on or off. planning_only (fast default) thinks on the
    # planning and final answer steps; off disables it everywhere;
    # provider_default lets the server think on every step, which is much
    # slower. OpenRouter keeps its own reasoning budgets.
    thinking: Literal["provider_default", "planning_only", "off"] = "planning_only"
    spending_budget: LLMSpendingBudgetSettings = Field(
        default_factory=LLMSpendingBudgetSettings
    )


class SearchAPIKeySettings(ConfigModel):
    # Brave and Tavily keys come from BRAVE_API_KEY / TAVILY_API_KEY.
    provider: str = Field("auto")


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


class ResearchJobSettings(ConfigModel):
    # Each job makes many model calls; more at once costs more and loads the
    # machine more. Extra jobs wait their turn.
    max_running_per_project: int = Field(3, ge=1, le=100)
    # One page read is budgeted per link, so this bounds a job's size.
    max_links: int = Field(50, ge=1, le=1_000)
    # A job cut off by a restart reruns from the start and pays for its model
    # calls again, so by default it waits as `interrupted` for the user.
    resume_after_restart: bool = False


class ChatCompactionSettings(ConfigModel):
    # Turns that no longer fit the prompt are folded into one summary per chat
    # by the extraction model, after a turn ends.
    enabled: bool = True
    summary_max_tokens: int = Field(2_000, ge=100, le=20_000)
    # Wait until this many tokens have dropped out, so most turns cost nothing.
    min_dropped_tokens: int = Field(4_000, ge=1, le=200_000)
    # Most dropped text one summary pass reads; the rest waits for the next.
    max_input_tokens: int = Field(60_000, ge=1_000, le=500_000)


class IngestionNoticeSettings(ConfigModel):
    # How often a folder import's documents are checked, and how long a chat
    # waits for them before it is told the import is still running.
    poll_seconds: float = Field(10.0, ge=1.0, le=3_600.0, allow_inf_nan=False)
    max_wait_minutes: int = Field(24 * 60, ge=1, le=7 * 24 * 60)


class ToolSettings(ConfigModel):
    # Groups a chat gets when neither it nor its agent picks tools. Web stays
    # opt-in (research mode adds it); maintenance belongs to the Maintenance agent.
    default_groups: List[Literal["core", "web", "maintenance"]] = Field(
        default_factory=lambda: ["core"], min_length=1
    )


class RunEventSettings(ConfigModel):
    """Process-local event history used when a client reconnects to a run."""

    max_buffered_events: int = Field(10_000, ge=1)
    max_buffered_bytes: int = Field(8 * 1024 * 1024, ge=1_024)
    max_retained_runs: int = Field(20, ge=1)


class DeveloperSettings(ConfigModel):
    ingestion: IngestionSettings = Field(default_factory=IngestionSettings)
    jobs: JobSettings = Field(default_factory=JobSettings)
    limits: AgentLimitSettings = Field(default_factory=AgentLimitSettings)
    prompt_memory: PromptMemorySettings = Field(default_factory=PromptMemorySettings)
    research_jobs: ResearchJobSettings = Field(default_factory=ResearchJobSettings)
    tools: ToolSettings = Field(default_factory=ToolSettings)
    chat_compaction: ChatCompactionSettings = Field(default_factory=ChatCompactionSettings)
    ingestion_notices: IngestionNoticeSettings = Field(default_factory=IngestionNoticeSettings)
    run_events: RunEventSettings = Field(default_factory=RunEventSettings)
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
    user_aliases: List[str] = Field(default_factory=list)
    llm: LLMSettings = Field(default_factory=LLMSettings)
    jev: JevSettings = Field(default_factory=JevSettings)
    search: SearchAPIKeySettings = Field(default_factory=SearchAPIKeySettings)
    developer_settings: DeveloperSettings = Field(default_factory=DeveloperSettings)
