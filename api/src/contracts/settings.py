"""Credential-free settings and apply-status responses."""

from pydantic import BaseModel, ConfigDict

from common.api_keys import brave_api_key, jev_api_key, llm_api_key, tavily_api_key
from common.schema.jev import JevPolicy
from common.schema.settings import (
    DeveloperSettings,
    LLMSpendingBudgetSettings,
    RootConfig,
)
from infrastructure.llm_client import local_thinking_warning


class APIModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class LLMSettingsView(APIModel):
    agent_model: str
    extraction_model: str
    merge_model: str
    base_url: str | None
    request_timeout_seconds: float
    thinking: str
    # Set when the chosen thinking mode makes a local model much slower.
    thinking_warning: str | None
    spending_budget: LLMSpendingBudgetSettings
    api_key_configured: bool


class SearchSettingsView(APIModel):
    provider: str
    brave_api_key_configured: bool
    tavily_api_key_configured: bool


class JevSettingsView(JevPolicy):
    endpoint: str
    shutdown_timeout_seconds: float
    api_key_configured: bool


class SettingsView(APIModel):
    user_aliases: list[str]
    llm: LLMSettingsView
    search: SearchSettingsView
    jev: JevSettingsView
    developer_settings: DeveloperSettings

    @classmethod
    def from_config(cls, config: RootConfig) -> "SettingsView":
        return cls(
            user_aliases=config.user_aliases,
            llm=LLMSettingsView(
                agent_model=config.llm.agent_model,
                extraction_model=config.llm.extraction_model,
                merge_model=config.llm.merge_model,
                base_url=config.llm.base_url,
                request_timeout_seconds=config.llm.request_timeout_seconds,
                thinking=config.llm.thinking,
                thinking_warning=local_thinking_warning(
                    config.llm.base_url, config.llm.thinking
                ),
                spending_budget=config.llm.spending_budget,
                api_key_configured=bool(llm_api_key()),
            ),
            search=SearchSettingsView(
                provider=config.search.provider,
                brave_api_key_configured=bool(brave_api_key()),
                tavily_api_key_configured=bool(tavily_api_key()),
            ),
            jev=JevSettingsView(
                **config.jev.model_dump(),
                api_key_configured=bool(jev_api_key()),
            ),
            developer_settings=config.developer_settings,
        )


class SettingsStatus(APIModel):
    generation: int
    persisted: bool
    activated: bool
    failed_subscriptions: tuple[int, ...]
    pending_subscriptions: tuple[int, ...]
    restart_required_fields: tuple[str, ...]
    fully_applied: bool


def settings_status(runtime) -> SettingsStatus:
    status = runtime.config_manager.last_apply_status
    current = runtime.config_manager.config
    startup = runtime.startup_config
    restart_fields: tuple[str, ...] = ()
    if startup is not None:
        compared = (
            ("user_aliases", startup.user_aliases, current.user_aliases),
            (
                "developer_settings.documents.project_library_root",
                startup.developer_settings.documents.project_library_root,
                current.developer_settings.documents.project_library_root,
            ),
            (
                "developer_settings.documents.rerank_enabled",
                startup.developer_settings.documents.rerank_enabled,
                current.developer_settings.documents.rerank_enabled,
            ),
            (
                "developer_settings.documents.rerank_candidates",
                startup.developer_settings.documents.rerank_candidates,
                current.developer_settings.documents.rerank_candidates,
            ),
            (
                "developer_settings.jobs.document_indexing.reconciliation_interval_seconds",
                startup.developer_settings.jobs.document_indexing.reconciliation_interval_seconds,
                current.developer_settings.jobs.document_indexing.reconciliation_interval_seconds,
            ),
            (
                "developer_settings.search",
                startup.developer_settings.search,
                current.developer_settings.search,
            ),
        )
        restart_fields = tuple(
            path for path, before, after in compared if before != after
        )
    return SettingsStatus(
        generation=status.generation,
        persisted=status.persisted,
        activated=status.activated,
        failed_subscriptions=status.failed_subscriptions,
        pending_subscriptions=status.pending_subscriptions,
        restart_required_fields=restart_fields,
        fully_applied=status.fully_applied and not restart_fields,
    )
