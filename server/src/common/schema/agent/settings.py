"""Validated limits owned by the agent runtime."""

from collections.abc import Mapping
from typing import Dict, Literal

from pydantic import (
    Field,
    FiniteFloat,
    StrictBool,
    StrictInt,
    field_validator,
    model_validator,
)

from common.schema.config import ConfigModel

ProjectBriefingMode = Literal["always", "adaptive"]
PromptCategory = Literal[
    "conversation", "project_context", "run_notebook", "selected_notebook"
]


class PromptMemoryBudget(ConfigModel):
    """The input budget for an admitted agent run."""

    # When the provider reports the model's context size, the budget is
    # rescaled so input uses input_share_of_context of it, up to the ceiling,
    # and each share grows or shrinks by the same factor. These numbers then
    # only set the proportions. Turn off to use them exactly. The rest of the
    # context is room for the response, thinking, and tokenizer differences.
    # The ceiling (60% of 256K) keeps 1M-token models from sending huge, slow
    # prompts on every step.
    fit_to_model: StrictBool = True
    input_share_of_context: FiniteFloat = Field(0.6, gt=0, le=1)
    max_fitted_input_tokens: StrictInt = Field(157_286, ge=1_024)
    # About 6K tokens are fixed (system prompt and tool schemas) before any
    # memory is added.
    input_tokens: StrictInt = Field(48_000, ge=1_024)
    response_reserve_tokens: StrictInt = Field(2_000, ge=256)
    project_context_tokens: StrictInt = Field(12_000, ge=0)
    run_notebook_tokens: StrictInt = Field(16_000, ge=0)
    selected_notebook_tokens: StrictInt = Field(6_000, ge=0)
    conversation_tokens: StrictInt = Field(8_000, ge=0)
    spare_priority: tuple[PromptCategory, PromptCategory, PromptCategory, PromptCategory] = (
        "conversation", "project_context", "run_notebook", "selected_notebook"
    )

    @model_validator(mode="after")
    def validate_priority(self):
        if len(set(self.spare_priority)) != 4:
            raise ValueError("spare_priority must contain each prompt category once")
        return self


class AgentModelPromptMemory(ConfigModel):
    agent_id: str = Field(min_length=1)
    model: str = Field(min_length=1)
    budget: PromptMemoryBudget


class PromptMemorySettings(ConfigModel):
    default: PromptMemoryBudget = Field(default_factory=PromptMemoryBudget)
    overrides: list[AgentModelPromptMemory] = Field(default_factory=list)

    @model_validator(mode="after")
    def unique_pairs(self):
        pairs = [(row.agent_id, row.model) for row in self.overrides]
        if len(pairs) != len(set(pairs)):
            raise ValueError("agent/model prompt memory overrides must be unique")
        return self

    def for_run(self, agent_id: str, model: str) -> PromptMemoryBudget:
        for row in self.overrides:
            if row.agent_id == agent_id and row.model == model:
                return row.budget
        return self.default


class AgentLimitSettings(ConfigModel):
    """Mutable application limits compiled into an immutable agent run."""

    max_tool_calls: StrictInt = Field(12, ge=1)
    tool_timeout: FiniteFloat = Field(30.0, gt=0)
    max_attempts: StrictInt = Field(15, ge=1)
    max_consecutive_errors: StrictInt = Field(3, ge=1)
    # Replans that may find nothing before the run answers with what it has;
    # measured runs on an empty memory searched the same query 15 times.
    empty_replans_before_answer: StrictInt = Field(2, ge=1)
    max_accumulated_messages: StrictInt = Field(30, ge=1)
    max_accumulated_profiles: StrictInt = Field(20, ge=1)
    max_accumulated_graph: StrictInt = Field(40, ge=1)
    max_accumulated_paths: StrictInt = Field(8, ge=1)
    max_accumulated_episodes: StrictInt = Field(8, ge=1)
    max_accumulated_documents: StrictInt = Field(30, ge=1)
    max_accumulated_web_discoveries: StrictInt = Field(12, ge=1)
    max_accumulated_web_reads: StrictInt = Field(12, ge=1)
    max_accumulated_actions: StrictInt = Field(12, ge=1)
    max_accumulated_next_steps: StrictInt = Field(12, ge=1)
    max_accumulated_summary_chars: StrictInt = Field(4000, ge=1)
    # How much of each read passage (web page, document, message) the model
    # sees. A 150-line read_web_page call measured 5-9K characters on docs
    # pages (30-40K on Wikipedia); showing 1.2K made models reread the same
    # range in a loop. A longer passage says the line to continue from.
    max_passage_chars: StrictInt = Field(12_000, ge=200)
    max_notebook_render_tokens: StrictInt = Field(48_000, ge=1)
    # Findings a research run may record with record_finding.
    max_research_findings: StrictInt = Field(30, ge=1)
    # ``adaptive`` only skips persistent Project material for a deliberately
    # narrow conversational fast path. All uncertain or substantive prompts
    # retain the Brief/Context before the first model step.
    project_briefing_mode: ProjectBriefingMode = Field("adaptive")
    # The registry owns defaults. Configuration may only alter selected values.
    tool_limit_overrides: Dict[str, StrictInt] = Field(default_factory=dict)

    @field_validator("tool_limit_overrides", mode="before")
    @classmethod
    def normalize_tool_limit_override_names(cls, value):
        if value is None:
            return {}
        if not isinstance(value, Mapping):
            raise ValueError("tool_limit_overrides must be a mapping")

        normalized = {}
        for raw_name, limit in value.items():
            if not isinstance(raw_name, str):
                raise ValueError("tool limit override names must be strings")
            name = raw_name.strip().lower()
            if not name:
                raise ValueError("tool limit override names must not be blank")
            if name in normalized:
                raise ValueError(
                    f"duplicate tool limit override after normalization: {name}"
                )
            normalized[name] = limit
        return normalized


def validate_tool_limit_overrides(
    settings: AgentLimitSettings,
    allowed_tool_names: set[str] | frozenset[str],
) -> None:
    """Validate registry-dependent overrides at an application boundary."""

    invalid_limits = sorted(
        name
        for name, limit in settings.tool_limit_overrides.items()
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1
    )
    if invalid_limits:
        raise ValueError(
            "Tool limit overrides must be positive integers: "
            + ", ".join(invalid_limits)
        )

    unknown = sorted(set(settings.tool_limit_overrides) - set(allowed_tool_names))
    if unknown:
        raise ValueError("Unknown tool limit overrides: " + ", ".join(unknown))
