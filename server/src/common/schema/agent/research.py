"""Contracts for user-selectable research execution profiles.

The executor owns mode semantics. Profiles retain only immutable output defaults
and budget scaling resolved into one run snapshot by the orchestrator.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt

ResearchMode = Literal["normal", "research", "deep_research"]


class ResearchProfile(BaseModel):
    """Stable mode description shared by request, run, and completion layers."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    mode: ResearchMode
    default_artifact_kind: (
        Literal["general", "research_brief", "research_report"] | None
    ) = None
    tool_call_budget_multiplier: StrictInt = Field(default=1, ge=1, le=4)
    attempt_budget_multiplier: StrictInt = Field(default=1, ge=1, le=4)
    source_budget_multiplier: StrictInt = Field(default=1, ge=1, le=4)


DEFAULT_RESEARCH_PROFILES: dict[ResearchMode, ResearchProfile] = {
    "normal": ResearchProfile(
        mode="normal",
        default_artifact_kind=None,
    ),
    "research": ResearchProfile(
        mode="research",
        default_artifact_kind="research_brief",
        tool_call_budget_multiplier=2,
        attempt_budget_multiplier=2,
        source_budget_multiplier=2,
    ),
    "deep_research": ResearchProfile(
        mode="deep_research",
        default_artifact_kind="research_report",
        tool_call_budget_multiplier=3,
        attempt_budget_multiplier=3,
        source_budget_multiplier=3,
    ),
}


def resolve_research_profile(mode: ResearchMode | None) -> ResearchProfile:
    """Resolve one user-selected mode into an immutable execution profile."""

    selected = mode or "normal"
    try:
        return DEFAULT_RESEARCH_PROFILES[selected]
    except KeyError as exc:
        raise ValueError(f"Unknown research mode: {selected}") from exc
