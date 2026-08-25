"""Contracts for user-selectable research execution profiles.

These values describe the requested mode, artifact policy, and budget scaling.
The orchestrator resolves them into one immutable run snapshot.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt

ResearchMode = Literal["normal", "research", "deep_research"]
ArtifactPolicy = Literal["none", "optional", "default", "required"]


class ResearchProfile(BaseModel):
    """Stable mode description shared by request, run, and completion layers."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    mode: ResearchMode
    artifact_policy: ArtifactPolicy
    default_artifact_kind: Literal["general", "research_brief", "research_report"] | None = None
    minimum_source_count: StrictInt = Field(default=0, ge=0)
    iterative: StrictBool = False
    tool_call_budget_multiplier: StrictInt = Field(default=1, ge=1, le=4)
    attempt_budget_multiplier: StrictInt = Field(default=1, ge=1, le=4)
    source_budget_multiplier: StrictInt = Field(default=1, ge=1, le=4)


DEFAULT_RESEARCH_PROFILES: dict[ResearchMode, ResearchProfile] = {
    "normal": ResearchProfile(
        mode="normal",
        artifact_policy="none",
        default_artifact_kind=None,
    ),
    "research": ResearchProfile(
        mode="research",
        artifact_policy="default",
        default_artifact_kind="research_brief",
        minimum_source_count=1,
        tool_call_budget_multiplier=2,
        attempt_budget_multiplier=2,
        source_budget_multiplier=2,
    ),
    "deep_research": ResearchProfile(
        mode="deep_research",
        artifact_policy="required",
        default_artifact_kind="research_report",
        minimum_source_count=2,
        iterative=True,
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
