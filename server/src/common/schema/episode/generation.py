"""LLM-boundary and resolved schemas for episode generation."""

from typing import Any, List, Literal, Optional

from pydantic import ConfigDict, Field, field_validator, model_validator

from common.schema.episode.models import EpisodeNarrative
from common.schema.llm import (
    StructuredLLMOutput,
    normalize_optional_text,
    normalize_required_text,
)


def validate_episode_decision_shape(decision: Any) -> None:
    """Apply shared action rules to internal and model-facing episode decisions."""

    if decision.action == "skip":
        if not decision.skip_reason:
            raise ValueError("skip decisions require skip_reason")
        if (
            decision.target_episode_id
            or decision.summary
            or decision.new_developments
            or decision.updates
            or decision.unresolved
            or decision.message_influences
        ):
            raise ValueError("skip decisions must not include episode content")
        return

    if not decision.summary:
        raise ValueError("create and consolidate decisions require summary")
    if not decision.message_influences:
        raise ValueError("create and consolidate decisions require message influences")
    if decision.action == "consolidate" and not decision.target_episode_id:
        raise ValueError("consolidate decisions require target_episode_id")
    if decision.action == "create" and decision.target_episode_id:
        raise ValueError("create decisions must not include target_episode_id")
    if decision.skip_reason:
        raise ValueError("non-skip decisions must not include skip_reason")


class EpisodeDecision(EpisodeNarrative):
    """Resolved internal decision for one bounded episodic-memory window."""

    action: Literal["create", "consolidate", "skip"]
    target_episode_id: Optional[str] = Field(None, min_length=1)
    message_influences: List[int] = Field(default_factory=list)
    skip_reason: Optional[str] = Field(None, min_length=1)

    @model_validator(mode="after")
    def validate_action_shape(self) -> "EpisodeDecision":
        validate_episode_decision_shape(self)
        return self


class EpisodeConsolidation(EpisodeNarrative):
    """Resolved internal regeneration for one selected episode."""

    summary: str = Field(..., min_length=1)
    message_influences: List[int] = Field(..., min_length=1)


class LLMEpisodeDecision(EpisodeNarrative):
    """Model-facing episode decision that contains only local references."""

    model_config = ConfigDict(extra="forbid")

    action: Literal["create", "consolidate", "skip"]
    target_episode_id: Optional[str] = Field(None, pattern=r"^episode:[1-9]\d*$")
    message_influences: List[str] = Field(default_factory=list)
    skip_reason: Optional[str] = Field(None, min_length=1)

    @model_validator(mode="after")
    def validate_action_shape(self) -> "LLMEpisodeDecision":
        validate_episode_decision_shape(self)
        return self

    @field_validator("summary", "skip_reason")
    @classmethod
    def validate_optional_text(cls, value: Optional[str], info) -> Optional[str]:
        return normalize_optional_text(value, field_name=info.field_name)

    @field_validator("message_influences")
    @classmethod
    def validate_message_references(cls, values: List[str]) -> List[str]:
        if any(
            not isinstance(value, str)
            or not value.startswith("message:")
            or not value.removeprefix("message:").isdigit()
            or value == "message:0"
            for value in values
        ):
            raise ValueError("message_influences must contain message:N references")
        return values

    @field_validator("new_developments", "updates", "unresolved")
    @classmethod
    def validate_narrative_lists(cls, values: List[str], info) -> List[str]:
        return [
            normalize_required_text(value, field_name=info.field_name)
            for value in values
        ]


class LLMEpisodeWindowDecision(StructuredLLMOutput):
    """The one model response for a project episode window.

    An empty proposal list is the explicit, grounded decision to retain no
    episodic memory from this window.  A proposal is never a standalone
    ``skip`` because that makes source ownership ambiguous.
    """

    model_config = ConfigDict(extra="forbid")

    proposals: List[LLMEpisodeDecision] = Field(default_factory=list, max_length=3)

    @model_validator(mode="after")
    def validate_proposals(self) -> "LLMEpisodeWindowDecision":
        source_ids: set[str] = set()
        target_ids: set[str] = set()
        for proposal in self.proposals:
            if proposal.action == "skip":
                raise ValueError("episode window proposals cannot use skip")
            proposal_sources = {
                influence for influence in proposal.message_influences
            }
            if source_ids.intersection(proposal_sources):
                raise ValueError("episode proposals cannot share source messages")
            source_ids.update(proposal_sources)
            if proposal.action == "consolidate":
                assert proposal.target_episode_id is not None
                if proposal.target_episode_id in target_ids:
                    raise ValueError("episode proposals must target distinct episodes")
                target_ids.add(proposal.target_episode_id)
        return self


class LLMEpisodeConsolidation(EpisodeNarrative):
    """Model-facing full-evidence consolidation decision."""

    model_config = ConfigDict(extra="forbid")

    action: Literal["consolidate", "keep_separate"]
    summary: Optional[str] = Field(None, min_length=1)
    message_influences: List[str] = Field(default_factory=list)

    @field_validator("summary")
    @classmethod
    def validate_summary(cls, value: Optional[str]) -> Optional[str]:
        return normalize_optional_text(value, field_name="summary")

    @field_validator("new_developments", "updates", "unresolved")
    @classmethod
    def validate_narrative_lists(cls, values: List[str], info) -> List[str]:
        return [
            normalize_required_text(value, field_name=info.field_name)
            for value in values
        ]

    @field_validator("message_influences")
    @classmethod
    def validate_message_references(cls, values: List[str]) -> List[str]:
        if any(
            not isinstance(value, str)
            or not value.startswith("message:")
            or not value.removeprefix("message:").isdigit()
            or value == "message:0"
            for value in values
        ):
            raise ValueError("message_influences must contain message:N references")
        if len(values) != len(set(values)):
            raise ValueError("message_influences must not contain duplicates")
        return values

    @model_validator(mode="after")
    def validate_action_shape(self) -> "LLMEpisodeConsolidation":
        has_narrative = bool(
            self.summary
            or self.new_developments
            or self.updates
            or self.unresolved
        )
        if self.action == "consolidate":
            if not self.summary or not self.message_influences:
                raise ValueError(
                    "consolidate results require narrative and message references"
                )
            return self
        if has_narrative or self.message_influences:
            raise ValueError("keep_separate results must not include episode content")
        return self
