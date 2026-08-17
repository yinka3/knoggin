"""LLM-boundary and resolved schemas for episode generation."""

from typing import Any, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from common.schema.episode.models import EpisodeNarrative
from common.schema.llm import (
    StructuredLLMOutput,
    normalize_optional_text,
    normalize_required_text,
)


class EpisodeMessageInfluence(BaseModel):
    """Resolved influence for one message in an eligible episode window."""

    message_id: int = Field(..., gt=0)
    influence_weight: float = Field(..., ge=0.0)
    influence_reason: Optional[str] = None


class EpisodeFocusEntitySelection(BaseModel):
    """Resolved focus marker selected from a candidate window's entities."""

    entity_id: int = Field(..., gt=0)
    prominence_weight: float = Field(..., ge=0.0)
    role: Optional[str] = None


class EpisodeCentralRelationshipSelection(BaseModel):
    """Resolved central relationship selected from a candidate window."""

    relationship_id: str = Field(..., min_length=1)
    prominence_weight: float = Field(..., ge=0.0)


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
            or decision.importance
            or decision.message_influences
            or decision.focus_entities
            or decision.central_relationships
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
    message_influences: List[EpisodeMessageInfluence] = Field(default_factory=list)
    focus_entities: List[EpisodeFocusEntitySelection] = Field(default_factory=list)
    central_relationships: List[EpisodeCentralRelationshipSelection] = Field(
        default_factory=list
    )
    skip_reason: Optional[str] = Field(None, min_length=1)

    @model_validator(mode="after")
    def validate_action_shape(self) -> "EpisodeDecision":
        validate_episode_decision_shape(self)
        return self


class EpisodeConsolidation(EpisodeNarrative):
    """Resolved internal regeneration for one selected episode."""

    summary: str = Field(..., min_length=1)
    message_influences: List[EpisodeMessageInfluence] = Field(..., min_length=1)
    focus_entities: List[EpisodeFocusEntitySelection] = Field(default_factory=list)
    central_relationships: List[EpisodeCentralRelationshipSelection] = Field(
        default_factory=list
    )


class LLMEpisodeMessageInfluence(StructuredLLMOutput):
    """One model-selected influence with a local message reference."""

    message_id: str = Field(..., pattern=r"^m[1-9]\d*$")
    influence_weight: float = Field(..., ge=0.0)
    influence_reason: Optional[str] = None


class LLMEpisodeFocusEntitySelection(StructuredLLMOutput):
    """One model-selected focus entity with a local entity reference."""

    entity_id: str = Field(..., pattern=r"^e[1-9]\d*$")
    prominence_weight: float = Field(..., ge=0.0)
    role: Optional[str] = None


class LLMEpisodeCentralRelationshipSelection(StructuredLLMOutput):
    """One model-selected relationship with a local relationship reference."""

    relationship_id: str = Field(..., pattern=r"^r[1-9]\d*$")
    prominence_weight: float = Field(..., ge=0.0)


class LLMEpisodeDecision(EpisodeNarrative):
    """Model-facing episode decision that contains only local references."""

    model_config = ConfigDict(extra="forbid")

    action: Literal["create", "consolidate", "skip"]
    target_episode_id: Optional[str] = Field(None, pattern=r"^ep[1-9]\d*$")
    message_influences: List[LLMEpisodeMessageInfluence] = Field(default_factory=list)
    focus_entities: List[LLMEpisodeFocusEntitySelection] = Field(default_factory=list)
    central_relationships: List[LLMEpisodeCentralRelationshipSelection] = Field(
        default_factory=list
    )
    skip_reason: Optional[str] = Field(None, min_length=1)

    @model_validator(mode="after")
    def validate_action_shape(self) -> "LLMEpisodeDecision":
        validate_episode_decision_shape(self)
        return self

    @field_validator("summary", "skip_reason")
    @classmethod
    def validate_optional_text(cls, value: Optional[str], info) -> Optional[str]:
        return normalize_optional_text(value, field_name=info.field_name)

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
                influence.message_id for influence in proposal.message_influences
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
    """Model-facing episode regeneration output with local references."""

    model_config = ConfigDict(extra="forbid")

    summary: str = Field(..., min_length=1)
    message_influences: List[LLMEpisodeMessageInfluence] = Field(..., min_length=1)
    focus_entities: List[LLMEpisodeFocusEntitySelection] = Field(default_factory=list)
    central_relationships: List[LLMEpisodeCentralRelationshipSelection] = Field(
        default_factory=list
    )

    @field_validator("summary")
    @classmethod
    def validate_summary(cls, value: str) -> str:
        return normalize_required_text(value, field_name="summary")

    @field_validator("new_developments", "updates", "unresolved")
    @classmethod
    def validate_narrative_lists(cls, values: List[str], info) -> List[str]:
        return [
            normalize_required_text(value, field_name=info.field_name)
            for value in values
        ]
