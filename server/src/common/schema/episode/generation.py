"""LLM-boundary and resolved schemas for episode generation."""

from typing import List

from pydantic import ConfigDict, Field, field_validator, model_validator

from common.schema.episode.models import EpisodeNarrative
from common.schema.llm import StructuredLLMOutput, normalize_required_text


class EpisodeDecision(EpisodeNarrative):
    """Resolved proposal for one new episode from a semantic window."""

    model_config = ConfigDict(extra="forbid")

    summary: str = Field(..., min_length=1)
    message_influences: List[int] = Field(..., min_length=1)

    @field_validator("summary")
    @classmethod
    def validate_summary(cls, value: str) -> str:
        return normalize_required_text(value, field_name="summary")

    @field_validator("message_influences")
    @classmethod
    def validate_message_influences(cls, values: List[int]) -> List[int]:
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value <= 0
            for value in values
        ):
            raise ValueError("message_influences must contain positive message IDs")
        if len(values) != len(set(values)):
            raise ValueError("message_influences must not contain duplicates")
        return values


class LLMEpisodeDecision(EpisodeNarrative):
    """Model-facing new-episode proposal using only local references."""

    model_config = ConfigDict(extra="forbid")

    summary: str = Field(..., min_length=1)
    message_influences: List[str] = Field(..., min_length=1)

    @field_validator("summary")
    @classmethod
    def validate_summary(cls, value: str) -> str:
        return normalize_required_text(value, field_name="summary")

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

    @field_validator("new_developments", "updates", "unresolved")
    @classmethod
    def validate_narrative_lists(cls, values: List[str], info) -> List[str]:
        return [
            normalize_required_text(value, field_name=info.field_name)
            for value in values
        ]


class LLMEpisodeWindowDecision(StructuredLLMOutput):
    """The one model response for a project episode window.

    An empty proposal list is the grounded decision to retain no episodic
    memory from this window. Every proposal creates one new episode from its
    own window-local message references.
    """

    model_config = ConfigDict(extra="forbid")

    proposals: List[LLMEpisodeDecision] = Field(default_factory=list, max_length=3)

    @model_validator(mode="after")
    def validate_proposals(self) -> "LLMEpisodeWindowDecision":
        source_ids: set[str] = set()
        for proposal in self.proposals:
            proposal_sources = set(proposal.message_influences)
            if source_ids.intersection(proposal_sources):
                raise ValueError("episode proposals cannot share source messages")
            source_ids.update(proposal_sources)
        return self
