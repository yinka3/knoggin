"""LLM-boundary and resolved schemas for episode generation."""

import re
from typing import List

from loguru import logger
from pydantic import ConfigDict, Field, TypeAdapter, ValidationError, field_validator

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


_MESSAGE_HANDLE = re.compile(r"\s*message:\s*([1-9][0-9]*)(?![0-9])")


class LLMEpisodeDecision(EpisodeNarrative):
    """Model-facing new-episode proposal using only local references."""

    model_config = ConfigDict(extra="forbid")

    summary: str = Field(..., min_length=1)
    message_influences: List[str] = Field(..., min_length=1)

    @field_validator("summary")
    @classmethod
    def validate_summary(cls, value: str) -> str:
        return normalize_required_text(value, field_name="summary")

    @field_validator("message_influences", mode="before")
    @classmethod
    def keep_message_handles(cls, values: object) -> object:
        # Models add notes after a handle ("message:3 - asked about X") or write
        # prose instead. Keep the handle, drop what is not one.
        if not isinstance(values, list):
            return values
        handles = []
        for value in values:
            match = _MESSAGE_HANDLE.match(value) if isinstance(value, str) else None
            if match and (handle := f"message:{match[1]}") not in handles:
                handles.append(handle)
        return handles

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

    # Count and source-overlap rules are enforced per proposal by
    # ProjectEpisodeBuild, so one bad proposal cannot fail the whole response.
    proposals: List[LLMEpisodeDecision] = Field(...)

    @field_validator("proposals", mode="before")
    @classmethod
    def keep_valid_proposals(cls, values: object) -> object:
        if not isinstance(values, list):
            return values
        kept = []
        for value in values:
            try:
                kept.append(_PROPOSAL.validate_python(value))
            except ValidationError as exc:
                logger.warning(
                    "Dropped malformed Episode proposal: {}", exc.errors()[0]["msg"]
                )
        return kept


_PROPOSAL = TypeAdapter(LLMEpisodeDecision)
