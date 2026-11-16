"""LLM-boundary schemas for per-Episode entity and relationship extraction."""

import re
from typing import List, Optional

from loguru import logger
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    field_validator,
)

from common.schema.llm import (
    StructuredLLMOutput,
    normalize_optional_text,
    normalize_required_text,
)

_MESSAGE_HANDLE = re.compile(r"^M[1-9]\d*$")
_ENTITY_HANDLE = re.compile(r"^e[1-9]\d*$")


def _message_handles(value: List[str]) -> List[str]:
    if not value or len(value) != len(set(value)):
        raise ValueError("messages must list one or more unique M handles")
    if any(not _MESSAGE_HANDLE.match(item) for item in value):
        raise ValueError("messages must contain local M handles")
    return value


def _keep_valid(values: object, adapter: TypeAdapter, what: str) -> object:
    # Check each item alone: one bad item must not reject the others and
    # fail the whole window.
    if not isinstance(values, list):
        return values
    kept = []
    for value in values:
        try:
            kept.append(adapter.validate_python(value))
        except ValidationError as exc:
            logger.warning("Dropped malformed {}: {}", what, exc.errors()[0]["msg"])
    return kept


class EpisodeEntityMention(BaseModel):
    """One named entity and the Episode messages that mention it."""

    model_config = ConfigDict(extra="forbid")

    messages: List[str] = Field(..., max_length=72)
    name: str
    type: str

    @field_validator("messages")
    @classmethod
    def validate_messages(cls, value: List[str]) -> List[str]:
        return _message_handles(value)

    @field_validator("name", "type")
    @classmethod
    def validate_required_text(cls, value: str, info) -> str:
        return normalize_required_text(value, field_name=info.field_name)


_ENTITY_MENTION = TypeAdapter(EpisodeEntityMention)


class EpisodeEntityExtraction(StructuredLLMOutput):
    """VP-01 output for one Episode."""

    mentions: List[EpisodeEntityMention] = Field(default_factory=list)

    @field_validator("mentions", mode="before")
    @classmethod
    def keep_valid_mentions(cls, values: object) -> object:
        return _keep_valid(values, _ENTITY_MENTION, "entity mention")


class EpisodeRelationshipMention(BaseModel):
    """One relationship between two candidate handles, cited to messages."""

    model_config = ConfigDict(extra="forbid")

    messages: List[str] = Field(..., max_length=72)
    entity_a: str
    entity_b: str
    relationship: str
    context: Optional[str] = None

    @field_validator("messages")
    @classmethod
    def validate_messages(cls, value: List[str]) -> List[str]:
        return _message_handles(value)

    @field_validator("entity_a", "entity_b")
    @classmethod
    def validate_entity_handle(cls, value: str, info) -> str:
        value = normalize_required_text(value, field_name=info.field_name)
        if not _ENTITY_HANDLE.match(value):
            raise ValueError(f"{info.field_name} must be a local eN entity handle")
        return value

    @field_validator("relationship")
    @classmethod
    def validate_required_text(cls, value: str, info) -> str:
        return normalize_required_text(value, field_name=info.field_name)

    @field_validator("context")
    @classmethod
    def validate_context(cls, value: Optional[str]) -> Optional[str]:
        return normalize_optional_text(value, field_name="context")


_RELATIONSHIP_MENTION = TypeAdapter(EpisodeRelationshipMention)


class EpisodeRelationshipExtraction(StructuredLLMOutput):
    """VP-02 output for one Episode."""

    connections: List[EpisodeRelationshipMention] = Field(default_factory=list)

    @field_validator("connections", mode="before")
    @classmethod
    def keep_valid_connections(cls, values: object) -> object:
        return _keep_valid(values, _RELATIONSHIP_MENTION, "relationship")
