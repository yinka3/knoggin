"""Strict LLM-boundary schemas for entity and relationship extraction."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from common.schema.llm import (
    StructuredLLMOutput,
    normalize_optional_text,
    normalize_required_text,
)


class EntityMention(BaseModel):
    """One model-returned mention with a canonical entity type."""

    model_config = ConfigDict(extra="forbid")

    msg_id: str = Field(..., pattern=r"^m[1-9]\d*$")
    name: str
    type: str
    confidence: float = Field(1.0, ge=0.0, le=1.0)

    @field_validator("name", "type")
    @classmethod
    def validate_required_text(cls, value: str, info) -> str:
        return normalize_required_text(value, field_name=info.field_name)


class EntityExtraction(StructuredLLMOutput):
    """Top-level model response for entity-mention extraction."""

    mentions: List[EntityMention] = Field(default_factory=list)


class RelationshipMention(StructuredLLMOutput):
    """One model-returned relationship with a local message reference."""

    model_config = ConfigDict(extra="forbid")

    msg_id: str = Field(..., pattern=r"^m[1-9]\d*$")
    entity_a: str = Field(..., description="Name of the first entity")
    entity_b: str = Field(..., description="Name of the second entity")
    relationship: str = Field(..., description="Evidence-grounded relationship label")
    confidence: float = Field(1.0, ge=0.0, le=1.0)
    context: Optional[str] = Field(
        None, description="Short excerpt or paraphrase grounding the relationship"
    )
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("entity_a", "entity_b", "relationship")
    @classmethod
    def validate_required_text(cls, value: str, info) -> str:
        return normalize_required_text(value, field_name=info.field_name)

    @field_validator("context")
    @classmethod
    def validate_context(cls, value: Optional[str]) -> Optional[str]:
        return normalize_optional_text(value, field_name="context")


class IdentityRelationshipMention(StructuredLLMOutput):
    """One model-returned identity relationship with a local message reference."""

    entity_name: str
    relationship: str
    confidence: float = Field(1.0, ge=0.0, le=1.0)
    context: Optional[str] = None
    msg_id: str = Field(..., pattern=r"^m[1-9]\d*$")

    @field_validator("entity_name", "relationship")
    @classmethod
    def validate_required_text(cls, value: str, info) -> str:
        return normalize_required_text(value, field_name=info.field_name)

    @field_validator("context")
    @classmethod
    def validate_context(cls, value: Optional[str]) -> Optional[str]:
        return normalize_optional_text(value, field_name="context")


class RelationshipExtraction(StructuredLLMOutput):
    """Top-level model response for relationship extraction."""

    connections: List[RelationshipMention] = Field(default_factory=list)
    user_connections: List[IdentityRelationshipMention] = Field(default_factory=list)
