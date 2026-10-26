"""HTTP inputs for knowledge maintenance decisions."""

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class Decision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["apply", "dismiss"]
    expected_state: dict[str, Any] | None = None
    reason: str | None = Field(default=None, max_length=2000)


class AdvisoryDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["accept", "edit", "dismiss", "reopen", "suppress"]
    relationship_type: str | None = Field(default=None, max_length=40)
    note: str | None = Field(default=None, max_length=2000)


class RollbackSelection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    approved_mutation_ids: tuple[int, ...] = Field(default=(), max_length=10000)

    @field_validator("approved_mutation_ids")
    @classmethod
    def valid_ids(cls, values: tuple[int, ...]) -> tuple[int, ...]:
        if any(value <= 0 for value in values) or len(set(values)) != len(values):
            raise ValueError("Mutation IDs must be unique positive integers")
        return values


class FlagJevContextDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    capability: Literal["extraction", "classification"]
    window_id: UUID
    pass_number: int = Field(ge=1, le=2)
    occurrence_key: str = Field(min_length=1, max_length=500)
    entity_id: int = Field(gt=0)
    entity_type: str | None = Field(default=None, min_length=1, max_length=200)
    topic: str | None = Field(default=None, min_length=1, max_length=200)
    reasoning: str = Field(min_length=1, max_length=4000)

    @model_validator(mode="after")
    def target_required(self):
        if self.entity_type is None and self.topic is None:
            raise ValueError("Provide an entity type or topic correction")
        return self


class ReopenJevContextCorrection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_review_id: str = Field(min_length=1, max_length=200)
