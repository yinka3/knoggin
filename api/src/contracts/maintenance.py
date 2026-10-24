"""HTTP inputs for knowledge maintenance decisions."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


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
