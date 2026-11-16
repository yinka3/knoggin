"""Requests for project-owned control actions."""

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SemanticParticipationSelection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_ids: list[str] = Field(max_length=1000)

    @field_validator("session_ids")
    @classmethod
    def valid_ids(cls, value: list[str]) -> list[str]:
        if any(not item.strip() or item != item.strip() for item in value):
            raise ValueError("session IDs must be non-blank")
        if len(value) != len(set(value)):
            raise ValueError("session IDs must be unique")
        return value
