"""Editable agent settings; engine-owned brain state stays private."""

from pydantic import BaseModel, ConfigDict, Field, field_validator

from common.schema.session_input import normalise_enabled_tools


class Persona(BaseModel):
    model_config = ConfigDict(extra="forbid")

    attention_bias: str = Field(min_length=1)
    reasoning_style: str = Field(min_length=1)
    social_temperament: str = Field(min_length=1)
    communication_signature: str = Field(min_length=1)
    productive_flaw: str = Field(min_length=1)


class CreateAgent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=200)
    persona: Persona
    model: str | None = None
    temperature: float = Field(default=0.7, ge=0, le=2)
    enabled_tools: list[str] | None = None

    _tools = field_validator("enabled_tools")(normalise_enabled_tools)


class UpdateAgent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=200)
    model: str | None = None
    temperature: float | None = Field(default=None, ge=0, le=2)
    enabled_tools: list[str] | None = None

    _tools = field_validator("enabled_tools")(normalise_enabled_tools)
