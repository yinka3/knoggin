"""Session metadata accepted by the engine, independent of API transport."""

from pydantic import BaseModel, ConfigDict, Field, field_validator


def normalise_enabled_tools(value: list[str] | None) -> list[str] | None:
    if value is None:
        return None
    result: list[str] = []
    seen: set[str] = set()
    for name in value:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("enabled_tools must contain non-blank names")
        canonical = name.strip().lower()
        if canonical in seen:
            raise ValueError(f"enabled_tools contains duplicate tool: {canonical}")
        seen.add(canonical)
        result.append(canonical)
    return result


class SessionMetadataInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    model: str | None = Field(default=None, min_length=1)
    agent_id: str | None = Field(default=None, min_length=1)
    enabled_tools: list[str] | None = None

    @field_validator("model", "agent_id", mode="before")
    @classmethod
    def _strip_identifiers(cls, value):
        return value.strip() if isinstance(value, str) else value

    _normalise_tools = field_validator("enabled_tools")(normalise_enabled_tools)
