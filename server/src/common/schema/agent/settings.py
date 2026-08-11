"""Validated limits owned by the agent runtime."""

from collections.abc import Mapping
from typing import Dict

from pydantic import Field, FiniteFloat, StrictInt, field_validator

from common.schema.config import ConfigModel


class AgentLimitSettings(ConfigModel):
    """Mutable application limits compiled into an immutable agent run."""

    agent_history_turns: StrictInt = Field(7, ge=1)
    max_tool_calls: StrictInt = Field(12, ge=1)
    tool_timeout: FiniteFloat = Field(30.0, gt=0)
    max_attempts: StrictInt = Field(15, ge=1)
    max_consecutive_errors: StrictInt = Field(3, ge=1)
    max_accumulated_messages: StrictInt = Field(30, ge=1)
    conversation_context_turns: StrictInt = Field(10, ge=1)
    max_conversation_history: StrictInt = Field(10000, ge=1)
    # The registry owns defaults. Configuration may only alter selected values.
    tool_limit_overrides: Dict[str, StrictInt] = Field(default_factory=dict)

    @field_validator("tool_limit_overrides", mode="before")
    @classmethod
    def normalize_tool_limit_override_names(cls, value):
        if value is None:
            return {}
        if not isinstance(value, Mapping):
            raise ValueError("tool_limit_overrides must be a mapping")

        normalized = {}
        for raw_name, limit in value.items():
            if not isinstance(raw_name, str):
                raise ValueError("tool limit override names must be strings")
            name = raw_name.strip().lower()
            if not name:
                raise ValueError("tool limit override names must not be blank")
            if name in normalized:
                raise ValueError(
                    f"duplicate tool limit override after normalization: {name}"
                )
            normalized[name] = limit
        return normalized


def validate_tool_limit_overrides(
    settings: AgentLimitSettings,
    allowed_tool_names: set[str] | frozenset[str],
) -> None:
    """Validate registry-dependent overrides at an application boundary."""

    invalid_limits = sorted(
        name
        for name, limit in settings.tool_limit_overrides.items()
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1
    )
    if invalid_limits:
        raise ValueError(
            "Tool limit overrides must be positive integers: "
            + ", ".join(invalid_limits)
        )

    unknown = sorted(set(settings.tool_limit_overrides) - set(allowed_tool_names))
    if unknown:
        raise ValueError("Unknown tool limit overrides: " + ", ".join(unknown))
