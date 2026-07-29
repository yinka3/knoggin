"""Validated configuration owned by the agent runtime."""

from typing import Dict

from pydantic import BaseModel, Field


class AgentLimitSettings(BaseModel):
    """Mutable application limits compiled into an immutable agent run."""

    agent_history_turns: int = Field(7, ge=1)
    max_tool_calls: int = Field(12, ge=1)
    tool_timeout: float = Field(30.0, gt=0)
    max_attempts: int = Field(15, ge=1)
    max_consecutive_errors: int = Field(3, ge=1)
    max_accumulated_messages: int = Field(30, ge=1)
    conversation_context_turns: int = Field(10, ge=1)
    max_conversation_history: int = Field(10000, ge=1)
    # The registry owns defaults. Configuration may only alter selected values.
    tool_limit_overrides: Dict[str, int] = Field(default_factory=dict)
