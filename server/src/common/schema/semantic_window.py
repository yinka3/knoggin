"""Strict durable contracts for project-level semantic windows."""

from __future__ import annotations

from enum import StrEnum
from uuid import UUID

from pydantic import Field, model_validator

from common.schema.config import ConfigModel


class ExchangeState(StrEnum):
    OPEN = "open"
    CLOSED = "closed"


class ExchangeOutcome(StrEnum):
    ASSISTANT_FINAL = "assistant_final"
    CLARIFICATION = "clarification"
    FAILED = "failed"
    CANCELLED = "cancelled"
    USER_ONLY = "user_only"


class SemanticWindowOrigin(StrEnum):
    CONVERSATION = "conversation"
    HUMAN_EDIT = "human_edit"


class SemanticWindowStage(StrEnum):
    CLAIMED = "claimed"
    CONTEXT_COMMITTED = "context_committed"
    KNOWLEDGE_COMMITTED = "knowledge_committed"
    COMPLETED = "completed"


class SemanticWindowMessage(ConfigModel):
    """One immutable message membership record, ordered inside one window."""

    message_id: int = Field(gt=0)
    session_id: str = Field(min_length=1)
    exchange_user_message_id: int = Field(gt=0)
    role: str = Field(pattern=r"^(user|assistant)$")
    ordinal: int = Field(ge=0)


class SemanticWindowRecord(ConfigModel):
    """The durable state needed to resume one frozen semantic window."""

    window_id: UUID
    user_name: str = Field(min_length=1)
    project_id: str = Field(min_length=1)
    origin: SemanticWindowOrigin
    stage: SemanticWindowStage
    domain_version: int = Field(ge=0)
    policy_snapshot: dict[str, object] = Field(default_factory=dict)
    source_token_count: int = Field(ge=0)
    token_estimator: str = Field(min_length=1)
    token_estimator_version: str = Field(min_length=1)
    overfill_tokens: int = Field(default=0, ge=0)
    overfill_ratio: float = Field(default=0.0, ge=0.0)
    episode_result_recorded: bool = False
    context_revision_id: UUID | None = None
    attempt_count: int = Field(default=0, ge=0)
    last_failure_stage: str | None = None
    last_failure_code: str | None = None
    last_failure_at_ms: int | None = Field(default=None, ge=0)
    last_error_summary: str | None = None
    next_retry_at_ms: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_failure_metadata(self) -> "SemanticWindowRecord":
        failure_values = (
            self.last_failure_stage,
            self.last_failure_code,
            self.last_failure_at_ms,
            self.last_error_summary,
        )
        if any(value is not None for value in failure_values) and not all(
            value is not None for value in failure_values
        ):
            raise ValueError("semantic window failure metadata must be complete")
        return self


class SemanticWindowClaimResult(ConfigModel):
    """The active window returned by an atomic claim attempt."""

    window: SemanticWindowRecord
    claimed: bool
