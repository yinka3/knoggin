"""Small JEV contracts shared by configuration, ingestion, and transport."""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

Capability = Literal["identity", "extraction", "classification"]
Mode = Literal["disabled", "observe", "active"]
Probability = Annotated[float, Field(ge=0, le=1, allow_inf_nan=False, strict=True)]


class JevModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class JevPolicy(JevModel):
    """Serializable admitted policy; contains no credentials or endpoint."""

    identity_mode: Mode = "disabled"
    extraction_mode: Mode = "disabled"
    classification_mode: Mode = "disabled"
    model: str = Field("jev-1.13.0", pattern=r"^jev-\d+\.\d+\.\d+$")
    identity_question_version: Literal["identity-v1"] = "identity-v1"
    extraction_question_version: Literal["extraction-v1"] = "extraction-v1"
    classification_question_version: Literal["classification-v1"] = "classification-v1"
    # Active acceptance is implemented/evaluated by the later consumer phases.
    acceptance_policy_version: Literal["observe-v1"] = "observe-v1"
    max_candidates: int = Field(8, ge=1, le=50)
    max_calls_per_window: int = Field(12, ge=1, le=100)
    max_questions_per_request: int = Field(16, ge=1, le=100)
    max_options_per_choice: int = Field(64, ge=2, le=255)
    max_request_bytes: int = Field(24_000, ge=256, le=32_000)
    request_timeout_seconds: float = Field(5, gt=0, le=30, allow_inf_nan=False)
    total_timeout_seconds: float = Field(12, gt=0, le=60, allow_inf_nan=False)
    accounting_timeout_seconds: float = Field(2, gt=0, le=10, allow_inf_nan=False)
    max_retries: int = Field(1, ge=0, le=3)

    @model_validator(mode="after")
    def validate_timeouts(self):
        if self.request_timeout_seconds > self.total_timeout_seconds:
            raise ValueError("request timeout must fit within total timeout")
        return self

    def mode_for(self, capability: Capability) -> Mode:
        if capability not in {"identity", "extraction", "classification"}:
            raise ValueError("Unknown JEV capability")
        return getattr(self, f"{capability}_mode")


class JevSettings(JevPolicy):
    # Like llm.api_key, this is persisted in private settings. Hide it in repr;
    # policy snapshots and decision records explicitly exclude credentials.
    api_key: str = Field("", repr=False)
    endpoint: str = "https://api.typesafe.ai/v1/systemone"
    shutdown_timeout_seconds: float = Field(5, gt=0, le=30, allow_inf_nan=False)

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, value):
        from urllib.parse import urlsplit

        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or not parsed.path.endswith("/v1/systemone")
        ):
            raise ValueError("JEV endpoint must be an HTTPS /v1/systemone URL")
        return value

    def capture_policy(self) -> JevPolicy:
        return JevPolicy.model_validate(
            self.model_dump(exclude={"api_key", "endpoint", "shutdown_timeout_seconds"})
        )


class ChoiceQuestion(JevModel):
    type: Literal["choice"] = "choice"
    instructions: str = Field(min_length=1)
    criteria: dict[str, str | None] = Field(min_length=2, max_length=255)

    @field_validator("criteria")
    @classmethod
    def validate_keys(cls, value):
        if any(not key.strip() for key in value):
            raise ValueError("Choice options must have nonblank handles")
        return value


class NoulQuestion(JevModel):
    type: Literal["noul"] = "noul"
    instructions: str = Field(min_length=1)


Question = Annotated[ChoiceQuestion | NoulQuestion, Field(discriminator="type")]


class ChoiceAnswer(JevModel):
    type: Literal["choice"]
    choice: str
    probabilities: dict[str, Probability]
    confidence: Probability


class NoulAnswer(JevModel):
    type: Literal["noul"]
    noul: Probability


Answer = Annotated[ChoiceAnswer | NoulAnswer, Field(discriminator="type")]


class JevUsage(JevModel):
    input_tokens: int = Field(ge=0, strict=True)
    output_tokens: int = Field(ge=0, strict=True)


class JevResponse(JevModel):
    model: str = Field(min_length=1)
    answers: dict[str, Answer]
    usage: JevUsage


class JevResult(JevModel):
    outcome: Literal["available", "unavailable", "skipped"]
    reason: str | None = None
    response: JevResponse | None = None
    attempts: int = 0
    elapsed_seconds: float = 0
    reported_model: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float | None = None
    approximate_usage: bool = False
    accounting_pending: bool = False


class JevDecisionRecord(JevModel):
    """Private decision provenance, not proof or an accepted classification.

    This contract is JSON serializable. Durable ownership is described in the
    Phase A journal; consumers must not claim the record is persisted yet.
    """

    capability: Capability
    mode: Mode
    project_id: str = Field(min_length=1)
    window_id: UUID
    pass_number: int = Field(ge=1, le=2)
    occurrence_key: str = Field(min_length=1)
    # UUIDs identify immutable block versions in the current Context schema.
    evidence_block_ids: tuple[UUID, ...]
    domain_version: int = Field(ge=0)
    question_version: str
    acceptance_policy_version: str
    pinned_model: str
    option_mapping: dict[str, str]
    result: JevResult
    baseline_outcome: str
    acceptance_status: Literal[
        "proposed", "accepted", "rejected", "unavailable", "conflicting"
    ] = "proposed"

    @model_validator(mode="after")
    def observe_cannot_accept(self):
        if self.mode != "active" and self.acceptance_status == "accepted":
            raise ValueError("Only active judgments may be accepted")
        return self
