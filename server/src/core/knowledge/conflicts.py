"""Workflow-neutral conflict-group contracts over immutable relationship evidence."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

ConflictOrigin = Literal[
    "background_discovery",
    "agent_discovery",
    "user_created",
]
ConflictStatus = Literal["open", "resolved"]
ConflictKind = Literal[
    "possible_contradiction",
    "temporal_ambiguity",
    "possible_state_change",
    "identity_or_entity_ambiguity",
]
ConflictResolutionKind = Literal[
    "confirmed_conflict",
    "normal_temporal_change",
    "not_a_conflict",
    "insufficient_evidence",
    "custom",
]


@dataclass(frozen=True, slots=True)
class ConflictGroup:
    conflict_id: str
    user_name: str
    project_id: str
    status: ConflictStatus
    origin: ConflictOrigin
    kind: ConflictKind
    rationale: str
    confidence: float | None
    evidence_signature: str
    resolution_kind: ConflictResolutionKind | None = None
    resolution_note: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ConflictWriteResult:
    group: ConflictGroup
    created: bool
    evidence_added: int = 0

    @property
    def should_notify(self) -> bool:
        return self.created or self.evidence_added > 0


@dataclass(frozen=True, slots=True)
class ConflictDiscoveryCursor:
    """The one durable progress marker for a project's evidence review."""

    user_name: str
    project_id: str
    last_reviewed_observation_id: int


@dataclass(frozen=True, slots=True)
class ConflictDiscoveryPackage:
    cursor: ConflictDiscoveryCursor
    observations: tuple[dict[str, Any], ...]
    next_observation_id: int
    prompt: str
    estimated_tokens: int
    compacted: bool = False


class LLMConflictCandidate(BaseModel):
    """A grounded candidate returned by conflict discovery, never a resolution."""

    evidence_ids: list[int] = Field(min_length=2, max_length=32)
    kind: ConflictKind
    rationale: str = Field(min_length=1, max_length=2_000)
    confidence: float = Field(ge=0.0, le=1.0)

    @field_validator("evidence_ids")
    @classmethod
    def distinct_evidence_ids(cls, values: list[int]) -> list[int]:
        if any(value <= 0 for value in values):
            raise ValueError("evidence_ids must contain positive observation IDs")
        if len(set(values)) != len(values):
            raise ValueError("evidence_ids must be distinct")
        return values


class LLMConflictDiscoveryResult(BaseModel):
    candidates: list[LLMConflictCandidate] = Field(default_factory=list, max_length=32)
