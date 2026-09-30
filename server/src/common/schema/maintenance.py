"""Public-safe typed maintenance impact contracts."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

ImpactKind = Literal[
    "entity",
    "relationship_observation",
    "relationship",
    "project_entity_context",
    "context_block_entity",
    "episode_entity_link",
    "merge_mutation",
    "domain_config",
    "age_projection",
    "search_projection",
    "live_entity_cache",
]
ImpactMode = Literal["direct_mutation", "derived_rebuild", "cache_invalidation"]


class MaintenanceImpactItem(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: ImpactKind
    mode: ImpactMode
    identifiers: tuple[str, ...] = Field(default=(), max_length=128)
    total_count: int = Field(ge=0)
    truncated: bool = False


class MaintenanceImpactPreview(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    review_id: str = Field(min_length=1, max_length=200)
    evidence_state_token: str = Field(pattern=r"^[0-9a-f]{64}$")
    impacts: tuple[MaintenanceImpactItem, ...] = Field(default=(), max_length=32)
    no_applicable_impact: str | None = Field(default=None, max_length=500)


SemanticBlockageKind = Literal["retry_scheduled", "retry_exhausted"]


class SemanticWindowBlockage(BaseModel):
    """Bounded operator view of one failed active semantic window."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    window_id: UUID
    stage: str = Field(min_length=1, max_length=100)
    kind: SemanticBlockageKind
    attempt_count: int = Field(ge=0)
    failure_stage: str = Field(min_length=1, max_length=100)
    failure_code: str = Field(min_length=1, max_length=200)
    failed_at_ms: int = Field(ge=0)
    next_retry_at_ms: int | None = Field(default=None, ge=0)


class OrphanedExchangeBlockage(BaseModel):
    """One old open exchange whose session has no live runtime owner."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    session_id: str = Field(min_length=1, max_length=200)
    user_message_id: int = Field(gt=0)
    opened_at_ms: int = Field(ge=0)


class SessionBlockageInspection(BaseModel):
    """Bounded project inspection with explicit truncation state."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    exchanges: tuple[OrphanedExchangeBlockage, ...] = Field(max_length=100)
    total_count: int = Field(ge=0)
    truncated: bool
