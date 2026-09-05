"""Public-safe typed maintenance impact contracts."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

ImpactKind = Literal[
    "entity",
    "relationship_observation",
    "relationship",
    "project_entity_context",
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
