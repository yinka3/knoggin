"""Validated Redis/DLQ representation of an internal ingestion batch."""

from typing import Any, Dict, List, Optional, Set

from pydantic import BaseModel, Field

from common.schema.contracts import (
    BatchResult,
    CandidateSuggestion,
    EngineScope,
    EngineWorkUnit,
    ExtractionTrace,
    RelationshipObservation,
    ResolutionResult,
    ValidationIssue,
)


class DLQResolutionPayload(BaseModel):
    """JSON-safe boundary form of the pipeline-owned resolution result."""

    entity_ids: List[int] = Field(default_factory=list)
    new_ids: Set[int] = Field(default_factory=set)
    alias_ids: Set[int] = Field(default_factory=set)
    entity_msg_map: Dict[int, List[int]] = Field(default_factory=dict)
    alias_updates: Dict[int, List[str]] = Field(default_factory=dict)
    candidate_suggestions: List[Dict[str, Any]] = Field(default_factory=list)

    @classmethod
    def from_resolution(cls, resolution: ResolutionResult) -> "DLQResolutionPayload":
        return cls(
            entity_ids=resolution.entity_ids,
            new_ids=resolution.new_ids,
            alias_ids=resolution.alias_ids,
            entity_msg_map=resolution.entity_msg_map,
            alias_updates=resolution.alias_updates,
            candidate_suggestions=[
                suggestion.to_dict()
                for suggestion in resolution.candidate_suggestions
            ],
        )

    def to_resolution(self) -> ResolutionResult:
        return ResolutionResult(
            entity_ids=self.entity_ids,
            new_ids=self.new_ids,
            alias_ids=self.alias_ids,
            entity_msg_map=self.entity_msg_map,
            alias_updates=self.alias_updates,
            candidate_suggestions=[
                CandidateSuggestion.from_dict(item)
                for item in self.candidate_suggestions
            ],
        )


class DLQPayload(BaseModel):
    """The only serialized representation accepted by ingestion DLQ replay."""

    scope: Optional[EngineScope] = None
    work_unit: Optional[EngineWorkUnit] = None
    trace: ExtractionTrace = Field(default_factory=ExtractionTrace)
    issues: List[ValidationIssue] = Field(default_factory=list)
    resolution: DLQResolutionPayload = Field(default_factory=DLQResolutionPayload)
    relationship_observations: List[RelationshipObservation] = Field(
        default_factory=list
    )
    success: bool = True
    error: Optional[str] = None

    @classmethod
    def from_batch(cls, batch: BatchResult) -> "DLQPayload":
        return cls(
            scope=batch.scope,
            work_unit=batch.work_unit,
            trace=batch.trace,
            issues=batch.issues,
            resolution=DLQResolutionPayload.from_resolution(batch.resolution),
            relationship_observations=batch.relationship_observations,
            success=batch.success,
            error=batch.error,
        )

    def to_batch(self) -> BatchResult:
        return BatchResult(
            scope=self.scope,
            work_unit=self.work_unit,
            trace=self.trace,
            issues=self.issues,
            resolution=self.resolution.to_resolution(),
            relationship_observations=self.relationship_observations,
            success=self.success,
            error=self.error,
        )
