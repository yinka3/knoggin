"""Versioned Redis/DLQ snapshot for one ingestion aggregate."""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from common.schema.ingestion.contracts import (
    CandidateSuggestion,
    ExecutionScope,
    ExtractionTrace,
    RelationshipObservation,
    ValidationIssue,
)
from core.ingestion.batch import (
    IngestionBatch,
    IngestionMilestone,
    IngestionStage,
)
from core.ingestion.policy import IngestionPolicy
from infrastructure.work_record import WorkRecord


class DLQPayload(BaseModel):
    """The only serialized ingestion replay representation for this release."""

    schema_version: Literal[3] = 3
    batch_id: str
    batch_stage: str
    sealed: bool = False
    revision: int = 0
    validated_revision: Optional[int] = None
    milestones: List[IngestionMilestone] = Field(default_factory=list)
    messages: List[Dict[str, Any]] = Field(default_factory=list)
    session_text: str = ""
    scope: ExecutionScope
    work_record: Dict[str, Any]
    policy: Dict[str, Any]
    trace: ExtractionTrace = Field(default_factory=ExtractionTrace)
    issues: List[ValidationIssue] = Field(default_factory=list)
    entity_ids: List[int] = Field(default_factory=list)
    new_entity_ids: set[int] = Field(default_factory=set)
    alias_updated_ids: set[int] = Field(default_factory=set)
    entity_message_map: Dict[int, List[int]] = Field(default_factory=dict)
    alias_updates: Dict[int, List[str]] = Field(default_factory=dict)
    candidate_suggestions: List[Dict[str, Any]] = Field(default_factory=list)
    relationship_observations: List[RelationshipObservation] = Field(
        default_factory=list
    )
    checkpoint_interval: Optional[int] = None
    checkpoint_count: Optional[int] = None
    success: bool = True
    error: Optional[str] = None

    @classmethod
    def from_ingestion_batch(cls, batch: IngestionBatch) -> "DLQPayload":
        if not isinstance(batch, IngestionBatch):
            raise TypeError("from_ingestion_batch requires an IngestionBatch")
        return cls(
            batch_id=batch.batch_id,
            batch_stage=batch.stage.value,
            sealed=batch.sealed,
            revision=batch.revision,
            validated_revision=batch.validated_revision,
            milestones=sorted(batch.milestones, key=str),
            messages=[dict(message) for message in batch.messages],
            session_text=batch.session_text,
            scope=batch.scope,
            work_record=batch.work_unit.snapshot(),
            policy=batch.policy.to_dict(),
            trace=batch.trace,
            issues=list(batch.issues),
            entity_ids=list(batch.entity_ids),
            new_entity_ids=set(batch.new_entity_ids),
            alias_updated_ids=set(batch.alias_updated_ids),
            entity_message_map={
                entity_id: list(message_ids)
                for entity_id, message_ids in batch.entity_message_map.items()
            },
            alias_updates={
                entity_id: list(aliases)
                for entity_id, aliases in batch.alias_updates.items()
            },
            candidate_suggestions=[
                suggestion.to_dict() for suggestion in batch.candidate_suggestions
            ],
            relationship_observations=list(batch.relationship_observations),
            checkpoint_interval=batch.checkpoint_interval,
            checkpoint_count=batch.checkpoint_count,
            success=batch.success,
            error=batch.error,
        )

    def to_ingestion_batch(self) -> IngestionBatch:
        """Hydrate aggregate state for replay without legacy result objects."""

        batch = IngestionBatch.open(
            user_name=self.scope.user_name,
            project_id=self.scope.project_id,
            session_id=self.scope.session_id,
            messages=self.messages,
            session_text=self.session_text,
            policy=IngestionPolicy.from_dict(self.policy),
            batch_id=self.batch_id,
        )
        batch.work_unit = WorkRecord.from_snapshot(self.work_record)
        batch.trace = self.trace
        batch.issues = list(self.issues)
        batch.entity_ids = list(self.entity_ids)
        batch.new_entity_ids = set(self.new_entity_ids)
        batch.alias_updated_ids = set(self.alias_updated_ids)
        batch.entity_message_map = {
            entity_id: list(message_ids)
            for entity_id, message_ids in self.entity_message_map.items()
        }
        batch.alias_updates = {
            entity_id: list(aliases)
            for entity_id, aliases in self.alias_updates.items()
        }
        batch.candidate_suggestions = [
            CandidateSuggestion.from_dict(item) for item in self.candidate_suggestions
        ]
        batch.relationship_observations = list(self.relationship_observations)
        batch.checkpoint_interval = self.checkpoint_interval
        batch.checkpoint_count = self.checkpoint_count
        batch.success = self.success
        batch.error = self.error
        batch.milestones = set(self.milestones)
        batch.revision = self.revision
        batch.validated_revision = self.validated_revision
        # Graph buffers are deliberately rebuilt only when graph persistence
        # was not completed.  A checkpoint-only replay must retain the sealed
        # durable state so it can advance directly to COMMITTED.
        if not self.success:
            batch.stage = IngestionStage.FAILED
            batch.sealed = self.sealed
        elif IngestionMilestone.CHECKPOINT_COMMITTED in batch.milestones:
            batch.stage = IngestionStage.COMMITTED
            batch.sealed = True
        elif IngestionMilestone.GRAPH_COMMITTED in batch.milestones:
            batch.stage = IngestionStage.GRAPH_COMMITTED
            batch.sealed = True
        else:
            batch.stage = IngestionStage.COMPLETED
            batch.sealed = False
        return batch
