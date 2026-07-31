"""Workflow-owned runtime state for one ingestion operation.

`IngestionBatch` is intentionally not a serialized boundary. Redis/DLQ payloads
serialize an explicit aggregate snapshot at that boundary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Dict, Iterable, List, Optional, Set
from uuid import uuid4

from common.schema.contracts import (
    AliasUpdate,
    CandidateSuggestion,
    EntityWrite,
    EpisodeEligibility,
    ExecutionScope,
    ExtractionTrace,
    MessageEntityRef,
    RelationshipObservation,
    RelationshipWrite,
    SkippedRelationship,
    ValidationIssue,
)
from infrastructure.work_record import WorkRecord


class IngestionStage(StrEnum):
    """The pipeline-local stages that precede persistence."""

    RAW = "raw"
    INPUT_VALIDATED = "input_validated"
    EXTRACTED = "extracted"
    RESOLVED = "resolved"
    RELATIONSHIPS_EXTRACTED = "relationships_extracted"
    COMPLETED = "completed"
    GRAPH_PREPARED = "graph_prepared"
    SEALED = "sealed"
    GRAPH_COMMITTED = "graph_committed"
    COMMITTED = "committed"
    FAILED = "failed"


class IngestionMilestone(StrEnum):
    """Durable side effects completed by an ingestion batch."""

    MESSAGE_LOGS_HANDLED = "message_logs_handled"
    CANDIDATE_SUGGESTIONS_HANDLED = "candidate_suggestions_handled"
    GRAPH_COMMITTED = "graph_committed"
    CHECKPOINT_COMMITTED = "checkpoint_committed"


_ALLOWED_TRANSITIONS = {
    IngestionStage.RAW: {IngestionStage.INPUT_VALIDATED, IngestionStage.FAILED},
    IngestionStage.INPUT_VALIDATED: {
        IngestionStage.EXTRACTED,
        IngestionStage.FAILED,
    },
    IngestionStage.EXTRACTED: {
        IngestionStage.RESOLVED,
        IngestionStage.COMPLETED,
        IngestionStage.FAILED,
    },
    IngestionStage.RESOLVED: {
        IngestionStage.RELATIONSHIPS_EXTRACTED,
        IngestionStage.FAILED,
    },
    IngestionStage.RELATIONSHIPS_EXTRACTED: {
        IngestionStage.COMPLETED,
        IngestionStage.FAILED,
    },
    IngestionStage.COMPLETED: {
        IngestionStage.GRAPH_PREPARED,
        IngestionStage.FAILED,
    },
    IngestionStage.GRAPH_PREPARED: {IngestionStage.SEALED, IngestionStage.FAILED},
    IngestionStage.SEALED: {
        IngestionStage.GRAPH_COMMITTED,
        IngestionStage.FAILED,
    },
    IngestionStage.GRAPH_COMMITTED: {
        IngestionStage.COMMITTED,
        IngestionStage.FAILED,
    },
    IngestionStage.COMMITTED: set(),
    IngestionStage.FAILED: set(),
}


@dataclass(slots=True)
class IngestionBatch:
    """Mutable owner of one pipeline run's runtime state.

    The aggregate owns the runtime values required to carry a message batch
    from extraction through graph persistence.
    """

    batch_id: str
    scope: ExecutionScope
    messages: List[Dict[str, Any]]
    session_text: str
    work_unit: WorkRecord
    stage: IngestionStage = IngestionStage.RAW
    revision: int = 0
    validated_revision: Optional[int] = None
    sealed: bool = False
    released: bool = False
    milestones: Set[IngestionMilestone] = field(default_factory=set)
    trace: ExtractionTrace = field(default_factory=ExtractionTrace)
    issues: List[ValidationIssue] = field(default_factory=list)
    entity_ids: List[int] = field(default_factory=list)
    new_entity_ids: Set[int] = field(default_factory=set)
    alias_updated_ids: Set[int] = field(default_factory=set)
    entity_message_map: Dict[int, List[int]] = field(default_factory=dict)
    alias_updates: Dict[int, List[str]] = field(default_factory=dict)
    candidate_suggestions: List[CandidateSuggestion] = field(default_factory=list)
    relationship_observations: List[RelationshipObservation] = field(
        default_factory=list
    )
    graph_work_unit: Optional[WorkRecord] = None
    safe_entity_ids: Set[int] = field(default_factory=set)
    graph_alias_updates: List[AliasUpdate] = field(default_factory=list)
    entity_writes: List[EntityWrite] = field(default_factory=list)
    relationship_writes: List[RelationshipWrite] = field(default_factory=list)
    message_entity_refs: List[MessageEntityRef] = field(default_factory=list)
    eligible_messages: List[EpisodeEligibility] = field(default_factory=list)
    skipped_relationships: List[SkippedRelationship] = field(default_factory=list)
    zombie_entity_ids: Set[int] = field(default_factory=set)
    dirty_entity_ids: Set[int] = field(default_factory=set)
    success: bool = True
    error: Optional[str] = None

    @classmethod
    def open(
        cls,
        *,
        user_name: str,
        project_id: Optional[str],
        session_id: str,
        messages: Iterable[Dict[str, Any]],
        session_text: str,
        batch_id: Optional[str] = None,
    ) -> "IngestionBatch":
        """Allocate the one aggregate that owns a live pipeline operation."""

        scope = ExecutionScope(
            user_name=user_name,
            session_id=session_id,
            project_id=project_id,
        )
        owned_messages = list(messages)
        return cls(
            batch_id=batch_id or str(uuid4()),
            scope=scope,
            messages=owned_messages,
            session_text=session_text,
            work_unit=WorkRecord.for_ingestion(
                scope,
                [
                    message.get("id") if isinstance(message, dict) else None
                    for message in owned_messages
                ],
            ),
        )

    def _require_mutable(self) -> None:
        if self.released:
            raise RuntimeError("IngestionBatch has been released")
        if self.sealed:
            raise RuntimeError("IngestionBatch has been sealed")

    def _mark_changed(self) -> None:
        self.revision += 1
        self.validated_revision = None

    def advance_to(self, stage: IngestionStage) -> None:
        """Advance through one legal processing transition."""

        self._require_mutable()
        if stage not in _ALLOWED_TRANSITIONS[self.stage]:
            raise ValueError(
                f"Illegal ingestion transition: {self.stage.value} -> {stage.value}"
            )
        self.stage = stage
        self._mark_changed()

    def _advance_durable(self, stage: IngestionStage) -> None:
        """Advance a sealed batch only through its external-commit lifecycle."""

        if self.released:
            raise RuntimeError("IngestionBatch has been released")
        if stage not in _ALLOWED_TRANSITIONS[self.stage]:
            raise ValueError(
                f"Illegal ingestion transition: {self.stage.value} -> {stage.value}"
            )
        self.stage = stage

    def _mark_milestone(self, milestone: IngestionMilestone) -> None:
        if milestone in self.milestones:
            return
        self.milestones.add(milestone)
        if not self.sealed:
            self._mark_changed()

    def validate_input(self) -> None:
        """Validate the minimum shape the current pipeline already depends on."""

        self._require_mutable()
        if self.stage is not IngestionStage.RAW:
            raise ValueError("Input can only be validated from the raw stage")
        if not isinstance(self.session_text, str):
            raise ValueError("IngestionBatch.session_text must be a string")
        for message in self.messages:
            if not isinstance(message, dict):
                raise ValueError("IngestionBatch.messages must contain dictionaries")
            if "id" not in message or "message" not in message:
                raise ValueError(
                    "IngestionBatch messages require both 'id' and 'message'"
                )
        self.advance_to(IngestionStage.INPUT_VALIDATED)
        self.validated_revision = self.revision

    def mark_extracted(self) -> None:
        self.advance_to(IngestionStage.EXTRACTED)

    def set_resolution(
        self,
        *,
        entity_ids: Iterable[int],
        new_entity_ids: Iterable[int],
        alias_updated_ids: Iterable[int],
        entity_message_map: Dict[int, List[int]],
        alias_updates: Dict[int, List[str]],
        candidate_suggestions: Iterable[CandidateSuggestion],
    ) -> None:
        """Apply resolved entity state directly to this batch exactly once."""

        self._require_mutable()
        if self.stage is not IngestionStage.EXTRACTED:
            raise ValueError("Resolution can only be applied after extraction")
        self.entity_ids = list(entity_ids)
        self.new_entity_ids = set(new_entity_ids)
        self.alias_updated_ids = set(alias_updated_ids)
        self.entity_message_map = {
            entity_id: list(message_ids)
            for entity_id, message_ids in entity_message_map.items()
        }
        self.alias_updates = {
            entity_id: list(aliases) for entity_id, aliases in alias_updates.items()
        }
        self.candidate_suggestions = list(candidate_suggestions)
        self.advance_to(IngestionStage.RESOLVED)

    def set_relationship_observations(
        self, observations: Iterable[RelationshipObservation]
    ) -> None:
        """Attach relationship observations produced for the resolved entities."""

        self._require_mutable()
        if self.stage is not IngestionStage.RESOLVED:
            raise ValueError(
                "Relationship observations can only be set after resolution"
            )
        self.relationship_observations = list(observations)
        self.advance_to(IngestionStage.RELATIONSHIPS_EXTRACTED)

    def set_graph_write_buffers(
        self,
        *,
        graph_work_unit: WorkRecord,
        safe_entity_ids: Set[int],
        graph_alias_updates: List[AliasUpdate],
        entity_writes: List[EntityWrite],
        relationship_writes: List[RelationshipWrite],
        message_entity_refs: List[MessageEntityRef],
        eligible_messages: List[EpisodeEligibility],
        skipped_relationships: List[SkippedRelationship],
        zombie_entity_ids: Set[int],
        dirty_entity_ids: Set[int],
    ) -> None:
        """Attach prepared persistence commands without opening a transaction."""

        self._require_mutable()
        if self.stage is not IngestionStage.COMPLETED:
            raise ValueError("Graph writes can only be prepared after completion")
        self.graph_work_unit = graph_work_unit
        self.safe_entity_ids = safe_entity_ids
        self.graph_alias_updates = graph_alias_updates
        self.entity_writes = entity_writes
        self.relationship_writes = relationship_writes
        self.message_entity_refs = message_entity_refs
        self.eligible_messages = eligible_messages
        self.skipped_relationships = skipped_relationships
        self.zombie_entity_ids = zombie_entity_ids
        self.dirty_entity_ids = dirty_entity_ids
        self.advance_to(IngestionStage.GRAPH_PREPARED)

    def validate_graph_writes(self) -> None:
        """Validate prepared graph commands before the persistence boundary."""

        if self.stage is not IngestionStage.GRAPH_PREPARED:
            raise ValueError("Graph writes must be prepared before persistence")
        if self.graph_work_unit is None:
            raise RuntimeError("Prepared graph writes require a graph work unit")

        for field_name, entity_ids in (
            ("entity_ids", self.entity_ids),
            ("safe_entity_ids", self.safe_entity_ids),
            ("new_entity_ids", self.new_entity_ids),
            ("zombie_entity_ids", self.zombie_entity_ids),
            ("dirty_entity_ids", self.dirty_entity_ids),
        ):
            for entity_id in entity_ids:
                if not isinstance(entity_id, int) or isinstance(entity_id, bool):
                    raise TypeError(
                        f"IngestionBatch.{field_name} must contain integer IDs"
                    )
                if entity_id <= 0:
                    raise ValueError(
                        f"IngestionBatch.{field_name} must contain positive IDs"
                    )

        if not self.new_entity_ids.issubset(self.safe_entity_ids):
            raise ValueError(
                "IngestionBatch.new_entity_ids must be a subset of safe_entity_ids"
            )
        if self.safe_entity_ids & self.zombie_entity_ids:
            raise ValueError(
                "IngestionBatch.safe_entity_ids and zombie_entity_ids overlap"
            )
        if not self.dirty_entity_ids.issubset(self.safe_entity_ids):
            raise ValueError(
                "IngestionBatch.dirty_entity_ids must be a subset of safe_entity_ids"
            )

        for field_name, values, expected_type in (
            ("entity_writes", self.entity_writes, EntityWrite),
            ("graph_alias_updates", self.graph_alias_updates, AliasUpdate),
            ("message_entity_refs", self.message_entity_refs, MessageEntityRef),
            ("eligible_messages", self.eligible_messages, EpisodeEligibility),
            ("relationship_writes", self.relationship_writes, RelationshipWrite),
        ):
            if not all(isinstance(value, expected_type) for value in values):
                raise TypeError(
                    f"IngestionBatch.{field_name} must contain {expected_type.__name__}"
                )

    def mark_message_logs_handled(self) -> None:
        self._require_mutable()
        if self.stage is not IngestionStage.COMPLETED:
            raise ValueError("Message logs can only be handled after completion")
        self._mark_milestone(IngestionMilestone.MESSAGE_LOGS_HANDLED)

    def mark_candidate_suggestions_handled(self) -> None:
        self._require_mutable()
        if self.stage is not IngestionStage.COMPLETED:
            raise ValueError("Candidate suggestions can only be handled after completion")
        self._mark_milestone(IngestionMilestone.CANDIDATE_SUGGESTIONS_HANDLED)

    def complete(self) -> None:
        """Mark successful pipeline completion before the persistence boundary."""

        self._require_mutable()
        if self.stage not in {
            IngestionStage.INPUT_VALIDATED,
            IngestionStage.EXTRACTED,
            IngestionStage.RELATIONSHIPS_EXTRACTED,
        }:
            raise ValueError("IngestionBatch cannot complete from the current stage")
        self.advance_to(IngestionStage.COMPLETED)

    def fail(self, error: Exception | str) -> None:
        """Record a pipeline failure without discarding diagnostics."""

        if self.released:
            raise RuntimeError("IngestionBatch has been released")
        self.success = False
        self.error = str(error)
        if self.stage is not IngestionStage.FAILED:
            if self.sealed:
                self._advance_durable(IngestionStage.FAILED)
            else:
                self.advance_to(IngestionStage.FAILED)

    def seal_for_commit(self) -> None:
        """Freeze prepared data after validating it for graph persistence."""

        self._require_mutable()
        if self.stage is not IngestionStage.GRAPH_PREPARED:
            raise ValueError("Only a graph-prepared IngestionBatch can be sealed")
        self.validate_graph_writes()
        self.validated_revision = self.revision
        self.sealed = True
        self._advance_durable(IngestionStage.SEALED)

    def require_sealed_for_commit(self) -> None:
        if self.released:
            raise RuntimeError("IngestionBatch has been released")
        if not self.sealed or self.stage is not IngestionStage.SEALED:
            raise ValueError("Graph persistence requires a sealed IngestionBatch")
        if self.validated_revision != self.revision:
            raise ValueError("IngestionBatch validation is stale")

    def mark_graph_committed(self) -> None:
        self.require_sealed_for_commit()
        self._mark_milestone(IngestionMilestone.GRAPH_COMMITTED)
        self._advance_durable(IngestionStage.GRAPH_COMMITTED)

    def mark_checkpoint_committed(self) -> None:
        if self.released:
            raise RuntimeError("IngestionBatch has been released")
        if self.stage is not IngestionStage.GRAPH_COMMITTED:
            raise ValueError("Checkpointing requires a graph-committed IngestionBatch")
        self._mark_milestone(IngestionMilestone.CHECKPOINT_COMMITTED)
        self._advance_durable(IngestionStage.COMMITTED)

    def release(self) -> None:
        """Discard raw workflow-only input after its owner is finished with it."""

        if self.released:
            return
        self.messages.clear()
        self.session_text = ""
        self.released = True

    def has_graph_mutations(self) -> bool:
        return bool(
            self.relationship_observations
            or self.entity_message_map
            or self.new_entity_ids
            or self.alias_updated_ids
            or self.alias_updates
        )

    def has_graph_writes(self) -> bool:
        """Return whether the graph persistence boundary has work to perform."""

        return self.has_graph_mutations() or bool(self.trace.message_ids)
