"""In-memory state for one complete ingestion operation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, NotRequired, Optional, Set, TypedDict
from uuid import uuid4

from common.schema.ingestion.contracts import (
    EntityWrite,
    ExecutionScope,
    ExtractionTrace,
    RelationshipObservation,
    ValidationIssue,
)
from core.ingestion.policy import IngestionPolicy
from infrastructure.work_record import WorkRecord


class IngestionMessage(TypedDict):
    """Static shape of one canonical message claimed for ingestion."""

    id: int
    message: str
    timestamp: NotRequired[int | None]
    role: NotRequired[str]


@dataclass(slots=True)
class IngestionBatch:
    """Mutable pipeline state kept only until its durable commit completes."""

    batch_id: str
    scope: ExecutionScope
    messages: List[IngestionMessage]
    session_text: str
    work_unit: WorkRecord
    policy: IngestionPolicy
    trace: ExtractionTrace = field(default_factory=ExtractionTrace)
    issues: List[ValidationIssue] = field(default_factory=list)
    entity_ids: List[int] = field(default_factory=list)
    new_entity_ids: Set[int] = field(default_factory=set)
    alias_updated_ids: Set[int] = field(default_factory=set)
    entity_message_map: Dict[int, List[int]] = field(default_factory=dict)
    alias_updates: Dict[int, List[str]] = field(default_factory=dict)
    pending_entity_writes: Dict[int, EntityWrite] = field(default_factory=dict)
    relationship_observations: List[RelationshipObservation] = field(
        default_factory=list
    )
    released: bool = False

    @classmethod
    def open(
        cls,
        *,
        user_name: str,
        project_id: Optional[str],
        session_id: str,
        messages: Iterable[IngestionMessage],
        session_text: str,
        policy: IngestionPolicy,
        batch_id: Optional[str] = None,
    ) -> "IngestionBatch":
        """Allocate the state container for one live ingestion operation."""

        scope = ExecutionScope(
            user_name=user_name,
            session_id=session_id,
            project_id=project_id,
        )
        owned_messages = list(messages)
        work_unit = WorkRecord.for_ingestion(
            scope,
            [
                message.get("id") if isinstance(message, dict) else None
                for message in owned_messages
            ],
        )
        work_unit.metadata["domain_version"] = policy.domain.version
        return cls(
            batch_id=batch_id or str(uuid4()),
            scope=scope,
            messages=owned_messages,
            session_text=session_text,
            policy=policy,
            work_unit=work_unit,
        )

    def _require_active(self) -> None:
        if self.released:
            raise RuntimeError("IngestionBatch has been released")

    def validate_input(self) -> None:
        """Validate the canonical input the pipeline already depends on."""

        self._require_active()
        if not isinstance(self.session_text, str):
            raise ValueError("IngestionBatch.session_text must be a string")
        for message in self.messages:
            if not isinstance(message, dict):
                raise ValueError("IngestionBatch.messages must contain dictionaries")
            if "id" not in message or "message" not in message:
                raise ValueError(
                    "IngestionBatch messages require both 'id' and 'message'"
                )

    def set_resolution(
        self,
        *,
        entity_ids: Iterable[int],
        new_entity_ids: Iterable[int],
        alias_updated_ids: Iterable[int],
        entity_message_map: Dict[int, List[int]],
        alias_updates: Dict[int, List[str]],
        pending_entity_writes: Optional[Dict[int, EntityWrite]] = None,
    ) -> None:
        """Apply the entity-resolution decisions for this batch exactly once."""

        self._require_active()
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
        self.pending_entity_writes = dict(pending_entity_writes or {})

    def set_relationship_observations(
        self, observations: Iterable[RelationshipObservation]
    ) -> None:
        """Attach relationship observations produced for resolved entities."""

        self._require_active()
        self.relationship_observations = list(observations)

    def release(self) -> None:
        """Discard raw workflow-only input once the operation is finished."""

        if self.released:
            return
        self.messages.clear()
        self.session_text = ""
        self.released = True
