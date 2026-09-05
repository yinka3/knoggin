"""In-memory state for one complete ingestion operation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Mapping, NotRequired, Optional, Set, TypedDict
from uuid import UUID, uuid4

from common.schema.context import ContextBlockSupportRecord, ContextSnapshot
from common.schema.ingestion.contracts import (
    ContextBlockMention,
    ContextEntityResult,
    ContextRelationshipWrite,
    EntityWrite,
    ExecutionScope,
    ExtractionTrace,
    RelationshipObservation,
    ValidationIssue,
)
from common.schema.semantic_window import SemanticWindowRecord, SemanticWindowStage
from core.ingestion.policy import IngestionPolicy
from infrastructure.work_record import WorkRecord


class IngestionMessage(TypedDict):
    """Static shape of one canonical message claimed for ingestion."""

    id: int
    message: str
    timestamp: NotRequired[int | None]
    role: NotRequired[str]


@dataclass(slots=True)
class SemanticWindowBuild:
    """Non-writing Context-first entity build for one committed semantic window.

    This is deliberately separate from ``IngestionBatch``.  It has no session
    transcript and owns only a frozen Context revision, its durable impact
    closure, evidence catalog, policy snapshot, trace, and pending Knowledge
    changes.  Batch 7 is the first owner permitted to persist its result.
    """

    window_id: UUID
    user_name: str
    project_id: str
    context: ContextSnapshot
    impact_block_ids: frozenset[UUID]
    policy: IngestionPolicy
    policy_snapshot: Mapping[str, object]
    block_supports: Mapping[UUID, tuple[ContextBlockSupportRecord, ...]]
    message_text_by_id: Mapping[int, str]
    trace: ExtractionTrace = field(default_factory=ExtractionTrace)
    issues: List[ValidationIssue] = field(default_factory=list)
    mentions: tuple[ContextBlockMention, ...] = ()
    entity_result: ContextEntityResult | None = None
    relationship_writes: tuple[ContextRelationshipWrite, ...] = ()

    @classmethod
    def from_committed_window(
        cls,
        *,
        window: SemanticWindowRecord,
        context: ContextSnapshot,
        impact_block_ids: frozenset[UUID],
        block_supports: Mapping[UUID, tuple[ContextBlockSupportRecord, ...]],
        message_text_by_id: Mapping[int, str],
    ) -> "SemanticWindowBuild":
        """Reopen the exact Context-first entity build after a process restart."""

        if not isinstance(window, SemanticWindowRecord):
            raise TypeError("Semantic window build requires a SemanticWindowRecord")
        if window.stage is not SemanticWindowStage.CONTEXT_COMMITTED:
            raise ValueError("Semantic window build requires context_committed stage")
        if window.context_revision_id != context.revision_id:
            raise ValueError("Semantic window Context revision does not match its checkpoint")
        if window.project_id != context.project_id:
            raise ValueError("Semantic window and Context must share a project")
        if window.domain_version != context.domain_version:
            raise ValueError("Semantic window and Context domain versions differ")
        policy = IngestionPolicy.from_semantic_window_snapshot(
            window.policy_snapshot.get("ingestion_policy")
        )
        if policy.domain.version != window.domain_version:
            raise ValueError("Semantic window policy and domain versions differ")
        return cls(
            window_id=window.window_id,
            user_name=window.user_name,
            project_id=window.project_id,
            context=context,
            impact_block_ids=impact_block_ids,
            policy=policy,
            policy_snapshot=window.policy_snapshot,
            block_supports=block_supports,
            message_text_by_id=message_text_by_id,
        )

    def __post_init__(self) -> None:
        if not isinstance(self.window_id, UUID):
            raise TypeError("SemanticWindowBuild.window_id must be a UUID")
        if not isinstance(self.context, ContextSnapshot):
            raise TypeError("SemanticWindowBuild.context must be a ContextSnapshot")
        if self.context.project_id != self.project_id:
            raise ValueError("SemanticWindowBuild Context must belong to its project")
        if not isinstance(self.policy, IngestionPolicy):
            raise TypeError("SemanticWindowBuild.policy must be an IngestionPolicy")
        if self.policy.domain.version != self.context.domain_version:
            raise ValueError("SemanticWindowBuild policy and Context domain versions differ")
        if not isinstance(self.policy_snapshot, Mapping):
            raise TypeError("SemanticWindowBuild.policy_snapshot must be a mapping")
        current_ids = {block.block_id for block in self.context.blocks}
        if not self.impact_block_ids:
            raise ValueError("SemanticWindowBuild requires a durable impact closure")
        if not all(isinstance(block_id, UUID) for block_id in self.impact_block_ids):
            raise TypeError("SemanticWindowBuild impact block IDs must be UUIDs")
        for block_id, supports in self.block_supports.items():
            if block_id not in current_ids:
                raise ValueError("Context supports must belong to current Context blocks")
            if not isinstance(supports, tuple) or any(
                not isinstance(support, ContextBlockSupportRecord)
                for support in supports
            ):
                raise TypeError("Context supports must be typed support records")
            if any(support.block_id != block_id for support in supports):
                raise ValueError("Context support block IDs must match their mapping key")
        for message_id, text in self.message_text_by_id.items():
            if not isinstance(message_id, int) or message_id <= 0 or not isinstance(text, str):
                raise TypeError("Context evidence messages must map positive IDs to text")

    @property
    def knowledge_input_blocks(self):
        """Current extractable Context blocks inside the durable impact closure."""

        from common.schema.context import AssertionKind

        return tuple(
            block
            for block in self.context.blocks
            if block.block_id in self.impact_block_ids
            and block.assertion_kind
            in {
                AssertionKind.USER_ASSERTED,
                AssertionKind.SOURCE_GROUNDED,
                AssertionKind.HUMAN_ASSERTED,
            }
        )

    def set_mentions(self, mentions: Iterable[ContextBlockMention]) -> None:
        values = tuple(mentions)
        if any(not isinstance(mention, ContextBlockMention) for mention in values):
            raise TypeError("SemanticWindowBuild mentions must be ContextBlockMention")
        input_ids = {block.block_id for block in self.knowledge_input_blocks}
        if any(not set(mention.block_ids).issubset(input_ids) for mention in values):
            raise ValueError("Context mentions must cite eligible input block versions")
        self.mentions = values

    def set_entity_result(self, result: ContextEntityResult) -> None:
        if not isinstance(result, ContextEntityResult):
            raise TypeError("SemanticWindowBuild entity result must be ContextEntityResult")
        input_ids = {block.block_id for block in self.knowledge_input_blocks}
        if any(
            association.block_id not in input_ids
            for association in result.block_entity_associations
        ):
            raise ValueError("Context associations must cite eligible current blocks")
        self.entity_result = result

    def set_relationship_writes(
        self, writes: Iterable[ContextRelationshipWrite]
    ) -> None:
        """Attach Context-only VP-02 output after entity resolution."""

        values = tuple(writes)
        if any(not isinstance(write, ContextRelationshipWrite) for write in values):
            raise TypeError("SemanticWindowBuild relationship writes must be typed")
        if self.entity_result is None:
            raise ValueError("Context relationship extraction requires resolved entities")
        input_ids = {block.block_id for block in self.knowledge_input_blocks}
        entity_ids = set(self.entity_result.entity_ids)
        from common.scoping import IDENTITY_ENTITY_ID

        allowed_entity_ids = entity_ids | {IDENTITY_ENTITY_ID}
        if any(
            not set(write.support_block_ids).issubset(input_ids)
            or write.entity_a_id not in allowed_entity_ids
            or write.entity_b_id not in allowed_entity_ids
            for write in values
        ):
            raise ValueError(
                "Context relationship writes must cite eligible blocks and resolved endpoints"
            )
        self.relationship_writes = values


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
