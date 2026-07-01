"""Engine contract models for LLM outputs and pipeline handoffs."""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Set
from uuid import uuid4

from pydantic import BaseModel, Field

from common.schema.primitives import (
    ConnectionRecord,
    EntityRecord,
    Fact,
    FactRecord,
    ProfileUpdate,
)
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import get_now, get_now_unix


class NERResult(BaseModel):
    """Collection model for NER batch extraction."""

    mentions: List[EntityRecord] = Field(default_factory=list)


class UserConnectionRecord(BaseModel):
    """Connection between the identity root and a candidate entity."""

    entity_name: str
    relationship: str
    confidence: float = Field(1.0, ge=0.0, le=1.0)
    context: Optional[str] = None
    msg_id: int
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ConnectionsResult(BaseModel):
    """Collection model for extracted connections."""

    connections: List[ConnectionRecord] = Field(default_factory=list)
    user_connections: List[UserConnectionRecord] = Field(default_factory=list)


class EntityProfilesResult(BaseModel):
    """Collection model for profile extraction results."""

    profiles: List[ProfileUpdate] = Field(default_factory=list)


class MergeJudgment(BaseModel):
    """Model for deciding if two entities should be merged."""

    should_merge: bool = Field(
        ..., description="True if entities refer to the same real-world concept"
    )
    reasoning: str = Field(..., description="Justification for the decision")
    confidence: float = Field(..., ge=0.0, le=1.0)
    new_canonical_name: Optional[str] = Field(
        None, description="Suggested better name if merging"
    )


class RelevanceResult(BaseModel):
    """Structured response for a single relevance check."""

    index: int = Field(..., description="The 1-based index from the input list")
    is_relevant: bool = Field(
        ..., description="Whether the message relates to the entity's facts"
    )


class BulkRelevanceResult(BaseModel):
    """Collection of relevance results."""

    judgments: List[RelevanceResult] = Field(default_factory=list)


class ContradictionJudgment(BaseModel):
    """Result for a single contradiction check."""

    index: int = Field(..., description="The 1-based index from the input list")
    is_contradiction: bool = Field(..., description="Whether FACT_B contradicts FACT_A")


class BulkContradictionResult(BaseModel):
    """Collection of contradiction judgments."""

    judgments: List[ContradictionJudgment] = Field(default_factory=list)


class TopicDetail(BaseModel):
    """Model for a single topic's configuration."""

    active: bool = Field(default=True)
    labels: List[str] = Field(default_factory=list)
    aliases: List[str] = Field(default_factory=list)
    hierarchy: Dict[str, List[str]] = Field(default_factory=dict)


class TopicConfigResult(BaseModel):
    """Model for the full topic configuration."""

    topics: Dict[str, TopicDetail] = Field(
        ..., description="Map of TopicName to its configuration"
    )


class EngineScope(BaseModel):
    """Execution scope for engine work and serialized batch diagnostics."""

    user_name: str
    session_id: str
    project_id: Optional[str] = None


class ValidationIssue(BaseModel):
    """Structured validation or fallback issue recorded during engine processing."""

    stage: str
    code: str
    message: str
    severity: Literal["info", "warning", "error"] = "warning"
    item_ref: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ExtractionTrace(BaseModel):
    """Counters and labels describing an ingestion extraction pass."""

    message_ids: List[int] = Field(default_factory=list)
    batch_size: int = 0
    entity_model: Optional[str] = None
    entity_prompt: Optional[str] = None
    relationship_model: Optional[str] = None
    relationship_prompt: Optional[str] = None
    known_mentions: int = 0
    gliner_raw_mentions: int = 0
    gliner_accepted_mentions: int = 0
    llm_mentions_seen: int = 0
    llm_mentions_accepted: int = 0
    llm_mentions_rejected: int = 0
    relationships_seen: int = 0
    relationships_accepted: int = 0
    relationships_rejected: int = 0
    user_relationships_seen: int = 0
    user_relationships_accepted: int = 0
    user_relationships_rejected: int = 0
    fallbacks: List[Dict[str, str]] = Field(default_factory=list)


EngineWorkKind = Literal[
    "message_batch",
    "graph_write",
    "dlq_replay",
    "profile_refinement",
    "merge_detection",
    "topic_evolution",
    "cleanup",
    "archival",
]

EngineWorkStatus = Literal[
    "pending",
    "running",
    "succeeded",
    "failed",
    "deferred",
    "skipped",
]


class EngineResourceProfile(BaseModel):
    """Resource expectations for future engine scheduling and allocation."""

    cpu_weight: int = 1
    memory_weight: int = 1
    llm_calls_expected: int = 0
    embedding_calls_expected: int = 0
    graph_write_expected: bool = False


class EngineWorkTrace(BaseModel):
    """Timing and attempt metadata for an engine work unit."""

    created_at: datetime = Field(default_factory=get_now)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_ms: Optional[int] = None
    attempt: int = 1
    stage: Optional[str] = None
    summary: Optional[str] = None


class EngineWorkUnit(BaseModel):
    """Lightweight envelope for observable engine work."""

    id: str = Field(default_factory=lambda: uuid4().hex)
    kind: EngineWorkKind
    scope: EngineScope
    status: EngineWorkStatus = "pending"
    priority: int = 100
    resource_profile: EngineResourceProfile = Field(
        default_factory=EngineResourceProfile
    )
    trace: EngineWorkTrace = Field(default_factory=EngineWorkTrace)
    issues: List[ValidationIssue] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def for_message_batch(
        cls, scope: EngineScope, message_ids: List[int], priority: int = 100
    ) -> "EngineWorkUnit":
        return cls(
            kind="message_batch",
            scope=scope,
            priority=priority,
            resource_profile=EngineResourceProfile(
                cpu_weight=2,
                memory_weight=2,
                llm_calls_expected=2,
                embedding_calls_expected=1,
                graph_write_expected=True,
            ),
            trace=EngineWorkTrace(stage="message_batch"),
            metadata={"message_ids": list(message_ids), "batch_size": len(message_ids)},
        )

    @classmethod
    def for_graph_write(
        cls,
        scope: EngineScope,
        batch_id: Optional[str] = None,
        priority: int = 90,
    ) -> "EngineWorkUnit":
        metadata = {"batch_work_unit_id": batch_id} if batch_id else {}
        return cls(
            kind="graph_write",
            scope=scope,
            priority=priority,
            resource_profile=EngineResourceProfile(graph_write_expected=True),
            trace=EngineWorkTrace(stage="graph_write"),
            metadata=metadata,
        )

    @classmethod
    def for_dlq_replay(
        cls,
        scope: EngineScope,
        stage: str,
        attempt: int,
        priority: int = 80,
    ) -> "EngineWorkUnit":
        return cls(
            kind="dlq_replay",
            scope=scope,
            priority=priority,
            trace=EngineWorkTrace(attempt=attempt, stage=stage),
            metadata={"stage": stage},
        )

    def mark_running(self) -> None:
        self.status = "running"
        self.trace.started_at = get_now()

    def mark_succeeded(self, summary: Optional[str] = None) -> None:
        self.status = "succeeded"
        self.trace.summary = summary
        self._finish()

    def mark_failed(self, error: str) -> None:
        self.status = "failed"
        self.trace.summary = error
        self._finish()

    def mark_skipped(self, summary: Optional[str] = None) -> None:
        self.status = "skipped"
        self.trace.summary = summary
        self._finish()

    def add_issue(
        self,
        stage: str,
        code: str,
        message: str,
        severity: Literal["info", "warning", "error"] = "warning",
        item_ref: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.issues.append(
            ValidationIssue(
                stage=stage,
                code=code,
                message=message,
                severity=severity,
                item_ref=item_ref,
                metadata=metadata or {},
            )
        )

    def _finish(self) -> None:
        self.trace.finished_at = get_now()
        if self.trace.started_at:
            delta = self.trace.finished_at - self.trace.started_at
            self.trace.duration_ms = int(delta.total_seconds() * 1000)


class MessageConnections(BaseModel):
    """Relationship observations grounded to a single source message."""

    message_id: int
    entity_pairs: List[ConnectionRecord] = Field(default_factory=list)


class MessageUserConnections(BaseModel):
    """User-root relationship observations grounded to a single source message."""

    message_id: int
    user_connections: List[UserConnectionRecord] = Field(default_factory=list)


class EntityWrite(BaseModel):
    """Typed entity payload intended for graph persistence."""

    id: int
    is_new: bool
    canonical_name: str
    type: str
    confidence: float = 1.0
    topic: str = "General"
    embedding: Optional[List[float]] = None
    aliases: List[str] = Field(default_factory=list)
    user_name: str
    session_id: str
    project_id: str


class RelationshipWrite(BaseModel):
    """Typed relationship payload intended for graph persistence."""

    entity_a: str
    entity_b: str
    entity_a_id: int
    entity_b_id: int
    message_id: str
    evidence_ref: Dict[str, Any]
    user_name: str
    session_id: str
    project_id: str
    confidence: float
    context: Optional[str] = None


class UserRelationshipWrite(BaseModel):
    """Typed user-root relationship payload intended for graph persistence."""

    user_entity_id: int
    entity_name: str
    entity_id: int
    message_id: str
    evidence_ref: Dict[str, Any]
    user_name: str
    session_id: str
    project_id: str
    confidence: float
    context: Optional[str] = None

    def to_relationship_payload(self) -> dict:
        return {
            "entity_a": self.user_name,
            "entity_b": self.entity_name,
            "entity_a_id": self.user_entity_id,
            "entity_b_id": self.entity_id,
            "message_id": self.message_id,
            "evidence_ref": self.evidence_ref,
            "user_name": self.user_name,
            "session_id": self.session_id,
            "project_id": self.project_id,
            "confidence": self.confidence,
            "context": self.context,
        }


class AliasUpdate(BaseModel):
    """Aliases to persist for a canonical entity."""

    entity_id: int
    aliases: List[str] = Field(default_factory=list)


class SkippedRelationship(BaseModel):
    """Relationship observation skipped before graph persistence."""

    entity_a: Optional[str] = None
    entity_b: Optional[str] = None
    message_id: int
    reason: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class GraphMutationPlan(BaseModel):
    """Typed graph-write intent derived from a processed batch."""

    work_unit: EngineWorkUnit
    scope: EngineScope
    entity_ids: List[int] = Field(default_factory=list)
    safe_entity_ids: Set[int] = Field(default_factory=set)
    new_entity_ids: Set[int] = Field(default_factory=set)
    alias_updates: List[AliasUpdate] = Field(default_factory=list)
    entity_writes: List[EntityWrite] = Field(default_factory=list)
    relationship_writes: List[RelationshipWrite] = Field(default_factory=list)
    user_relationship_writes: List[UserRelationshipWrite] = Field(default_factory=list)
    skipped_relationships: List[SkippedRelationship] = Field(default_factory=list)
    dirty_entity_ids: Set[int] = Field(default_factory=set)
    zombie_entity_ids: Set[int] = Field(default_factory=set)

    def has_writes(self) -> bool:
        return bool(
            self.entity_writes
            or self.relationship_writes
            or self.user_relationship_writes
            or self.alias_updates
        )

    def to_graph_payloads(self) -> tuple[List[dict], List[dict]]:
        return (
            [entity.model_dump(mode="json") for entity in self.entity_writes],
            [
                relationship.model_dump(mode="json")
                for relationship in self.relationship_writes
            ]
            + [
                relationship.to_relationship_payload()
                for relationship in self.user_relationship_writes
            ],
        )


class GraphWriteSummary(BaseModel):
    """Counts from executing a graph mutation plan."""

    entities_written: int = 0
    relationships_written: int = 0
    user_relationships_written: int = 0
    aliases_updated: int = 0
    dirty_entities_marked: int = 0
    zombies_filtered: int = 0
    relationships_skipped: int = 0


class SkippedFactChange(BaseModel):
    """Fact change skipped or left unresolved during profile refinement."""

    content: Optional[str] = None
    reason: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class FactMergeResult(BaseModel):
    """Fact merge decisions derived from LLM-extracted profile facts."""

    to_invalidate: List[str] = Field(default_factory=list)
    new_contents: List[Fact] = Field(default_factory=list)
    skipped: List[SkippedFactChange] = Field(default_factory=list)
    missing_targets: List[SkippedFactChange] = Field(default_factory=list)


class FactResolutionSummary(BaseModel):
    """Result of applying fact merge decisions to graph persistence."""

    active_facts: List[FactRecord] = Field(default_factory=list)
    created_facts: List[FactRecord] = Field(default_factory=list)
    invalidated_fact_ids: List[str] = Field(default_factory=list)
    failed_invalidations: List[str] = Field(default_factory=list)
    contradicted_fact_ids: List[str] = Field(default_factory=list)
    invalid_source_msg_ids: List[int] = Field(default_factory=list)
    write_failed: bool = False
    error: Optional[str] = None


@dataclass
class ResolutionResult:
    """Result from EntityResolver batch resolution."""

    entity_ids: List[int]
    new_ids: Set[int]
    alias_ids: Set[int]
    entity_msg_map: Dict[int, List[int]]
    alias_updates: Dict[int, List[str]]


@dataclass
class BatchResult:
    """Result of processing a batch of messages."""

    scope: Optional[EngineScope] = None
    work_unit: Optional[EngineWorkUnit] = None
    trace: ExtractionTrace = field(default_factory=ExtractionTrace)
    issues: List[ValidationIssue] = field(default_factory=list)
    entity_ids: List[int] = field(default_factory=list)
    new_entity_ids: Set[int] = field(default_factory=set)
    alias_updated_ids: Set[int] = field(default_factory=set)
    alias_updates: Dict[int, List[str]] = field(default_factory=dict)
    relationship_observations: List[MessageConnections] = field(default_factory=list)
    user_relationship_observations: List[MessageUserConnections] = field(
        default_factory=list
    )
    success: bool = True
    error: Optional[str] = None

    def set_scope(
        self, user_name: str, session_id: str, project_id: Optional[str] = None
    ) -> None:
        self.scope = EngineScope(
            user_name=user_name, session_id=session_id, project_id=project_id
        )

    def add_issue(
        self,
        stage: str,
        code: str,
        message: str,
        severity: Literal["info", "warning", "error"] = "warning",
        item_ref: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.issues.append(
            ValidationIssue(
                stage=stage,
                code=code,
                message=message,
                severity=severity,
                item_ref=item_ref,
                metadata=metadata or {},
            )
        )

    def record_fallback(self, stage: str, fallback: str) -> None:
        self.trace.fallbacks.append({"stage": stage, "fallback": fallback})

    def has_graph_writes(self) -> bool:
        return bool(
            self.relationship_observations
            or self.user_relationship_observations
            or self.new_entity_ids
            or self.alias_updated_ids
            or self.alias_updates
        )

    def to_dict(self) -> dict:
        """Serialize for DLQ storage."""

        return {
            "scope": self.scope.model_dump(mode="json") if self.scope else None,
            "work_unit": (
                self.work_unit.model_dump(mode="json") if self.work_unit else None
            ),
            "trace": self.trace.model_dump(mode="json"),
            "issues": [issue.model_dump(mode="json") for issue in self.issues],
            "entity_ids": self.entity_ids,
            "new_entity_ids": list(self.new_entity_ids),
            "alias_updated_ids": list(self.alias_updated_ids),
            "alias_updates": {str(k): v for k, v in self.alias_updates.items()},
            "relationship_observations": [
                item.model_dump(mode="json") for item in self.relationship_observations
            ],
            "user_relationship_observations": [
                item.model_dump(mode="json")
                for item in self.user_relationship_observations
            ],
            "success": self.success,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "BatchResult":
        """Deserialize from DLQ storage."""

        return cls(
            scope=(
                EngineScope.model_validate(data["scope"])
                if data.get("scope")
                else None
            ),
            work_unit=(
                EngineWorkUnit.model_validate(data["work_unit"])
                if data.get("work_unit")
                else None
            ),
            trace=ExtractionTrace.model_validate(data.get("trace", {})),
            issues=[
                ValidationIssue.model_validate(item)
                for item in data.get("issues", [])
            ],
            entity_ids=data.get("entity_ids", []),
            new_entity_ids=set(data.get("new_entity_ids", [])),
            alias_updated_ids=set(data.get("alias_updated_ids", [])),
            alias_updates={
                int(k): v
                for k, v in data.get("alias_updates", {}).items()
                if str(k).isdigit()
            },
            relationship_observations=[
                MessageConnections.model_validate(item)
                for item in data.get("relationship_observations", [])
            ],
            user_relationship_observations=[
                MessageUserConnections.model_validate(item)
                for item in data.get("user_relationship_observations", [])
            ],
            success=data.get("success", True),
            error=data.get("error"),
        )


@dataclass
class DLQEntry:
    messages: List[Dict]
    session_text: str
    error: str
    attempt: int = 1
    timestamp: float = field(default_factory=get_now_unix)
    batch_size: int = field(init=False)

    def __post_init__(self):
        self.batch_size = len(self.messages)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str) -> "DLQEntry":
        data = safe_json_loads(raw)
        if not data:
            raise ValueError("Failed to parse DLQEntry")
        data.pop("batch_size", None)
        return cls(**data)

    def is_transient(self, transient_errors: List[str]) -> bool:
        return any(t in self.error for t in transient_errors)
