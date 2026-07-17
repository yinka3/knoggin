"""Engine contract models for LLM outputs and pipeline handoffs."""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Set
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator

from common.schema.primitives import (
    Connection,
    ConnectionRecord,
    Entity,
)
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import get_now, get_now_unix


class NERMention(Entity):
    """One model-returned entity mention with a local message reference."""

    msg_id: str = Field(..., pattern=r"^m[1-9]\d*$")


class NERResult(BaseModel):
    """Collection model for model-returned NER mentions."""

    mentions: List[NERMention] = Field(default_factory=list)


class ConnectionMention(Connection):
    """One model-returned relationship with a local message reference."""

    msg_id: str = Field(..., pattern=r"^m[1-9]\d*$")


class UserConnectionRecord(BaseModel):
    """Connection between the identity root and a candidate entity."""

    entity_name: str
    relationship: str
    confidence: float = Field(1.0, ge=0.0, le=1.0)
    context: Optional[str] = None
    msg_id: int
    metadata: Dict[str, Any] = Field(default_factory=dict)


class UserConnectionMention(BaseModel):
    """One model-returned identity relationship with a local message reference."""

    entity_name: str
    relationship: str
    confidence: float = Field(1.0, ge=0.0, le=1.0)
    context: Optional[str] = None
    msg_id: str = Field(..., pattern=r"^m[1-9]\d*$")


class ConnectionsResult(BaseModel):
    """Collection model for extracted connections."""

    connections: List[ConnectionMention] = Field(default_factory=list)
    user_connections: List[UserConnectionMention] = Field(default_factory=list)


class EpisodeMessageInfluence(BaseModel):
    """LLM-selected influence for one message in an eligible episode window."""

    message_id: int = Field(..., gt=0)
    influence_weight: float = Field(..., ge=0.0)
    influence_reason: Optional[str] = None


class EpisodeFocusEntitySelection(BaseModel):
    """A focus marker selected from the candidate window's entity set."""

    entity_id: int = Field(..., gt=0)
    prominence_weight: float = Field(..., ge=0.0)
    role: Optional[str] = None


class EpisodeCentralRelationshipSelection(BaseModel):
    """A central marker selected from the candidate window's relationships."""

    relationship_id: str = Field(..., min_length=1)
    prominence_weight: float = Field(..., ge=0.0)


def _validate_episode_decision_shape(decision: Any) -> None:
    """Apply the shared action rules to internal and model-facing decisions."""

    if decision.action == "skip":
        if not decision.skip_reason:
            raise ValueError("skip decisions require skip_reason")
        if (
            decision.target_episode_id
            or decision.summary
            or decision.new_developments
            or decision.updates
            or decision.unresolved
            or decision.importance
            or decision.message_influences
            or decision.focus_entities
            or decision.central_relationships
        ):
            raise ValueError("skip decisions must not include episode content")
        return

    if not decision.summary:
        raise ValueError("create and consolidate decisions require summary")
    if not decision.message_influences:
        raise ValueError("create and consolidate decisions require message influences")
    if decision.action == "consolidate" and not decision.target_episode_id:
        raise ValueError("consolidate decisions require target_episode_id")
    if decision.action == "create" and decision.target_episode_id:
        raise ValueError("create decisions must not include target_episode_id")
    if decision.skip_reason:
        raise ValueError("non-skip decisions must not include skip_reason")


class EpisodeDecision(BaseModel):
    """Resolved internal decision for one bounded episodic-memory window."""

    action: Literal["create", "consolidate", "skip"]
    target_episode_id: Optional[str] = Field(None, min_length=1)
    summary: Optional[str] = Field(None, min_length=1)
    new_developments: List[str] = Field(default_factory=list)
    updates: List[str] = Field(default_factory=list)
    unresolved: List[str] = Field(default_factory=list)
    importance: float = Field(0.0, ge=0.0, le=1.0)
    message_influences: List[EpisodeMessageInfluence] = Field(default_factory=list)
    focus_entities: List[EpisodeFocusEntitySelection] = Field(default_factory=list)
    central_relationships: List[EpisodeCentralRelationshipSelection] = Field(
        default_factory=list
    )
    skip_reason: Optional[str] = Field(None, min_length=1)

    @model_validator(mode="after")
    def validate_action_shape(self) -> "EpisodeDecision":
        _validate_episode_decision_shape(self)
        return self


class EpisodeConsolidation(BaseModel):
    """Resolved internal regeneration for one selected episode."""

    summary: str = Field(..., min_length=1)
    new_developments: List[str] = Field(default_factory=list)
    updates: List[str] = Field(default_factory=list)
    unresolved: List[str] = Field(default_factory=list)
    importance: float = Field(0.0, ge=0.0, le=1.0)
    message_influences: List[EpisodeMessageInfluence] = Field(
        ..., min_length=1
    )
    focus_entities: List[EpisodeFocusEntitySelection] = Field(default_factory=list)
    central_relationships: List[EpisodeCentralRelationshipSelection] = Field(
        default_factory=list
    )


class LLMEpisodeMessageInfluence(BaseModel):
    """One model-selected influence with a local message reference."""

    message_id: str = Field(..., pattern=r"^m[1-9]\d*$")
    influence_weight: float = Field(..., ge=0.0)
    influence_reason: Optional[str] = None


class LLMEpisodeFocusEntitySelection(BaseModel):
    """One model-selected focus entity with a local entity reference."""

    entity_id: str = Field(..., pattern=r"^e[1-9]\d*$")
    prominence_weight: float = Field(..., ge=0.0)
    role: Optional[str] = None


class LLMEpisodeCentralRelationshipSelection(BaseModel):
    """One model-selected relationship with a local relationship reference."""

    relationship_id: str = Field(..., pattern=r"^r[1-9]\d*$")
    prominence_weight: float = Field(..., ge=0.0)


class LLMEpisodeDecision(BaseModel):
    """Model-facing episode decision that contains only local references."""

    action: Literal["create", "consolidate", "skip"]
    target_episode_id: Optional[str] = Field(None, pattern=r"^ep[1-9]\d*$")
    summary: Optional[str] = Field(None, min_length=1)
    new_developments: List[str] = Field(default_factory=list)
    updates: List[str] = Field(default_factory=list)
    unresolved: List[str] = Field(default_factory=list)
    importance: float = Field(0.0, ge=0.0, le=1.0)
    message_influences: List[LLMEpisodeMessageInfluence] = Field(default_factory=list)
    focus_entities: List[LLMEpisodeFocusEntitySelection] = Field(
        default_factory=list
    )
    central_relationships: List[LLMEpisodeCentralRelationshipSelection] = Field(
        default_factory=list
    )
    skip_reason: Optional[str] = Field(None, min_length=1)

    @model_validator(mode="after")
    def validate_action_shape(self) -> "LLMEpisodeDecision":
        _validate_episode_decision_shape(self)
        return self


class LLMEpisodeConsolidation(BaseModel):
    """Model-facing episode regeneration output with local references."""

    summary: str = Field(..., min_length=1)
    new_developments: List[str] = Field(default_factory=list)
    updates: List[str] = Field(default_factory=list)
    unresolved: List[str] = Field(default_factory=list)
    importance: float = Field(0.0, ge=0.0, le=1.0)
    message_influences: List[LLMEpisodeMessageInfluence] = Field(
        ..., min_length=1
    )
    focus_entities: List[LLMEpisodeFocusEntitySelection] = Field(
        default_factory=list
    )
    central_relationships: List[LLMEpisodeCentralRelationshipSelection] = Field(
        default_factory=list
    )


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
    "merge_detection",
    "topic_evolution",
    "cleanup",
    "embedding",
    "rerank",
    "nli",
    "spacy",
    "gliner",
    "document_index",
    "model_load",
]

EngineWorkStatus = Literal[
    "pending",
    "running",
    "succeeded",
    "failed",
    "deferred",
    "skipped",
    "cancelled",
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
    queued_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    queue_wait_ms: Optional[int] = None
    duration_ms: Optional[int] = None
    attempt: int = 1
    stage: Optional[str] = None
    summary: Optional[str] = None


class EngineWorkUnit(BaseModel):
    """Lightweight envelope for observable engine work."""

    id: str = Field(default_factory=lambda: uuid4().hex)
    kind: EngineWorkKind
    scope: EngineScope
    parent_work_unit_id: Optional[str] = None
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

    @classmethod
    def for_model_operation(
        cls,
        kind: Literal["embedding", "rerank", "nli", "spacy", "gliner", "document_index", "model_load"],
        scope: EngineScope,
        *,
        parent_work_unit_id: Optional[str] = None,
        priority: int = 100,
        stage: Optional[str] = None,
    ) -> "EngineWorkUnit":
        embedding_calls = 1 if kind == "embedding" else 0
        cpu_weight = 2 if kind in {"spacy", "gliner", "document_index"} else 1
        memory_weight = 2 if kind in {"embedding", "rerank", "nli", "gliner"} else 1
        return cls(
            kind=kind,
            scope=scope,
            parent_work_unit_id=parent_work_unit_id,
            priority=priority,
            resource_profile=EngineResourceProfile(
                cpu_weight=cpu_weight,
                memory_weight=memory_weight,
                embedding_calls_expected=embedding_calls,
            ),
            trace=EngineWorkTrace(stage=stage or kind),
        )

    def mark_queued(self) -> None:
        self.status = "pending"
        self.trace.queued_at = get_now()

    def mark_running(self) -> None:
        self.status = "running"
        self.trace.started_at = get_now()
        queued_at = self.trace.queued_at or self.trace.created_at
        self.trace.queue_wait_ms = int(
            (self.trace.started_at - queued_at).total_seconds() * 1000
        )

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

    def mark_cancelled(self, summary: Optional[str] = None) -> None:
        self.status = "cancelled"
        self.trace.summary = summary
        self._finish()

    def add_model_work_summary(self, child: "EngineWorkUnit") -> None:
        summaries = self.metadata.setdefault("model_work", [])
        summaries.append(
            {
                "id": child.id,
                "kind": child.kind,
                "status": child.status,
                "priority": child.priority,
                "stage": child.trace.stage,
                "queue_wait_ms": child.trace.queue_wait_ms,
                "duration_ms": child.trace.duration_ms,
                "summary": child.trace.summary,
            }
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


class MessageEntityRef(BaseModel):
    """A resolved entity mention grounded to one canonical message."""

    message_id: int = Field(..., gt=0)
    entity_id: int = Field(..., gt=0)


class EpisodeEligibility(BaseModel):
    """Episode-processing eligibility attached to a canonical message."""

    message_id: int = Field(..., gt=0)
    episode_type: Optional[str] = None


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
    message_entity_refs: List[MessageEntityRef] = Field(default_factory=list)
    eligible_messages: List[EpisodeEligibility] = Field(default_factory=list)
    relationship_writes: List[RelationshipWrite] = Field(default_factory=list)
    user_relationship_writes: List[UserRelationshipWrite] = Field(default_factory=list)
    skipped_relationships: List[SkippedRelationship] = Field(default_factory=list)
    zombie_entity_ids: Set[int] = Field(default_factory=set)
    dirty_entity_ids: Set[int] = Field(default_factory=set)

    def has_writes(self) -> bool:
        return bool(
            self.entity_writes
            or self.message_entity_refs
            or self.eligible_messages
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

    def to_message_entity_payloads(self) -> List[dict]:
        return [
            reference.model_dump(mode="json")
            for reference in self.message_entity_refs
        ]


class GraphWriteSummary(BaseModel):
    """Counts from executing a graph mutation plan."""

    entities_written: int = 0
    relationships_written: int = 0
    user_relationships_written: int = 0
    aliases_updated: int = 0
    dirty_entities_marked: int = 0
    zombies_filtered: int = 0
    relationships_skipped: int = 0


@dataclass
class CandidateSuggestion:
    """Advisory entity-resolution candidate preserved for later review."""

    msg_id: int
    mention: str
    mention_type: str
    mention_topic: str
    candidate_id: int
    candidate_name: str
    base_score: float
    reasons: List[str] = field(default_factory=list)
    created_entity_id: Optional[int] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "CandidateSuggestion":
        return cls(
            msg_id=int(data.get("msg_id", 0)),
            mention=str(data.get("mention") or ""),
            mention_type=str(data.get("mention_type") or ""),
            mention_topic=str(data.get("mention_topic") or ""),
            candidate_id=int(data.get("candidate_id", 0)),
            candidate_name=str(data.get("candidate_name") or ""),
            base_score=float(data.get("base_score") or 0.0),
            reasons=list(data.get("reasons") or []),
            created_entity_id=(
                int(data["created_entity_id"])
                if data.get("created_entity_id") is not None
                else None
            ),
        )


@dataclass
class ResolutionResult:
    """Result from EntityResolver batch resolution."""

    entity_ids: List[int]
    new_ids: Set[int]
    alias_ids: Set[int]
    entity_msg_map: Dict[int, List[int]]
    alias_updates: Dict[int, List[str]]
    candidate_suggestions: List[CandidateSuggestion] = field(default_factory=list)


@dataclass
class BatchResult:
    """Result of processing a batch of messages."""

    scope: Optional[EngineScope] = None
    work_unit: Optional[EngineWorkUnit] = None
    trace: ExtractionTrace = field(default_factory=ExtractionTrace)
    issues: List[ValidationIssue] = field(default_factory=list)
    entity_ids: List[int] = field(default_factory=list)
    entity_message_map: Dict[int, List[int]] = field(default_factory=dict)
    new_entity_ids: Set[int] = field(default_factory=set)
    alias_updated_ids: Set[int] = field(default_factory=set)
    alias_updates: Dict[int, List[str]] = field(default_factory=dict)
    candidate_suggestions: List[CandidateSuggestion] = field(default_factory=list)
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
        legacy_trace_message_ids = (
            self.trace.get("message_ids", [])
            if isinstance(self.trace, dict)
            else []
        )
        trace_message_ids = (
            self.trace.message_ids
            if hasattr(self.trace, "message_ids")
            else legacy_trace_message_ids
        )
        return self.has_graph_mutations() or bool(trace_message_ids)

    def has_graph_mutations(self) -> bool:
        """Return whether entity, alias, or relationship state needs writing."""
        return bool(
            self.relationship_observations
            or self.user_relationship_observations
            or self.entity_message_map
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
            "entity_message_map": {
                str(entity_id): message_ids
                for entity_id, message_ids in self.entity_message_map.items()
            },
            "new_entity_ids": list(self.new_entity_ids),
            "alias_updated_ids": list(self.alias_updated_ids),
            "alias_updates": {str(k): v for k, v in self.alias_updates.items()},
            "candidate_suggestions": [
                suggestion.to_dict() for suggestion in self.candidate_suggestions
            ],
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
            entity_message_map={
                int(entity_id): [int(message_id) for message_id in message_ids]
                for entity_id, message_ids in data.get(
                    "entity_message_map", {}
                ).items()
                if str(entity_id).isdigit()
            },
            new_entity_ids=set(data.get("new_entity_ids", [])),
            alias_updated_ids=set(data.get("alias_updated_ids", [])),
            alias_updates={
                int(k): v
                for k, v in data.get("alias_updates", {}).items()
                if str(k).isdigit()
            },
            candidate_suggestions=[
                CandidateSuggestion.from_dict(item)
                for item in data.get("candidate_suggestions", [])
            ],
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
