"""Engine contract models for LLM outputs and pipeline handoffs."""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Set
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator

from common.schema.primitives import (
    Connection,
    Entity,
)
from common.schema.episode import EpisodeNarrative
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


class EpisodeDecision(EpisodeNarrative):
    """Resolved internal decision for one bounded episodic-memory window."""

    action: Literal["create", "consolidate", "skip"]
    target_episode_id: Optional[str] = Field(None, min_length=1)
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


class EpisodeConsolidation(EpisodeNarrative):
    """Resolved internal regeneration for one selected episode."""

    summary: str = Field(..., min_length=1)
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


class LLMEpisodeDecision(EpisodeNarrative):
    """Model-facing episode decision that contains only local references."""

    action: Literal["create", "consolidate", "skip"]
    target_episode_id: Optional[str] = Field(None, pattern=r"^ep[1-9]\d*$")
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


class LLMEpisodeConsolidation(EpisodeNarrative):
    """Model-facing episode regeneration output with local references."""

    summary: str = Field(..., min_length=1)
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
        kind: Literal[
            "embedding",
            "rerank",
            "nli",
            "spacy",
            "gliner",
            "document_index",
            "model_load",
        ],
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


class RelationshipObservation(BaseModel):
    """One validated relationship observation before durable endpoint resolution."""

    message_id: int = Field(..., gt=0)
    entity_a_name: str = Field(..., min_length=1)
    entity_b_name: str = Field(..., min_length=1)
    relationship_type: str = Field(..., min_length=1)
    confidence: float = Field(1.0, ge=0.0, le=1.0)
    context: Optional[str] = None
    identity_rooted: bool = False


def normalize_relationship_type(value: object) -> str:
    """Return the canonical storage identity for an observed relationship."""

    if not isinstance(value, str):
        raise ValueError("relationship_type must be text")
    normalized = " ".join(value.split()).casefold()
    if not normalized:
        raise ValueError("relationship_type must not be blank")
    return normalized


def relationship_identity(
    project_id: str,
    entity_a_id: int,
    entity_b_id: int,
    relationship_type: object,
) -> str:
    """Build the durable identity for one typed relationship observation."""

    if not isinstance(project_id, str) or not project_id.strip():
        raise ValueError("relationship identity requires a project_id")
    if (
        not isinstance(entity_a_id, int)
        or isinstance(entity_a_id, bool)
        or not isinstance(entity_b_id, int)
        or isinstance(entity_b_id, bool)
        or entity_a_id <= 0
        or entity_b_id <= 0
        or entity_a_id == entity_b_id
    ):
        raise ValueError("relationship identity requires distinct positive entity IDs")
    a_id, b_id = sorted((entity_a_id, entity_b_id))
    relationship_key = normalize_relationship_type(relationship_type)
    return f"{project_id}:{a_id}:{b_id}:{relationship_key}"


@dataclass(frozen=True, slots=True, kw_only=True)
class EntityWrite:
    """Typed entity payload intended for graph persistence."""

    entity_id: int
    is_new: bool
    canonical_name: str
    entity_type: str
    confidence: float
    topic: str
    embedding: Optional[List[float]]
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class RelationshipWrite:
    """Typed relationship payload intended for graph persistence."""

    entity_a_id: int
    entity_b_id: int
    relationship_type: str
    message_id: int
    confidence: float
    context: Optional[str] = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.entity_a_id, int)
            or isinstance(self.entity_a_id, bool)
            or not isinstance(self.entity_b_id, int)
            or isinstance(self.entity_b_id, bool)
            or self.entity_a_id <= 0
            or self.entity_b_id <= 0
            or self.entity_a_id == self.entity_b_id
        ):
            raise ValueError("RelationshipWrite requires distinct positive entity IDs")
        if (
            not isinstance(self.message_id, int)
            or isinstance(self.message_id, bool)
            or self.message_id <= 0
        ):
            raise ValueError("RelationshipWrite requires a positive message_id")
        entity_a_id, entity_b_id = sorted((self.entity_a_id, self.entity_b_id))
        object.__setattr__(self, "entity_a_id", entity_a_id)
        object.__setattr__(self, "entity_b_id", entity_b_id)
        object.__setattr__(
            self,
            "relationship_type",
            normalize_relationship_type(self.relationship_type),
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class MessageEntityRef:
    """A resolved entity mention grounded to one canonical message."""

    message_id: int
    entity_id: int


@dataclass(frozen=True, slots=True, kw_only=True)
class EpisodeEligibility:
    """Episode-processing eligibility attached to a canonical message."""

    message_id: int
    episode_type: Optional[str] = None


@dataclass(slots=True, kw_only=True)
class AliasUpdate:
    """Aliases to persist for a canonical entity."""

    entity_id: int
    aliases: List[str] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class SkippedRelationship:
    """Relationship observation skipped before graph persistence."""

    entity_a: Optional[str] = None
    entity_b: Optional[str] = None
    message_id: int
    reason: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True)
class GraphMutationPlan:
    """Typed graph-write intent derived from a processed batch."""

    work_unit: EngineWorkUnit
    scope: EngineScope
    entity_ids: List[int] = field(default_factory=list)
    safe_entity_ids: Set[int] = field(default_factory=set)
    new_entity_ids: Set[int] = field(default_factory=set)
    alias_updates: List[AliasUpdate] = field(default_factory=list)
    entity_writes: List[EntityWrite] = field(default_factory=list)
    message_entity_refs: List[MessageEntityRef] = field(default_factory=list)
    eligible_messages: List[EpisodeEligibility] = field(default_factory=list)
    relationship_writes: List[RelationshipWrite] = field(default_factory=list)
    skipped_relationships: List[SkippedRelationship] = field(default_factory=list)
    zombie_entity_ids: Set[int] = field(default_factory=set)
    dirty_entity_ids: Set[int] = field(default_factory=set)

    def has_writes(self) -> bool:
        return bool(
            self.entity_writes
            or self.message_entity_refs
            or self.eligible_messages
            or self.relationship_writes
            or self.alias_updates
        )


@dataclass(slots=True)
class GraphWriteSummary:
    """Counts from executing a graph mutation plan."""

    entities_written: int = 0
    relationships_written: int = 0
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

    entity_ids: List[int] = field(default_factory=list)
    new_ids: Set[int] = field(default_factory=set)
    alias_ids: Set[int] = field(default_factory=set)
    entity_msg_map: Dict[int, List[int]] = field(default_factory=dict)
    alias_updates: Dict[int, List[str]] = field(default_factory=dict)
    candidate_suggestions: List[CandidateSuggestion] = field(default_factory=list)


@dataclass(init=False)
class BatchResult:
    """Result of processing a batch of messages."""

    scope: Optional[EngineScope]
    work_unit: Optional[EngineWorkUnit]
    trace: ExtractionTrace
    issues: List[ValidationIssue]
    resolution: ResolutionResult
    relationship_observations: List[RelationshipObservation]
    success: bool
    error: Optional[str]

    def __init__(
        self,
        *,
        scope: Optional[EngineScope] = None,
        work_unit: Optional[EngineWorkUnit] = None,
        trace: Optional[ExtractionTrace] = None,
        issues: Optional[List[ValidationIssue]] = None,
        resolution: Optional[ResolutionResult] = None,
        relationship_observations: Optional[List[RelationshipObservation]] = None,
        success: bool = True,
        error: Optional[str] = None,
    ) -> None:
        self.scope = scope
        self.work_unit = work_unit
        self.trace = trace if trace is not None else ExtractionTrace()
        self.issues = list(issues or [])
        self.resolution = resolution or ResolutionResult()
        self.relationship_observations = list(relationship_observations or [])
        self.success = success
        self.error = error

    @property
    def entity_ids(self) -> List[int]:
        return self.resolution.entity_ids

    @entity_ids.setter
    def entity_ids(self, value: List[int]) -> None:
        self.resolution.entity_ids = value

    @property
    def entity_message_map(self) -> Dict[int, List[int]]:
        return self.resolution.entity_msg_map

    @entity_message_map.setter
    def entity_message_map(self, value: Dict[int, List[int]]) -> None:
        self.resolution.entity_msg_map = value

    @property
    def new_entity_ids(self) -> Set[int]:
        return self.resolution.new_ids

    @new_entity_ids.setter
    def new_entity_ids(self, value: Set[int]) -> None:
        self.resolution.new_ids = value

    @property
    def alias_updated_ids(self) -> Set[int]:
        return self.resolution.alias_ids

    @alias_updated_ids.setter
    def alias_updated_ids(self, value: Set[int]) -> None:
        self.resolution.alias_ids = value

    @property
    def alias_updates(self) -> Dict[int, List[str]]:
        return self.resolution.alias_updates

    @alias_updates.setter
    def alias_updates(self, value: Dict[int, List[str]]) -> None:
        self.resolution.alias_updates = value

    @property
    def candidate_suggestions(self) -> List[CandidateSuggestion]:
        return self.resolution.candidate_suggestions

    @candidate_suggestions.setter
    def candidate_suggestions(self, value: List[CandidateSuggestion]) -> None:
        self.resolution.candidate_suggestions = value

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
        return self.has_graph_mutations() or bool(self.trace.message_ids)

    def has_graph_mutations(self) -> bool:
        """Return whether entity, alias, or relationship state needs writing."""
        return bool(
            self.relationship_observations
            or self.entity_message_map
            or self.new_entity_ids
            or self.alias_updated_ids
            or self.alias_updates
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
