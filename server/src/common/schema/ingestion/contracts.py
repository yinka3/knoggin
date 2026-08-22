"""Typed contracts for ingestion and graph-persistence handoffs."""

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
)

from common.conf.relationship_config import normalize_observed_relationship
from common.schema.immutable import FrozenDict


class ExecutionScope(BaseModel):
    """Execution scope for engine work and serialized batch diagnostics."""

    model_config = ConfigDict(frozen=True)
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
    relationships_recognized: int = 0
    relationships_unrecognized: int = 0
    user_relationships_seen: int = 0
    user_relationships_accepted: int = 0
    user_relationships_rejected: int = 0
    fallbacks: List[Dict[str, str]] = Field(default_factory=list)


class RelationshipObservation(BaseModel):
    """One validated relationship observation before durable endpoint resolution."""

    message_id: int = Field(..., gt=0)
    entity_a_name: str = Field(..., min_length=1)
    entity_b_name: str = Field(..., min_length=1)
    relationship_type: str = Field(..., min_length=1)
    observed_label: Optional[str] = None
    canonical_type: Optional[str] = None
    domain_status: Literal["recognized", "unrecognized"] = "unrecognized"
    source_type: Optional[str] = None
    target_type: Optional[str] = None
    symmetric: bool = False
    domain_version: int = Field(0, ge=0)
    confidence: float = Field(1.0, ge=0.0, le=1.0)
    context: Optional[str] = None
    identity_rooted: bool = False

    @model_validator(mode="after")
    def normalize_relationship_fields(self):
        observed = normalize_observed_relationship(
            self.observed_label or self.relationship_type
        )
        self.observed_label = observed
        if self.canonical_type is not None:
            self.canonical_type = self.canonical_type.strip()
            if not self.canonical_type:
                self.canonical_type = None
        self.relationship_type = self.canonical_type or observed
        self.domain_status = (
            "recognized" if self.canonical_type is not None else "unrecognized"
        )
        return self

    @property
    def source_entity_name(self) -> str:
        """Directional source alias for the relationship contract."""

        return self.entity_a_name

    @property
    def target_entity_name(self) -> str:
        """Directional target alias for the relationship contract."""

        return self.entity_b_name


def normalize_relationship_type(value: object) -> str:
    """Return the canonical storage identity for an observed relationship."""

    if not isinstance(value, str):
        raise ValueError("relationship_type must be text")
    normalized = " ".join(value.split()).casefold()
    if not normalized:
        raise ValueError("relationship_type must not be blank")
    return normalized


def _require_positive_id(value: object, field_name: str) -> int:
    """Validate an externally supplied database identifier."""

    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _require_nonblank_text(value: object, field_name: str) -> str:
    """Validate and normalize text that is required for graph persistence."""

    if not isinstance(value, str) or not (normalized := value.strip()):
        raise ValueError(f"{field_name} must be a non-blank string")
    return normalized


def _require_confidence(value: object, field_name: str = "confidence") -> float:
    """Validate a finite, bounded graph confidence score."""

    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or not 0.0 <= value <= 1.0
    ):
        raise ValueError(f"{field_name} must be a finite number between 0 and 1")
    return float(value)


def relationship_identity(
    project_id: str,
    entity_a_id: int,
    entity_b_id: int,
    relationship_type: object,
    *,
    symmetric: bool = True,
) -> str:
    """Build a typed relationship identity with explicit symmetry semantics."""

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
    if symmetric:
        a_id, b_id = sorted((entity_a_id, entity_b_id))
    else:
        a_id, b_id = entity_a_id, entity_b_id
    relationship_key = normalize_relationship_type(relationship_type)
    return f"{project_id}:{a_id}:{b_id}:{relationship_key}"


@dataclass(frozen=True, slots=True, kw_only=True)
class EntityWrite:
    """Typed entity payload intended for graph persistence."""

    entity_id: int
    is_new: bool
    canonical_name: str
    entity_type: str
    topic: str
    embedding: Optional[tuple[float, ...]]
    aliases: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_positive_id(self.entity_id, "EntityWrite.entity_id")
        if not isinstance(self.is_new, bool):
            raise ValueError("EntityWrite.is_new must be a boolean")
        object.__setattr__(
            self,
            "canonical_name",
            _require_nonblank_text(self.canonical_name, "EntityWrite.canonical_name"),
        )
        object.__setattr__(
            self,
            "entity_type",
            _require_nonblank_text(self.entity_type, "EntityWrite.entity_type"),
        )
        object.__setattr__(
            self, "topic", _require_nonblank_text(self.topic, "EntityWrite.topic")
        )
        if self.embedding is not None:
            if not isinstance(self.embedding, (list, tuple)):
                raise ValueError("EntityWrite.embedding must be a sequence of numbers")
            if not self.embedding:
                raise ValueError("EntityWrite.embedding must not be empty")
            normalized_embedding = []
            for index, value in enumerate(self.embedding):
                if (
                    not isinstance(value, (int, float))
                    or isinstance(value, bool)
                    or not math.isfinite(value)
                ):
                    raise ValueError(
                        "EntityWrite.embedding values must be finite numbers "
                        f"(invalid index {index})"
                    )
                normalized_embedding.append(float(value))
            object.__setattr__(self, "embedding", tuple(normalized_embedding))

        if not isinstance(self.aliases, tuple):
            raise ValueError("EntityWrite.aliases must be a tuple of strings")
        object.__setattr__(
            self,
            "aliases",
            tuple(
                _require_nonblank_text(alias, "EntityWrite.aliases entry")
                for alias in self.aliases
            ),
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class RelationshipWrite:
    """Typed relationship payload intended for graph persistence."""

    entity_a_id: int
    entity_b_id: int
    relationship_type: str
    message_id: int
    confidence: float
    context: Optional[str] = None
    observed_label: Optional[str] = None
    canonical_type: Optional[str] = None
    domain_status: Literal["recognized", "unrecognized"] = "unrecognized"
    source_type: Optional[str] = None
    target_type: Optional[str] = None
    symmetric: bool = False
    domain_version: int = 0

    def __post_init__(self) -> None:
        _require_positive_id(self.entity_a_id, "RelationshipWrite.entity_a_id")
        _require_positive_id(self.entity_b_id, "RelationshipWrite.entity_b_id")
        if self.entity_a_id == self.entity_b_id:
            raise ValueError("RelationshipWrite requires distinct positive entity IDs")
        _require_positive_id(self.message_id, "RelationshipWrite.message_id")
        if self.symmetric:
            entity_a_id, entity_b_id = sorted((self.entity_a_id, self.entity_b_id))
            object.__setattr__(self, "entity_a_id", entity_a_id)
            object.__setattr__(self, "entity_b_id", entity_b_id)
        object.__setattr__(
            self,
            "relationship_type",
            normalize_relationship_type(self.relationship_type),
        )
        observed = self.observed_label or self.relationship_type
        object.__setattr__(
            self,
            "observed_label",
            normalize_observed_relationship(observed),
        )
        if self.canonical_type is not None:
            canonical = self.canonical_type.strip()
            object.__setattr__(self, "canonical_type", canonical or None)
        if self.canonical_type is not None:
            object.__setattr__(self, "domain_status", "recognized")
        else:
            object.__setattr__(self, "domain_status", "unrecognized")
        object.__setattr__(self, "confidence", _require_confidence(self.confidence))
        if not isinstance(self.domain_version, int) or isinstance(self.domain_version, bool) or self.domain_version < 0:
            raise ValueError("RelationshipWrite.domain_version must be a non-negative integer")
        if self.context is not None:
            if not isinstance(self.context, str):
                raise ValueError("RelationshipWrite.context must be a string or None")
            object.__setattr__(self, "context", self.context.strip() or None)

    @property
    def source_entity_id(self) -> int:
        """Directional source alias used by the domain relationship contract."""

        return self.entity_a_id

    @property
    def target_entity_id(self) -> int:
        """Directional target alias used by the domain relationship contract."""

        return self.entity_b_id


@dataclass(frozen=True, slots=True, kw_only=True)
class MessageEntityRef:
    """A resolved entity mention grounded to one canonical message."""

    message_id: int
    entity_id: int

    def __post_init__(self) -> None:
        _require_positive_id(self.message_id, "MessageEntityRef.message_id")
        _require_positive_id(self.entity_id, "MessageEntityRef.entity_id")


@dataclass(frozen=True, slots=True, kw_only=True)
class EpisodeEligibility:
    """Episode-processing eligibility attached to a canonical message."""

    message_id: int
    episode_type: Optional[str] = None

    def __post_init__(self) -> None:
        _require_positive_id(self.message_id, "EpisodeEligibility.message_id")
        if self.episode_type is not None:
            object.__setattr__(
                self,
                "episode_type",
                _require_nonblank_text(
                    self.episode_type, "EpisodeEligibility.episode_type"
                ),
            )


@dataclass(frozen=True, slots=True, kw_only=True)
class AliasUpdate:
    """Aliases to persist for a canonical entity."""

    entity_id: int
    aliases: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_positive_id(self.entity_id, "AliasUpdate.entity_id")
        if not isinstance(self.aliases, tuple):
            raise ValueError("AliasUpdate.aliases must be a tuple of strings")
        object.__setattr__(
            self,
            "aliases",
            tuple(
                _require_nonblank_text(alias, "AliasUpdate.aliases entry")
                for alias in self.aliases
            ),
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class IngestionCommit:
    """The complete durable change set for one claimed ingestion batch."""

    scope: ExecutionScope
    batch_id: str
    message_ids: tuple[int, ...]
    entity_writes: tuple[EntityWrite, ...] = ()
    alias_updates: tuple[AliasUpdate, ...] = ()
    message_entity_refs: tuple[MessageEntityRef, ...] = ()
    relationship_writes: tuple[RelationshipWrite, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.scope, ExecutionScope):
            raise TypeError("IngestionCommit.scope must be an ExecutionScope")
        if not self.scope.project_id:
            raise ValueError("IngestionCommit requires a project-scoped execution")
        object.__setattr__(
            self,
            "batch_id",
            _require_nonblank_text(self.batch_id, "IngestionCommit.batch_id"),
        )
        if not isinstance(self.message_ids, tuple):
            raise ValueError("IngestionCommit.message_ids must be a tuple")
        message_ids = tuple(
            _require_positive_id(message_id, "IngestionCommit.message_ids entry")
            for message_id in self.message_ids
        )
        if not message_ids or len(message_ids) != len(set(message_ids)):
            raise ValueError("IngestionCommit requires unique claimed message IDs")
        object.__setattr__(self, "message_ids", tuple(sorted(message_ids)))

        for field_name, values, expected_type in (
            ("entity_writes", self.entity_writes, EntityWrite),
            ("alias_updates", self.alias_updates, AliasUpdate),
            ("message_entity_refs", self.message_entity_refs, MessageEntityRef),
            ("relationship_writes", self.relationship_writes, RelationshipWrite),
        ):
            if not isinstance(values, tuple) or not all(
                isinstance(value, expected_type) for value in values
            ):
                raise TypeError(
                    f"IngestionCommit.{field_name} must contain "
                    f"{expected_type.__name__} instances"
                )

        claimed_ids = set(message_ids)
        evidence_ids = {
            reference.message_id for reference in self.message_entity_refs
        } | {relationship.message_id for relationship in self.relationship_writes}
        if not evidence_ids.issubset(claimed_ids):
            raise ValueError(
                "IngestionCommit evidence must belong to claimed messages"
            )


@dataclass(frozen=True, slots=True, kw_only=True)
class SkippedRelationship:
    """Relationship observation skipped before graph persistence."""

    entity_a: Optional[str] = None
    entity_b: Optional[str] = None
    message_id: int
    reason: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_positive_id(self.message_id, "SkippedRelationship.message_id")
        object.__setattr__(
            self,
            "reason",
            _require_nonblank_text(self.reason, "SkippedRelationship.reason"),
        )
        object.__setattr__(self, "metadata", FrozenDict(self.metadata))


@dataclass(slots=True)
class GraphWriteSummary:
    """Counts from executing a graph mutation plan."""

    entities_written: int = 0
    relationships_written: int = 0
    aliases_updated: int = 0
    dirty_entities_marked: int = 0
    zombies_filtered: int = 0
    relationships_skipped: int = 0
