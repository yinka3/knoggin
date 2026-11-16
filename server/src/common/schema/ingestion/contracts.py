"""Typed contracts for semantic-window Knowledge processing."""

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from common.conf.relationship_config import normalize_observed_relationship
from common.scoping import IDENTITY_ENTITY_ID


class ValidationIssue(BaseModel):
    """Structured validation or fallback issue recorded during engine processing."""

    stage: str
    code: str
    message: str
    severity: Literal["info", "warning", "error"] = "warning"
    item_ref: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ExtractionTrace(BaseModel):
    """Counters and labels describing one Knowledge extraction pass."""

    entity_model: Optional[str] = None
    relationship_model: Optional[str] = None
    episodes: int = 0
    episodes_refused: int = 0
    known_mentions: int = 0
    llm_mentions_seen: int = 0
    llm_mentions_accepted: int = 0
    llm_mentions_rejected: int = 0
    relationships_seen: int = 0
    relationships_accepted: int = 0
    relationships_rejected: int = 0
    relationships_recognized: int = 0
    relationships_unrecognized: int = 0
    identity_decisions: List[Dict[str, Any]] = Field(default_factory=list)
    classification_decisions: List[Dict[str, Any]] = Field(default_factory=list)
    classification_aggregates: List[Dict[str, Any]] = Field(default_factory=list)
    resolver_timings: List[Dict[str, Any]] = Field(default_factory=list)


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
class ProjectEntityClassification:
    """One target-project classification staged for a resolved identity.

    Identity creation is user-global, while ``entity_type`` and ``topic`` belong
    to one project. ``membership`` records what resolution observed so the
    commit boundary can distinguish an existing local context from one it must
    create and verify.
    """

    entity_id: int
    entity_type: str
    topic: str
    membership: Literal["existing", "missing"]

    def __post_init__(self) -> None:
        entity_id = _require_positive_id(
            self.entity_id,
            "ProjectEntityClassification.entity_id",
        )
        if entity_id == IDENTITY_ENTITY_ID:
            raise ValueError(
                "ProjectEntityClassification cannot classify the reserved identity"
            )
        object.__setattr__(
            self,
            "entity_type",
            _require_nonblank_text(
                self.entity_type,
                "ProjectEntityClassification.entity_type",
            ),
        )
        object.__setattr__(
            self,
            "topic",
            _require_nonblank_text(
                self.topic,
                "ProjectEntityClassification.topic",
            ),
        )
        if self.membership not in {"existing", "missing"}:
            raise ValueError(
                "ProjectEntityClassification.membership must be existing or missing"
            )


@dataclass(frozen=True, slots=True, kw_only=True)
class RelationshipWrite:
    """One message-grounded relationship support set ready for reconciliation.

    Its evidence is the set of Episode messages that state it. Separate
    support sets and observed labels remain separate durable observations.
    """

    support_message_ids: tuple[int, ...]
    entity_a_id: int
    entity_b_id: int
    relationship_type: str
    context: Optional[str] = None
    observed_label: Optional[str] = None
    canonical_type: Optional[str] = None
    interpretation_source: Literal["observed", "domain", "review"] | None = None
    domain_status: Literal["recognized", "unrecognized"] = "unrecognized"
    source_type: Optional[str] = None
    target_type: Optional[str] = None
    symmetric: bool = False
    domain_version: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.support_message_ids, tuple) or not self.support_message_ids:
            raise ValueError("RelationshipWrite requires supporting message IDs")
        if len(self.support_message_ids) != len(set(self.support_message_ids)):
            raise ValueError("RelationshipWrite support message IDs must be unique")
        for message_id in self.support_message_ids:
            _require_positive_id(message_id, "RelationshipWrite.support_message_ids entry")
        _require_positive_id(self.entity_a_id, "RelationshipWrite.entity_a_id")
        _require_positive_id(self.entity_b_id, "RelationshipWrite.entity_b_id")
        if self.entity_a_id == self.entity_b_id:
            raise ValueError(
                "RelationshipWrite requires distinct positive entity IDs"
            )
        if self.symmetric and self.entity_a_id > self.entity_b_id:
            # Each type belongs to its entity, so they swap together.
            for first, second in (("entity_a_id", "entity_b_id"), ("source_type", "target_type")):
                swapped = getattr(self, second), getattr(self, first)
                object.__setattr__(self, first, swapped[0])
                object.__setattr__(self, second, swapped[1])
        object.__setattr__(
            self,
            "relationship_type",
            normalize_relationship_type(self.relationship_type),
        )
        object.__setattr__(
            self,
            "observed_label",
            normalize_observed_relationship(self.observed_label or self.relationship_type),
        )
        if self.canonical_type is not None:
            canonical = self.canonical_type.strip()
            object.__setattr__(self, "canonical_type", canonical or None)
        object.__setattr__(
            self,
            "domain_status",
            "recognized" if self.canonical_type is not None else "unrecognized",
        )
        object.__setattr__(
            self,
            "interpretation_source",
            self.interpretation_source
            or ("domain" if self.canonical_type is not None else "observed"),
        )
        if (
            not isinstance(self.domain_version, int)
            or isinstance(self.domain_version, bool)
            or self.domain_version < 0
        ):
            raise ValueError(
                "RelationshipWrite.domain_version must be a non-negative integer"
            )
        if self.context is not None:
            if not isinstance(self.context, str):
                raise ValueError(
                    "RelationshipWrite.context must be a string or None"
                )
            object.__setattr__(self, "context", self.context.strip() or None)


@dataclass(frozen=True, slots=True, kw_only=True)
class MessageEntityRef:
    """A resolved entity mention grounded to one canonical message."""

    message_id: int
    entity_id: int

    def __post_init__(self) -> None:
        _require_positive_id(self.message_id, "MessageEntityRef.message_id")
        _require_positive_id(self.entity_id, "MessageEntityRef.entity_id")


@dataclass(frozen=True, slots=True, kw_only=True)
class EntityMention:
    """One extracted entity name and the Episode messages that contain it."""

    message_ids: tuple[int, ...]
    name: str
    entity_type: str
    topic: str
    origin: Literal["known_alias", "vp01"]

    def __post_init__(self) -> None:
        if not self.message_ids or len(self.message_ids) != len(set(self.message_ids)):
            raise ValueError("EntityMention requires unique supporting message IDs")
        for message_id in self.message_ids:
            _require_positive_id(message_id, "EntityMention.message_ids entry")
        object.__setattr__(
            self, "name", _require_nonblank_text(self.name, "EntityMention.name")
        )
        object.__setattr__(
            self,
            "entity_type",
            _require_nonblank_text(self.entity_type, "EntityMention.entity_type"),
        )
        object.__setattr__(
            self, "topic", _require_nonblank_text(self.topic, "EntityMention.topic")
        )
        if self.origin not in {"known_alias", "vp01"}:
            raise ValueError("EntityMention origin is unsupported")


@dataclass(frozen=True, slots=True, kw_only=True)
class ResolvedEntityMention:
    """One mention after identity resolution but before persistence."""

    mention: EntityMention
    entity_id: int
    occurrence_index: int
    identity_decision_key: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.mention, EntityMention):
            raise TypeError("ResolvedEntityMention.mention must be typed")
        _require_positive_id(self.entity_id, "ResolvedEntityMention.entity_id")
        if not isinstance(self.occurrence_index, int) or self.occurrence_index < 0:
            raise ValueError("ResolvedEntityMention occurrence index must be non-negative")
        if self.identity_decision_key is not None and not self.identity_decision_key:
            raise ValueError("ResolvedEntityMention identity decision key must not be blank")


@dataclass(frozen=True, slots=True, kw_only=True)
class EntityResult:
    """The non-persisted entity portion of one semantic-window build."""

    entity_ids: tuple[int, ...]
    new_entity_ids: frozenset[int]
    alias_updated_ids: frozenset[int]
    alias_updates: dict[int, tuple[str, ...]]
    pending_entity_writes: dict[int, EntityWrite]
    project_classifications: dict[int, ProjectEntityClassification]
    resolved_mentions: tuple[ResolvedEntityMention, ...] = ()

    @property
    def message_entity_refs(self) -> tuple[MessageEntityRef, ...]:
        """Each message that names each resolved entity, once."""

        refs = dict.fromkeys(
            (message_id, resolved.entity_id)
            for resolved in self.resolved_mentions
            for message_id in resolved.mention.message_ids
        )
        return tuple(
            MessageEntityRef(message_id=message_id, entity_id=entity_id)
            for message_id, entity_id in refs
        )

    def __post_init__(self) -> None:
        entity_ids = tuple(
            _require_positive_id(entity_id, "EntityResult.entity_ids entry")
            for entity_id in self.entity_ids
        )
        if len(entity_ids) != len(set(entity_ids)):
            raise ValueError("EntityResult entity IDs must be unique")
        object.__setattr__(self, "entity_ids", entity_ids)
        for field_name, values in (
            ("new_entity_ids", self.new_entity_ids),
            ("alias_updated_ids", self.alias_updated_ids),
        ):
            if not isinstance(values, frozenset):
                raise TypeError(f"EntityResult.{field_name} must be a frozenset")
            if not values.issubset(set(entity_ids)):
                raise ValueError(f"EntityResult.{field_name} must reference entity IDs")
        if set(self.pending_entity_writes) != set(self.new_entity_ids):
            raise ValueError("pending entity writes must cover exactly new entities")
        if any(
            not isinstance(entity_id, int)
            or not isinstance(write, EntityWrite)
            or write.entity_id != entity_id
            for entity_id, write in self.pending_entity_writes.items()
        ):
            raise TypeError("pending entity writes must be keyed EntityWrite values")
        if IDENTITY_ENTITY_ID in self.new_entity_ids:
            raise ValueError("Entity writes cannot create the reserved identity")
        classified_ids = set(entity_ids) - {IDENTITY_ENTITY_ID}
        if set(self.project_classifications) != classified_ids:
            raise ValueError("Project classifications must cover every non-identity entity")
        if any(
            not isinstance(entity_id, int)
            or not isinstance(classification, ProjectEntityClassification)
            or classification.entity_id != entity_id
            for entity_id, classification in self.project_classifications.items()
        ):
            raise TypeError("Project classifications must be keyed typed classifications")
        for entity_id in self.new_entity_ids:
            classification = self.project_classifications[entity_id]
            write = self.pending_entity_writes[entity_id]
            if classification.membership != "missing":
                raise ValueError("New identities require missing project membership")
            if (
                classification.entity_type != write.entity_type
                or classification.topic != write.topic
            ):
                raise ValueError("New identity classification must match its entity write")
        if any(
            entity_id not in set(entity_ids)
            or not isinstance(aliases, tuple)
            or any(not isinstance(alias, str) or not alias.strip() for alias in aliases)
            for entity_id, aliases in self.alias_updates.items()
        ):
            raise ValueError("Alias updates must be typed resolved entity aliases")
        if any(
            not isinstance(resolved, ResolvedEntityMention)
            or resolved.entity_id not in set(entity_ids)
            for resolved in self.resolved_mentions
        ):
            raise ValueError("Resolved mentions must reference resolved entities")
        if {resolved.entity_id for resolved in self.resolved_mentions} != set(entity_ids):
            raise ValueError("Every resolved entity needs a mention")
