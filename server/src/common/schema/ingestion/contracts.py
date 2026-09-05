"""Typed contracts for Context-first semantic processing."""

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from common.conf.relationship_config import normalize_observed_relationship


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
class ContextRelationshipWrite:
    """One Context-grounded relationship observation ready for reconciliation.

    Its evidence is one or more immutable Context block versions, never a
    single message or session.
    """

    support_block_ids: tuple[UUID, ...]
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
        if not isinstance(self.support_block_ids, tuple) or not self.support_block_ids:
            raise ValueError(
                "ContextRelationshipWrite requires supporting Context block IDs"
            )
        if len(self.support_block_ids) != len(set(self.support_block_ids)) or not all(
            isinstance(block_id, UUID) for block_id in self.support_block_ids
        ):
            raise ValueError(
                "ContextRelationshipWrite support block IDs must be unique UUIDs"
            )
        _require_positive_id(self.entity_a_id, "ContextRelationshipWrite.entity_a_id")
        _require_positive_id(self.entity_b_id, "ContextRelationshipWrite.entity_b_id")
        if self.entity_a_id == self.entity_b_id:
            raise ValueError(
                "ContextRelationshipWrite requires distinct positive entity IDs"
            )
        if self.symmetric:
            entity_a_id, entity_b_id = sorted((self.entity_a_id, self.entity_b_id))
            object.__setattr__(self, "entity_a_id", entity_a_id)
            object.__setattr__(self, "entity_b_id", entity_b_id)
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
                "ContextRelationshipWrite.domain_version must be a non-negative integer"
            )
        if self.context is not None:
            if not isinstance(self.context, str):
                raise ValueError(
                    "ContextRelationshipWrite.context must be a string or None"
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
class ContextBlockMention:
    """One VP-01 candidate grounded to its smallest supporting Context blocks.

    ``block_ids`` are block *versions*, never message identifiers.  A mention can
    span adjacent Context blocks, while ``literal_message_ids`` is intentionally
    left empty until the result assembler proves that a canonical message really
    contains the resolved mention.
    """

    block_ids: tuple[UUID, ...]
    name: str
    entity_type: str
    topic: str
    origin: Literal["known_alias", "vp01"]
    literal_message_ids: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        if not self.block_ids or len(self.block_ids) != len(set(self.block_ids)):
            raise ValueError("ContextBlockMention requires unique supporting block IDs")
        if not all(isinstance(block_id, UUID) for block_id in self.block_ids):
            raise TypeError("ContextBlockMention block_ids must be UUID instances")
        object.__setattr__(
            self,
            "name",
            _require_nonblank_text(self.name, "ContextBlockMention.name"),
        )
        object.__setattr__(
            self,
            "entity_type",
            _require_nonblank_text(
                self.entity_type, "ContextBlockMention.entity_type"
            ),
        )
        object.__setattr__(
            self,
            "topic",
            _require_nonblank_text(self.topic, "ContextBlockMention.topic"),
        )
        if self.origin not in {"known_alias", "vp01"}:
            raise ValueError("ContextBlockMention origin is unsupported")
        message_ids = tuple(
            _require_positive_id(
                message_id, "ContextBlockMention.literal_message_ids entry"
            )
            for message_id in self.literal_message_ids
        )
        if len(message_ids) != len(set(message_ids)):
            raise ValueError("ContextBlockMention literal message IDs must be unique")
        object.__setattr__(self, "literal_message_ids", message_ids)


@dataclass(frozen=True, slots=True, kw_only=True)
class ContextBlockEntityAssociation:
    """One pending current-block-to-entity association for Batch 7 persistence."""

    block_id: UUID
    entity_id: int
    mention_text: str

    def __post_init__(self) -> None:
        if not isinstance(self.block_id, UUID):
            raise TypeError("ContextBlockEntityAssociation.block_id must be a UUID")
        _require_positive_id(self.entity_id, "ContextBlockEntityAssociation.entity_id")
        object.__setattr__(
            self,
            "mention_text",
            _require_nonblank_text(
                self.mention_text, "ContextBlockEntityAssociation.mention_text"
            ),
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class ResolvedContextBlockMention:
    """One typed Context mention after identity resolution but before persistence."""

    mention: ContextBlockMention
    entity_id: int

    def __post_init__(self) -> None:
        if not isinstance(self.mention, ContextBlockMention):
            raise TypeError("ResolvedContextBlockMention.mention must be typed")
        _require_positive_id(self.entity_id, "ResolvedContextBlockMention.entity_id")


@dataclass(frozen=True, slots=True, kw_only=True)
class ContextEntityResult:
    """The non-persisted entity portion of one Context-first semantic build."""

    entity_ids: tuple[int, ...]
    new_entity_ids: frozenset[int]
    alias_updated_ids: frozenset[int]
    alias_updates: dict[int, tuple[str, ...]]
    pending_entity_writes: dict[int, EntityWrite]
    block_entity_associations: tuple[ContextBlockEntityAssociation, ...]
    message_entity_refs: tuple[MessageEntityRef, ...]

    def __post_init__(self) -> None:
        entity_ids = tuple(
            _require_positive_id(entity_id, "ContextEntityResult.entity_ids entry")
            for entity_id in self.entity_ids
        )
        if len(entity_ids) != len(set(entity_ids)):
            raise ValueError("ContextEntityResult entity IDs must be unique")
        object.__setattr__(self, "entity_ids", entity_ids)
        for field_name, values in (
            ("new_entity_ids", self.new_entity_ids),
            ("alias_updated_ids", self.alias_updated_ids),
        ):
            if not isinstance(values, frozenset):
                raise TypeError(f"ContextEntityResult.{field_name} must be a frozenset")
            if not values.issubset(set(entity_ids)):
                raise ValueError(f"ContextEntityResult.{field_name} must reference entity IDs")
        if set(self.pending_entity_writes) != set(self.new_entity_ids):
            raise ValueError("pending Context entity writes must cover exactly new entities")
        if any(
            not isinstance(entity_id, int)
            or not isinstance(write, EntityWrite)
            or write.entity_id != entity_id
            for entity_id, write in self.pending_entity_writes.items()
        ):
            raise TypeError("pending Context entity writes must be keyed EntityWrite values")
        if any(
            entity_id not in set(entity_ids)
            or not isinstance(aliases, tuple)
            or any(not isinstance(alias, str) or not alias.strip() for alias in aliases)
            for entity_id, aliases in self.alias_updates.items()
        ):
            raise ValueError("Context alias updates must be typed resolved entity aliases")
        if any(
            association.entity_id not in set(entity_ids)
            for association in self.block_entity_associations
        ):
            raise ValueError("Context block associations must reference resolved entities")
        if any(
            reference.entity_id not in set(entity_ids)
            for reference in self.message_entity_refs
        ):
            raise ValueError("Context message refs must reference resolved entities")
