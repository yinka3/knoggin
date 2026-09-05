"""Strict, bounded contracts for cross-layer evidence traversal."""

from __future__ import annotations

import re
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from common.schema.source.locators import SourceLocator

EvidenceKind = Literal[
    "message",
    "episode",
    "merge_mutation",
    "source_reference",
    "context_block",
    "relationship_observation",
    "maintenance_review",
]
EvidenceSubjectKind = Literal[
    "context_block",
    "relationship_observation",
    "maintenance_review",
]
EvidenceRelation = Literal[
    "source_owned_by_message",
    "supports_context_block",
    "supports_relationship_observation",
    "cited_by_maintenance_review",
]
EvidenceNodeStatus = Literal["active", "retired", "available", "missing"]

_SHA256 = re.compile(r"[0-9a-f]{64}")
_EMPTY_STATE_TOKEN = "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e9f414c0cce5cfd4e3d7e1b"
_INTEGER_KINDS = frozenset(
    {"message", "merge_mutation", "relationship_observation"}
)
_UUID_KINDS = frozenset({"source_reference", "context_block"})


class _FrozenEvidenceModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class EvidencePointer(_FrozenEvidenceModel):
    """A typed durable identity whose project scope comes from the read boundary."""

    kind: EvidenceKind
    identifier: str = Field(min_length=1, max_length=200)

    @model_validator(mode="after")
    def validate_identifier(self) -> "EvidencePointer":
        if self.identifier != self.identifier.strip():
            raise ValueError("evidence identifier must not contain outer whitespace")
        if self.kind in _INTEGER_KINDS:
            if not self.identifier.isascii() or not self.identifier.isdecimal():
                raise ValueError(f"{self.kind} evidence requires a positive integer")
            if int(self.identifier) <= 0:
                raise ValueError(f"{self.kind} evidence requires a positive integer")
        elif self.kind in _UUID_KINDS:
            try:
                normalized = str(UUID(self.identifier))
            except ValueError as exc:
                raise ValueError(f"{self.kind} evidence requires a UUID") from exc
            if normalized != self.identifier:
                raise ValueError(f"{self.kind} evidence requires a canonical UUID")
        return self

    @classmethod
    def for_observation(cls, observation_id: int) -> "EvidencePointer":
        if not isinstance(observation_id, int) or isinstance(observation_id, bool):
            raise TypeError("observation_id must be an integer")
        return cls(kind="relationship_observation", identifier=str(observation_id))


class EvidenceSubject(_FrozenEvidenceModel):
    """The durable object whose evidence is being explained."""

    kind: EvidenceSubjectKind
    identifier: str = Field(min_length=1, max_length=200)

    @model_validator(mode="after")
    def validate_identifier(self) -> "EvidenceSubject":
        EvidencePointer(kind=self.kind, identifier=self.identifier)
        return self


class EvidenceNode(_FrozenEvidenceModel):
    """One bounded evidence identity with safe display metadata."""

    pointer: EvidencePointer
    label: str | None = Field(default=None, max_length=200)
    role: Literal["user", "assistant"] | None = None
    source_kind: str | None = Field(default=None, max_length=40)
    content_hash: str | None = None
    locator: SourceLocator | None = None
    excerpt: str | None = Field(default=None, max_length=500)
    timestamp_ms: int | None = Field(default=None, ge=0)
    status: EvidenceNodeStatus = "available"

    @field_validator("label", "source_kind", "excerpt")
    @classmethod
    def normalize_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = " ".join(value.split())
        return normalized or None

    @field_validator("content_hash")
    @classmethod
    def validate_content_hash(cls, value: str | None) -> str | None:
        if value is not None and _SHA256.fullmatch(value) is None:
            raise ValueError("evidence content_hash must be a SHA-256 hex digest")
        return value


class EvidenceEdge(_FrozenEvidenceModel):
    """One directed durable support or ownership relationship."""

    source: EvidencePointer
    target: EvidencePointer
    relation: EvidenceRelation

    @model_validator(mode="after")
    def validate_shape(self) -> "EvidenceEdge":
        allowed = {
            "source_owned_by_message": {("source_reference", "message")},
            "supports_context_block": {
                ("message", "context_block"),
                ("source_reference", "context_block"),
            },
            "supports_relationship_observation": {
                ("context_block", "relationship_observation")
            },
            "cited_by_maintenance_review": {
                ("relationship_observation", "maintenance_review"),
                ("episode", "maintenance_review"),
                ("message", "maintenance_review"),
                ("source_reference", "maintenance_review"),
                ("context_block", "maintenance_review"),
                ("merge_mutation", "maintenance_review"),
            },
        }
        if (self.source.kind, self.target.kind) not in allowed[self.relation]:
            raise ValueError("evidence edge endpoints do not match its relation")
        return self


class EvidenceTraversalLimits(_FrozenEvidenceModel):
    """Hard request bounds for one evidence traversal."""

    max_observations: int = Field(default=128, ge=1, le=128)
    max_context_blocks: int = Field(default=256, ge=1, le=256)
    max_leaf_evidence: int = Field(default=512, ge=1, le=512)
    max_edges: int = Field(default=1_024, ge=1, le=1_024)


class EvidenceBundle(_FrozenEvidenceModel):
    """A deterministic, bounded explanation of one subject's provenance."""

    subject: EvidenceSubject
    nodes: tuple[EvidenceNode, ...] = Field(default=(), max_length=896)
    edges: tuple[EvidenceEdge, ...] = Field(default=(), max_length=1_024)
    total_nodes: int = Field(ge=0)
    total_edges: int = Field(ge=0)
    nodes_truncated: bool = False
    edges_truncated: bool = False
    state_token: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_counts(self) -> "EvidenceBundle":
        if self.total_nodes < len(self.nodes) or self.total_edges < len(self.edges):
            raise ValueError("evidence totals cannot be smaller than returned values")
        if self.nodes_truncated != (self.total_nodes > len(self.nodes)):
            raise ValueError("nodes_truncated must match the node counts")
        if self.edges_truncated != (self.total_edges > len(self.edges)):
            raise ValueError("edges_truncated must match the edge counts")
        return self


class EvidenceSnapshotFact(_FrozenEvidenceModel):
    """One bounded display fact captured when a review is created."""

    pointer: EvidencePointer
    label: str | None = Field(default=None, max_length=200)
    status: EvidenceNodeStatus = "available"

    @field_validator("label")
    @classmethod
    def normalize_label(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = " ".join(value.split())
        return normalized or None


class EvidenceSnapshot(_FrozenEvidenceModel):
    """Versioned immutable evidence summary stored with a maintenance review."""

    version: Literal[1] = 1
    pointers: tuple[EvidencePointer, ...] = Field(default=(), max_length=512)
    facts: tuple[EvidenceSnapshotFact, ...] = Field(default=(), max_length=128)
    total_nodes: int = Field(default=0, ge=0, le=100_000)
    total_edges: int = Field(default=0, ge=0, le=100_000)
    truncated: bool = False
    state_token: str = Field(
        default=_EMPTY_STATE_TOKEN,
        pattern=r"^[0-9a-f]{64}$",
    )

    @model_validator(mode="after")
    def validate_membership(self) -> "EvidenceSnapshot":
        pointer_keys = {(item.kind, item.identifier) for item in self.pointers}
        if len(pointer_keys) != len(self.pointers):
            raise ValueError("evidence snapshot pointers must be distinct")
        if any(
            (fact.pointer.kind, fact.pointer.identifier) not in pointer_keys
            for fact in self.facts
        ):
            raise ValueError("evidence snapshot facts must reference snapshot pointers")
        return self
