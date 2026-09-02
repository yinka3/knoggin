"""Typed, durable proposals for semantic knowledge maintenance.

Maintenance reviews are the only workflow envelope used by project and
user-global repair work.  The proposal itself is validated before it reaches
storage; callers never persist model-generated SQL or arbitrary JSON patches.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator

ReviewScope = Literal["project", "user-global"]
ReviewStatus = Literal["open", "applied", "dismissed", "stale"]
InterpretationSource = Literal["observed", "domain", "review"]


class EvidenceRef(BaseModel):
    """A stable reference to evidence used by a review."""

    model_config = ConfigDict(extra="forbid")

    kind: str = Field(min_length=1, max_length=40)
    id: str = Field(min_length=1, max_length=200)

    @classmethod
    def from_value(cls, value: Any) -> "EvidenceRef":
        if isinstance(value, (str, int)):
            return cls(kind="observation", id=str(value))
        return cls.model_validate(value)


class RelationshipInterpretationChange(BaseModel):
    """One in-place observation interpretation change."""

    model_config = ConfigDict(extra="forbid")

    observation_id: int = Field(gt=0)
    expected_relationship_id: str | None = None
    target_relationship_type: str | None = Field(default=None, min_length=1)
    interpretation_source: InterpretationSource

    @field_validator("target_relationship_type")
    @classmethod
    def normalize_type(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = "_".join(value.strip().upper().split())
        if not value:
            raise ValueError("target_relationship_type must not be blank")
        return value


class RelationshipInterpretationPlan(BaseModel):
    """Typed changes to the current interpretation of observations."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["relationship_interpretation"] = "relationship_interpretation"
    changes: list[RelationshipInterpretationChange] = Field(max_length=512)

    @field_validator("changes")
    @classmethod
    def validate_exclusions(cls, values):
        observation_ids: set[int] = set()
        for change in values:
            if change.observation_id in observation_ids:
                raise ValueError("relationship interpretation observation IDs must be unique")
            observation_ids.add(change.observation_id)
            if (
                change.target_relationship_type is None
                and change.interpretation_source != "review"
            ):
                raise ValueError(
                    "detaching an observation requires interpretation_source='review'"
                )
        return values


class RelationshipDomainChangePlan(BaseModel):
    """A reusable relationship-definition change, separate from evidence repair."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["relationship_domain_change"] = "relationship_domain_change"
    relationship_name: str = Field(min_length=1, max_length=40)
    add_labels: list[str] = Field(default_factory=list, max_length=64)
    source_types: list[str] = Field(default_factory=list, max_length=64)
    target_types: list[str] = Field(default_factory=list, max_length=64)
    symmetric: bool | None = None


class EntityContextChangePlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["entity_context_change"] = "entity_context_change"
    entity_id: int = Field(gt=0)
    project_id: str = Field(min_length=1)
    entity_type: str | None = Field(default=None, min_length=1)
    topic: str | None = Field(default=None, min_length=1)


class EntityMergePlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["entity_merge"] = "entity_merge"
    survivor_entity_id: int = Field(gt=0)
    retired_entity_id: int = Field(gt=0)
    context_choices: list["EntityContextMergeChoice"] = Field(default_factory=list)
    frontier_tokens: dict[str, str] = Field(default_factory=dict)
    definition_versions: dict[str, int] = Field(default_factory=dict)
    expected_state_hash: str | None = Field(default=None, min_length=1)

    @field_validator("retired_entity_id")
    @classmethod
    def distinct_entities(cls, value: int, info):
        survivor = info.data.get("survivor_entity_id")
        if survivor is not None and value == survivor:
            raise ValueError("merge entities must be distinct")
        return value


class EntityContextMergeChoice(BaseModel):
    """Explicit replacement for one conflicting project context."""

    model_config = ConfigDict(extra="forbid")

    project_id: str = Field(min_length=1, max_length=200)
    entity_type: str = Field(min_length=1, max_length=200)
    topic: str = Field(min_length=1, max_length=200)


class EntityMergeRollbackPlan(BaseModel):
    """Change-oriented inverse plan for a completed global merge."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["entity_merge_rollback"] = "entity_merge_rollback"
    merge_id: str = Field(min_length=1, max_length=200)
    safe_mutation_ids: list[int] = Field(default_factory=list)
    conflicting_mutation_ids: list[int] = Field(default_factory=list)
    required_decisions: list[str] = Field(default_factory=list)


class ConflictResolutionPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["conflict_resolution"] = "conflict_resolution"
    conflict_kind: str = Field(min_length=1, max_length=80)
    resolution: str | None = Field(default=None, max_length=80)
    note: str | None = Field(default=None, max_length=2_000)


class RelationshipAdvisoryPlan(BaseModel):
    """Typed decision metadata for an evidence-backed relationship advisory."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["relationship_advisory"] = "relationship_advisory"
    pattern_key: str = Field(min_length=1, max_length=500)
    observed_label: str | None = None
    proposed_relationship_type: str | None = None
    action: str | None = None
    note: str | None = None


MaintenancePlan = Annotated[
    Union[
        RelationshipInterpretationPlan,
        RelationshipDomainChangePlan,
        EntityContextChangePlan,
        EntityMergePlan,
        EntityMergeRollbackPlan,
        ConflictResolutionPlan,
        RelationshipAdvisoryPlan,
    ],
    Field(discriminator="kind"),
]

_PLAN_ADAPTER = TypeAdapter(MaintenancePlan)


def validate_plan(value: MaintenancePlan | dict[str, Any]) -> MaintenancePlan:
    """Validate one of the server-owned maintenance mutation vocabularies."""

    return _PLAN_ADAPTER.validate_python(value)


class MaintenanceReview(BaseModel):
    """A stable proposal/snapshot that can be explicitly applied or dismissed."""

    model_config = ConfigDict(extra="forbid")

    review_id: str = Field(min_length=1, max_length=200)
    user_name: str = Field(min_length=1, max_length=200)
    scope: ReviewScope
    project_id: str | None = None
    kind: str = Field(min_length=1, max_length=80)
    dedupe_key: str | None = Field(default=None, max_length=500)
    evidence_refs: list[EvidenceRef] = Field(default_factory=list, max_length=512)
    evidence_snapshot: dict[str, Any] = Field(default_factory=dict)
    reasoning: str = Field(min_length=1, max_length=8_000)
    proposed_plan: MaintenancePlan
    expected_state: dict[str, Any] = Field(default_factory=dict)
    status: ReviewStatus = "open"
    created_at: Any | None = None
    resolved_at: Any | None = None

    @field_validator("project_id")
    @classmethod
    def project_required_for_project_scope(cls, value: str | None, info):
        if info.data.get("scope") == "project" and not value:
            raise ValueError("project_id is required for project-scoped reviews")
        return value

    @field_validator("evidence_refs", mode="before")
    @classmethod
    def normalize_evidence_refs(cls, value: Any) -> list[EvidenceRef]:
        return [EvidenceRef.from_value(item) for item in (value or [])]

    @field_validator("proposed_plan", mode="before")
    @classmethod
    def normalize_plan(cls, value: Any) -> MaintenancePlan:
        return validate_plan(value)

    def evidence_ids(self, kind: str | None = None) -> tuple[int, ...]:
        refs = self.evidence_refs
        if kind is not None:
            refs = [ref for ref in refs if ref.kind == kind]
        values: list[int] = []
        for ref in refs:
            try:
                value = int(ref.id)
            except (TypeError, ValueError):
                continue
            if value > 0:
                values.append(value)
        return tuple(dict.fromkeys(values))


def review_from_row(row: dict[str, Any]) -> MaintenanceReview:
    """Hydrate a database row, accepting JSON strings from lightweight fakes."""

    import json

    payload = dict(row)
    for field in ("evidence_refs", "evidence_snapshot", "proposed_plan", "expected_state"):
        value = payload.get(field)
        if isinstance(value, str):
            payload[field] = json.loads(value)
    return MaintenanceReview.model_validate(payload)
