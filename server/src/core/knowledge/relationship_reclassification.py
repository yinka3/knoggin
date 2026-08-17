"""Deterministic planning for explicit historical relationship normalization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from common.conf.domain_config import CompiledDomain
from common.conf.relationship_config import normalize_relationship
from common.schema.ingestion.contracts import relationship_identity


@dataclass(frozen=True, slots=True)
class RelationshipReclassification:
    """One persisted relationship aggregate that can be normalized."""

    relationship_id: str
    project_id: str
    entity_a_id: int
    entity_b_id: int
    old_relationship_type: str
    old_canonical_relationship_type: str | None
    old_domain_status: str
    old_symmetric: bool
    observed_relationship_label: str
    source_type: str | None
    target_type: str | None
    new_relationship_id: str
    new_relationship_type: str
    new_canonical_relationship_type: str
    new_domain_status: str
    new_symmetric: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "relationship_id": self.relationship_id,
            "project_id": self.project_id,
            "entity_a_id": self.entity_a_id,
            "entity_b_id": self.entity_b_id,
            "old_relationship_type": self.old_relationship_type,
            "old_canonical_relationship_type": self.old_canonical_relationship_type,
            "old_domain_status": self.old_domain_status,
            "old_symmetric": self.old_symmetric,
            "observed_relationship_label": self.observed_relationship_label,
            "source_type": self.source_type,
            "target_type": self.target_type,
            "new_relationship_id": self.new_relationship_id,
            "new_relationship_type": self.new_relationship_type,
            "new_canonical_relationship_type": self.new_canonical_relationship_type,
            "new_domain_status": self.new_domain_status,
            "new_symmetric": self.new_symmetric,
        }


@dataclass(frozen=True, slots=True)
class RelationshipReclassificationPlan:
    """A bounded relationship plan that has not changed durable state."""

    domain_version: int
    scanned: int
    changes: tuple[RelationshipReclassification, ...] = ()
    unchanged: int = 0
    unmapped: int = 0
    incompatible: int = 0

    @property
    def changed(self) -> int:
        return len(self.changes)

    def to_dict(self) -> dict[str, Any]:
        return {
            "domain_version": self.domain_version,
            "scanned": self.scanned,
            "changed": self.changed,
            "unchanged": self.unchanged,
            "unmapped": self.unmapped,
            "incompatible": self.incompatible,
            "changes": [change.to_dict() for change in self.changes],
            "projection_rebuild_required": self.changed > 0,
        }


def _text(value: object) -> str:
    return str(value or "").strip()


def _normalized_source_types(row: Mapping[str, Any]) -> tuple[str | None, str | None]:
    source = _text(row.get("source_type")) or None
    target = _text(row.get("target_type")) or None
    return source, target


def plan_relationship_reclassification(
    rows: Iterable[Mapping[str, Any]],
    domain: CompiledDomain,
) -> RelationshipReclassificationPlan:
    """Plan only exact vocabulary and endpoint-compatible reclassifications.

    The planner deliberately does not infer relationship meaning from entity
    names, message text, or an arbitrary type guess. Rows without persisted
    endpoint types remain unmapped for explicit review.
    """

    if not isinstance(domain, CompiledDomain):
        raise TypeError("domain must be a CompiledDomain")

    scanned = unchanged = unmapped = incompatible = 0
    changes: list[RelationshipReclassification] = []
    for row in rows:
        scanned += 1
        try:
            relationship_id = _text(row["relationship_id"])
            project_id = _text(row["project_id"])
            entity_a_id = int(row["entity_a_id"])
            entity_b_id = int(row["entity_b_id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                "Relationship rows require relationship_id, project_id, "
                "and numeric endpoint IDs"
            ) from exc
        if not relationship_id or not project_id or entity_a_id <= 0 or entity_b_id <= 0:
            raise ValueError("Relationship rows contain invalid identity fields")

        observed = _text(row.get("observed_relationship_label")) or _text(
            row.get("relationship_type")
        )
        if not observed:
            unmapped += 1
            continue
        source_type, target_type = _normalized_source_types(row)
        if not source_type or not target_type:
            unmapped += 1
            continue

        normalization = normalize_relationship(
            domain,
            observed,
            source_type=source_type,
            target_type=target_type,
        )
        if normalization.domain_status != "recognized":
            if normalization.reason == "endpoint_type_mismatch":
                incompatible += 1
            else:
                unmapped += 1
            continue

        canonical_type = normalization.canonical_type
        if canonical_type is None:
            unmapped += 1
            continue
        new_relationship_id = relationship_identity(
            project_id,
            entity_a_id,
            entity_b_id,
            canonical_type,
            symmetric=normalization.symmetric,
        )
        old_type = _text(row.get("relationship_type"))
        old_canonical = _text(row.get("canonical_relationship_type")) or None
        old_status = _text(row.get("domain_status")) or "unrecognized"
        old_symmetric = bool(row.get("symmetric", False))
        if (
            relationship_id == new_relationship_id
            and old_type == canonical_type
            and old_canonical == canonical_type
            and old_status == "recognized"
            and old_symmetric == normalization.symmetric
        ):
            unchanged += 1
            continue

        changes.append(
            RelationshipReclassification(
                relationship_id=relationship_id,
                project_id=project_id,
                entity_a_id=entity_a_id,
                entity_b_id=entity_b_id,
                old_relationship_type=old_type,
                old_canonical_relationship_type=old_canonical,
                old_domain_status=old_status,
                old_symmetric=old_symmetric,
                observed_relationship_label=normalization.observed_label,
                source_type=normalization.source_type,
                target_type=normalization.target_type,
                new_relationship_id=new_relationship_id,
                new_relationship_type=canonical_type,
                new_canonical_relationship_type=canonical_type,
                new_domain_status="recognized",
                new_symmetric=normalization.symmetric,
            )
        )

    return RelationshipReclassificationPlan(
        domain_version=domain.version,
        scanned=scanned,
        changes=tuple(changes),
        unchanged=unchanged,
        unmapped=unmapped,
        incompatible=incompatible,
    )


# Short alias matching the entity planner's public naming convention.
plan_reclassification = plan_relationship_reclassification
