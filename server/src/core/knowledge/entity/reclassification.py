"""Deterministic planning for explicit historical entity reclassification."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from common.conf.domain_config import CompiledDomain
from common.scoping import IDENTITY_ENTITY_ID


@dataclass(frozen=True, slots=True)
class EntityReclassification:
    """One historical entity change approved by the active domain snapshot."""

    entity_id: int
    canonical_name: str
    old_type: str | None
    old_topic: str
    new_type: str
    new_topic: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "entity_id": self.entity_id,
            "canonical_name": self.canonical_name,
            "old_type": self.old_type,
            "old_topic": self.old_topic,
            "new_type": self.new_type,
            "new_topic": self.new_topic,
        }


@dataclass(frozen=True, slots=True)
class ReclassificationPlan:
    """A bounded, deterministic plan that has not changed durable state."""

    domain_version: int
    scanned: int
    changes: tuple[EntityReclassification, ...] = ()
    unchanged: int = 0
    unmapped: int = 0

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
            "changes": [change.to_dict() for change in self.changes],
            "search_index_rebuild_required": self.changed > 0,
        }


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _target_for_row(
    row: Mapping[str, Any],
    domain: CompiledDomain,
) -> tuple[str, str] | None:
    """Resolve an old canonical type or extraction label to a new target."""

    old_type = _clean_text(row.get("type"))
    if not old_type:
        return None

    new_type = domain.canonical_entity_type(old_type)
    if new_type is None:
        new_type = domain.resolve_entity_type(old_type)
    if new_type is None:
        return None

    new_topic = domain.topic_for_entity_type(new_type)
    if new_topic is None:
        return None
    return new_type, new_topic


def plan_reclassification(
    rows: Iterable[Mapping[str, Any]],
    domain: CompiledDomain,
) -> ReclassificationPlan:
    """Build a future-only change plan from canonical entity rows.

    The planner never guesses from names or message text. It only maps the
    persisted type through the active domain's canonical type and label
    lookups. Unmapped entities are intentionally left untouched for explicit
    user review.
    """

    if not isinstance(domain, CompiledDomain):
        raise TypeError("domain must be a CompiledDomain")

    scanned = 0
    unchanged = 0
    unmapped = 0
    changes: list[EntityReclassification] = []
    for row in rows:
        scanned += 1
        try:
            entity_id = int(row["entity_id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("Entity rows must include a numeric entity_id") from exc
        if entity_id == IDENTITY_ENTITY_ID:
            unchanged += 1
            continue

        target = _target_for_row(row, domain)
        if target is None:
            unmapped += 1
            continue

        new_type, new_topic = target
        old_type = _clean_text(row.get("type")) or None
        old_topic = _clean_text(row.get("topic"))
        if old_type == new_type and old_topic == new_topic:
            unchanged += 1
            continue

        changes.append(
            EntityReclassification(
                entity_id=entity_id,
                canonical_name=_clean_text(row.get("canonical_name")),
                old_type=old_type,
                old_topic=old_topic,
                new_type=new_type,
                new_topic=new_topic,
            )
        )

    return ReclassificationPlan(
        domain_version=domain.version,
        scanned=scanned,
        changes=tuple(changes),
        unchanged=unchanged,
        unmapped=unmapped,
    )
