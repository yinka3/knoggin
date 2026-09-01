"""Pure aggregation of unrecognized relationship observations."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable

from common.conf.relationship_config import normalize_observed_relationship

_RELATIONSHIP_NAME = re.compile(r"^[A-Z][A-Z0-9_]{1,39}$")
ADVISORY_DISPOSITIONS = frozenset(
    {"pending", "accepted", "dismissed", "suppressed"}
)
ADVISORY_ACTIONS = frozenset(
    {"accept", "edit", "dismiss", "reopen", "suppress", "merge"}
)


class RelationshipAdvisoryDecisionError(ValueError):
    """Raised when an advisory action violates its decision lifecycle."""


@dataclass(frozen=True, slots=True)
class AdvisoryThresholds:
    """Evidence floor required before an unknown pattern becomes advisory."""

    min_occurrences: int = 3
    min_distinct_entities: int = 2
    min_distinct_messages: int = 2

    def __post_init__(self) -> None:
        if self.min_occurrences < 1:
            raise ValueError("min_occurrences must be positive")
        if self.min_distinct_entities < 1:
            raise ValueError("min_distinct_entities must be positive")
        if self.min_distinct_messages < 1:
            raise ValueError("min_distinct_messages must be positive")


@dataclass(frozen=True, slots=True)
class RelationshipAdvisory:
    """A derived suggestion; it is not authority to mutate DomainConfig."""

    pattern_key: str
    observed_label: str
    source_type: str | None
    target_type: str | None
    occurrence_count: int
    distinct_source_entities: int
    distinct_target_entities: int
    message_ids: tuple[int, ...]
    first_observed_ms: int | None
    last_observed_ms: int | None
    observation_ids: tuple[int, ...] = ()
    disposition: str = "pending"
    proposed_relationship_type: str | None = None
    decision_note: str | None = None
    last_action: str | None = None
    decision_revision: int = 0

    @property
    def distinct_entities(self) -> int:
        return self.distinct_source_entities + self.distinct_target_entities

    def to_dict(self) -> dict[str, Any]:
        return {
            "pattern_key": self.pattern_key,
            "observed_label": self.observed_label,
            "source_type": self.source_type,
            "target_type": self.target_type,
            "occurrence_count": self.occurrence_count,
            "distinct_source_entities": self.distinct_source_entities,
            "distinct_target_entities": self.distinct_target_entities,
            "distinct_entities": self.distinct_entities,
            "message_ids": list(self.message_ids),
            "observation_ids": list(self.observation_ids),
            "first_observed_ms": self.first_observed_ms,
            "last_observed_ms": self.last_observed_ms,
            "disposition": self.disposition,
            "proposed_relationship_type": self.proposed_relationship_type,
            "decision_note": self.decision_note,
            "last_action": self.last_action,
            "decision_revision": self.decision_revision,
        }


@dataclass(frozen=True, slots=True)
class RelationshipAdvisoryDecision:
    """Current durable decision state for one advisory pattern."""

    pattern_key: str
    disposition: str = "pending"
    proposed_relationship_type: str | None = None
    last_action: str | None = None
    decision_note: str | None = None
    decided_by: str | None = None
    revision: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.pattern_key, str) or not self.pattern_key.strip():
            raise RelationshipAdvisoryDecisionError(
                "pattern_key must be a non-empty string"
            )
        if self.disposition not in ADVISORY_DISPOSITIONS:
            raise RelationshipAdvisoryDecisionError(
                f"Unknown advisory disposition: {self.disposition!r}"
            )
        if self.last_action is not None and self.last_action not in ADVISORY_ACTIONS:
            raise RelationshipAdvisoryDecisionError(
                f"Unknown advisory action: {self.last_action!r}"
            )
        if not isinstance(self.revision, int) or isinstance(self.revision, bool):
            raise RelationshipAdvisoryDecisionError("revision must be an integer")
        if self.revision < 0:
            raise RelationshipAdvisoryDecisionError("revision must be non-negative")

    def to_dict(self) -> dict[str, Any]:
        return {
            "pattern_key": self.pattern_key,
            "disposition": self.disposition,
            "proposed_relationship_type": self.proposed_relationship_type,
            "last_action": self.last_action,
            "decision_note": self.decision_note,
            "decided_by": self.decided_by,
            "revision": self.revision,
        }


def _relationship_type(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RelationshipAdvisoryDecisionError(
            "relationship_type must be a string when provided"
        )
    value = "_".join(value.strip().upper().split())
    if not _RELATIONSHIP_NAME.fullmatch(value):
        raise RelationshipAdvisoryDecisionError(
            f"Invalid proposed relationship type: {value!r}"
        )
    return value


def apply_advisory_action(
    current: RelationshipAdvisoryDecision | None,
    *,
    pattern_key: str,
    action: str,
    relationship_type: str | None = None,
    note: str | None = None,
    decided_by: str | None = None,
) -> RelationshipAdvisoryDecision:
    """Apply one explicit advisory action to the current durable state.

    Actions only change advisory disposition and decision metadata. They never
    activate a DomainConfig or rewrite historical relationship evidence.
    """

    if current is not None and current.pattern_key != pattern_key:
        raise RelationshipAdvisoryDecisionError(
            "Current advisory state does not match pattern_key"
        )
    action = action.strip().lower() if isinstance(action, str) else ""
    if action not in ADVISORY_ACTIONS:
        raise RelationshipAdvisoryDecisionError(f"Unknown advisory action: {action!r}")

    current = current or RelationshipAdvisoryDecision(pattern_key=pattern_key)
    proposed = _relationship_type(relationship_type)
    if proposed is None:
        proposed = current.proposed_relationship_type
    if action in {"accept", "merge"} and proposed is None:
        raise RelationshipAdvisoryDecisionError(
            f"{action} requires a proposed relationship type"
        )
    if action == "edit" and proposed is None:
        raise RelationshipAdvisoryDecisionError(
            "edit requires a proposed relationship type"
        )

    allowed = {
        "accept": {"pending"},
        "merge": {"pending"},
        "edit": {"pending"},
        "dismiss": {"pending"},
        "suppress": {"pending", "dismissed"},
        "reopen": {"accepted", "dismissed", "suppressed"},
    }
    if current.disposition not in allowed[action]:
        raise RelationshipAdvisoryDecisionError(
            f"Cannot {action} an advisory in {current.disposition} state"
        )

    disposition = current.disposition
    if action == "accept" or action == "merge":
        disposition = "accepted"
    elif action == "dismiss":
        disposition = "dismissed"
    elif action == "suppress":
        disposition = "suppressed"
    elif action == "reopen":
        disposition = "pending"

    normalized_note = None
    if note is not None:
        if not isinstance(note, str):
            raise RelationshipAdvisoryDecisionError("note must be a string")
        normalized_note = " ".join(note.split()) or None

    return RelationshipAdvisoryDecision(
        pattern_key=pattern_key,
        disposition=disposition,
        proposed_relationship_type=proposed,
        last_action=action,
        decision_note=normalized_note,
        decided_by=decided_by,
        revision=current.revision + 1,
    )


def _text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split())
    return normalized or None


def _pattern_key(observed_label: str, source_type: str | None, target_type: str | None):
    return "|".join(
        (
            observed_label,
            (source_type or "").casefold(),
            (target_type or "").casefold(),
        )
    )


def build_relationship_advisories(
    observations: Iterable[dict[str, Any]],
    *,
    thresholds: AdvisoryThresholds | None = None,
) -> list[RelationshipAdvisory]:
    """Group unknown observations by directional wording and endpoint types."""

    thresholds = thresholds or AdvisoryThresholds()
    groups: dict[str, dict[str, Any]] = {}
    for row in observations:
        if row.get("interpretation_source", "observed") != "observed":
            continue
        if row.get("domain_status", "unrecognized") != "unrecognized":
            continue
        raw_label = (
            row.get("observed_relationship_label")
            or row.get("observed_label")
            or row.get("relationship_type")
        )
        if not raw_label:
            continue
        try:
            observed_label = normalize_observed_relationship(raw_label)
        except ValueError:
            continue
        source_type = _text(row.get("source_type") or row.get("source_entity_type"))
        target_type = _text(row.get("target_type") or row.get("target_entity_type"))
        key = _pattern_key(observed_label, source_type, target_type)
        group = groups.setdefault(
            key,
            {
                "observed_label": observed_label,
                "source_type": source_type,
                "target_type": target_type,
                "occurrences": 0,
                "message_ids": set(),
                "observation_ids": set(),
                "source_entities": set(),
                "target_entities": set(),
                "first_observed_ms": None,
                "last_observed_ms": None,
            },
        )
        group["occurrences"] += 1
        group["message_ids"].add(int(row["message_id"]))
        observation_id = row.get("observation_id")
        if observation_id is not None:
            group["observation_ids"].add(int(observation_id))
        source_id = row.get("source_entity_id") or row.get("entity_a_id")
        target_id = row.get("target_entity_id") or row.get("entity_b_id")
        if source_id is not None:
            group["source_entities"].add(int(source_id))
        if target_id is not None:
            group["target_entities"].add(int(target_id))
        observed_ms = row.get("observed_at_ms")
        if observed_ms is not None:
            observed_ms = int(observed_ms)
            first = group["first_observed_ms"]
            last = group["last_observed_ms"]
            group["first_observed_ms"] = (
                observed_ms if first is None else min(first, observed_ms)
            )
            group["last_observed_ms"] = (
                observed_ms if last is None else max(last, observed_ms)
            )

    advisories = []
    for key, group in groups.items():
        occurrence_count = group["occurrences"]
        distinct_entities = len(group["source_entities"] | group["target_entities"])
        if (
            occurrence_count < thresholds.min_occurrences
            or distinct_entities < thresholds.min_distinct_entities
            or len(group["message_ids"]) < thresholds.min_distinct_messages
        ):
            continue
        advisories.append(
            RelationshipAdvisory(
                pattern_key=key,
                observed_label=group["observed_label"],
                source_type=group["source_type"],
                target_type=group["target_type"],
                occurrence_count=occurrence_count,
                distinct_source_entities=len(group["source_entities"]),
                distinct_target_entities=len(group["target_entities"]),
                message_ids=tuple(sorted(group["message_ids"])),
                observation_ids=tuple(sorted(group["observation_ids"])),
                first_observed_ms=group["first_observed_ms"],
                last_observed_ms=group["last_observed_ms"],
            )
        )
    return sorted(
        advisories,
        key=lambda item: (-item.occurrence_count, item.pattern_key),
    )
