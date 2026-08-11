"""Canonical and observed relationship normalization."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

from common.conf.domain_config import CompiledDomain

RelationshipDomainStatus = Literal["recognized", "unrecognized"]


def normalize_observed_relationship(value: object) -> str:
    """Normalize extracted wording while retaining its evidence-level meaning."""

    if not isinstance(value, str):
        raise ValueError("observed relationship label must be text")
    normalized = " ".join(value.split()).casefold()
    if not normalized:
        raise ValueError("observed relationship label must not be blank")
    return normalized


def relationship_name_key(value: object) -> str:
    """Normalize a configured name and model wording for exact matching."""

    normalized = normalize_observed_relationship(value)
    return re.sub(r"[-\s]+", "_", normalized)


@dataclass(frozen=True, slots=True)
class RelationshipNormalization:
    """One relationship's canonical interpretation and source evidence."""

    observed_label: str
    canonical_type: str | None
    domain_status: RelationshipDomainStatus
    source_type: str | None = None
    target_type: str | None = None
    symmetric: bool = False
    reason: str | None = None

    @property
    def persistence_type(self) -> str:
        """Return the stable aggregate identity label for graph persistence."""

        return self.canonical_type or self.observed_label


def normalize_relationship(
    domain: CompiledDomain,
    observed_label: object,
    *,
    source_type: str | None,
    target_type: str | None,
) -> RelationshipNormalization:
    """Map observed wording to a constrained canonical relationship when valid.

    Unknown wording and known wording with incompatible endpoint types are both
    valid evidence. They remain directional and retain the normalized observed
    label rather than being silently coerced into a configured relationship.
    """

    if not isinstance(domain, CompiledDomain):
        raise TypeError("domain must be a CompiledDomain")
    observed = normalize_observed_relationship(observed_label)
    if isinstance(source_type, str):
        source_type = source_type.strip() or None
    if isinstance(target_type, str):
        target_type = target_type.strip() or None
    source_type = (
        domain.canonical_entity_type(source_type or "")
        or domain.resolve_entity_type(source_type or "")
        or (source_type if isinstance(source_type, str) else None)
    )
    target_type = (
        domain.canonical_entity_type(target_type or "")
        or domain.resolve_entity_type(target_type or "")
        or (target_type if isinstance(target_type, str) else None)
    )
    definition = domain.relationship_by_key(relationship_name_key(observed))
    if definition is None:
        return RelationshipNormalization(
            observed_label=observed,
            canonical_type=None,
            domain_status="unrecognized",
            source_type=source_type,
            target_type=target_type,
            reason="unknown_relationship",
        )

    if not domain.relationship_allows(
        definition.name,
        source_type or "",
        target_type or "",
    ):
        return RelationshipNormalization(
            observed_label=observed,
            canonical_type=None,
            domain_status="unrecognized",
            source_type=source_type,
            target_type=target_type,
            symmetric=False,
            reason="endpoint_type_mismatch",
        )

    return RelationshipNormalization(
        observed_label=observed,
        canonical_type=definition.name,
        domain_status="recognized",
        source_type=source_type,
        target_type=target_type,
        symmetric=definition.symmetric,
    )
