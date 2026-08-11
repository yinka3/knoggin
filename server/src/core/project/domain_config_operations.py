"""Pure candidate lifecycle operations for project domain configuration.

The active configuration is stored and installed by :class:`ProjectState` and
``DomainConfigStore``.  This module owns the user-facing workflow around that
state: turn a complete candidate into a validated value, describe its
future-facing impact, and only then request an optimistic activation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping

from common.conf.domain_config import DomainConfig, DomainConfigError

if TYPE_CHECKING:
    from core.project.domain_config_store import DomainActivation
    from core.project.state import ProjectState


DomainCandidate = DomainConfig | Mapping[str, Any]


def _candidate_config(candidate: DomainCandidate) -> DomainConfig:
    """Parse and compile a complete candidate without changing any state."""

    if isinstance(candidate, DomainConfig):
        # Round-tripping an instance keeps direct dataclass construction from
        # bypassing the same validation and canonicalization as mappings.
        candidate = candidate.to_dict()
    if not isinstance(candidate, Mapping):
        raise DomainConfigError("Domain configuration candidate must be an object")
    config = DomainConfig.from_mapping(candidate)
    config.compile()
    semantic_errors = _semantic_errors(config)
    if semantic_errors:
        raise DomainConfigError("; ".join(semantic_errors))
    return config


def parse_candidate(candidate: DomainCandidate) -> DomainConfig:
    """Return the canonical, compiled-ready form of one complete candidate."""

    return _candidate_config(candidate)


@dataclass(frozen=True, slots=True)
class DomainValidation:
    """The result of validating one complete candidate configuration."""

    valid: bool
    config: DomainConfig | None = None
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "config": self.config.to_dict() if self.config is not None else None,
        }


def _semantic_warnings(config: DomainConfig) -> tuple[str, ...]:
    warnings: list[str] = []
    for topic in config.topics:
        if not topic.description:
            warnings.append(
                f"Topic {topic.name!r} has no description; add one if its meaning "
                "could be ambiguous."
            )
    for entity_type in config.entity_types:
        if not entity_type.description:
            warnings.append(
                f"Entity type {entity_type.name!r} has no description; add one "
                "if its meaning could be ambiguous."
            )
    return tuple(warnings)


def _semantic_errors(config: DomainConfig) -> tuple[str, ...]:
    if not config.topics:
        return ("A domain configuration requires at least one topic.",)
    if not config.entity_types:
        return ("A domain configuration requires at least one entity type.",)
    # Relationship names are the configured meanings.  Distinct canonical
    # meanings may intentionally share the same endpoint constraints (for
    # example, USES and DEPLOYS_TO between Project and Technology).  Duplicate
    # names are already rejected by DomainConfig.from_mapping.
    return ()


def validate_domain_config(candidate: DomainCandidate) -> DomainValidation:
    """Validate and compile a candidate, returning errors instead of mutating.

    The operation is deliberately non-throwing for ordinary user input so an
    API or UI can present all validation failures as one response.  Programming
    errors such as a broken candidate type are represented in the same result.
    """

    try:
        config = _candidate_config(candidate)
    except (DomainConfigError, KeyError, TypeError, ValueError) as exc:
        return DomainValidation(valid=False, errors=(str(exc),))
    return DomainValidation(
        valid=True,
        config=config,
        warnings=_semantic_warnings(config),
    )


def _keyed(values, key):
    return {key(value): value for value in values}


def _changed_names(current, candidate, key, different):
    current_by_key = _keyed(current, key)
    candidate_by_key = _keyed(candidate, key)
    return tuple(
        candidate_by_key[item_key].name
        for item_key in sorted(current_by_key.keys() & candidate_by_key.keys())
        if different(current_by_key[item_key], candidate_by_key[item_key])
    )


@dataclass(frozen=True, slots=True)
class DomainPreview:
    """Deterministic, future-only impact summary for a candidate edit."""

    current_version: int
    candidate_version: int
    topics_added: tuple[str, ...] = ()
    topics_removed: tuple[str, ...] = ()
    topics_activated: tuple[str, ...] = ()
    topics_deactivated: tuple[str, ...] = ()
    entity_types_added: tuple[str, ...] = ()
    entity_types_removed: tuple[str, ...] = ()
    entity_types_changed: tuple[str, ...] = ()
    relationships_added: tuple[str, ...] = ()
    relationships_removed: tuple[str, ...] = ()
    relationships_changed: tuple[str, ...] = ()
    future_effects: tuple[str, ...] = ()

    @property
    def next_version(self) -> int:
        """The revision activation would assign after this preview."""

        return self.current_version + 1

    @property
    def has_changes(self) -> bool:
        return any(
            (
                self.topics_added,
                self.topics_removed,
                self.topics_activated,
                self.topics_deactivated,
                self.entity_types_added,
                self.entity_types_removed,
                self.entity_types_changed,
                self.relationships_added,
                self.relationships_removed,
                self.relationships_changed,
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "current_version": self.current_version,
            "candidate_version": self.candidate_version,
            "next_version": self.next_version,
            "has_changes": self.has_changes,
            "topics_added": list(self.topics_added),
            "topics_removed": list(self.topics_removed),
            "topics_activated": list(self.topics_activated),
            "topics_deactivated": list(self.topics_deactivated),
            "entity_types_added": list(self.entity_types_added),
            "entity_types_removed": list(self.entity_types_removed),
            "entity_types_changed": list(self.entity_types_changed),
            "relationships_added": list(self.relationships_added),
            "relationships_removed": list(self.relationships_removed),
            "relationships_changed": list(self.relationships_changed),
            "future_effects": list(self.future_effects),
        }


def _entity_changed(left, right) -> bool:
    return (
        left.topic.casefold() != right.topic.casefold()
        or left.description != right.description
        or tuple(label.casefold() for label in left.labels)
        != tuple(label.casefold() for label in right.labels)
    )


def _relationship_changed(left, right) -> bool:
    return (
        tuple(item.casefold() for item in left.source_types)
        != tuple(item.casefold() for item in right.source_types)
        or tuple(item.casefold() for item in left.target_types)
        != tuple(item.casefold() for item in right.target_types)
        or left.symmetric != right.symmetric
    )


def preview_domain_config(
    current: DomainConfig | None,
    candidate: DomainCandidate,
) -> DomainPreview:
    """Compare a candidate with the active config without persistence or I/O."""

    candidate_config = _candidate_config(candidate)
    current = current or DomainConfig(
        version=0,
        topics=(),
        entity_types=(),
        relationships=(),
    )

    current_topics = _keyed(current.topics, lambda item: item.name.casefold())
    candidate_topics = _keyed(
        candidate_config.topics,
        lambda item: item.name.casefold(),
    )
    topics_added = tuple(
        candidate_topics[key].name
        for key in sorted(candidate_topics.keys() - current_topics.keys())
    )
    topics_removed = tuple(
        current_topics[key].name
        for key in sorted(current_topics.keys() - candidate_topics.keys())
    )
    topics_activated = tuple(
        candidate_topics[key].name
        for key in sorted(current_topics.keys() & candidate_topics.keys())
        if not current_topics[key].active and candidate_topics[key].active
    )
    topics_deactivated = tuple(
        candidate_topics[key].name
        for key in sorted(current_topics.keys() & candidate_topics.keys())
        if current_topics[key].active and not candidate_topics[key].active
    )

    current_entities = _keyed(
        current.entity_types,
        lambda item: item.name.casefold(),
    )
    candidate_entities = _keyed(
        candidate_config.entity_types,
        lambda item: item.name.casefold(),
    )
    entity_types_added = tuple(
        candidate_entities[key].name
        for key in sorted(candidate_entities.keys() - current_entities.keys())
    )
    entity_types_removed = tuple(
        current_entities[key].name
        for key in sorted(current_entities.keys() - candidate_entities.keys())
    )
    entity_types_changed = _changed_names(
        current.entity_types,
        candidate_config.entity_types,
        lambda item: item.name.casefold(),
        _entity_changed,
    )

    current_relationships = _keyed(
        current.relationships,
        lambda item: item.name.casefold(),
    )
    candidate_relationships = _keyed(
        candidate_config.relationships,
        lambda item: item.name.casefold(),
    )
    relationships_added = tuple(
        candidate_relationships[key].name
        for key in sorted(candidate_relationships.keys() - current_relationships.keys())
    )
    relationships_removed = tuple(
        current_relationships[key].name
        for key in sorted(current_relationships.keys() - candidate_relationships.keys())
    )
    relationships_changed = _changed_names(
        current.relationships,
        candidate_config.relationships,
        lambda item: item.name.casefold(),
        _relationship_changed,
    )

    effects: list[str] = []
    for name in topics_added:
        effects.append(f"Future ingestion can assign entities to topic {name!r}.")
    for name in topics_activated:
        effects.append(f"Future ingestion will begin using topic {name!r}.")
    for name in topics_deactivated:
        effects.append(
            f"Future ingestion will stop assigning new entities to topic {name!r}; "
            "existing entities remain unchanged."
        )
    for name in entity_types_added:
        effects.append(
            f"Future matching entities may use entity type {name!r}; "
            "existing entities remain unchanged."
        )
    for name in entity_types_changed:
        effects.append(
            f"Future extraction for entity type {name!r} will use its updated "
            "topic, labels, or description; existing entities remain unchanged."
        )
    for name in entity_types_removed:
        effects.append(
            f"Existing entities of removed type {name!r} remain unchanged; "
            "reclassification is an explicit maintenance action."
        )
    for name in relationships_added:
        effects.append(
            f"Future matching relationships may use canonical relationship {name!r}; "
            "existing observations are not normalized automatically."
        )
    for name in relationships_changed:
        effects.append(
            f"Future matching relationships may use the updated definition of "
            f"{name!r}; existing relationships and observations remain unchanged."
        )
    if relationships_added:
        effects.append(
            "A later maintenance pass may evaluate existing unrecognized "
            "observations for eligibility."
        )
    if any(
        (
            topics_removed,
            topics_deactivated,
            entity_types_removed,
            relationships_removed,
        )
    ):
        effects.append(
            "Deactivation affects future ingestion; previously admitted batches "
            "retain their captured domain snapshot."
        )
    if not effects:
        effects.append("No future ingestion behavior changes were detected.")

    return DomainPreview(
        current_version=current.version,
        candidate_version=candidate_config.version,
        topics_added=topics_added,
        topics_removed=topics_removed,
        topics_activated=topics_activated,
        topics_deactivated=topics_deactivated,
        entity_types_added=entity_types_added,
        entity_types_removed=entity_types_removed,
        entity_types_changed=entity_types_changed,
        relationships_added=relationships_added,
        relationships_removed=relationships_removed,
        relationships_changed=relationships_changed,
        future_effects=tuple(effects),
    )


class DomainConfigOperations:
    """Convenience facade for the complete candidate lifecycle."""

    @staticmethod
    def edit(candidate: DomainCandidate) -> DomainConfig:
        """Materialize a detached candidate for the next workflow step."""

        return parse_candidate(candidate)

    @staticmethod
    def validate(candidate: DomainCandidate) -> DomainValidation:
        return validate_domain_config(candidate)

    @staticmethod
    def preview(
        current: DomainConfig | None,
        candidate: DomainCandidate,
    ) -> DomainPreview:
        return preview_domain_config(current, candidate)

    @staticmethod
    async def activate(
        project: "ProjectState",
        candidate: DomainCandidate,
        *,
        expected_version: int,
    ) -> "DomainActivation":
        """Validate a candidate, then delegate guarded activation to the state."""

        config = _candidate_config(candidate)
        return await project.activate_domain_config(
            config,
            expected_version=expected_version,
        )
