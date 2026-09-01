"""User-owned domain configuration and its immutable runtime compilation."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping

_NAME_PATTERN = re.compile(r"^[A-Z][A-Za-z0-9 _-]{1,39}$")
_RELATIONSHIP_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]{1,39}$")


class DomainConfigError(ValueError):
    """Raised when a domain configuration cannot be validated."""


def _text(value: object, field: str, *, required: bool = True) -> str:
    if not isinstance(value, str):
        raise DomainConfigError(f"{field} must be a string")
    value = " ".join(value.split())
    if required and not value:
        raise DomainConfigError(f"{field} must not be blank")
    return value


def _name(value: object, field: str, pattern: re.Pattern[str] = _NAME_PATTERN) -> str:
    value = _text(value, field)
    if not pattern.fullmatch(value):
        raise DomainConfigError(f"Invalid {field}: {value!r}")
    return value


def _unique_texts(value: object, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple)):
        raise DomainConfigError(f"{field} must be a list of strings")
    values: list[str] = []
    seen: set[str] = set()
    for index, item in enumerate(value):
        item = _text(item, f"{field}[{index}]")
        key = item.casefold()
        if key in seen:
            raise DomainConfigError(f"{field} contains a duplicate: {item!r}")
        seen.add(key)
        values.append(item)
    return tuple(values)


def _frozen_mapping(values: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return a read-only view for one compiled lookup table."""

    return MappingProxyType(dict(values))


@dataclass(frozen=True, slots=True)
class TopicDefinition:
    name: str
    description: str = ""
    active: bool = True


@dataclass(frozen=True, slots=True)
class EntityTypeDefinition:
    name: str
    topic: str
    description: str = ""
    labels: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RelationshipDefinition:
    name: str
    source_types: tuple[str, ...]
    target_types: tuple[str, ...]
    symmetric: bool = False
    labels: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DomainConfig:
    """Complete validated project configuration.

    ``version`` is the active project revision. The activation repository
    assigns the next revision; callers should not use it as an arbitrary
    schema-version field.
    """

    version: int
    topics: tuple[TopicDefinition, ...]
    entity_types: tuple[EntityTypeDefinition, ...]
    relationships: tuple[RelationshipDefinition, ...] = ()

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "DomainConfig":
        if not isinstance(payload, Mapping):
            raise DomainConfigError("Domain configuration must be an object")

        unknown_keys = set(payload) - {
            "version",
            "topics",
            "entity_types",
            "relationships",
        }
        if unknown_keys:
            raise DomainConfigError(
                f"Unknown domain configuration fields: {sorted(unknown_keys)!r}"
            )

        version = payload.get("version", 0)
        if not isinstance(version, int) or isinstance(version, bool) or version < 0:
            raise DomainConfigError("version must be a non-negative integer")

        topics_payload = payload.get("topics", {})
        if not isinstance(topics_payload, Mapping):
            raise DomainConfigError("topics must be an object")
        topics: list[TopicDefinition] = []
        topic_keys: set[str] = set()
        for raw_name, raw_value in topics_payload.items():
            name = _name(raw_name, "topic name")
            key = name.casefold()
            if key in topic_keys:
                raise DomainConfigError(f"Duplicate topic name: {name}")
            topic_keys.add(key)
            if raw_value is None:
                raw_value = {}
            if not isinstance(raw_value, Mapping):
                raise DomainConfigError(f"Topic {name!r} must be an object")
            unknown_fields = set(raw_value) - {"description", "active"}
            if unknown_fields:
                raise DomainConfigError(
                    f"Unknown fields for topic {name!r}: {sorted(unknown_fields)!r}"
                )
            active = raw_value.get("active", True)
            if not isinstance(active, bool):
                raise DomainConfigError(f"Topic {name!r}.active must be a boolean")
            topics.append(
                TopicDefinition(
                    name=name,
                    description=_text(
                        raw_value.get("description", ""),
                        f"Topic {name!r}.description",
                        required=False,
                    ),
                    active=active,
                )
            )

        entity_payload = payload.get("entity_types", {})
        if not isinstance(entity_payload, Mapping):
            raise DomainConfigError("entity_types must be an object")
        topic_names = {topic.name.casefold(): topic.name for topic in topics}
        entity_types: list[EntityTypeDefinition] = []
        entity_keys: set[str] = set()
        label_owners: dict[str, str] = {}
        for raw_name, raw_value in entity_payload.items():
            name = _name(raw_name, "entity type name")
            key = name.casefold()
            if key in entity_keys:
                raise DomainConfigError(f"Duplicate entity type name: {name}")
            entity_keys.add(key)
            if not isinstance(raw_value, Mapping):
                raise DomainConfigError(f"Entity type {name!r} must be an object")
            unknown_fields = set(raw_value) - {"topic", "description", "labels"}
            if unknown_fields:
                raise DomainConfigError(
                    f"Unknown fields for entity type {name!r}: "
                    f"{sorted(unknown_fields)!r}"
                )
            raw_topic = _text(
                raw_value.get("topic"),
                f"Entity type {name!r}.topic",
            )
            topic = topic_names.get(raw_topic.casefold())
            if topic is None:
                raise DomainConfigError(
                    f"Entity type {name!r} references unknown topic: {raw_topic}"
                )
            labels = _unique_texts(
                raw_value.get("labels", []),
                f"Entity type {name!r}.labels",
            )
            labels = tuple(label.casefold() for label in labels)
            for label in labels:
                label_key = label.casefold()
                owner = label_owners.get(label_key)
                if owner is not None and owner != name:
                    raise DomainConfigError(
                        f"Extraction label {label!r} is claimed by both {owner!r} and {name!r}"
                    )
                label_owners[label_key] = name
            entity_types.append(
                EntityTypeDefinition(
                    name=name,
                    topic=topic,
                    description=_text(
                        raw_value.get("description", ""),
                        f"Entity type {name!r}.description",
                        required=False,
                    ),
                    labels=labels,
                )
            )

        relationships_payload = payload.get("relationships", {})
        if relationships_payload is None:
            relationships_payload = {}
        if not isinstance(relationships_payload, Mapping):
            raise DomainConfigError("relationships must be an object")
        entity_names = {entity.name.casefold(): entity.name for entity in entity_types}
        relationships: list[RelationshipDefinition] = []
        relationship_keys: set[str] = set()
        for raw_name, raw_value in relationships_payload.items():
            name = _name(
                raw_name,
                "relationship name",
                pattern=_RELATIONSHIP_PATTERN,
            )
            key = name.casefold()
            if key in relationship_keys:
                raise DomainConfigError(f"Duplicate relationship name: {name}")
            relationship_keys.add(key)
            if not isinstance(raw_value, Mapping):
                raise DomainConfigError(f"Relationship {name!r} must be an object")
            unknown_fields = set(raw_value) - {
                "source_types",
                "target_types",
                "symmetric",
                "labels",
                "aliases",
            }
            if unknown_fields:
                raise DomainConfigError(
                    f"Unknown fields for relationship {name!r}: "
                    f"{sorted(unknown_fields)!r}"
                )
            source_types = _unique_texts(
                raw_value.get("source_types", []),
                f"Relationship {name!r}.source_types",
            )
            target_types = _unique_texts(
                raw_value.get("target_types", []),
                f"Relationship {name!r}.target_types",
            )
            known_entities = set(entity_names)
            unknown = [
                endpoint
                for endpoint in (*source_types, *target_types)
                if endpoint.casefold() not in known_entities
            ]
            if unknown:
                raise DomainConfigError(
                    f"Relationship {name!r} references unknown entity types: {unknown}"
                )
            source_types = tuple(entity_names[item.casefold()] for item in source_types)
            target_types = tuple(entity_names[item.casefold()] for item in target_types)
            symmetric = raw_value.get("symmetric", False)
            if not isinstance(symmetric, bool):
                raise DomainConfigError(
                    f"Relationship {name!r}.symmetric must be a boolean"
                )
            if not source_types or not target_types:
                raise DomainConfigError(
                    f"Relationship {name!r} must define source_types and target_types"
                )
            if symmetric and {item.casefold() for item in source_types} != {
                item.casefold() for item in target_types
            }:
                raise DomainConfigError(
                    f"Symmetric relationship {name!r} must use the same source "
                    "and target entity types"
                )
            labels = _unique_texts(
                raw_value.get("labels", raw_value.get("aliases", [])),
                f"Relationship {name!r}.labels",
            )
            aliases = _unique_texts(
                raw_value.get("aliases", []),
                f"Relationship {name!r}.aliases",
            )
            labels = tuple(
                dict.fromkeys(
                    " ".join(item.split()).casefold()
                    for item in (*labels, *aliases)
                )
            )
            relationships.append(
                RelationshipDefinition(
                    name=name,
                    source_types=source_types,
                    target_types=target_types,
                    symmetric=symmetric,
                    labels=labels,
                )
            )

        return cls(
            version=version,
            topics=tuple(topics),
            entity_types=tuple(entity_types),
            relationships=tuple(relationships),
        )

    def with_version(self, version: int) -> "DomainConfig":
        if not isinstance(version, int) or isinstance(version, bool) or version < 0:
            raise DomainConfigError("version must be a non-negative integer")
        return DomainConfig(
            version=version,
            topics=self.topics,
            entity_types=self.entity_types,
            relationships=self.relationships,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "topics": {
                topic.name: {
                    "description": topic.description,
                    "active": topic.active,
                }
                for topic in self.topics
            },
            "entity_types": {
                entity.name: {
                    "topic": entity.topic,
                    "description": entity.description,
                    "labels": list(entity.labels),
                }
                for entity in self.entity_types
            },
            "relationships": {
                relationship.name: {
                    "source_types": list(relationship.source_types),
                    "target_types": list(relationship.target_types),
                    "symmetric": relationship.symmetric,
                    "labels": list(relationship.labels),
                }
                for relationship in self.relationships
            },
        }

    def compile(self) -> "CompiledDomain":
        topics = {topic.name.casefold(): topic for topic in self.topics}
        label_to_type: dict[str, str] = {}
        entity_to_topic: dict[str, str] = {}
        active_entity_types: list[str] = []
        entity_descriptions: dict[str, str] = {}
        for entity in self.entity_types:
            topic = topics[entity.topic.casefold()]
            entity_to_topic[entity.name.casefold()] = topic.name
            if entity.description:
                entity_descriptions[entity.name] = entity.description
            if not topic.active:
                continue
            active_entity_types.append(entity.name)
            for label in entity.labels:
                label_to_type[label.casefold()] = entity.name
        relationship_map = {
            relationship.name.casefold(): relationship
            for relationship in self.relationships
        }
        return CompiledDomain(
            version=self.version,
            active_topics=tuple(topic.name for topic in self.topics if topic.active),
            active_entity_types=tuple(active_entity_types),
            label_to_entity_type=_frozen_mapping(label_to_type),
            entity_type_to_topic=_frozen_mapping(entity_to_topic),
            relationships=_frozen_mapping(relationship_map),
            relationship_labels=_frozen_mapping(
                {
                    key: relationship
                    for relationship in self.relationships
                    for key in {
                        relationship.name.casefold(),
                        *relationship.labels,
                    }
                }
            ),
            topic_aliases=_frozen_mapping(
                {topic.name.casefold(): topic.name for topic in self.topics}
            ),
            topic_descriptions=_frozen_mapping(
                {
                    topic.name: topic.description
                    for topic in self.topics
                    if topic.description
                }
            ),
            descriptions=_frozen_mapping(entity_descriptions),
        )


@dataclass(frozen=True, slots=True)
class CompiledDomain:
    """Read-only lookup snapshot captured by runtime work."""

    version: int
    active_topics: tuple[str, ...]
    active_entity_types: tuple[str, ...]
    label_to_entity_type: Mapping[str, str]
    entity_type_to_topic: Mapping[str, str]
    relationships: Mapping[str, RelationshipDefinition]
    topic_aliases: Mapping[str, str]
    descriptions: Mapping[str, str]
    relationship_labels: Mapping[str, RelationshipDefinition] = field(
        default_factory=lambda: MappingProxyType({})
    )
    topic_descriptions: Mapping[str, str] = field(
        default_factory=lambda: MappingProxyType({})
    )

    @classmethod
    def empty(cls, version: int = 0) -> "CompiledDomain":
        """Create an explicit no-domain snapshot for transitional runtimes."""

        if not isinstance(version, int) or isinstance(version, bool) or version < 0:
            raise ValueError("CompiledDomain version must be a non-negative integer")
        return cls(
            version=version,
            active_topics=(),
            active_entity_types=(),
            label_to_entity_type=_frozen_mapping({}),
            entity_type_to_topic=_frozen_mapping({}),
            relationships=_frozen_mapping({}),
            relationship_labels=_frozen_mapping({}),
            topic_aliases=_frozen_mapping({}),
            descriptions=_frozen_mapping({}),
            topic_descriptions=_frozen_mapping({}),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible detached representation of the snapshot."""

        return {
            "version": self.version,
            "active_topics": list(self.active_topics),
            "active_entity_types": list(self.active_entity_types),
            "label_to_entity_type": dict(self.label_to_entity_type),
            "entity_type_to_topic": dict(self.entity_type_to_topic),
            "relationships": {
                key: {
                    "name": value.name,
                    "source_types": list(value.source_types),
                    "target_types": list(value.target_types),
                    "symmetric": value.symmetric,
                    "labels": list(value.labels),
                }
                for key, value in self.relationships.items()
            },
            "topic_aliases": dict(self.topic_aliases),
            "descriptions": dict(self.descriptions),
            "topic_descriptions": dict(self.topic_descriptions),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "CompiledDomain":
        """Hydrate a detached compiled snapshot for policy replay."""

        if not isinstance(payload, Mapping):
            raise ValueError("Compiled domain payload must be an object")
        try:
            version = payload["version"]
            active_topics = tuple(payload["active_topics"])
            active_entity_types = tuple(payload["active_entity_types"])
            label_to_entity_type = dict(payload["label_to_entity_type"])
            entity_type_to_topic = dict(payload["entity_type_to_topic"])
            topic_aliases = dict(payload["topic_aliases"])
            descriptions = dict(payload["descriptions"])
            topic_descriptions = dict(payload.get("topic_descriptions", {}))
            relationships = {
                key: RelationshipDefinition(
                    name=value["name"],
                    source_types=tuple(value["source_types"]),
                    target_types=tuple(value["target_types"]),
                    symmetric=value.get("symmetric", False),
                    labels=tuple(value.get("labels", value.get("aliases", []))),
                )
                for key, value in payload["relationships"].items()
            }
        except (KeyError, TypeError, AttributeError, ValueError) as exc:
            raise ValueError("Invalid compiled domain payload") from exc

        if not isinstance(version, int) or isinstance(version, bool) or version < 0:
            raise ValueError("CompiledDomain version must be a non-negative integer")
        return cls(
            version=version,
            active_topics=active_topics,
            active_entity_types=active_entity_types,
            label_to_entity_type=_frozen_mapping(label_to_entity_type),
            entity_type_to_topic=_frozen_mapping(entity_type_to_topic),
            relationships=_frozen_mapping(relationships),
            relationship_labels=_frozen_mapping(
                {
                    key: relationship
                    for relationship in relationships.values()
                    for key in {
                        relationship.name.casefold(),
                        *relationship.labels,
                    }
                }
            ),
            topic_aliases=_frozen_mapping(topic_aliases),
            descriptions=_frozen_mapping(descriptions),
            topic_descriptions=_frozen_mapping(topic_descriptions),
        )

    @property
    def entity_descriptions(self) -> Mapping[str, str]:
        """Explicit alias for callers that distinguish both description maps."""

        return self.descriptions

    def resolve_entity_type(self, label: str) -> str | None:
        if not isinstance(label, str):
            return None
        return self.label_to_entity_type.get(label.strip().casefold())

    def canonical_entity_type(self, entity_type: str) -> str | None:
        """Return the configured spelling for one active entity type."""

        if not isinstance(entity_type, str):
            return None
        key = entity_type.strip().casefold()
        for configured in self.active_entity_types:
            if configured.casefold() == key:
                return configured
        return None

    def topic_for_entity_type(self, entity_type: str) -> str | None:
        if not isinstance(entity_type, str):
            return None
        key = entity_type.strip().casefold()
        if key not in {item.casefold() for item in self.active_entity_types}:
            return None
        return self.entity_type_to_topic.get(key)

    def is_active_entity_type(self, entity_type: str) -> bool:
        if not isinstance(entity_type, str):
            return False
        return entity_type.strip().casefold() in {
            item.casefold() for item in self.active_entity_types
        }

    def normalize_topic(self, topic: str) -> str | None:
        if not isinstance(topic, str):
            return None
        canonical = self.topic_aliases.get(topic.strip().casefold())
        return canonical if canonical in self.active_topics else None

    def relationship(self, name: str) -> RelationshipDefinition | None:
        if not isinstance(name, str):
            return None
        return self.relationship_by_key(name.strip().casefold())

    def relationship_by_key(self, name: str) -> RelationshipDefinition | None:
        """Resolve a configured relationship by canonicalized name or wording."""

        if not isinstance(name, str):
            return None
        key = name.strip().casefold()
        direct = self.relationship_labels.get(key)
        if direct is not None:
            return direct
        normalized = key.replace("-", "_").replace(" ", "_")
        direct = self.relationship_labels.get(normalized)
        if direct is not None:
            return direct
        for relationship in self.relationships.values():
            configured = relationship.name.casefold()
            if configured == normalized or any(
                label.replace("-", "_").replace(" ", "_") == normalized
                for label in relationship.labels
            ):
                return relationship
        return None

    def relationship_allows(
        self,
        name: str,
        source_type: str,
        target_type: str,
    ) -> bool:
        """Check a canonical relationship's endpoint type constraint."""

        definition = self.relationship(name)
        if definition is None:
            return False
        if not isinstance(source_type, str) or not isinstance(target_type, str):
            return False
        source_key = source_type.strip().casefold()
        target_key = target_type.strip().casefold()
        source_types = {item.casefold() for item in definition.source_types}
        target_types = {item.casefold() for item in definition.target_types}
        if source_key in source_types and target_key in target_types:
            return True
        return (
            definition.symmetric
            and target_key in source_types
            and source_key in target_types
        )

    @property
    def label_block(self) -> str:
        lines: list[str] = []
        for entity_type in self.active_entity_types:
            topic = self.topic_for_entity_type(entity_type)
            if topic is None:
                continue
            labels = sorted(
                label
                for label, configured_type in self.label_to_entity_type.items()
                if configured_type == entity_type
            )
            lines.extend(
                (
                    f"Topic: {topic}",
                    f"Entity Type: {entity_type}",
                    f"  Labels: {', '.join(labels)}",
                )
            )
            description = self.entity_descriptions.get(entity_type)
            if description:
                lines.append(f"  Description: {description}")
            lines.append("")
        return "\n".join(lines).rstrip()

    @property
    def relationship_block(self) -> str:
        """Prompt-ready canonical relationship vocabulary and constraints."""

        if not self.relationships:
            return "(none configured; preserve the observed relationship wording)"
        lines: list[str] = []
        for relationship in sorted(
            self.relationships.values(), key=lambda item: item.name.casefold()
        ):
            direction = " <-> " if relationship.symmetric else " -> "
            lines.append(
                f"{relationship.name}: "
                f"{', '.join(relationship.source_types)}{direction}"
                f"{', '.join(relationship.target_types)}"
            )
            if relationship.labels:
                lines.append(f"  Labels: {', '.join(relationship.labels)}")
        return "\n".join(lines)

    @property
    def labels(self) -> tuple[str, ...]:
        """Configured extraction labels in deterministic order."""

        return tuple(sorted(self.label_to_entity_type))
