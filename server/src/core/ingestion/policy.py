"""Immutable configuration captured at ingestion-batch creation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, is_dataclass
from typing import Any, Mapping

from common.conf.domain_config import CompiledDomain
from common.conf.topics_config import TopicConfig
from common.schema.settings import (
    EntityResolutionSettings,
    IngestionSettings,
    TextProcessorSettings,
)


@dataclass(frozen=True, slots=True)
class TopicRule:
    """One immutable topic definition used by a single ingestion operation."""

    name: str
    active: bool
    labels: tuple[str, ...]
    aliases: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TopicPolicy:
    """Detached, read-only topic schema for one ingestion operation."""

    rules: tuple[TopicRule, ...]

    @classmethod
    def capture(cls, topic_config: TopicConfig) -> "TopicPolicy":
        return cls(
            rules=tuple(
                TopicRule(
                    name=name,
                    active=schema.active,
                    labels=tuple(schema.labels),
                    aliases=tuple(schema.aliases),
                )
                for name, schema in topic_config.snapshot().items()
            )
        )

    def normalize_topic(self, topic: str) -> str | None:
        normalized = (topic or "").strip().casefold()
        if not normalized:
            return None
        for rule in self.rules:
            if normalized == rule.name.casefold() or normalized in {
                alias.casefold() for alias in rule.aliases
            }:
                return rule.name
        return None

    def is_active(self, topic: str) -> bool:
        return any(rule.name == topic and rule.active for rule in self.rules)

    @property
    def active_topics(self) -> tuple[str, ...]:
        return tuple(rule.name for rule in self.rules if rule.active)

    @property
    def labels(self) -> tuple[str, ...]:
        labels: list[str] = []
        seen: set[str] = set()
        for rule in self.rules:
            if not rule.active:
                continue
            for label in rule.labels:
                normalized = label.casefold()
                if normalized in seen:
                    continue
                seen.add(normalized)
                labels.append(label)
        return tuple(labels)

    def label_topics(self, label: str) -> tuple[str, ...]:
        normalized = (label or "").strip().casefold()
        if not normalized:
            return ()
        return tuple(
            rule.name
            for rule in self.rules
            if rule.active
            and normalized in {configured.casefold() for configured in rule.labels}
        )

    @property
    def label_block(self) -> str:
        lines = []
        for rule in self.rules:
            if rule.name == "Identity" or not rule.active or not rule.labels:
                continue
            lines.extend(
                (
                    f"Topic: {rule.name}",
                    f"  Labels: {', '.join(rule.labels)}",
                    "",
                )
            )
        return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class IngestionPolicy:
    """All rules that must remain stable for one ingestion batch and replay."""

    version: str
    checkpoint_interval: int
    graph_write_timeout_seconds: float
    gliner_threshold: float
    vp01_min_confidence: float
    llm_ner: bool
    candidate_fuzzy_threshold: int
    candidate_vector_threshold: float
    resolution_threshold: float
    common_word_frequency_threshold: float
    sparse_context_verbs: tuple[str, ...]
    topics: TopicPolicy
    domain: CompiledDomain

    @classmethod
    def capture(
        cls,
        *,
        ingestion: IngestionSettings,
        text_processor: TextProcessorSettings,
        entity_resolution: EntityResolutionSettings,
        topic_config: TopicConfig,
        compiled_domain: CompiledDomain,
    ) -> "IngestionPolicy":
        if not isinstance(compiled_domain, CompiledDomain):
            raise TypeError("IngestionPolicy requires an active CompiledDomain")
        payload = {
            "checkpoint_interval": ingestion.checkpoint_interval,
            "graph_write_timeout_seconds": ingestion.batch_timeout,
            "gliner_threshold": text_processor.gliner_threshold,
            "vp01_min_confidence": text_processor.vp01_min_confidence,
            "llm_ner": text_processor.llm_ner,
            "candidate_fuzzy_threshold": entity_resolution.candidate_fuzzy_threshold,
            "candidate_vector_threshold": entity_resolution.candidate_vector_threshold,
            "resolution_threshold": entity_resolution.resolution_threshold,
            "common_word_frequency_threshold": (
                entity_resolution.common_word_frequency_threshold
            ),
            "sparse_context_verbs": tuple(
                verb.strip().casefold()
                for verb in entity_resolution.sparse_context_verbs
                if verb and verb.strip()
            ),
            "topics": TopicPolicy.capture(topic_config),
            "domain": compiled_domain,
        }
        version = cls._version_for(payload)
        return cls(version=version, **payload)

    def to_dict(self) -> dict[str, Any]:
        return _serialize(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "IngestionPolicy":
        try:
            topics = TopicPolicy(
                rules=tuple(
                    TopicRule(
                        name=rule["name"],
                        active=rule["active"],
                        labels=tuple(rule["labels"]),
                        aliases=tuple(rule["aliases"]),
                    )
                    for rule in payload["topics"]["rules"]
                )
            )
            domain = CompiledDomain.from_dict(payload["domain"])
            values = {
                "checkpoint_interval": payload["checkpoint_interval"],
                "graph_write_timeout_seconds": payload["graph_write_timeout_seconds"],
                "gliner_threshold": payload["gliner_threshold"],
                "vp01_min_confidence": payload["vp01_min_confidence"],
                "llm_ner": payload["llm_ner"],
                "candidate_fuzzy_threshold": payload["candidate_fuzzy_threshold"],
                "candidate_vector_threshold": payload["candidate_vector_threshold"],
                "resolution_threshold": payload["resolution_threshold"],
                "common_word_frequency_threshold": payload[
                    "common_word_frequency_threshold"
                ],
                "sparse_context_verbs": tuple(payload["sparse_context_verbs"]),
                "topics": topics,
                "domain": domain,
            }
            version = payload["version"]
        except (KeyError, TypeError) as exc:
            raise ValueError("Invalid ingestion policy payload") from exc

        expected_version = cls._version_for(values)
        if version != expected_version:
            raise ValueError("Ingestion policy version does not match its contents")
        return cls(version=version, **values)

    @staticmethod
    def _version_for(values: Mapping[str, Any]) -> str:
        serializable = {key: _serialize(value) for key, value in values.items()}
        serialized = json.dumps(
            serializable,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]


def _serialize(value: Any) -> Any:
    """Convert frozen policy/domain values to deterministic JSON data."""

    if is_dataclass(value):
        return {
            field_name: _serialize(getattr(value, field_name))
            for field_name in value.__dataclass_fields__
        }
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, Mapping):
        return {str(key): _serialize(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_serialize(item) for item in value]
    return value
