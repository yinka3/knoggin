"""Immutable configuration captured at ingestion-batch creation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, is_dataclass
from typing import Any, Mapping

from common.conf.domain_config import CompiledDomain
from common.schema.settings import (
    EntityResolutionSettings,
    IngestionSettings,
    TextProcessorSettings,
)


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
    domain: CompiledDomain

    @classmethod
    def capture(
        cls,
        *,
        ingestion: IngestionSettings,
        text_processor: TextProcessorSettings,
        entity_resolution: EntityResolutionSettings,
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
            "domain": compiled_domain,
        }
        version = cls._version_for(payload)
        return cls(version=version, **payload)

    def to_dict(self) -> dict[str, Any]:
        return _serialize(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "IngestionPolicy":
        try:
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
                "domain": domain,
            }
            version = payload["version"]
        except (KeyError, TypeError) as exc:
            raise ValueError("Invalid ingestion policy payload") from exc

        expected_version = cls._version_for(values)
        legacy_topics = payload.get("topics")
        if legacy_topics is not None:
            # Jobs admitted before DomainConfig became the single source of
            # topic rules include a redundant, detached topic snapshot. Its
            # hash remains part of their durable policy contract, so accept
            # it only when the original payload remains intact. Runtime work
            # uses the compiled domain exclusively.
            expected_version = cls._version_for(
                {**values, "topics": legacy_topics}
            )
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
