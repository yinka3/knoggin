"""Immutable configuration captured when an ingestion batch begins."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from common.conf.domain_config import CompiledDomain
from common.schema.settings import (
    EntityResolutionSettings,
    TextProcessorSettings,
)


@dataclass(frozen=True, slots=True)
class IngestionPolicy:
    """Runtime rules that remain stable for one in-memory ingestion batch."""

    gliner_threshold: float
    llm_ner_mode: str
    candidate_fuzzy_threshold: int
    resolution_threshold: float
    resolution_margin: float
    common_word_frequency_threshold: float
    sparse_context_verbs: tuple[str, ...]
    domain: CompiledDomain

    @classmethod
    def capture(
        cls,
        *,
        text_processor: TextProcessorSettings,
        entity_resolution: EntityResolutionSettings,
        compiled_domain: CompiledDomain,
    ) -> "IngestionPolicy":
        if not isinstance(compiled_domain, CompiledDomain):
            raise TypeError("IngestionPolicy requires an active CompiledDomain")
        return cls(
            gliner_threshold=text_processor.gliner_threshold,
            llm_ner_mode=text_processor.llm_ner_mode,
            candidate_fuzzy_threshold=entity_resolution.candidate_fuzzy_threshold,
            resolution_threshold=entity_resolution.resolution_threshold,
            resolution_margin=entity_resolution.resolution_margin,
            common_word_frequency_threshold=(
                entity_resolution.common_word_frequency_threshold
            ),
            sparse_context_verbs=tuple(
                verb.strip().casefold()
                for verb in entity_resolution.sparse_context_verbs
                if verb and verb.strip()
            ),
            domain=compiled_domain,
        )

    def semantic_window_snapshot(self) -> dict[str, object]:
        """Serialize every Context-entity decision input for durable replay."""

        return {
            "gliner_threshold": self.gliner_threshold,
            "llm_ner_mode": self.llm_ner_mode,
            "candidate_fuzzy_threshold": self.candidate_fuzzy_threshold,
            "resolution_threshold": self.resolution_threshold,
            "resolution_margin": self.resolution_margin,
            "common_word_frequency_threshold": self.common_word_frequency_threshold,
            "sparse_context_verbs": list(self.sparse_context_verbs),
            "compiled_domain": self.domain.to_dict(),
        }

    @classmethod
    def from_semantic_window_snapshot(cls, payload: object) -> "IngestionPolicy":
        """Hydrate the exact Context-entity policy captured at admission."""

        if not isinstance(payload, Mapping):
            raise ValueError("Ingestion policy snapshot must be an object")
        expected = {
            "gliner_threshold",
            "llm_ner_mode",
            "candidate_fuzzy_threshold",
            "resolution_threshold",
            "resolution_margin",
            "common_word_frequency_threshold",
            "sparse_context_verbs",
            "compiled_domain",
        }
        if set(payload) != expected:
            raise ValueError("Invalid ingestion policy snapshot shape")
        try:
            gliner_threshold = payload["gliner_threshold"]
            llm_ner_mode = payload["llm_ner_mode"]
            candidate_fuzzy_threshold = payload["candidate_fuzzy_threshold"]
            resolution_threshold = payload["resolution_threshold"]
            resolution_margin = payload["resolution_margin"]
            common_word_frequency_threshold = payload[
                "common_word_frequency_threshold"
            ]
            sparse_context_verbs = tuple(payload["sparse_context_verbs"])
            domain = CompiledDomain.from_dict(payload["compiled_domain"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("Invalid ingestion policy snapshot") from exc
        if (
            not isinstance(gliner_threshold, (int, float))
            or isinstance(gliner_threshold, bool)
            or llm_ner_mode not in {"disabled", "fallback"}
            or not isinstance(candidate_fuzzy_threshold, int)
            or isinstance(candidate_fuzzy_threshold, bool)
            or not isinstance(resolution_threshold, (int, float))
            or isinstance(resolution_threshold, bool)
            or not isinstance(resolution_margin, (int, float))
            or isinstance(resolution_margin, bool)
            or not isinstance(common_word_frequency_threshold, (int, float))
            or isinstance(common_word_frequency_threshold, bool)
            or any(not isinstance(verb, str) for verb in sparse_context_verbs)
        ):
            raise ValueError("Invalid ingestion policy snapshot values")
        return cls(
            gliner_threshold=float(gliner_threshold),
            llm_ner_mode=str(llm_ner_mode),
            candidate_fuzzy_threshold=candidate_fuzzy_threshold,
            resolution_threshold=float(resolution_threshold),
            resolution_margin=float(resolution_margin),
            common_word_frequency_threshold=float(common_word_frequency_threshold),
            sparse_context_verbs=sparse_context_verbs,
            domain=domain,
        )
