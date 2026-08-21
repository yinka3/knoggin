"""Immutable configuration captured when an ingestion batch begins."""

from __future__ import annotations

from dataclasses import dataclass

from common.conf.domain_config import CompiledDomain
from common.schema.settings import (
    EntityResolutionSettings,
    TextProcessorSettings,
)


@dataclass(frozen=True, slots=True)
class IngestionPolicy:
    """Runtime rules that remain stable for one in-memory ingestion batch."""

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
        text_processor: TextProcessorSettings,
        entity_resolution: EntityResolutionSettings,
        compiled_domain: CompiledDomain,
    ) -> "IngestionPolicy":
        if not isinstance(compiled_domain, CompiledDomain):
            raise TypeError("IngestionPolicy requires an active CompiledDomain")
        return cls(
            gliner_threshold=text_processor.gliner_threshold,
            vp01_min_confidence=text_processor.vp01_min_confidence,
            llm_ner=text_processor.llm_ner,
            candidate_fuzzy_threshold=entity_resolution.candidate_fuzzy_threshold,
            candidate_vector_threshold=entity_resolution.candidate_vector_threshold,
            resolution_threshold=entity_resolution.resolution_threshold,
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
