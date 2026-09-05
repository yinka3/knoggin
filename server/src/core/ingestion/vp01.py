"""Local GLiNER2.5 adapter for Context-first VP-01 entity extraction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol

from common.conf.domain_config import CompiledDomain

GLINER25_ENGLISH_MODEL = "fastino/gliner2.5-base-v1"
GLINER25_MULTILINGUAL_MODEL = "fastino/gliner2.5-multi-v1"


@dataclass(frozen=True, slots=True)
class VP01EntitySpan:
    """One GLiNER2.5 entity with offsets into the exact supplied text."""

    text: str
    label: str
    start: int
    end: int

    def __post_init__(self) -> None:
        if not isinstance(self.text, str) or not self.text.strip():
            raise ValueError("VP-01 spans require non-blank text")
        if not isinstance(self.label, str) or not self.label.strip():
            raise ValueError("VP-01 spans require a non-blank label")
        if (
            not isinstance(self.start, int)
            or not isinstance(self.end, int)
            or self.start < 0
            or self.end <= self.start
        ):
            raise ValueError("VP-01 span offsets must form a non-empty range")


class VP01EntityExtractor(Protocol):
    """The small synchronous model boundary scheduled by ``ModelWorkCoordinator``."""

    def extract_entities(
        self,
        text: str,
        domain: CompiledDomain,
        *,
        threshold: float,
    ) -> list[VP01EntitySpan]: ...


def model_id_for_language(language: str) -> str:
    """Choose the one approved VP-01 checkpoint for a frozen domain language."""

    if language == "en":
        return GLINER25_ENGLISH_MODEL
    if language == "multilingual":
        return GLINER25_MULTILINGUAL_MODEL
    raise ValueError("VP-01 language must be 'en' or 'multilingual'")


def entity_schema(domain: CompiledDomain) -> dict[str, str]:
    """Build GLiNER's typed entity schema from the frozen compiled domain.

    Labels are the configured model vocabulary.  Their descriptions are the
    model-facing instruction channel; arbitrary project or Context prose never
    becomes model instructions.
    """

    if not isinstance(domain, CompiledDomain):
        raise TypeError("VP-01 requires a CompiledDomain")
    schema: dict[str, str] = {}
    for label in domain.labels:
        entity_type = domain.resolve_entity_type(label)
        if entity_type is None:
            continue
        schema[label] = domain.entity_descriptions.get(entity_type, "")
    return schema


class GLiNER25VP01Adapter:
    """Normalize ``gliner2.AutoExtractor`` output for Knoggin's VP-01 contract."""

    def __init__(self, model: Any, *, model_id: str) -> None:
        if not isinstance(model_id, str) or not model_id.strip():
            raise ValueError("GLiNER2.5 model_id must be non-blank")
        self._model = model
        self.model_id = model_id

    @classmethod
    def load(cls, *, language: str, device: str | None = None) -> "GLiNER25VP01Adapter":
        """Load through AutoExtractor so boundary and span checkpoints stay safe."""

        from gliner2 import AutoExtractor

        model_id = model_id_for_language(language)
        kwargs: dict[str, object] = {}
        if device:
            kwargs["map_location"] = device
        model = AutoExtractor.from_pretrained(model_id, **kwargs)
        return cls(model, model_id=model_id)

    def extract_entities(
        self,
        text: str,
        domain: CompiledDomain,
        *,
        threshold: float,
    ) -> list[VP01EntitySpan]:
        if not isinstance(text, str):
            raise TypeError("VP-01 text must be a string")
        if not text.strip() or not (schema := entity_schema(domain)):
            return []
        if not isinstance(threshold, (int, float)) or isinstance(threshold, bool):
            raise TypeError("VP-01 threshold must be numeric")

        result = self._model.extract_entities(
            text,
            schema,
            threshold=float(threshold),
            include_spans=True,
            include_confidence=True,
        )
        return self._normalize_result(result, text=text)

    @staticmethod
    def _normalize_result(result: object, *, text: str) -> list[VP01EntitySpan]:
        if not isinstance(result, Mapping):
            raise TypeError("GLiNER2.5 returned an invalid entity result")
        entities = result.get("entities", {})
        if not isinstance(entities, Mapping):
            raise TypeError("GLiNER2.5 entity result must contain an entity mapping")

        spans: list[VP01EntitySpan] = []
        for raw_label, raw_values in entities.items():
            if not isinstance(raw_label, str) or not isinstance(raw_values, list):
                continue
            for raw_value in raw_values:
                if not isinstance(raw_value, Mapping):
                    # Offsets are mandatory for Context-block provenance.  A
                    # text-only response cannot safely be assigned to a block.
                    continue
                raw_text = raw_value.get("text")
                start = raw_value.get("start")
                end = raw_value.get("end")
                if (
                    not isinstance(raw_text, str)
                    or not isinstance(start, int)
                    or not isinstance(end, int)
                    or start < 0
                    or end > len(text)
                    or end <= start
                    or text[start:end] != raw_text
                ):
                    continue
                spans.append(
                    VP01EntitySpan(
                        text=raw_text,
                        label=raw_label,
                        start=start,
                        end=end,
                    )
                )
        return spans
