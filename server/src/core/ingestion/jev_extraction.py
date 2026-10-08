"""Bounded, literal JEV candidate typing for Context extraction."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from common.schema.jev import (
    ChoiceAnswer,
    ChoiceQuestion,
    JevPolicy,
    JevResult,
    NoulAnswer,
    NoulQuestion,
)
from core.ingestion.policy import IngestionPolicy


@dataclass(frozen=True, slots=True)
class ExtractionCandidate:
    block_id: UUID
    name: str
    evidence_origin: str
    support_text: str
    proposed_type: str | None
    source_start: int | None
    source_end: int | None


def prepare_extraction_request(
    candidate: ExtractionCandidate,
    policy: IngestionPolicy,
) -> tuple[dict, dict, dict[str, str], bool]:
    """Build independent entity-evidence and optional type questions."""

    mapping: dict[str, str] = {}
    questions: dict[str, ChoiceQuestion | NoulQuestion] = {}
    candidate_set_truncated = False
    if candidate.proposed_type is None:
        limit = min(
            len(policy.domain.active_entity_types),
            policy.jev.max_options_per_choice - 2,
        )
        candidate_set_truncated = limit < len(policy.domain.active_entity_types)
        criteria: dict[str, str | None] = {}
        for index, entity_type in enumerate(
            policy.domain.active_entity_types[:limit], start=1
        ):
            handle = f"type_{index}"
            mapping[handle] = entity_type
            description = policy.domain.descriptions.get(entity_type)
            criteria[handle] = description or f"The mention is a {entity_type}."
        criteria["not_an_entity"] = "The text is not naming an entity in context."
        criteria["insufficient_evidence"] = (
            "The supplied context cannot support one configured entity type."
        )
        questions["type_choice"] = ChoiceQuestion(
            instructions=(
                "Which configured entity type best describes this literal mention? "
                "Use not_an_entity for ordinary text and insufficient_evidence when "
                "one type cannot be supported."
            ),
            criteria=criteria,
        )
    questions["entity_evidence"] = NoulQuestion(
        instructions=(
            "The supplied context supports treating the literal mention as an entity "
            "rather than ordinary text or an unsupported guess."
        )
    )
    state = {
        "candidate": {
            "name": candidate.name[:200],
            "proposed_type": candidate.proposed_type,
            "evidence_origin": candidate.evidence_origin,
        },
        "support_text": candidate.support_text[:6000],
        "support_truncated": len(candidate.support_text) > 6000,
        "active_entity_types": [
            {
                "name": entity_type,
                "description": policy.domain.descriptions.get(entity_type),
            }
            for entity_type in policy.domain.active_entity_types
        ],
    }
    return state, questions, mapping, candidate_set_truncated


def accepted_extraction_type(
    result: JevResult,
    candidate: ExtractionCandidate,
    mapping: dict[str, str],
    policy: JevPolicy,
    *,
    candidate_set_truncated: bool = False,
) -> str | None:
    """Return one positively supported type under the versioned active gate."""

    if result.outcome != "available" or result.response is None:
        return None
    evidence = result.response.answers.get("entity_evidence")
    if (
        not isinstance(evidence, NoulAnswer)
        or evidence.noul < policy.extraction_min_entity_noul
    ):
        return None
    if candidate.proposed_type is not None:
        return candidate.proposed_type
    if candidate_set_truncated:
        return None
    choice = result.response.answers.get("type_choice")
    if (
        not isinstance(choice, ChoiceAnswer)
        or choice.confidence < policy.extraction_min_choice_confidence
        or choice.choice not in mapping
    ):
        return None
    return mapping[choice.choice]
