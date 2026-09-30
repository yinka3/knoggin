"""Bounded, occurrence-specific JEV questions for identity observation."""

from __future__ import annotations

from dataclasses import dataclass

from common.schema.ingestion.contracts import ContextBlockMention
from common.schema.jev import (
    ChoiceAnswer,
    ChoiceQuestion,
    JevPolicy,
    JevResult,
    NoulQuestion,
)
from core.knowledge.entity.candidates import EntityCandidateSnapshot
from core.knowledge.entity.profile import EntityProfile


@dataclass(frozen=True, slots=True)
class IdentityOption:
    entity_id: int
    profile: EntityProfile


def prepare_identity_request(
    mention: ContextBlockMention,
    support_text: str,
    options: list[IdentityOption],
    snapshot: EntityCandidateSnapshot,
    policy: JevPolicy,
    *,
    project_id: str,
) -> tuple[dict, dict, dict[str, str]] | None:
    """Expose only local handles and bounded evidence from the visible snapshot."""

    count = min(policy.max_candidates, policy.max_options_per_choice - 2)
    if count < 1 or not options:
        return None
    mapping: dict[str, str] = {}
    candidates = []
    criteria = {}
    for index, option in enumerate(options[:count], start=1):
        handle = f"candidate_{index}"
        mapping[handle] = str(option.entity_id)
        candidates.append(
            {
                "handle": handle,
                "canonical_name": option.profile.canonical_name[:200],
                "aliases": [
                    alias[:200]
                    for alias in snapshot.get_mentions(option.entity_id)[:8]
                    if alias != option.profile.canonical_name.casefold()
                ],
                "classification": (
                    {
                        "entity_type": option.profile.entity_type,
                        "topic": option.profile.topic,
                    }
                    if option.profile.is_classified_in(project_id)
                    else "foreign_project_classification_not_applicable"
                ),
            }
        )
        criteria[handle] = (
            f"The mention names {handle}, the same underlying identity, "
            "rather than merely a related entity."
        )
    criteria["none_of_these"] = "The mention names a different identity."
    criteria["insufficient_evidence"] = (
        "The supplied context cannot distinguish one identity from the alternatives."
    )
    state = {
        "mention": {
            "name": mention.name[:200],
            "entity_type": mention.entity_type[:100],
            "topic": mention.topic[:100],
        },
        "support_text": support_text[:6000],
        "support_truncated": len(support_text) > 6000,
        "candidates": candidates,
    }
    questions = {
        "identity_choice": ChoiceQuestion(
            instructions=(
                "Which supplied candidate is the same identity as the mention? "
                "Use none_of_these for a different identity and insufficient_evidence "
                "when the evidence does not distinguish one."
            ),
            criteria=criteria,
        ),
        "identity_evidence": NoulQuestion(
            instructions=(
                "The context provides enough evidence to distinguish exactly one "
                "supplied candidate as the same identity, rather than merely a "
                "similar name or related entity."
            )
        ),
    }
    return state, questions, mapping


def proposed_identity(result: JevResult, mapping: dict[str, str]) -> int | None:
    """A suggestion for diagnostics only; no acceptance threshold is implied."""

    if result.outcome != "available" or result.response is None:
        return None
    answer = result.response.answers.get("identity_choice")
    if not isinstance(answer, ChoiceAnswer) or answer.choice not in mapping:
        return None
    return int(mapping[answer.choice])
