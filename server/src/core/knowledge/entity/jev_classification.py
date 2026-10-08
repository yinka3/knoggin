"""Bounded JEV topic questions for first-entry project classification."""

from __future__ import annotations

from common.conf.domain_config import CompiledDomain
from common.schema.ingestion.contracts import (
    ContextBlockMention,
    ProjectEntityClassification,
)
from common.schema.jev import (
    ChoiceAnswer,
    ChoiceQuestion,
    JevPolicy,
    JevResult,
    NoulAnswer,
    NoulQuestion,
)


def prepare_topic_request(
    mention: ContextBlockMention,
    support_text: str,
    domain: CompiledDomain,
    policy: JevPolicy,
) -> tuple[dict, dict, dict[str, str], bool] | None:
    """Build an observe-only topic request from the frozen domain snapshot."""

    allowed_topics = domain.allowed_topics_for_entity_type(mention.entity_type)
    if len(allowed_topics) <= 1:
        return None
    count = min(len(allowed_topics), policy.max_options_per_choice - 1)
    if count < 1:
        return None
    option_set_truncated = count < len(allowed_topics)
    mapping: dict[str, str] = {}
    criteria: dict[str, str | None] = {}
    for index, topic in enumerate(allowed_topics[:count], start=1):
        handle = f"topic_{index}"
        mapping[handle] = topic
        criteria[handle] = (
            domain.topic_descriptions.get(topic)
            or f"The entity belongs to the configured {topic} topic."
        )
    criteria["insufficient_evidence"] = (
        "Choose this unless the supplied context directly connects this entity "
        "to exactly one allowed topic. A name-only mention, attendance, generic "
        "update, future possibility, or relationship without topic-specific "
        "activity is insufficient."
    )
    state = {
        "mention": {
            "name": mention.name[:200],
            "entity_type": mention.entity_type[:100],
        },
        "support_text": support_text[:6000],
        "support_truncated": len(support_text) > 6000,
        "allowed_topics": [
            {
                "handle": handle,
                "name": topic,
                "description": domain.topic_descriptions.get(topic),
            }
            for handle, topic in mapping.items()
        ],
    }
    choice_instructions = (
        "Select an allowed topic only when the supplied context directly connects "
        "the named entity to topic-specific activity. Do not infer a topic from the "
        "entity type, a general association, or the entity's presence. When the "
        "evidence is generic, indirect, future, or fits multiple topics, choose "
        "insufficient_evidence."
    )
    evidence_instructions = (
        "The supplied context directly connects this entity to one specific allowed "
        "topic through topic-specific activity, rather than merely mentioning the "
        "entity or describing a general association."
    )
    # Admitted windows retain their original template on restart.
    if policy.classification_question_version == "classification-v1":
        state["mention"]["default_topic"] = domain.topic_for_entity_type(
            mention.entity_type
        )
        criteria["insufficient_evidence"] = (
            "The supplied context does not support one allowed topic."
        )
        choice_instructions = (
            "Which allowed project topic best classifies this entity in the "
            "supplied context? Use insufficient_evidence when one topic is not "
            "supported."
        )
        evidence_instructions = (
            "The supplied evidence supports assigning one specific allowed "
            "topic to this entity in the current project."
        )
    questions = {
        "topic_choice": ChoiceQuestion(
            instructions=choice_instructions,
            criteria=criteria,
        ),
        "topic_evidence": NoulQuestion(instructions=evidence_instructions),
    }
    return state, questions, mapping, option_set_truncated


def proposed_topic(
    result: JevResult,
    mapping: dict[str, str],
    *,
    baseline_topic: str,
    option_set_truncated: bool,
    question_version: str = "classification-v2",
) -> str | None:
    """Return an observable override suggestion; it never changes classification."""

    if option_set_truncated or result.outcome != "available" or result.response is None:
        return None
    answer = result.response.answers.get("topic_choice")
    if not isinstance(answer, ChoiceAnswer) or answer.choice not in mapping:
        return None
    topic = mapping[answer.choice]
    if topic == baseline_topic and question_version != "classification-v1":
        return None
    return topic


def accepted_classification_topic(
    result: JevResult,
    mapping: dict[str, str],
    baseline_topic: str,
    policy: JevPolicy,
    *,
    option_set_truncated: bool,
) -> str | None:
    """Return one strongly supported non-default override under the active gate."""

    if (
        policy.classification_acceptance_policy_version != "override-positive-v1"
        or policy.classification_question_version != "classification-v2"
    ):
        return None
    topic = proposed_topic(
        result,
        mapping,
        baseline_topic=baseline_topic,
        option_set_truncated=option_set_truncated,
    )
    if topic is None or result.response is None:
        return None
    choice = result.response.answers.get("topic_choice")
    evidence = result.response.answers.get("topic_evidence")
    if not isinstance(choice, ChoiceAnswer) or not isinstance(evidence, NoulAnswer):
        return None
    selected = choice.probabilities.get(choice.choice)
    if selected is None:
        return None
    runner_up = max(
        (value for handle, value in choice.probabilities.items() if handle != choice.choice),
        default=0.0,
    )
    if (
        choice.confidence < policy.classification_min_choice_confidence
        or selected < policy.classification_min_choice_probability
        or selected - runner_up < policy.classification_min_probability_margin
        or evidence.noul < policy.classification_min_evidence_noul
    ):
        return None
    return topic


def aggregate_topic_proposals(
    observations: list[dict],
    classifications: dict[int, ProjectEntityClassification],
    domain: CompiledDomain,
) -> tuple[dict, ...]:
    """Group occurrence proposals by resolved entity without changing storage."""

    grouped: dict[int, list[dict]] = {}
    for observation in observations:
        entity_id = observation.get("entity_id")
        if not isinstance(entity_id, int) or isinstance(entity_id, bool):
            raise ValueError("JEV topic proposal requires a resolved entity ID")
        grouped.setdefault(entity_id, []).append(observation)

    aggregates = []
    for entity_id in sorted(grouped):
        items = grouped[entity_id]
        classification = classifications.get(entity_id)
        if classification is None or classification.membership != "missing":
            raise ValueError("JEV topic proposals require a first-entry classification")
        entity_types = {item.get("entity_type") for item in items}
        if len(entity_types) != 1 or not all(
            isinstance(entity_type, str) and entity_type.strip()
            for entity_type in entity_types
        ):
            raise ValueError("JEV topic proposals conflict on entity type")
        entity_type = next(iter(entity_types))
        allowed_topics = domain.allowed_topics_for_entity_type(entity_type)
        if classification.topic not in allowed_topics:
            raise ValueError("First-entry classification has no valid default topic")
        if any(item.get("baseline_topic") != classification.topic for item in items):
            raise ValueError("JEV topic proposals disagree with the staged default")
        proposed = {
            item["suggested_topic"]
            for item in items
            if item.get("suggested_topic") is not None
        }
        if not proposed.issubset(set(allowed_topics)):
            raise ValueError("JEV topic proposal is outside the allowed topic set")
        ordered_proposals = tuple(
            topic for topic in allowed_topics if topic in proposed
        )
        status = (
            "conflicting_proposals"
            if len(ordered_proposals) > 1
            else "consistent_proposal"
            if len(ordered_proposals) == 1
            else "no_proposal"
        )
        aggregate = {
            "entity_id": entity_id,
            "entity_type": entity_type,
            "membership": "missing",
            "status": status,
            "proposed_topics": list(ordered_proposals),
            "operational_topic": classification.topic,
            "observation_count": len(items),
        }
        aggregates.append(aggregate)
        for item in items:
            item["aggregation"] = dict(aggregate)
            if status == "conflicting_proposals" and item.get(
                "suggested_topic"
            ) is not None:
                item["record"]["acceptance_status"] = "conflicting"
    return tuple(aggregates)
