"""Topic gates and aggregates reject incomplete or conflicting metadata."""

from copy import deepcopy

import pytest

from common.schema.ingestion.contracts import ProjectEntityClassification
from common.schema.jev import JevResult
from core.knowledge.entity.jev_classification import (
    accepted_classification_topic,
    aggregate_topic_proposals,
)
from tests.unit.core.knowledge.test_jev_commit_validation import staged


@pytest.mark.parametrize(
    "mutation,error",
    [
        ("id", "resolved entity ID"),
        ("missing", "first-entry"),
        ("existing", "first-entry"),
        ("type", "conflict on entity type"),
        ("blank_type", "conflict on entity type"),
        ("topic", "valid default"),
        ("baseline", "staged default"),
        ("suggestion", "allowed topic set"),
    ],
)
def test_aggregation_rejects_invalid_proposals(mutation, error):
    current, _ = staged("classification")
    observations = deepcopy(current.trace.classification_decisions)
    classifications = dict(current.entity_result.project_classifications)
    if mutation == "id":
        observations[0]["entity_id"] = True
    elif mutation == "missing":
        classifications.clear()
    elif mutation == "existing":
        classifications[10] = ProjectEntityClassification(
            entity_id=10, entity_type="Company", topic="Work", membership="existing"
        )
    elif mutation == "type":
        observations.append({**observations[0], "entity_type": "Project"})
    elif mutation == "blank_type":
        observations[0]["entity_type"] = ""
    elif mutation == "topic":
        classifications[10] = ProjectEntityClassification(
            entity_id=10, entity_type="Company", topic="Other", membership="missing"
        )
    elif mutation == "baseline":
        observations[0]["baseline_topic"] = "Finance"
    else:
        observations[0]["suggested_topic"] = "Other"
    with pytest.raises(ValueError, match=error):
        aggregate_topic_proposals(observations, classifications, current.policy.domain)


@pytest.mark.parametrize(
    "mutation",
    [
        "no_topic",
        "wrong_answer",
        "missing_probability",
        "weak_confidence",
        "weak_probability",
        "weak_margin",
        "weak_evidence",
    ],
)
def test_active_gate_rejects_incomplete_or_weak_topic_response(mutation):
    current, _ = staged("classification")
    observation = current.trace.classification_decisions[0]
    payload = observation["record"]["result"]
    answers = payload["response"]["answers"]
    choice = answers["topic_choice"]
    if mutation == "no_topic":
        choice["choice"] = "insufficient_evidence"
    elif mutation == "wrong_answer":
        answers["topic_evidence"] = dict(choice)
    elif mutation == "missing_probability":
        del choice["probabilities"]["topic_2"]
    elif mutation == "weak_confidence":
        choice["confidence"] = 0.1
    elif mutation == "weak_probability":
        choice["probabilities"]["topic_2"] = 0.4
    elif mutation == "weak_margin":
        choice["probabilities"]["topic_1"] = 0.6
    else:
        answers["topic_evidence"]["noul"] = 0.1
    policy = current.policy.jev.model_copy(
        update={"classification_acceptance_policy_version": "override-positive-v1"}
    )
    assert (
        accepted_classification_topic(
            JevResult.model_validate(payload),
            observation["record"]["option_mapping"],
            "Work",
            policy,
            option_set_truncated=False,
        )
        is None
    )
