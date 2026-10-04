"""Offline quality-gate tests for topic classification."""

import pytest

from tests.fixtures.evaluate_jev_classification import (
    evaluate_classification_observations,
)


def _pair(number, expected, actual, *, aggregate_status=None):
    occurrence_key = f"{number}:block:topic"
    label = {
        "window_id": f"window-{number}",
        "pass_number": 1,
        "occurrence_key": occurrence_key,
        "review_status": "reviewed",
        "default_topic": "Work",
        "expected_topic": expected,
        "expected_choice": "topic" if expected else "insufficient_evidence",
    }
    mapping = {"topic_1": "Work", "topic_2": "Finance"}
    choice = (
        next(key for key, value in mapping.items() if value == actual)
        if actual is not None
        else "insufficient_evidence"
    )
    observation = {
        "entity_id": number,
        "option_set_truncated": False,
        "record": {
            "window_id": f"window-{number}",
            "pass_number": 1,
            "occurrence_key": occurrence_key,
            "option_mapping": mapping,
            "result": {
                "outcome": "available",
                "response": {
                    "answers": {"topic_choice": {"choice": choice}},
                },
            },
        },
    }
    expected_topics = [] if expected is None else [expected]
    aggregate_label = {
        "window_id": f"window-{number}",
        "pass_number": 1,
        "anchor_occurrence_key": occurrence_key,
        "review_status": "reviewed",
        "expected_status": (
            "no_proposal" if expected is None else "consistent_proposal"
        ),
        "expected_proposed_topics": expected_topics,
        "observation_count": 1,
    }
    actual_aggregate_status = aggregate_status or (
        "no_proposal" if actual is None else "consistent_proposal"
    )
    aggregate = {
        "window_id": f"window-{number}",
        "entity_id": number,
        "status": actual_aggregate_status,
        "proposed_topics": (
            [actual]
            if actual_aggregate_status == "consistent_proposal" and actual is not None
            else ["Work", "Finance"]
            if actual_aggregate_status == "conflicting_proposals"
            else []
        ),
        "observation_count": 1,
    }
    return label, aggregate_label, observation, aggregate


def test_scores_topic_accuracy_and_false_conflicts_separately():
    pairs = [
        _pair(1, "Finance", "Finance"),
        _pair(2, "Work", "Finance"),
        _pair(3, None, None),
        _pair(4, "Finance", "Finance", aggregate_status="conflicting_proposals"),
    ]
    report = evaluate_classification_observations(
        {
            "occurrences": [item[0] for item in pairs],
            "aggregates": [item[1] for item in pairs],
        },
        [item[2] for item in pairs],
        [item[3] for item in pairs],
    )

    assert report["judgments_scored"] == 4
    assert report["judgments_correct"] == 3
    assert report["judgment_accuracy"] == 0.75
    assert report["non_default_accuracy"] == 1.0
    assert report["aggregates_correct"] == 2
    assert report["conflicts_observed"] == 1
    assert report["false_conflicts"] == 1
    assert report["false_conflict_rate"] == 0.25
    assert report["active_ready"] is False


def test_unreviewed_labels_cannot_produce_quality_claims():
    label, aggregate_label, observation, aggregate = _pair(1, "Finance", "Finance")
    label["review_status"] = "proposed"

    with pytest.raises(ValueError, match="human reviewed"):
        evaluate_classification_observations(
            {"occurrences": [label], "aggregates": [aggregate_label]},
            [observation],
            [aggregate],
        )


def test_duplicate_observation_keys_are_rejected():
    label, aggregate_label, observation, aggregate = _pair(1, "Finance", "Finance")

    with pytest.raises(ValueError, match="Duplicate classification observation"):
        evaluate_classification_observations(
            {"occurrences": [label], "aggregates": [aggregate_label]},
            [observation, observation],
            [aggregate],
        )


def test_missing_and_indeterminate_observations_count_as_unscorable():
    first = _pair(1, "Finance", "Finance")
    second = _pair(2, "Finance", "Finance")
    second[2]["option_set_truncated"] = True
    report = evaluate_classification_observations(
        {
            "occurrences": [first[0], second[0]],
            "aggregates": [first[1], second[1]],
        },
        [second[2]],
        [second[3]],
    )

    assert report["observation_missing"] == 1
    assert report["unavailable_or_indeterminate"] == 1
    assert report["unscorable_rate"] == 1.0
    assert report["aggregates_missing"] == 1
    assert report["aggregate_unscorable_rate"] == 0.5


def test_active_readiness_accepts_the_configured_balanced_clean_set():
    pairs = [
        _pair(
            number,
            "Finance" if number <= 10 else "Work",
            "Finance" if number <= 10 else "Work",
        )
        for number in range(1, 51)
    ]
    for label, aggregate_label, _observation, aggregate in pairs[:10]:
        aggregate_label["observation_count"] = 2
        aggregate["observation_count"] = 2

    report = evaluate_classification_observations(
        {
            "occurrences": [item[0] for item in pairs],
            "aggregates": [item[1] for item in pairs],
        },
        [item[2] for item in pairs],
        [item[3] for item in pairs],
    )

    assert report["active_ready"] is True
    assert report["failed_gates"] == []
