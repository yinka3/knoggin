"""Score JEV topic observations against independently reviewed labels.

Run from server:
    python -m tests.fixtures.evaluate_jev_classification labels.json observations.json aggregates.json

Proposed labels are rejected because provider output cannot establish truth.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

MIN_REVIEWED_OCCURRENCES = 50
MIN_NON_DEFAULT_OCCURRENCES = 10
MIN_AMBIGUOUS_OCCURRENCES = 10
MIN_REPEATED_ENTITY_AGGREGATES = 10
MIN_ACCURACY = 0.95
MAX_UNSCORABLE_RATE = 0.10
MAX_FALSE_CONFLICT_RATE = 0.02


def _key(item: dict) -> tuple[str, int, str]:
    return (item["window_id"], item["pass_number"], item["occurrence_key"])


def evaluate_classification_observations(
    labels: dict, observations: list[dict], aggregates: list[dict]
) -> dict[str, int | float | bool | list[str]]:
    occurrence_labels = labels.get("occurrences", [])
    aggregate_labels = labels.get("aggregates", [])
    if not occurrence_labels or any(
        item.get("review_status") != "reviewed" for item in occurrence_labels
    ):
        raise ValueError("Classification labels must be human reviewed before scoring")
    if any(item.get("review_status") != "reviewed" for item in aggregate_labels):
        raise ValueError("Classification labels must be human reviewed before scoring")
    occurrence_label_keys = [_key(item) for item in occurrence_labels]
    if len(occurrence_label_keys) != len(set(occurrence_label_keys)):
        raise ValueError("Duplicate classification occurrence label key")
    aggregate_label_keys = [
        (item["window_id"], item["pass_number"], item["anchor_occurrence_key"])
        for item in aggregate_labels
    ]
    if len(aggregate_label_keys) != len(set(aggregate_label_keys)):
        raise ValueError("Duplicate classification aggregate label key")

    keyed_observations = {}
    for observation in observations:
        key = _key(observation["record"])
        if key in keyed_observations:
            raise ValueError("Duplicate classification observation key")
        keyed_observations[key] = observation

    counts: dict[str, int | float] = {
        "reviewed_occurrences": len(occurrence_labels),
        "observed": 0,
        "observation_missing": 0,
        "judgments_scored": 0,
        "judgments_correct": 0,
        "judgments_wrong": 0,
        "decisive_reviewed": 0,
        "decisive_scored": 0,
        "decisive_correct": 0,
        "ambiguous_reviewed": 0,
        "ambiguous_scored": 0,
        "ambiguous_correct": 0,
        "override_expected": 0,
        "override_proposed": 0,
        "override_true_positive": 0,
        "override_false_positive": 0,
        "unavailable_or_indeterminate": 0,
        "non_default_reviewed": 0,
        "non_default_scored": 0,
        "non_default_correct": 0,
        "reviewed_aggregates": len(aggregate_labels),
        "repeated_entity_aggregates": 0,
        "aggregates_scored": 0,
        "aggregates_missing": 0,
        "aggregates_correct": 0,
        "aggregates_wrong": 0,
        "conflicts_observed": 0,
        "false_conflicts": 0,
    }
    for label in occurrence_labels:
        expected_topic = label.get("expected_topic")
        if expected_topic is None:
            counts["ambiguous_reviewed"] += 1
        else:
            counts["decisive_reviewed"] += 1
        if expected_topic is not None and expected_topic != label["default_topic"]:
            counts["non_default_reviewed"] += 1
            counts["override_expected"] += 1
        observation = keyed_observations.get(_key(label))
        if observation is None:
            counts["observation_missing"] += 1
            continue
        counts["observed"] += 1
        record = observation["record"]
        if (
            observation.get("option_set_truncated")
            or record["result"]["outcome"] != "available"
            or record["result"].get("response") is None
        ):
            counts["unavailable_or_indeterminate"] += 1
            continue
        choice = record["result"]["response"]["answers"]["topic_choice"]["choice"]
        actual_topic = record["option_mapping"].get(choice)
        correct = (
            actual_topic == expected_topic
            if expected_topic is not None
            else choice == label["expected_choice"]
        )
        counts["judgments_scored"] += 1
        counts["judgments_correct" if correct else "judgments_wrong"] += 1
        if expected_topic is None:
            counts["ambiguous_scored"] += 1
            if correct:
                counts["ambiguous_correct"] += 1
        else:
            counts["decisive_scored"] += 1
            if correct:
                counts["decisive_correct"] += 1
        if actual_topic is not None and actual_topic != label["default_topic"]:
            counts["override_proposed"] += 1
            if correct:
                counts["override_true_positive"] += 1
            else:
                counts["override_false_positive"] += 1
        if expected_topic is not None and expected_topic != label["default_topic"]:
            counts["non_default_scored"] += 1
            if correct:
                counts["non_default_correct"] += 1

    aggregate_by_key = {}
    for aggregate in aggregates:
        key = (aggregate["window_id"], aggregate["entity_id"])
        if key in aggregate_by_key:
            raise ValueError("Duplicate classification aggregate key")
        aggregate_by_key[key] = aggregate
    for label in aggregate_labels:
        if label["observation_count"] > 1:
            counts["repeated_entity_aggregates"] += 1
        anchor = keyed_observations.get(
            (label["window_id"], label["pass_number"], label["anchor_occurrence_key"])
        )
        if anchor is None:
            counts["aggregates_missing"] += 1
            continue
        aggregate = aggregate_by_key.get((label["window_id"], anchor["entity_id"]))
        if aggregate is None:
            counts["aggregates_missing"] += 1
            continue
        counts["aggregates_scored"] += 1
        conflict = aggregate["status"] == "conflicting_proposals"
        if conflict:
            counts["conflicts_observed"] += 1
            if label["expected_status"] != "conflicting_proposals":
                counts["false_conflicts"] += 1
        correct = (
            aggregate["status"] == label["expected_status"]
            and aggregate["proposed_topics"] == label["expected_proposed_topics"]
            and aggregate["observation_count"] == label["observation_count"]
        )
        counts["aggregates_correct" if correct else "aggregates_wrong"] += 1

    scored = int(counts["judgments_scored"])
    non_default_scored = int(counts["non_default_scored"])
    aggregate_scored = int(counts["aggregates_scored"])
    reviewed = int(counts["reviewed_occurrences"])
    decisive_scored = int(counts["decisive_scored"])
    ambiguous_scored = int(counts["ambiguous_scored"])
    override_expected = int(counts["override_expected"])
    override_proposed = int(counts["override_proposed"])
    counts["judgment_accuracy"] = (
        int(counts["judgments_correct"]) / scored if scored else 0.0
    )
    counts["non_default_accuracy"] = (
        int(counts["non_default_correct"]) / non_default_scored
        if non_default_scored
        else 0.0
    )
    counts["decisive_accuracy"] = (
        int(counts["decisive_correct"]) / decisive_scored
        if decisive_scored
        else 0.0
    )
    counts["abstention_accuracy"] = (
        int(counts["ambiguous_correct"]) / ambiguous_scored
        if ambiguous_scored
        else 0.0
    )
    counts["override_precision"] = (
        int(counts["override_true_positive"]) / override_proposed
        if override_proposed
        else 0.0
    )
    counts["override_recall"] = (
        int(counts["override_true_positive"]) / override_expected
        if override_expected
        else 0.0
    )
    counts["aggregate_accuracy"] = (
        int(counts["aggregates_correct"]) / aggregate_scored
        if aggregate_scored
        else 0.0
    )
    counts["unscorable_rate"] = (
        (int(counts["observation_missing"]) + int(counts["unavailable_or_indeterminate"]))
        / reviewed
        if reviewed
        else 1.0
    )
    counts["false_conflict_rate"] = (
        int(counts["false_conflicts"]) / aggregate_scored
        if aggregate_scored
        else 0.0
    )
    counts["aggregate_unscorable_rate"] = (
        int(counts["aggregates_missing"]) / len(aggregate_labels)
        if aggregate_labels
        else 1.0
    )
    failures = []
    gates = (
        (reviewed >= MIN_REVIEWED_OCCURRENCES, "reviewed_occurrences"),
        (
            int(counts["non_default_reviewed"]) >= MIN_NON_DEFAULT_OCCURRENCES,
            "non_default_reviewed",
        ),
        (
            int(counts["ambiguous_reviewed"]) >= MIN_AMBIGUOUS_OCCURRENCES,
            "ambiguous_reviewed",
        ),
        (
            int(counts["repeated_entity_aggregates"])
            >= MIN_REPEATED_ENTITY_AGGREGATES,
            "repeated_entity_aggregates",
        ),
        (counts["judgment_accuracy"] >= MIN_ACCURACY, "judgment_accuracy"),
        (counts["decisive_accuracy"] >= MIN_ACCURACY, "decisive_accuracy"),
        (counts["abstention_accuracy"] >= MIN_ACCURACY, "abstention_accuracy"),
        (counts["override_precision"] >= MIN_ACCURACY, "override_precision"),
        (counts["override_recall"] >= MIN_ACCURACY, "override_recall"),
        (counts["non_default_accuracy"] >= MIN_ACCURACY, "non_default_accuracy"),
        (counts["aggregate_accuracy"] >= MIN_ACCURACY, "aggregate_accuracy"),
        (
            counts["aggregate_unscorable_rate"] <= MAX_UNSCORABLE_RATE,
            "aggregate_unscorable_rate",
        ),
        (counts["unscorable_rate"] <= MAX_UNSCORABLE_RATE, "unscorable_rate"),
        (
            counts["false_conflict_rate"] <= MAX_FALSE_CONFLICT_RATE,
            "false_conflict_rate",
        ),
    )
    failures.extend(name for passed, name in gates if not passed)
    counts["active_ready"] = not failures
    counts["failed_gates"] = failures
    return counts


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit(
            "Usage: evaluate_jev_classification labels.json observations.json aggregates.json"
        )
    labels = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    observations = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
    aggregates = json.loads(Path(sys.argv[3]).read_text(encoding="utf-8"))
    print(
        json.dumps(
            evaluate_classification_observations(labels, observations, aggregates),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
