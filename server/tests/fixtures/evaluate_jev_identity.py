"""Score exported identity observations against independently reviewed labels.

Run from server:
    python -m tests.fixtures.evaluate_jev_identity labels.json observations.json

This refuses proposed labels; provider behavior alone cannot establish truth.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from common.schema.jev import JevPolicy, JevResult
from core.knowledge.entity.jev_identity import accepted_identity
from tests.fixtures.jev_review_validation import index_unique


def _key(record):
    return (record["window_id"], record["pass_number"], record["occurrence_key"])


def _validated_observations(labels, observations):
    if any(label.get("review_status") != "reviewed" for label in labels):
        raise ValueError("Identity labels must be human reviewed before scoring")
    index_unique(labels, _key, "identity label")
    return index_unique(
        observations, lambda item: _key(item["record"]), "identity observation"
    )


def evaluate_identity_observations(
    labels: list[dict], observations: list[dict]
) -> dict:
    keyed = _validated_observations(labels, observations)

    counts = {
        "reviewed": 0,
        "observed": 0,
        "observation_missing": 0,
        "observation_missing_reasons": {},
        "candidate_found": 0,
        "candidate_missing": 0,
        "candidate_recall_unknown": 0,
        "candidate_not_offered": 0,
        "judgments_scored": 0,
        "judgments_correct": 0,
        "judgments_wrong": 0,
        "judgments_unavailable_or_indeterminate": 0,
    }
    for label in labels:
        counts["reviewed"] += 1
        key = (label["window_id"], label["pass_number"], label["occurrence_key"])
        observation = keyed.get(key)
        if observation is None:
            counts["observation_missing"] += 1
            reason = label.get("observation_missing_reason") or "unknown"
            reasons = counts["observation_missing_reasons"]
            reasons[reason] = reasons.get(reason, 0) + 1
            if label.get("correct_entity_id") is not None:
                if label.get("observation_missing_reason") == "candidate_discovery_miss":
                    counts["candidate_missing"] += 1
                else:
                    counts["candidate_recall_unknown"] += 1
            continue
        counts["observed"] += 1
        gold_id = label.get("correct_entity_id")
        if gold_id is not None:
            if gold_id in observation["eligible_candidate_ids"]:
                counts["candidate_found"] += 1
            elif observation["candidate_catalog_truncated"]:
                counts["candidate_recall_unknown"] += 1
                continue
            else:
                counts["candidate_missing"] += 1
                continue
            if str(gold_id) not in observation["record"]["option_mapping"].values():
                counts["candidate_not_offered"] += 1
                continue

        record = observation["record"]
        if (
            observation["candidate_set_truncated"]
            or record["result"]["outcome"] != "available"
        ):
            counts["judgments_unavailable_or_indeterminate"] += 1
            continue
        choice = record["result"]["response"]["answers"]["identity_choice"]["choice"]
        expected = label["expected_choice"]
        correct = (
            record["option_mapping"].get(choice) == str(gold_id)
            if expected == "candidate"
            else choice == expected
        )
        counts["judgments_scored"] += 1
        counts["judgments_correct" if correct else "judgments_wrong"] += 1
    recall_total = counts["candidate_found"] + counts["candidate_missing"]
    counts["candidate_recall"] = (
        counts["candidate_found"] / recall_total if recall_total else 0.0
    )
    counts["judgment_accuracy"] = (
        counts["judgments_correct"] / counts["judgments_scored"]
        if counts["judgments_scored"]
        else 0.0
    )
    return counts


def evaluate_identity_acceptance(
    labels: list[dict], observations: list[dict]
) -> dict[str, int | float]:
    """Score the frozen positive-v1 gate separately from raw JEV judgment."""

    keyed = _validated_observations(labels, observations)
    policy = JevPolicy(
        identity_mode="active",
        acceptance_policy_version="identity-positive-v1",
    )
    counts: dict[str, int | float] = {
        "reviewed": len(labels),
        "observation_missing": 0,
        "accepted": 0,
        "correct_accepts": 0,
        "wrong_accepts": 0,
        "abstained": 0,
    }
    for label in labels:
        key = (label["window_id"], label["pass_number"], label["occurrence_key"])
        observation = keyed.get(key)
        accepted_id = None
        if observation is None:
            counts["observation_missing"] += 1
        else:
            record = observation["record"]
            accepted_id = accepted_identity(
                JevResult.model_validate(record["result"]),
                record["option_mapping"],
                policy,
                candidate_set_truncated=observation["candidate_set_truncated"],
            )
        if accepted_id is None:
            counts["abstained"] += 1
            continue
        counts["accepted"] += 1
        if accepted_id == label.get("correct_entity_id"):
            counts["correct_accepts"] += 1
        else:
            counts["wrong_accepts"] += 1
    accepted = int(counts["accepted"])
    counts["acceptance_precision"] = (
        int(counts["correct_accepts"]) / accepted if accepted else 0.0
    )
    counts["wrong_reuse_rate"] = (
        int(counts["wrong_accepts"]) / accepted if accepted else 0.0
    )
    return counts


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("Usage: evaluate_jev_identity labels.json observations.json")
    labels = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    observations = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
    print(
        json.dumps(
            {
                "observation_quality": evaluate_identity_observations(
                    labels, observations
                ),
                "active_policy": evaluate_identity_acceptance(labels, observations),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
