"""Candidate discovery and JEV judgment are scored as separate questions."""

from tests.fixtures.evaluate_jev_identity import (
    evaluate_identity_acceptance,
    evaluate_identity_observations,
)


def _case(
    number,
    *,
    gold_id,
    eligible,
    offered,
    choice,
    outcome="available",
    confidence=0.95,
    evidence_noul=0.95,
):
    key = f"case-{number}"
    label = {
        "window_id": "window-1",
        "pass_number": 1,
        "occurrence_key": key,
        "review_status": "reviewed",
        "correct_entity_id": gold_id,
        "expected_choice": "candidate" if gold_id is not None else "none_of_these",
    }
    observation = {
        "eligible_candidate_ids": eligible,
        "candidate_catalog_truncated": False,
        "candidate_set_truncated": len(eligible) > len(offered),
        "record": {
            "window_id": "window-1",
            "pass_number": 1,
            "occurrence_key": key,
            "option_mapping": {
                f"candidate_{index}": str(entity_id)
                for index, entity_id in enumerate(offered, 1)
            },
            "result": {
                "outcome": outcome,
                "response": {
                    "model": "jev-1.13.0",
                    "answers": {
                        "identity_choice": {
                            "type": "choice",
                            "choice": choice,
                            "probabilities": {
                                handle: float(handle == choice)
                                for handle in [
                                    *observation_handle_names(offered),
                                    "none_of_these",
                                    "insufficient_evidence",
                                ]
                            },
                            "confidence": confidence,
                        },
                        "identity_evidence": {
                            "type": "noul",
                            "noul": evidence_noul,
                        },
                    },
                    "usage": {"input_tokens": 10, "output_tokens": 0},
                }
                if outcome == "available"
                else None,
            },
        },
    }
    return label, observation


def observation_handle_names(offered):
    return [f"candidate_{index}" for index, _ in enumerate(offered, 1)]


def test_candidate_recall_and_judgment_quality_are_scored_separately():
    pairs = [
        _case(1, gold_id=42, eligible=[42], offered=[42], choice="candidate_1"),
        _case(2, gold_id=43, eligible=[42], offered=[42], choice="candidate_1"),
        _case(3, gold_id=43, eligible=[42, 43], offered=[42], choice="candidate_1"),
        _case(4, gold_id=42, eligible=[42], offered=[42], choice="none_of_these"),
        _case(5, gold_id=None, eligible=[42], offered=[42], choice="none_of_these"),
        _case(
            6,
            gold_id=42,
            eligible=[42],
            offered=[42],
            choice="candidate_1",
            outcome="unavailable",
        ),
    ]
    counts = evaluate_identity_observations(
        [label for label, _ in pairs], [observation for _, observation in pairs]
    )

    assert counts == {
        "reviewed": 6,
        "observed": 6,
        "observation_missing": 0,
        "observation_missing_reasons": {},
        "candidate_found": 4,
        "candidate_missing": 1,
        "candidate_recall_unknown": 0,
        "candidate_not_offered": 1,
        "judgments_scored": 3,
        "judgments_correct": 2,
        "judgments_wrong": 1,
        "judgments_unavailable_or_indeterminate": 1,
        "candidate_recall": 0.8,
        "judgment_accuracy": 2 / 3,
    }


def test_unreviewed_labels_cannot_produce_quality_claims():
    label, observation = _case(
        1, gold_id=42, eligible=[42], offered=[42], choice="candidate_1"
    )
    label["review_status"] = "proposed"

    try:
        evaluate_identity_observations([label], [observation])
    except ValueError as exc:
        assert "human reviewed" in str(exc)
    else:
        raise AssertionError("Unreviewed labels were accepted")


def test_active_policy_reports_wrong_reuse_separately_from_abstention():
    pairs = [
        _case(1, gold_id=42, eligible=[42], offered=[42], choice="candidate_1"),
        _case(2, gold_id=43, eligible=[42], offered=[42], choice="candidate_1"),
        _case(
            3,
            gold_id=42,
            eligible=[42],
            offered=[42],
            choice="candidate_1",
            evidence_noul=0.2,
        ),
    ]

    report = evaluate_identity_acceptance(
        [label for label, _ in pairs], [observation for _, observation in pairs]
    )

    assert report == {
        "reviewed": 3,
        "observation_missing": 0,
        "accepted": 2,
        "correct_accepts": 1,
        "wrong_accepts": 1,
        "abstained": 1,
        "acceptance_precision": 0.5,
        "wrong_reuse_rate": 0.5,
    }


def test_missing_observation_counts_as_candidate_discovery_miss():
    label, _observation = _case(
        1, gold_id=42, eligible=[], offered=[], choice="none_of_these"
    )
    label["observation_missing_reason"] = "candidate_discovery_miss"

    quality = evaluate_identity_observations([label], [])
    acceptance = evaluate_identity_acceptance([label], [])

    assert quality == {
        "reviewed": 1,
        "observed": 0,
        "observation_missing": 1,
        "observation_missing_reasons": {"candidate_discovery_miss": 1},
        "candidate_found": 0,
        "candidate_missing": 1,
        "candidate_recall_unknown": 0,
        "candidate_not_offered": 0,
        "judgments_scored": 0,
        "judgments_correct": 0,
        "judgments_wrong": 0,
        "judgments_unavailable_or_indeterminate": 0,
        "candidate_recall": 0.0,
        "judgment_accuracy": 0.0,
    }
    assert acceptance == {
        "reviewed": 1,
        "observation_missing": 1,
        "accepted": 0,
        "correct_accepts": 0,
        "wrong_accepts": 0,
        "abstained": 1,
        "acceptance_precision": 0.0,
        "wrong_reuse_rate": 0.0,
    }


def test_unexplained_missing_observation_does_not_claim_discovery_failure():
    label, _ = _case(1, gold_id=42, eligible=[42], offered=[42], choice="candidate_1")
    quality = evaluate_identity_observations([label], [])
    assert quality["candidate_missing"] == 0
    assert quality["candidate_recall_unknown"] == 1
    assert quality["observation_missing_reasons"] == {"unknown": 1}


def test_sampling_and_observation_limits_are_reported_separately():
    labels = []
    for index, reason in enumerate(("not_sampled", "observation_limit")):
        label, _ = _case(index, gold_id=42, eligible=[42], offered=[42], choice="candidate_1")
        label["observation_missing_reason"] = reason
        labels.append(label)
    quality = evaluate_identity_observations(labels, [])
    assert quality["candidate_missing"] == 0
    assert quality["candidate_recall_unknown"] == 2
    assert quality["observation_missing_reasons"] == {"not_sampled": 1, "observation_limit": 1}
