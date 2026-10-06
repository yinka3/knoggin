"""Offline checks for the live identity pilot harness."""

import json

import pytest

from tests.fixtures.run_jev_identity_pilot import (
    build_review_labels,
    load_reviewed_cases,
    provider_usage_summary,
)


def test_live_pilot_refuses_unreviewed_packet(tmp_path):
    packet = tmp_path / "cases.json"
    packet.write_text(
        json.dumps(
            {
                "review_status": "pending_human_review",
                "cases": [
                    {
                        "id": "case-1",
                        "proposed_choice": "none_of_these",
                        "reason": "No match",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="human reviewed"):
        load_reviewed_cases(packet)


def test_review_choices_become_stable_evaluator_labels():
    labels = build_review_labels(
        [
            {
                "id": "reuse",
                "proposed_choice": 42,
                "reason": "Explicit name",
            },
            {
                "id": "ambiguous",
                "proposed_choice": "insufficient_evidence",
                "reason": "Two possible people",
            },
            {
                "id": "missing",
                "proposed_choice": "candidate_recall_failure",
                "reason": "Alias is absent",
                "stored_identity": {"entity_id": 77},
            },
        ]
    )

    assert [(item["correct_entity_id"], item["expected_choice"]) for item in labels] == [
        (42, "candidate"),
        (None, "insufficient_evidence"),
        (77, "candidate"),
    ]
    assert len({item["window_id"] for item in labels}) == 3
    assert all(item["review_status"] == "reviewed" for item in labels)


def test_provider_usage_uses_exact_reported_cost():
    observations = [
        {
            "record": {
                "result": {
                    "input_tokens": 50,
                    "output_tokens": 10,
                    "cost_usd": 0.00002,
                }
            }
        }
    ]

    assert provider_usage_summary(observations) == {
        "requests": 1,
        "cost_reported_requests": 1,
        "input_tokens": 50,
        "output_tokens": 10,
        "reported_cost_usd": 0.00002,
    }
