"""Offline checks for the live topic-classification pilot harness."""

import json
from pathlib import Path

import pytest

from tests.fixtures.run_jev_classification_pilot import (
    build_review_labels,
    load_reviewed_cases,
    provider_usage_summary,
    review_packet_coverage,
    validate_review_cases,
)


def test_live_pilot_refuses_unreviewed_packet(tmp_path):
    packet = tmp_path / "cases.json"
    packet.write_text(
        json.dumps(
            {
                "review_status": "pending_human_review",
                "cases": [
                    {
                        "id": "finance",
                        "occurrences": [
                            {
                                "context": "Acme manages the budget.",
                                "proposed_choice": "Finance",
                            }
                        ],
                        "reason": "Budget evidence",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="human reviewed"):
        load_reviewed_cases(packet)


def test_review_choices_become_stable_occurrence_and_aggregate_labels():
    case = {
        "id": "finance",
        "default_topic": "Work",
        "occurrences": [
            {"context": "Acme manages the budget.", "proposed_choice": "Finance"},
            {
                "context": "Acme manages the budget.",
                "proposed_choice": "insufficient_evidence",
            },
        ],
        "proposed_aggregate_status": "consistent_proposal",
        "proposed_topics": ["Finance"],
    }

    labels = build_review_labels([case])

    assert [item["expected_topic"] for item in labels["occurrences"]] == [
        "Finance",
        None,
    ]
    assert labels["aggregates"][0]["anchor_occurrence_key"] == labels[
        "occurrences"
    ][0]["occurrence_key"]
    assert labels["aggregates"][0]["observation_count"] == 2
    assert all(
        item["review_status"] == "reviewed"
        for item in labels["occurrences"] + labels["aggregates"]
    )


def test_bundled_review_packet_meets_the_agreed_sample_composition():
    packet = json.loads(
        Path("tests/fixtures/jev_classification_review_cases.json").read_text(
            encoding="utf-8"
        )
    )

    assert packet["review_status"] == "reviewed"
    assert validate_review_cases(packet["cases"]) is None
    assert review_packet_coverage(packet["cases"]) == {
        "occurrences": 50,
        "non_default_occurrences": 24,
        "ambiguous_occurrences": 10,
        "repeated_entity_aggregates": 10,
        "meets_sample_gate": True,
    }


def test_heldout_packet_is_frozen_reviewed_and_meets_sample_gate():
    packet = json.loads(
        Path("tests/fixtures/jev_classification_heldout_cases.json").read_text(
            encoding="utf-8"
        )
    )

    assert packet["review_status"] == "reviewed"
    assert validate_review_cases(packet["cases"]) is None
    assert review_packet_coverage(packet["cases"]) == {
        "occurrences": 50,
        "non_default_occurrences": 28,
        "ambiguous_occurrences": 10,
        "repeated_entity_aggregates": 10,
        "meets_sample_gate": True,
    }


def test_provider_usage_is_reported_without_configured_budget_pricing():
    observations = [
        {
            "record": {
                "result": {
                    "input_tokens": 100,
                    "output_tokens": 20,
                    "cost_usd": 0.00012,
                }
            }
        },
        {
            "record": {
                "result": {
                    "input_tokens": 80,
                    "output_tokens": 10,
                    "cost_usd": None,
                }
            }
        },
    ]

    assert provider_usage_summary(observations) == {
        "requests": 2,
        "cost_reported_requests": 1,
        "input_tokens": 180,
        "output_tokens": 30,
        "reported_cost_usd": 0.00012,
    }
