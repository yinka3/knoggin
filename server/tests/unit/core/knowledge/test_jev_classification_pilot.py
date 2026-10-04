"""Offline checks for the live topic-classification pilot harness."""

import json
from pathlib import Path

import pytest

from tests.fixtures.run_jev_classification_pilot import (
    build_review_labels,
    load_reviewed_cases,
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
        "repeated_entity_aggregates": 10,
        "meets_sample_gate": True,
    }
