from pathlib import Path

from tests.fixtures.run_jev_extraction_pilot import evaluate, load_reviewed_cases


def test_bundled_extraction_packet_is_reviewed():
    path = Path("tests/fixtures/jev_extraction_review_cases.json")
    assert len(load_reviewed_cases(path)) == 12


def test_extraction_report_separates_raw_quality_and_active_acceptance():
    cases = [
        {"id": "positive", "expected_type": "Company"},
        {"id": "negative", "expected_type": None, "expected_choice": "not_an_entity"},
    ]
    observations = [
        {
            "case_id": "positive",
            "result": {"outcome": "available"},
            "raw_choice": "Company",
            "accepted_type": "Company",
        },
        {
            "case_id": "negative",
            "result": {"outcome": "available"},
            "raw_choice": "not_an_entity",
            "accepted_type": None,
        },
    ]

    report = evaluate(cases, observations)

    assert report["raw_accuracy"] == 1.0
    assert report["acceptance_precision"] == 1.0
    assert report["positive_recall"] == 1.0
