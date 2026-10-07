"""Readiness reports must preserve raw evidence and count unique examples."""

import json
from dataclasses import replace

import pytest

from common.schema.jev import JevResponse, JevResult
from tests.fixtures import run_jev_extraction_pilot as extraction
from tests.fixtures import run_jev_identity_pilot as identity
from tests.fixtures.evaluate_jev_identity import (
    evaluate_identity_acceptance,
    evaluate_identity_observations,
)
from tests.unit.core.knowledge.test_jev_identity_evaluation import _case


async def test_known_type_raw_evidence_is_independent_of_acceptance_threshold(
    monkeypatch,
):
    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def evaluate(self, **kwargs):
            return JevResult(
                outcome="available",
                response=JevResponse.model_validate(
                    {
                        "model": kwargs["policy"].model,
                        "answers": {"entity_evidence": {"type": "noul", "noul": 0.79}},
                        "usage": {"input_tokens": 10, "output_tokens": 0},
                    }
                ),
            )

        async def close(self):
            pass

    monkeypatch.setattr(extraction, "JevClient", FakeClient)
    original_policy = extraction._policy
    cases = [
        {
            "id": "one",
            "candidate": "Acme",
            "context": "Acme sells software.",
            "proposed_type": "Company",
            "expected_type": "Company",
        }
    ]
    reports = []
    for threshold in (0.80, 0.70):

        def frozen_policy(settings, threshold=threshold):
            frozen = original_policy(settings)
            return replace(
                frozen,
                jev=frozen.jev.model_copy(
                    update={"extraction_min_entity_noul": threshold}
                ),
            )

        monkeypatch.setattr(extraction, "_policy", frozen_policy)
        observations, _ = await extraction.run(cases, "offline-placeholder")
        assert observations[0]["raw_choice"] is None
        assert observations[0]["raw_entity_noul"] == 0.79
        reports.append(extraction.evaluate(cases, observations))
    assert [report["accepted"] for report in reports] == [0, 1]
    assert all(report["raw_accuracy"] is None for report in reports)
    assert all(report["raw_choice_scored"] == 0 for report in reports)
    assert all(report["known_type_evidence_scored"] == 1 for report in reports)
    assert all(
        report["known_type_evidence_brier_score"] == pytest.approx(0.0441)
        for report in reports
    )


def test_mixed_report_separates_type_choice_and_entity_evidence():
    cases = [
        {"id": "unknown", "proposed_type": None, "expected_type": "Company"},
        {"id": "known", "proposed_type": "Person", "expected_type": None},
    ]
    observations = [
        {
            "case_id": "unknown",
            "result": {"outcome": "available"},
            "raw_choice": "Company",
            "accepted_type": None,
        },
        {
            "case_id": "known",
            "result": {
                "outcome": "available",
                "response": {"answers": {"entity_evidence": {"noul": 0.2}}},
            },
            "raw_choice": "Person",
            "accepted_type": None,
        },
    ]
    report = extraction.evaluate(cases, observations)
    assert report["raw_choice_scored"] == 1
    assert report["raw_accuracy"] == 1
    assert report["known_type_evidence_brier_score"] == pytest.approx(0.04)
    assert report["accepted"] == 0


@pytest.mark.parametrize(
    "scorer", [evaluate_identity_observations, evaluate_identity_acceptance]
)
@pytest.mark.parametrize("duplicate", ["labels", "observations"])
def test_identity_scorers_reject_duplicate_keys(scorer, duplicate):
    label, observation = _case(
        1, gold_id=42, eligible=[42], offered=[42], choice="candidate_1"
    )
    labels = [label] * (2 if duplicate == "labels" else 1)
    observations = [observation] * (2 if duplicate == "observations" else 1)
    with pytest.raises(ValueError, match="Duplicate identity"):
        scorer(labels, observations)


@pytest.mark.parametrize("duplicate", ["cases", "observations"])
def test_extraction_scorer_rejects_duplicate_keys(duplicate):
    case = {"id": "one", "expected_type": "Company"}
    item = {
        "case_id": "one",
        "result": {"outcome": "available"},
        "raw_choice": "Company",
        "accepted_type": "Company",
    }
    with pytest.raises(ValueError, match="Duplicate"):
        extraction.evaluate(
            [case] * (2 if duplicate == "cases" else 1),
            [item] * (2 if duplicate == "observations" else 1),
        )


@pytest.mark.parametrize("runner", [identity, extraction])
def test_packet_loaders_reject_duplicate_case_ids(tmp_path, runner):
    case = {
        "id": "one",
        "reason": "Reviewed",
        "proposed_choice": "none_of_these",
        "expected_type": "Company",
    }
    packet = tmp_path / "packet.json"
    packet.write_text(
        json.dumps({"review_status": "reviewed", "cases": [case, case]}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Duplicate review case"):
        runner.load_reviewed_cases(packet)


@pytest.mark.parametrize("runner,entry", [(identity, "run_pilot"), (extraction, "run")])
async def test_duplicate_cases_are_rejected_before_client_creation(
    monkeypatch, runner, entry
):
    def forbidden_client(*args, **kwargs):
        pytest.fail("Duplicate cases must be rejected before provider work")

    monkeypatch.setattr(runner, "JevClient", forbidden_client)
    with pytest.raises(ValueError, match="Duplicate review case"):
        await getattr(runner, entry)([{"id": "one"}] * 2, "offline-placeholder")
