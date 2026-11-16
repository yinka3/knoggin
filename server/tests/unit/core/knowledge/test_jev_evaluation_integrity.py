"""Readiness reports must preserve raw evidence and count unique examples."""

import json

import pytest

from tests.fixtures import run_jev_identity_pilot as identity
from tests.fixtures.evaluate_jev_identity import (
    evaluate_identity_acceptance,
    evaluate_identity_observations,
)
from tests.unit.core.knowledge.test_jev_identity_evaluation import _case


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


def test_packet_loaders_reject_duplicate_case_ids(tmp_path):
    runner = identity
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


async def test_duplicate_cases_are_rejected_before_client_creation(monkeypatch):
    runner, entry = identity, "run_pilot"
    def forbidden_client(*args, **kwargs):
        pytest.fail("Duplicate cases must be rejected before provider work")

    monkeypatch.setattr(runner, "JevClient", forbidden_client)
    with pytest.raises(ValueError, match="Duplicate review case"):
        await getattr(runner, entry)([{"id": "one"}] * 2, "offline-placeholder")
