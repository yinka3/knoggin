import json
from pathlib import Path

import pytest

from tests.fixtures.run_jev_extraction_pilot import (
    load_reviewed_cases as load_extraction,
)
from tests.fixtures.run_jev_identity_pilot import load_reviewed_cases as load_identity


@pytest.mark.parametrize(
    "filename,loader,count",
    [
        ("jev_identity_readiness_cases.json", load_identity, 200),
        ("jev_extraction_readiness_cases.json", load_extraction, 60),
    ],
)
def test_readiness_packets_are_approved_and_loadable(filename, loader, count):
    path = Path("tests/fixtures") / filename
    packet = json.loads(path.read_text(encoding="utf-8"))
    assert packet["evaluation_role"] == "synthetic_stress"
    assert len(packet["cases"]) == count
    assert len({case["id"] for case in packet["cases"]}) == count
    assert all(case.get("family") and case.get("reason") for case in packet["cases"])
    assert packet["review_status"] == "reviewed"
    assert len(loader(path)) == count


def test_identity_sample_includes_ambiguity_no_match_and_discovery():
    packet = json.loads(
        Path("tests/fixtures/jev_identity_readiness_cases.json").read_text(encoding="utf-8")
    )
    choices = [case["proposed_choice"] for case in packet["cases"]]
    assert choices.count("insufficient_evidence") == 40
    assert choices.count("none_of_these") == 15
    assert choices.count("candidate_recall_failure") == 5
    for case in packet["cases"]:
        if isinstance(case["proposed_choice"], int):
            assert case["proposed_choice"] in {
                candidate["entity_id"] for candidate in case["candidates"]
            }
