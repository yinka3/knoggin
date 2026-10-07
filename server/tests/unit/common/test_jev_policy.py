from uuid import uuid4

import pytest
from pydantic import ValidationError

from common.schema.jev import JevDecisionRecord, JevPolicy, JevResult, JevSettings
from common.schema.settings import (
    EntityResolutionSettings,
    RootConfig,
    TextProcessorSettings,
)
from core.ingestion.policy import IngestionPolicy
from tests.fixtures.factories import make_domain_config, make_project_state


def policy(jev=None):
    return IngestionPolicy.capture(
        text_processor=TextProcessorSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=make_domain_config().compile(),
        jev=jev,
    )


def test_disabled_defaults_and_private_key_round_trip():
    config = RootConfig(jev=JevSettings(api_key="private-key"))
    assert config.jev.identity_mode == "disabled"
    assert "private-key" not in repr(config.jev)
    assert (
        RootConfig.model_validate(config.model_dump(mode="json")).jev.api_key
        == "private-key"
    )
    assert "api_key" not in config.jev.capture_policy().model_dump()


def test_openrouter_decisions_settings_are_supported():
    settings = JevSettings(
        endpoint="https://openrouter.ai/api/alpha/decisions",
        model="typesafe/jev-1.13",
    )

    assert settings.capture_policy().model == "typesafe/jev-1.13"


def test_frozen_snapshot_round_trip_and_legacy_disabled_replay():
    captured = policy(
        JevSettings(api_key="private-key", identity_mode="observe").capture_policy()
    )
    snapshot = captured.semantic_window_snapshot()
    assert "private-key" not in str(snapshot)
    assert "endpoint" not in snapshot["jev_policy"]
    assert IngestionPolicy.from_semantic_window_snapshot(snapshot) == captured
    # New runtime settings do not change admitted modes or model.
    assert (
        JevSettings(identity_mode="active").identity_mode != captured.jev.identity_mode
    )
    del snapshot["jev_policy"]
    assert IngestionPolicy.from_semantic_window_snapshot(snapshot).jev == JevPolicy()


@pytest.mark.parametrize("mode", ["disabled", "observe"])
def test_phase_a_v1_snapshot_retains_admitted_question_version(mode):
    snapshot = policy(JevPolicy(classification_mode=mode)).semantic_window_snapshot()
    snapshot["jev_policy"]["classification_question_version"] = "classification-v1"
    reopened = IngestionPolicy.from_semantic_window_snapshot(snapshot)
    assert reopened.jev.classification_question_version == "classification-v1"
    assert reopened.jev.classification_mode == mode
    assert reopened.semantic_window_snapshot() == snapshot


@pytest.mark.parametrize(
    "update",
    [
        {"model": "jev-latest"},
        {"max_retries": 9},
        {"max_calls_per_window": 0},
        {"identity_match_sample_rate": 1.1},
        {"identity_min_choice_confidence": 1.1},
        {"identity_min_choice_probability": -0.1},
        {"identity_min_probability_margin": 1.1},
        {"identity_min_evidence_noul": -0.1},
        {"extraction_min_choice_confidence": 1.1},
        {"extraction_min_entity_noul": -0.1},
        {"request_timeout_seconds": 10, "total_timeout_seconds": 1},
        {"total_timeout_seconds": float("nan")},
        {"max_elapsed_seconds_per_window": 0},
        {"identity_question_version": "unknown"},
        {"endpoint": "http://api.typesafe.ai/v1/systemone"},
        {"endpoint": "https://key:secret@api.typesafe.ai/v1/systemone"},
        {"endpoint": "https://openrouter.ai/api/v1/chat/completions"},
    ],
)
def test_invalid_settings_rejected(update):
    with pytest.raises(ValidationError):
        JevSettings(**update)


def test_unknown_snapshot_keys_and_versions_rejected():
    snapshot = policy().semantic_window_snapshot()
    snapshot["jev_policy"]["api_key"] = "secret"
    with pytest.raises(ValueError):
        IngestionPolicy.from_semantic_window_snapshot(snapshot)


def test_runtime_credentials_cannot_be_captured_as_policy():
    with pytest.raises(TypeError, match="runtime settings"):
        policy(JevSettings(api_key="secret"))


async def test_project_admission_captures_live_jev_settings_once():
    state = make_project_state(text_processor=TextProcessorSettings())
    state._config_manager.config.jev = JevSettings(
        identity_mode="observe", api_key="secret"
    )
    captured = await state.capture_semantic_policy()
    state._config_manager.config.jev = JevSettings(identity_mode="active")
    assert captured.jev.identity_mode == "observe"
    assert "secret" not in str(captured.semantic_window_snapshot())


def test_observe_provenance_serializes_but_cannot_be_accepted():
    data = dict(
        capability="identity",
        mode="observe",
        project_id="work",
        window_id=uuid4(),
        pass_number=1,
        occurrence_key="b2:0:4",
        evidence_block_ids=(uuid4(),),
        domain_version=1,
        question_version="identity-v1",
        acceptance_policy_version="observe-v1",
        pinned_model="jev-1.13.0",
        option_mapping={"candidate_1": "42"},
        result=JevResult(outcome="unavailable", reason="missing_api_key"),
        baseline_outcome="new_id",
        acceptance_status="unavailable",
    )
    record = JevDecisionRecord(**data)
    assert JevDecisionRecord.model_validate_json(record.model_dump_json()) == record
    assert JevDecisionRecord(
        **(data | {"acceptance_status": "indeterminate"})
    ).acceptance_status == "indeterminate"
    with pytest.raises(ValidationError, match="active"):
        JevDecisionRecord(**(data | {"acceptance_status": "accepted"}))
