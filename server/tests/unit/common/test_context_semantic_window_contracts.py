import pytest
from pydantic import ValidationError

from common.schema.context import (
    ContextAdd,
    ContextDelete,
    ContextEditOperationKind,
    LocalContextBlockReference,
)
from common.schema.semantic_window import (
    ExchangeOutcome,
    ExchangeState,
    SemanticWindowStage,
)
from common.schema.settings import RootConfig


@pytest.mark.unit
@pytest.mark.no_network
def test_semantic_window_defaults_to_the_configurable_128k_target_and_retry_policy():
    settings = RootConfig().developer_settings.ingestion

    assert settings.semantic_window_tokens == 128_000
    assert settings.semantic_window_retry.max_attempts == 3
    assert settings.semantic_window_retry.initial_backoff_seconds == 30
    assert settings.semantic_window_retry.max_backoff_seconds == 300


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    "payload",
    [
        {"semantic_window_tokens": 0},
        {
            "semantic_window_retry": {
                "initial_backoff_seconds": 60,
                "max_backoff_seconds": 30,
            }
        },
        {"semantic_window_retry": {"max_attempts": 0}},
    ],
)
def test_semantic_window_settings_reject_invalid_targets_and_retry_policy(payload):
    with pytest.raises(ValidationError):
        RootConfig.model_validate({"developer_settings": {"ingestion": payload}})


@pytest.mark.unit
@pytest.mark.no_network
def test_context_edit_and_reference_contracts_are_strict():
    assert LocalContextBlockReference(handle=" C12 ").handle == "C12"
    assert ContextAdd(
        section_key="current_state",
        markdown="The project is active.",
    ).operation is ContextEditOperationKind.ADD
    assert ContextDelete(
        section_key="current_state",
        target={"handle": "C1"},
    ).target.handle == "C1"
    assert ExchangeState.CLOSED.value == "closed"
    assert ExchangeOutcome.USER_ONLY.value == "user_only"
    assert SemanticWindowStage.KNOWLEDGE_COMMITTED.value == "knowledge_committed"

    with pytest.raises(ValidationError):
        LocalContextBlockReference(handle="context-1")
    with pytest.raises(ValidationError):
        ContextAdd(
            operation="delete",
            section_key="current_state",
            markdown="The project is active.",
        )
    with pytest.raises(ValidationError):
        ContextDelete(
            section_key="not-a-section",
            target={"handle": "C1"},
        )
