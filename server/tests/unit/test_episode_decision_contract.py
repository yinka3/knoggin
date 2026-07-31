import pytest
from pydantic import ValidationError

from common.schema.episode_output import (
    EpisodeConsolidation,
    EpisodeDecision,
    LLMEpisodeDecision,
)
from core.ingestion.prompts import (
    get_episode_consolidation_prompt,
    get_episode_generation_prompt,
)


def test_episode_decision_accepts_complete_create_and_skip_shapes():
    create = EpisodeDecision(
        action="create",
        summary="The team decided to store episode attachments in Postgres.",
        message_influences=[{"message_id": 11, "influence_weight": 0.9}],
    )
    skip = EpisodeDecision(action="skip", skip_reason="Only acknowledgement text.")

    assert create.action == "create"
    assert skip.skip_reason == "Only acknowledgement text."


def test_episode_generation_prompt_renders_the_user_name():
    prompt = get_episode_generation_prompt("Ada")

    assert "Ada's conversation" in prompt
    assert "target_episode_id" in prompt


def test_episode_consolidation_prompt_and_contract_require_all_influences():
    prompt = get_episode_consolidation_prompt("Ada")
    consolidation = EpisodeConsolidation(
        summary="The stored episode was regenerated from all source messages.",
        message_influences=[{"message_id": 11, "influence_weight": 0.9}],
    )

    assert "all of its source messages" in prompt
    assert consolidation.message_influences[0].message_id == 11


def test_llm_episode_decision_requires_typed_local_references():
    decision = LLMEpisodeDecision(
        action="consolidate",
        target_episode_id="ep1",
        summary="The conversation continued an existing thread.",
        message_influences=[{"message_id": "m1", "influence_weight": 0.9}],
        focus_entities=[{"entity_id": "e1", "prominence_weight": 0.8}],
        central_relationships=[
            {"relationship_id": "r1", "prominence_weight": 0.7}
        ],
    )

    assert decision.target_episode_id == "ep1"

    with pytest.raises(ValidationError):
        LLMEpisodeDecision(
            action="create",
            summary="A summary.",
            message_influences=[{"message_id": 11, "influence_weight": 0.9}],
        )


@pytest.mark.parametrize(
    "payload",
    [
        {"action": "skip"},
        {
            "action": "create",
            "summary": "A summary.",
            "target_episode_id": "episode-1",
            "message_influences": [{"message_id": 11, "influence_weight": 0.8}],
        },
        {
            "action": "consolidate",
            "summary": "A summary.",
            "message_influences": [{"message_id": 11, "influence_weight": 0.8}],
        },
        {
            "action": "skip",
            "skip_reason": "Low signal.",
            "message_influences": [{"message_id": 11, "influence_weight": 0.8}],
        },
    ],
)
def test_episode_decision_rejects_invalid_action_shapes(payload):
    with pytest.raises(ValidationError):
        EpisodeDecision(**payload)
