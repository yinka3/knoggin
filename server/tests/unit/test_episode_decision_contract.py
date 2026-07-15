import pytest
from pydantic import ValidationError

from common.schema.contracts import EpisodeDecision
from core.ingestion.prompts import get_episode_generation_prompt


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
