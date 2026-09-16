import pytest
from pydantic import ValidationError

from common.schema.episode.generation import (
    EpisodeDecision,
    LLMEpisodeDecision,
    LLMEpisodeWindowDecision,
)
from core.knowledge.episodes.prompts import (
    get_episode_generation_prompt,
    get_episode_narrative_repair_prompt,
)


def test_episode_decision_accepts_one_new_window_local_episode_shape():
    decision = EpisodeDecision(
        summary="The team decided to store episode attachments in Postgres.",
        message_influences=[11],
    )

    assert decision.summary.startswith("The team decided")
    assert decision.message_influences == [11]


def test_episode_generation_prompt_renders_window_source_limits():
    prompt = get_episode_generation_prompt(
        "Ada",
        prompt_narrative_chars=3600,
        max_narrative_chars=4000,
        max_episode_source_messages=72,
        max_episode_source_tokens=12000,
    )

    assert "Ada" in prompt
    assert "3600" in prompt
    assert "4000" in prompt
    assert "72" in prompt
    assert "12000" in prompt
    assert "target_episode_id" not in prompt
    assert "consolidate" not in prompt


def test_episode_repair_prompt_preserves_source_references_only():
    prompt = get_episode_narrative_repair_prompt("Ada", max_narrative_chars=4000)

    assert "4000" in prompt
    assert "Preserve the source references" in prompt
    assert "consolidation" not in prompt


def test_llm_episode_decision_requires_typed_local_references():
    decision = LLMEpisodeDecision(
        summary="The conversation established the episode boundary.",
        message_influences=["message:1"],
    )

    assert decision.message_influences == ["message:1"]

    with pytest.raises(ValidationError):
        LLMEpisodeDecision(
            summary="A summary.",
            message_influences=[11],
        )


@pytest.mark.parametrize(
    "payload",
    [
        {"message_influences": [11]},
        {
            "action": "create",
            "summary": "A summary.",
            "message_influences": [11],
        },
        {
            "summary": "A summary.",
            "message_influences": [11, 11],
        },
    ],
)
def test_episode_decision_rejects_retired_or_invalid_shapes(payload):
    with pytest.raises(ValidationError):
        EpisodeDecision(**payload)


def test_window_rejects_proposals_that_share_a_source_message():
    proposal = LLMEpisodeDecision(
        summary="The project selected durable memory.",
        message_influences=["message:1"],
    )

    with pytest.raises(ValidationError, match="cannot share source messages"):
        LLMEpisodeWindowDecision(proposals=[proposal, proposal])


def test_empty_window_proposals_are_the_explicit_skip_result():
    assert LLMEpisodeWindowDecision().proposals == []
