import pytest

from common.schema.episode.generation import (
    LLMEpisodeDecision,
    LLMEpisodeWindowDecision,
)
from common.schema.episode.models import EpisodeNarrativeLimitError
from common.schema.settings import EpisodeSettings
from core.knowledge.episodes.build import ProjectEpisodeBuild
from core.knowledge.episodes.policy import EpisodeGenerationPolicy


def _build(*, settings=None):
    settings = settings or EpisodeSettings()
    return ProjectEpisodeBuild(
        project_id="project-1",
        policy=EpisodeGenerationPolicy.capture(settings=settings),
        messages=[
            {
                "message_id": 10,
                "session_id": "session-a",
                "role": "user",
                "content": "Plan launch",
                "timestamp_ms": 1,
            },
            {
                "message_id": 11,
                "session_id": "session-a",
                "role": "assistant",
                "content": "Drafted plan",
                "timestamp_ms": 2,
                "user_msg_id": 10,
            },
            {
                "message_id": 12,
                "session_id": "session-b",
                "role": "user",
                "content": "Budget approved",
                "timestamp_ms": 3,
            },
        ],
    )


def test_brief_is_readable_and_preserves_source_sessions():
    build = _build()
    build.prepare_local_references()

    brief = build.evidence_brief()

    assert "Session session-a:" in brief
    assert "[message:1] source-position=1 USER: Plan launch" in brief
    assert "paired-with message:1" in brief
    assert '"message_id"' not in brief
    assert "Prior project episodes" not in brief


def test_brief_marks_clarification_as_an_unresolved_question():
    build = _build()
    build.messages[1]["exchange_outcome"] = "clarification"
    build.prepare_local_references()

    brief = build.evidence_brief()

    assert (
        "[message:2] source-position=2 "
        "ASSISTANT CLARIFICATION (UNRESOLVED QUESTION): Drafted plan" in brief
    )


def test_window_rejects_overlapping_proposals():
    proposal = LLMEpisodeDecision(
        summary="Launch planning",
        message_influences=["message:1"],
    )

    with pytest.raises(ValueError, match="cannot share source messages"):
        LLMEpisodeWindowDecision(proposals=[proposal, proposal])


def test_server_rejects_a_narrative_over_the_hard_character_limit():
    build = _build()
    build.prepare_local_references()
    output = LLMEpisodeWindowDecision(
        proposals=[
            LLMEpisodeDecision(
                summary="x" * 4001,
                message_influences=["message:1"],
            )
        ]
    )

    with pytest.raises(EpisodeNarrativeLimitError, match="limit is 4000"):
        build.apply_llm_output(output)


def test_server_rejects_unknown_catalog_message_reference():
    build = _build()
    build.prepare_local_references()
    output = LLMEpisodeWindowDecision(
        proposals=[
            LLMEpisodeDecision(
                summary="Ada described the plan.",
                message_influences=["message:99"],
            )
        ]
    )

    with pytest.raises(ValueError, match="Unknown local ID"):
        build.apply_llm_output(output)


def test_server_assigns_source_positions_instead_of_using_llm_order():
    build = _build()
    build.prepare_local_references()
    output = LLMEpisodeWindowDecision(
        proposals=[
            LLMEpisodeDecision(
                summary="The plan and approval are one coherent thread.",
                message_influences=["message:3", "message:1"],
            )
        ]
    )

    build.apply_llm_output(output)
    episode = build.create_episodes()[0]

    assert [
        (message.message_id, message.message_position) for message in episode.messages
    ] == [(10, 0), (12, 1)]


def test_over_capacity_create_proposal_fails_the_window_result():
    build = _build(settings=EpisodeSettings(max_episode_source_messages=1))
    build.prepare_local_references()

    with pytest.raises(ValueError, match="source message limit"):
        build.apply_llm_output(
            LLMEpisodeWindowDecision(
                proposals=[
                    LLMEpisodeDecision(
                        summary="Two source messages do not fit.",
                        message_influences=["message:1", "message:2"],
                    )
                ]
            )
        )


def test_over_token_capacity_create_proposal_fails_the_window_result():
    build = _build(settings=EpisodeSettings(max_episode_source_tokens=1))
    build.prepare_local_references()

    with pytest.raises(ValueError, match="source token limit"):
        build.apply_llm_output(
            LLMEpisodeWindowDecision(
                proposals=[
                    LLMEpisodeDecision(
                        summary="The plan is over the source-token budget.",
                        message_influences=["message:1"],
                    )
                ]
            )
        )


def test_window_creates_multiple_distinct_immutable_episodes():
    build = _build()
    build.prepare_local_references()
    output = LLMEpisodeWindowDecision(
        proposals=[
            LLMEpisodeDecision(
                summary="The team planned the launch.",
                message_influences=["message:1", "message:2"],
            ),
            LLMEpisodeDecision(
                summary="The budget received approval.",
                message_influences=["message:3"],
            ),
        ]
    )

    build.apply_llm_output(output)
    episodes = build.create_episodes()

    assert len(episodes) == 2
    assert len({episode.episode_id for episode in episodes}) == 2
    assert [message.message_id for message in episodes[0].messages] == [10, 11]
    assert [message.message_id for message in episodes[1].messages] == [12]
    assert all("decision_action" not in episode.generator_metadata for episode in episodes)
    assert all("episode_policy" in episode.generator_metadata for episode in episodes)


def test_empty_window_output_creates_no_episodes():
    build = _build()
    build.prepare_local_references()

    assert build.apply_llm_output(LLMEpisodeWindowDecision()) == []
    assert build.create_episodes() == []
