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


def test_overlapping_proposal_is_dropped_and_the_first_is_kept():
    build = _build()
    build.prepare_local_references()
    proposal = LLMEpisodeDecision(
        summary="Launch planning",
        message_influences=["message:1"],
    )

    decisions = build.apply_llm_output(
        LLMEpisodeWindowDecision(proposals=[proposal, proposal])
    )

    assert [decision.message_influences for decision in decisions] == [[10]]
    assert len(build.rejected_proposals) == 1
    assert "cannot share source messages" in build.rejected_proposals[0]


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

    assert build.apply_llm_output(output) == []
    assert "Unknown local ID" in build.rejected_proposals[0]


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


def test_over_capacity_multi_exchange_proposal_is_dropped_not_fatal():
    build = _build(settings=EpisodeSettings(max_episode_source_messages=2))
    build.prepare_local_references()

    decisions = build.apply_llm_output(
        LLMEpisodeWindowDecision(
            proposals=[
                LLMEpisodeDecision(
                    summary="Three messages across two exchanges do not fit.",
                    message_influences=["message:1", "message:2", "message:3"],
                ),
                LLMEpisodeDecision(
                    summary="The launch plan alone fits.",
                    message_influences=["message:1", "message:2"],
                ),
            ]
        )
    )

    assert [decision.message_influences for decision in decisions] == [[10, 11]]
    assert "source message limit" in build.rejected_proposals[0]


def test_one_oversized_exchange_is_always_a_valid_episode():
    build = _build(settings=EpisodeSettings(max_episode_source_tokens=1))
    build.prepare_local_references()

    decisions = build.apply_llm_output(
        LLMEpisodeWindowDecision(
            proposals=[
                LLMEpisodeDecision(
                    summary="One exchange cannot be split further.",
                    message_influences=["message:1", "message:2"],
                ),
                LLMEpisodeDecision(
                    summary="Two exchanges over the token budget.",
                    message_influences=["message:3", "message:1"],
                ),
            ]
        )
    )

    assert [decision.message_influences for decision in decisions] == [[10, 11]]
    assert len(build.rejected_proposals) == 1


def test_proposals_beyond_the_window_limit_are_dropped():
    build = _build()
    build.prepare_local_references()
    proposals = [
        LLMEpisodeDecision(summary=f"Thread {index}.", message_influences=[f"message:{index}"])
        for index in (1, 2, 3)
    ] + [LLMEpisodeDecision(summary="Extra thread.", message_influences=["message:3"])]

    decisions = build.apply_llm_output(LLMEpisodeWindowDecision(proposals=proposals))

    assert len(decisions) == 3
    assert "3-proposal limit" in build.rejected_proposals[0]


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

    assert build.apply_llm_output(LLMEpisodeWindowDecision(proposals=[])) == []
    assert build.create_episodes() == []


def test_large_windows_allow_enough_episodes_to_cover_them():
    build = _build(settings=EpisodeSettings(max_episode_source_tokens=2))
    build.prepare_local_references()
    proposals = [
        LLMEpisodeDecision(summary=f"Thread {index}.", message_influences=[f"message:{index}"])
        for index in (1, 2, 3)
    ]

    # Three short messages need several 2-token Episodes, so the cap grows.
    assert build.max_proposals > 3
    assert len(build.apply_llm_output(LLMEpisodeWindowDecision(proposals=proposals))) == 3
    assert _build().max_proposals == 3
