from common.schema.episode.generation import (
    LLMEpisodeConsolidation,
    LLMEpisodeDecision,
    LLMEpisodeWindowDecision,
)
from common.schema.episode.models import (
    Episode,
    EpisodeNarrativeLimitError,
    MessageEpisode,
)
from common.schema.settings import EpisodeSettings
from core.knowledge.episodes.build import ProjectEpisodeBuild
from core.knowledge.episodes.policy import EpisodeGenerationPolicy


def _build(*, prior=None, settings=None):
    settings = settings or EpisodeSettings()
    return ProjectEpisodeBuild(
        project_id="project-1",
        policy=EpisodeGenerationPolicy.capture(
            settings=settings, episode_window_size=8
        ),
        messages=[
            {"message_id": 10, "session_id": "session-a", "role": "user", "content": "Plan launch", "timestamp_ms": 1},
            {"message_id": 11, "session_id": "session-a", "role": "assistant", "content": "Drafted plan", "timestamp_ms": 2, "user_msg_id": 10},
            {"message_id": 12, "session_id": "session-b", "role": "user", "content": "Budget approved", "timestamp_ms": 3},
        ],
        prior_episodes=prior or [],
    )


def test_brief_is_readable_and_preserves_source_sessions():
    build = _build()
    build.prepare_local_references()

    brief = build.evidence_brief()

    assert "Session session-a:" in brief
    assert "[message:1] source-position=1 USER: Plan launch" in brief
    assert "paired-with message:1" in brief
    assert '"message_id"' not in brief


def test_window_rejects_overlapping_proposals():
    proposal = LLMEpisodeDecision(
        action="create", summary="Launch planning", message_influences=["message:1"],
    )
    try:
        LLMEpisodeWindowDecision(proposals=[proposal, proposal])
    except ValueError as exc:
        assert "cannot share source messages" in str(exc)
    else:
        raise AssertionError("overlapping proposals must be rejected")


def test_user_modified_prior_episode_is_not_an_automation_target():
    prior = Episode(
        episode_id="prior", project_id="project-1", session_id="session-a", summary="Existing plan",
        messages=[MessageEpisode(message_id=1, session_id="session-a", message_position=0)],
        user_modified=True,
    )
    build = _build(prior=[prior])
    build.prepare_local_references()
    # User-modified episodes are deliberately omitted from the model-visible
    # revision candidates, so their local target cannot be resolved.
    output = LLMEpisodeWindowDecision(proposals=[LLMEpisodeDecision(
        action="consolidate", target_episode_id="episode:1", summary="Changed",
        message_influences=["message:1"],
    )])
    try:
        build.apply_llm_output(output)
    except ValueError as exc:
        assert "Unknown local ID" in str(exc)
    else:
        raise AssertionError("user-modified episode must not be a target")


def test_server_rejects_a_narrative_over_the_hard_character_limit():
    build = _build()
    build.prepare_local_references()
    output = LLMEpisodeWindowDecision(proposals=[LLMEpisodeDecision(
        action="create",
        summary="x" * 4001,
        message_influences=["message:1"],
    )])

    try:
        build.apply_llm_output(output)
    except EpisodeNarrativeLimitError as exc:
        assert "limit is 4000" in str(exc)
    else:
        raise AssertionError("over-limit episode narrative must be rejected")


def test_server_rejects_unknown_catalog_message_reference():
    build = _build()
    build.prepare_local_references()

    output = LLMEpisodeWindowDecision(proposals=[LLMEpisodeDecision(
        action="create",
        summary="Ada described the plan.",
        message_influences=["message:99"],
    )])

    try:
        build.apply_llm_output(output)
    except ValueError as exc:
        assert "Unknown local ID" in str(exc)
    else:
        raise AssertionError("unknown catalog evidence must be rejected")


def test_server_assigns_source_positions_instead_of_using_llm_order():
    build = _build()
    build.prepare_local_references()
    output = LLMEpisodeWindowDecision(proposals=[LLMEpisodeDecision(
        action="create",
        summary="The plan and approval are one coherent thread.",
        message_influences=[
            "message:3",
            "message:1",
        ],
    )])

    build.apply_llm_output(output)
    episode = build.create_episodes()[0]

    assert [(message.message_id, message.message_position) for message in episode.messages] == [
        (10, 0),
        (12, 1),
    ]


def test_over_capacity_create_proposal_is_dropped_before_persistence():
    build = _build(settings=EpisodeSettings(max_episode_source_messages=1))
    build.prepare_local_references()

    decisions = build.apply_llm_output(LLMEpisodeWindowDecision(proposals=[
        LLMEpisodeDecision(
            action="create",
            summary="Two source messages do not fit.",
            message_influences=["message:1", "message:2"],
        )
    ]))

    assert decisions == []
    assert build.create_episodes() == []


def test_consolidation_preflight_uses_complete_source_messages_and_capacity():
    prior = Episode(
        episode_id="prior",
        project_id="project-1",
        session_id="session-a",
        summary="Existing launch plan",
        messages=[
            MessageEpisode(message_id=1, session_id="session-a", message_position=0),
            MessageEpisode(message_id=2, session_id="session-a", message_position=1),
        ],
    )
    build = _build(prior=[prior])
    build.prepare_local_references()
    decision = build.apply_llm_output(LLMEpisodeWindowDecision(proposals=[
        LLMEpisodeDecision(
            action="consolidate",
            target_episode_id="episode:1",
            summary="The launch plan continued.",
            message_influences=["message:1", "message:2"],
        )
    ]))[0]
    source_messages = [
        {"message_id": 1, "session_id": "session-a", "role": "user", "content": "Plan launch", "timestamp_ms": 1},
        {"message_id": 2, "session_id": "session-a", "role": "assistant", "content": "Drafted plan", "timestamp_ms": 2},
    ]

    assert build.preflight_consolidation(decision, source_messages)
    assert "Plan launch" in build.consolidation_brief(decision)
    assert "Existing launch plan" not in build.consolidation_brief(decision)

    result = LLMEpisodeConsolidation(
        action="consolidate",
        summary="The complete launch plan.",
        message_influences=["message:1", "message:2", "message:3", "message:4"],
    )
    refs = build.resolve_consolidation_references(
        decision, result.message_influences[:2]
    )
    assert refs == [1, 10]


def test_consolidation_capacity_failure_keeps_new_units_separate():
    prior = Episode(
        episode_id="prior",
        project_id="project-1",
        session_id="session-a",
        summary="Existing launch plan",
        messages=[
            MessageEpisode(message_id=1, session_id="session-a", message_position=0),
            MessageEpisode(message_id=2, session_id="session-a", message_position=1),
        ],
    )
    build = _build(
        prior=[prior], settings=EpisodeSettings(max_episode_source_messages=3)
    )
    build.prepare_local_references()
    decision = build.apply_llm_output(LLMEpisodeWindowDecision(proposals=[
        LLMEpisodeDecision(
            action="consolidate",
            target_episode_id="episode:1",
            summary="The launch plan continued.",
            message_influences=["message:1", "message:2"],
        )
    ]))[0]
    assert not build.preflight_consolidation(decision, [
        {"message_id": 1, "session_id": "session-a", "role": "user", "content": "old", "timestamp_ms": 1},
        {"message_id": 2, "session_id": "session-a", "role": "assistant", "content": "old reply", "timestamp_ms": 2},
    ])
    build.keep_consolidation_separate(decision)
    episodes = build.create_episodes()

    assert len(episodes) == 1
    assert episodes[0].episode_id != "prior"
    assert [message.message_id for message in episodes[0].messages] == [10, 11]
