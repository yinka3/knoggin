from common.schema.episode.generation import (
    LLMEpisodeDecision,
    LLMEpisodeMessageInfluence,
    LLMEpisodeWindowDecision,
)
from common.schema.episode.models import (
    Episode,
    EpisodeNarrativeLimitError,
    MessageEpisode,
)
from common.schema.settings import EpisodeSettings
from core.ingestion.episode_policy import EpisodeGenerationPolicy
from core.ingestion.project_episode_build import ProjectEpisodeBuild


def _build(*, prior=None):
    return ProjectEpisodeBuild(
        project_id="project-1",
        policy=EpisodeGenerationPolicy.capture(
            settings=EpisodeSettings(), episode_window_size=8
        ),
        messages=[
            {"message_id": 10, "session_id": "session-a", "role": "user", "content": "Plan launch", "timestamp_ms": 1},
            {"message_id": 11, "session_id": "session-a", "role": "assistant", "content": "Drafted plan", "timestamp_ms": 2, "user_msg_id": 10},
            {"message_id": 12, "session_id": "session-b", "role": "user", "content": "Budget approved", "timestamp_ms": 3},
        ],
        entity_ids_by_message={}, relationship_ids_by_message={},
        entity_catalog=[], relationship_catalog=[], prior_episodes=prior or [],
    )


def test_brief_is_readable_and_preserves_source_sessions():
    build = _build()
    build.prepare_local_references()

    brief = build.evidence_brief()

    assert "Session session-a:" in brief
    assert "[m1] USER: Plan launch" in brief
    assert "paired-with m1" in brief
    assert '"message_id"' not in brief


def test_window_rejects_overlapping_proposals():
    proposal = LLMEpisodeDecision(
        action="create", summary="Launch planning", message_influences=[
            LLMEpisodeMessageInfluence(message_id="m1", influence_weight=1.0)
        ],
    )
    try:
        LLMEpisodeWindowDecision(proposals=[proposal, proposal])
    except ValueError as exc:
        assert "cannot share source messages" in str(exc)
    else:
        raise AssertionError("overlapping proposals must be rejected")


def test_user_modified_prior_episode_is_not_an_automation_target():
    prior = Episode(
        episode_id="prior", project_id="project-1", summary="Existing plan",
        messages=[MessageEpisode(message_id=1, session_id="session-a", message_position=0)],
        user_modified=True,
    )
    build = _build(prior=[prior])
    build.prepare_local_references()
    # User-modified episodes are deliberately omitted from the model-visible
    # revision candidates, so their local target cannot be resolved.
    output = LLMEpisodeWindowDecision(proposals=[LLMEpisodeDecision(
        action="consolidate", target_episode_id="ep1", summary="Changed",
        message_influences=[LLMEpisodeMessageInfluence(message_id="m1", influence_weight=1.0)],
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
        message_influences=[
            LLMEpisodeMessageInfluence(message_id="m1", influence_weight=1.0)
        ],
    )])

    try:
        build.apply_llm_output(output)
    except EpisodeNarrativeLimitError as exc:
        assert "limit is 4000" in str(exc)
    else:
        raise AssertionError("over-limit episode narrative must be rejected")
