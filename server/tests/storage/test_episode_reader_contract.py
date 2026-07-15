from datetime import datetime, timezone

import pytest

from core.knowledge.db.readers.episode_reader import EpisodeReader
from tests.fixtures.fakes import RecordingPostgresClient


def episode_row(episode_id="episode-1"):
    now = datetime.now(timezone.utc)
    return {
        "episode_id": episode_id,
        "project_id": "project-1",
        "session_id": "session-1",
        "summary": "The team selected the episodic-memory storage slice.",
        "new_developments": '["Episode tables are available."]',
        "updates": '[]',
        "unresolved": '[]',
        "importance": 0.8,
        "generator_metadata": '{"prompt_version": "episode-v1"}',
        "created_at": now,
        "updated_at": now,
    }


def attachment_results(*, focus=False):
    return [
        [
            {
                "message_id": 11,
                "influence_weight": 0.9,
                "influence_reason": "introduced the decision",
                "message_position": 0,
            }
        ],
        [
            {
                "entity_id": 2,
                "prominence_weight": 0.9,
                "role": "subject",
                "is_focus_entity": focus,
                "source_message_count": 1,
            }
        ],
        [
            {
                "relationship_id": "project-1:2:3",
                "prominence_weight": 0.7,
                "is_central_relationship": True,
                "source_message_count": 1,
            }
        ],
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_hydrates_one_complete_episode_aggregate():
    client = RecordingPostgresClient(
        fetch_one_results=[episode_row()],
        fetch_all_results=attachment_results(focus=True),
    )
    reader = EpisodeReader(client)

    episode = await reader.get_episode(
        "episode-1",
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert episode is not None
    assert episode.new_developments == ["Episode tables are available."]
    assert episode.messages[0].message_id == 11
    assert episode.entities[0].is_focus_entity is True
    assert episode.relationships[0].is_central_relationship is True
    query, params = client.calls[0][1], client.calls[0][2]
    assert "JOIN sessions s" in query
    assert params == ("episode-1", "ada", "project-1", "session-1")


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_entity_lookup_includes_non_focus_memberships():
    client = RecordingPostgresClient(
        fetch_all_results=[[episode_row()], *attachment_results(focus=False)],
    )
    reader = EpisodeReader(client)

    episodes = await reader.get_episodes_for_entity(
        2,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert [episode.episode_id for episode in episodes] == ["episode-1"]
    assert episodes[0].entities[0].is_focus_entity is False
    query, params = client.calls[0][1], client.calls[0][2]
    assert "ee.is_focus_entity DESC" in query
    assert "ee.is_focus_entity = TRUE" not in query
    assert params == (2, "ada", "project-1", "session-1", 10)


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_ranks_prior_episodes_by_source_entity_overlap():
    client = RecordingPostgresClient(
        fetch_all_results=[[episode_row()], *attachment_results()]
    )
    reader = EpisodeReader(client)

    episodes = await reader.get_episodes_for_entities(
        [3, 2, 3],
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        limit=3,
    )

    assert [episode.episode_id for episode in episodes] == ["episode-1"]
    query, params = client.calls[0][1], client.calls[0][2]
    assert "COUNT(DISTINCT ee.entity_id) AS entity_overlap" in query
    assert "ORDER BY entity_overlap DESC, e.updated_at DESC" in query
    assert params == ([2, 3], "ada", "project-1", "session-1", 3)


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_loads_the_immediately_previous_episode():
    client = RecordingPostgresClient(
        fetch_all_results=[[episode_row()], *attachment_results()]
    )
    reader = EpisodeReader(client)

    episodes = await reader.get_recent_episodes(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert [episode.episode_id for episode in episodes] == ["episode-1"]
    query, params = client.calls[0][1], client.calls[0][2]
    assert "ORDER BY e.updated_at DESC" in query
    assert params == ("ada", "project-1", "session-1", 1)


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_expands_source_messages_in_episode_order():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "message_id": 11,
                    "role": "user",
                    "content": "Build the storage slice first.",
                    "timestamp_ms": 1700000000000,
                    "influence_weight": 0.9,
                    "influence_reason": "introduced the decision",
                    "message_position": 0,
                }
            ]
        ]
    )
    reader = EpisodeReader(client)

    messages = await reader.get_episode_source_messages(
        "episode-1",
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert messages[0]["content"] == "Build the storage slice first."
    query, params = client.calls[0][1], client.calls[0][2]
    assert "ORDER BY em.message_position" in query
    assert params == (
        "episode-1",
        "ada",
        "project-1",
        "session-1",
        "ada",
        "project-1",
        "session-1",
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_returns_zero_checkpoint_before_any_episode_work():
    client = RecordingPostgresClient(fetch_one_results=[{"message_id": 0}])
    reader = EpisodeReader(client)

    checkpoint = await reader.get_last_evaluated_message_id(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert checkpoint == 0
    query, params = client.calls[0][1], client.calls[0][2]
    assert "LEFT JOIN episode_processing_checkpoints" in query
    assert params == ("ada", "project-1", "session-1")


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_requires_a_complete_eligible_window():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "message_id": 11,
                    "role": "user",
                    "content": "First complete message.",
                    "timestamp_ms": 1700000000000,
                    "is_episode_eligible": True,
                },
                {
                    "message_id": 12,
                    "role": "assistant",
                    "content": "Second complete message.",
                    "timestamp_ms": 1700000001000,
                    "is_episode_eligible": True,
                },
            ]
        ]
    )
    reader = EpisodeReader(client)

    messages = await reader.get_next_episode_window(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        after_message_id=10,
        message_count=2,
    )

    assert [message["message_id"] for message in messages] == [11, 12]
    assert all("is_episode_eligible" not in message for message in messages)
    query, params = client.calls[0][1], client.calls[0][2]
    assert "LEFT JOIN episode_eligible_messages" in query
    assert params == ("ada", "project-1", "session-1", 10, 2)


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_rejects_window_with_ineligible_message():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "message_id": 11,
                    "role": "user",
                    "content": "Processed.",
                    "timestamp_ms": 1700000000000,
                    "is_episode_eligible": True,
                },
                {
                    "message_id": 12,
                    "role": "assistant",
                    "content": "Still processing.",
                    "timestamp_ms": 1700000001000,
                    "is_episode_eligible": False,
                },
            ]
        ]
    )
    reader = EpisodeReader(client)

    messages = await reader.get_next_episode_window(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        after_message_id=10,
        message_count=2,
    )

    assert messages == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_loads_relationship_evidence_for_source_messages():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {"message_id": 11, "relationship_id": "project-1:2:3"},
                {"message_id": 11, "relationship_id": "project-1:2:4"},
            ]
        ]
    )
    reader = EpisodeReader(client)

    relationships = await reader.get_relationship_ids_for_messages(
        [12, 11],
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert relationships == {
        11: ["project-1:2:3", "project-1:2:4"],
        12: [],
    }
    query, params = client.calls[0][1], client.calls[0][2]
    assert "FROM relationship_evidence_refs" in query
    assert params == (
        [11, 12],
        "ada",
        "session-1",
        "project-1",
        "ada",
        "project-1",
        "session-1",
    )
