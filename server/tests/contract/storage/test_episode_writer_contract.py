import json
from datetime import datetime, timezone

import pytest

from common.schema.episode.models import (
    EntityEpisode,
    Episode,
    MessageEpisode,
    RelationshipEpisode,
)
from core.knowledge.db.writers.episode_writer import EpisodeWriter
from tests.fixtures.fakes import RecordingPostgresClient


def make_episode(**overrides):
    episode = Episode(
        episode_id="episode-1",
        project_id="project-1",
        session_id="session-1",
        summary="The team selected the episodic-memory storage slice.",
        importance=0.8,
        messages=[
            MessageEpisode(
                message_id=11,
                session_id="session-1",
                influence_weight=0.6,
                message_position=0,
            ),
            MessageEpisode(
                message_id=12,
                session_id="session-1",
                influence_weight=0.9,
                message_position=1,
            ),
        ],
        entities=[
            EntityEpisode(
                entity_id=2,
                prominence_weight=0.95,
                role="subject",
                is_focus_entity=True,
            )
        ],
        relationships=[
            RelationshipEpisode(
                relationship_id="project-1:2:3",
                prominence_weight=0.8,
                is_central_relationship=True,
            )
        ],
    )
    return episode.model_copy(update=overrides)


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_writer_attaches_complete_derived_context_idempotently():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {"message_id": 11, "timestamp_ms": 1700000000000},
                {"message_id": 12, "timestamp_ms": 1700000001000},
            ],
            [
                {"message_id": 11, "entity_id": 2},
                {"message_id": 12, "entity_id": 2},
                {"message_id": 12, "entity_id": 3},
            ],
            [
                {"message_id": 11, "relationship_id": "project-1:2:3"},
                {"message_id": 12, "relationship_id": "project-1:2:3"},
            ],
        ],
        fetch_one_results=[{"episode_id": "episode-1"}],
    )
    writer = EpisodeWriter(client)

    await writer.create_episode(make_episode(), user_name="ada")

    sql = [call[1] for call in client.calls]
    assert any("INSERT INTO episodes" in query for query in sql)
    assert sum("INSERT INTO episode_messages" in query for query in sql) == 2
    assert sum("INSERT INTO episode_entities" in query for query in sql) == 2
    assert sum("INSERT INTO episode_relationships" in query for query in sql) == 1
    assert all("ON CONFLICT" in query for query in sql if "episode_" in query)

    entity_calls = [
        call for call in client.calls if "INSERT INTO episode_entities" in call[1]
    ]
    assert entity_calls[0][2] == (
        "episode-1",
        "project-1",
        2,
        1.5,
        "subject",
        True,
        2,
        datetime.fromtimestamp(1700000000000 / 1000, tz=timezone.utc),
        datetime.fromtimestamp(1700000001000 / 1000, tz=timezone.utc),
    )
    assert entity_calls[1][2] == (
        "episode-1",
        "project-1",
        3,
        0.9,
        None,
        False,
        1,
        datetime.fromtimestamp(1700000001000 / 1000, tz=timezone.utc),
        datetime.fromtimestamp(1700000001000 / 1000, tz=timezone.utc),
    )
    episode_call = next(
        call for call in client.calls if "INSERT INTO episodes" in call[1]
    )
    assert episode_call[2][8:11] == (
        2,
        datetime.fromtimestamp(1700000000000 / 1000, tz=timezone.utc),
        datetime.fromtimestamp(1700000001000 / 1000, tz=timezone.utc),
    )
    message_call = next(
        call for call in client.calls if "INSERT INTO episode_messages" in call[1]
    )
    assert message_call[2][:4] == ("episode-1", "project-1", "session-1", 11)


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_writer_persists_an_episode_embedding():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [{"message_id": 11}, {"message_id": 12}],
            [{"message_id": 11, "entity_id": 2}],
            [],
        ],
        fetch_one_results=[{"episode_id": "episode-1"}],
    )
    writer = EpisodeWriter(client)

    await writer.create_episode(
        make_episode(entities=[], relationships=[], embedding=[0.25] * 1024),
        user_name="ada",
    )

    insert_call = next(
        call for call in client.calls if "INSERT INTO episodes" in call[1]
    )
    assert "embedding" in insert_call[1]
    assert insert_call[2][11].startswith("[0.25")


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_writer_snapshots_before_consolidation_and_keeps_two_versions():
    now = datetime.now(timezone.utc)
    prior_versions = [
        {
            "version": version,
            "saved_at": now.isoformat(),
            "summary": f"Version {version}",
            "new_developments": [],
            "updates": [],
            "unresolved": [],
            "importance": 0.5,
            "source_message_ids": [version],
            "generator_metadata": {},
        }
        for version in (1, 2)
    ]
    client = RecordingPostgresClient(
        fetch_one_results=[
            {
                "summary": "Previous summary.",
                "new_developments": '["Previous development"]',
                "updates": '["Previous update"]',
                "unresolved": "[]",
                "importance": 0.7,
                "first_message_at": now,
                "last_message_at": now,
                "generator_metadata": '{"effective_action": "create"}',
                "version_history": json.dumps(prior_versions),
            }
        ],
        fetch_all_results=[[{"message_id": 11}, {"message_id": 12}]],
    )

    async with client.transaction() as cur:
        history = await EpisodeWriter._snapshot_before_consolidation(
            cur,
            make_episode(
                generator_metadata={"effective_action": "consolidate"},
            ),
        )

    assert len(history) == 3
    assert history[-1]["version"] == 3
    assert [item["version"] for item in history] == [1, 2, 3]
    assert history[-1]["summary"] == "Previous summary."
    assert history[-1]["source_message_ids"] == [11, 12]


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_writer_rejects_ranked_context_outside_source_messages():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [{"message_id": 11}, {"message_id": 12}],
            [{"message_id": 11, "entity_id": 2}],
            [],
        ]
    )
    writer = EpisodeWriter(client)
    episode = make_episode(
        entities=[EntityEpisode(entity_id=99, is_focus_entity=True)],
        relationships=[],
    )

    with pytest.raises(ValueError, match="must be derived from source messages"):
        await writer.create_episode(episode, user_name="ada")

    assert not any("INSERT INTO episodes" in call[1] for call in client.calls)


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_writer_advances_chronological_checkpoint_for_skip_window():
    client = RecordingPostgresClient(
        fetch_one_results=[
            {"session_id": "session-1"},
            {
                "last_evaluated_message_id": 0,
                "last_evaluated_timestamp_ms": None,
            },
        ],
        fetch_all_results=[
            [
                {"message_id": 102, "timestamp_ms": 1000},
                {"message_id": 103, "timestamp_ms": 2000},
            ],
            [
                {"message_id": 102, "timestamp_ms": 1000},
                {"message_id": 103, "timestamp_ms": 2000},
            ],
            [{"message_id": 102}, {"message_id": 103}],
        ],
    )
    writer = EpisodeWriter(client)

    written = await writer.write_episode_window(
        None,
        [102, 103],
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert written is True
    update_call = next(
        call
        for call in client.calls
        if "UPDATE episode_processing_checkpoints" in call[1]
    )
    assert update_call[2] == (103, 2000, "project-1", "session-1")
