import json
from datetime import datetime, timezone

import pytest

from common.schema.episode.models import EpisodeCheckpoint
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
        "updates": "[]",
        "unresolved": "[]",
        "source_message_count": 1,
        "first_message_at": now,
        "last_message_at": now,
        "generator_metadata": '{"prompt_version": "episode-v1"}',
        "created_at": now,
        "updated_at": now,
    }


def attachment_results(*, focus=False):
    return [
        [
            {
                "message_id": 11,
                "session_id": "session-1",
                "message_position": 0,
                "attached_at": datetime.now(timezone.utc),
            }
        ],
        [
            {
                "entity_id": 2,
                "source_message_count": 1,
                "first_seen_at": datetime.now(timezone.utc),
                "last_seen_at": datetime.now(timezone.utc),
            }
        ],
        [
            {
                "relationship_id": "project-1:2:3",
                "source_message_count": 1,
            }
        ],
    ]


def card_attachment_results():
    return [
        [
            {
                "entity_id": 2,
                "source_message_count": 1,
                "first_seen_at": datetime.now(timezone.utc),
                "last_seen_at": datetime.now(timezone.utc),
            }
        ],
        [
            {
                "relationship_id": "project-1:2:3",
                "source_message_count": 1,
            }
        ],
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_merge_evidence_selects_episode_session_before_serializing_it():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [],
            [
                {
                    "entity_id": 2,
                    "episode_id": "episode-1",
                    "session_id": "session-1",
                    "summary": "Ada chose the episodic-memory approach.",
                }
            ],
            [],
        ]
    )

    evidence = await EpisodeReader(client).get_merge_evidence_for_entities(
        [2], project_id="project-1"
    )

    assert evidence[2] == [
        {
            "kind": "episode",
            "episode_id": "episode-1",
            "text": "Ada chose the episodic-memory approach.",
        }
    ]
    _, query, _ = client.calls[1]
    assert "e.session_id" not in query


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
    assert episode.source_message_count == 1
    assert episode.first_message_at is not None
    assert episode.messages[0].message_id == 11
    assert episode.messages[0].attached_at is not None
    assert episode.entities[0].first_seen_at is not None
    query, params = client.calls[0][1], client.calls[0][2]
    assert "JOIN sessions s" in query
    assert params == ("episode-1", "ada", "project-1", "session-1")


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_entity_lookup_includes_non_focus_memberships():
    client = RecordingPostgresClient(
        fetch_all_results=[[episode_row()], *card_attachment_results()],
    )
    reader = EpisodeReader(client)

    episodes = await reader.get_episodes_for_entity(
        2,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert [episode.episode_id for episode in episodes] == ["episode-1"]
    assert not hasattr(episodes[0], "messages")
    assert len(client.calls) == 3
    assert all("FROM messages" not in call[1] for call in client.calls)
    query, params = client.calls[0][1], client.calls[0][2]
    assert "e.last_message_at DESC" in query
    assert params == (2, "ada", "project-1", "session-1", 10)


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_returns_scoped_semantic_matches():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [{**episode_row(), "similarity": 0.86}],
            *card_attachment_results(),
        ]
    )
    reader = EpisodeReader(client)

    matches = await reader.search_episodes_by_embedding(
        [0.1] * 1024,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        limit=3,
        score_threshold=0.5,
    )

    assert [(episode.episode_id, score) for episode, score in matches] == [
        ("episode-1", 0.86)
    ]
    query, params = client.calls[0][1], client.calls[0][2]
    assert "e.embedding <=> %s::vector" in query
    assert "e.embedding IS NOT NULL" in query
    assert json.loads(params[0]) == [0.1] * 1024
    assert params[1:4] == ("ada", "project-1", "session-1")
    assert params[5] == 0.5
    assert params[-1] == 3


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_uses_the_stored_lexical_search_vector():
    client = RecordingPostgresClient(
        fetch_all_results=[[episode_row()], *card_attachment_results()]
    )
    reader = EpisodeReader(client)

    episodes = await reader.search_episodes(
        "episodic memory",
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        limit=4,
    )

    assert [episode.episode_id for episode in episodes] == ["episode-1"]
    query, params = client.calls[0][1], client.calls[0][2]
    assert "e.search_tsvector @@ q.terms" in query
    assert "ts_rank_cd(e.search_tsvector, q.terms)" in query
    assert "to_tsvector" not in query
    assert params == ("episodic memory", "ada", "project-1", "session-1", 4)


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_ranks_prior_episodes_by_source_entity_overlap():
    client = RecordingPostgresClient(
        fetch_all_results=[[episode_row()], *card_attachment_results()]
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
    assert "ORDER BY entity_overlap DESC, e.last_message_at DESC NULLS LAST" in query
    assert params == ([2, 3], "ada", "project-1", "session-1", 3)


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_loads_the_immediately_previous_episode():
    client = RecordingPostgresClient(
        fetch_all_results=[[episode_row()], *card_attachment_results()]
    )
    reader = EpisodeReader(client)

    episodes = await reader.get_recent_episodes(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert [episode.episode_id for episode in episodes] == ["episode-1"]
    query, params = client.calls[0][1], client.calls[0][2]
    assert "ORDER BY e.last_message_at DESC NULLS LAST, e.episode_id DESC" in query
    assert params == ("ada", "project-1", "session-1", 1)


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_selects_nearby_candidates_by_source_session_and_time():
    client = RecordingPostgresClient(
        fetch_all_results=[[episode_row()], *attachment_results()]
    )
    reader = EpisodeReader(client)

    episodes = await reader.get_nearby_project_episodes(
        user_name="ada",
        project_id="project-1",
        session_ids=["session-1"],
        before_message_id=20,
        before_timestamp_ms=1700000001000,
        limit=3,
    )

    assert [episode.episode_id for episode in episodes] == ["episode-1"]
    query, params = client.calls[0][1], client.calls[0][2]
    assert "m.session_id = ANY(%s)" in query
    assert "m.timestamp_ms < %s" in query
    assert "e.user_modified = FALSE" in query
    assert "entity_overlap" not in query
    assert params == (
        "project-1",
        "ada",
        ["session-1"],
        1700000001000,
        1700000001000,
        1700000001000,
        20,
        1700000001000,
        20,
        3,
    )


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
                    "message_position": 0,
                    "attached_at": datetime.now(timezone.utc),
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
async def test_episode_reader_returns_initial_checkpoint_before_any_episode_work():
    client = RecordingPostgresClient(
        fetch_one_results=[{"message_id": 0, "last_evaluated_timestamp_ms": None}]
    )
    reader = EpisodeReader(client)

    checkpoint = await reader.get_episode_checkpoint(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert checkpoint == EpisodeCheckpoint()
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
                    "user_message_id": 11,
                    "user_content": "First complete message.",
                    "user_timestamp_ms": 1700000000000,
                    "assistant_message_id": 12,
                    "assistant_content": "Second complete message.",
                    "assistant_timestamp_ms": 1700000001000,
                },
            ]
        ]
    )
    reader = EpisodeReader(client)

    messages = await reader.get_next_episode_window(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        checkpoint=EpisodeCheckpoint(
            last_evaluated_message_id=10,
            last_evaluated_timestamp_ms=1700000000000,
        ),
        message_count=2,
    )

    assert [message["message_id"] for message in messages] == [11, 12]
    assert messages[1]["user_msg_id"] == 11
    query, params = client.calls[0][1], client.calls[0][2]
    assert "JOIN messages AS assistant_message" in query
    assert "user_message.ingestion_state = 'processed'" in query
    assert "assistant_message.ingestion_state = 'excluded'" in query
    assert "episode_eligible" not in query
    assert "ORDER BY user_message.timestamp_ms ASC NULLS LAST" in query
    assert "user_message.timestamp_ms > %s" in query
    assert params == (
        "ada",
        "project-1",
        "session-1",
        10,
        1700000000000,
        1700000000000,
        1700000000000,
        1700000000000,
        10,
        1700000000000,
        10,
        10,
        1,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_rejects_window_with_ineligible_message():
    client = RecordingPostgresClient(
        fetch_all_results=[[]]
    )
    reader = EpisodeReader(client)

    messages = await reader.get_next_episode_window(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        checkpoint=EpisodeCheckpoint(
            last_evaluated_message_id=10,
            last_evaluated_timestamp_ms=1700000000000,
        ),
        message_count=2,
    )

    assert messages == []


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_episode_reader_isolates_user_project_and_session_scopes(
    real_postgres_client,
):
    """Reader queries must not expose another user's or project's episodes."""

    await real_postgres_client.execute(
        """
        INSERT INTO projects (project_id, user_name, name, domain_config)
        VALUES (
            'project-3', 'bob', 'Bob project',
            '{"version":1,"topics":{"Identity":{"active":true},"General":{"active":true}},"entity_types":{"Identity":{"topic":"Identity","labels":["person"]},"Concept":{"topic":"General","labels":["concept"]}},"relationships":{}}'::jsonb
        );

        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES
            ('session-1', 'ada', 'project-1'),
            ('session-2', 'ada', 'project-2'),
            ('session-3', 'bob', 'project-3');

        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms
        ) VALUES
            ('ada', 'session-1', 101, 'project-1', 'user', 'Project one source', 1000),
            ('ada', 'session-2', 102, 'project-2', 'user', 'Project two source', 2000),
            ('bob', 'session-3', 103, 'project-3', 'user', 'Bob project source', 3000);

        INSERT INTO episodes (
            episode_id, project_id, summary, source_message_count,
            first_message_at, last_message_at, created_at, updated_at
        ) VALUES
            (
                'episode-1', 'project-1',
                'Visible project one memory', 1,
                TIMESTAMPTZ '2026-01-01 00:00:01+00',
                TIMESTAMPTZ '2026-01-01 00:00:01+00',
                TIMESTAMPTZ '2026-01-01 00:00:01+00',
                TIMESTAMPTZ '2026-01-01 00:00:01+00'
            ),
            (
                'episode-2', 'project-2',
                'Private project two memory', 1,
                TIMESTAMPTZ '2026-01-02 00:00:01+00',
                TIMESTAMPTZ '2026-01-02 00:00:01+00',
                TIMESTAMPTZ '2026-01-02 00:00:01+00',
                TIMESTAMPTZ '2026-01-02 00:00:01+00'
            ),
            (
                'episode-3', 'project-3',
                'Private Bob memory', 1,
                TIMESTAMPTZ '2026-01-03 00:00:01+00',
                TIMESTAMPTZ '2026-01-03 00:00:01+00',
                TIMESTAMPTZ '2026-01-03 00:00:01+00',
                TIMESTAMPTZ '2026-01-03 00:00:01+00'
            );

        INSERT INTO episode_messages (
            episode_id, project_id, session_id, message_id, message_position
        ) VALUES
            ('episode-1', 'project-1', 'session-1', 101, 0),
            ('episode-2', 'project-2', 'session-2', 102, 0),
            ('episode-3', 'project-3', 'session-3', 103, 0);
        """
    )

    reader = EpisodeReader(real_postgres_client)

    visible = await reader.get_episode(
        "episode-1",
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )
    assert visible is not None
    assert visible.summary == "Visible project one memory"
    assert [message.message_id for message in visible.messages] == [101]

    assert await reader.get_episode(
        "episode-2",
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    ) is None
    assert await reader.get_episode(
        "episode-3",
        user_name="ada",
        project_id="project-3",
        session_id="session-3",
    ) is None
    assert await reader.get_episode(
        "episode-1",
        user_name="bob",
        project_id="project-1",
        session_id="session-1",
    ) is None

    recent = await reader.get_recent_episodes(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        limit=10,
    )
    assert [episode.episode_id for episode in recent] == ["episode-1"]

    search_matches = await reader.search_episodes(
        "visible",
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        limit=10,
    )
    assert [episode.episode_id for episode in search_matches] == ["episode-1"]

    assert await reader.get_episode_source_messages(
        "episode-2",
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    ) == []
    assert await reader.get_episode_source_messages(
        "episode-3",
        user_name="ada",
        project_id="project-3",
        session_id="session-3",
    ) == []
