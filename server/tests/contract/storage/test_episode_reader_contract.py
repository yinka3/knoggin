from datetime import datetime, timezone

import pytest

from core.knowledge.db.readers.episode_reader import EpisodeReader
from tests.fixtures.fakes import RecordingPostgresClient


def episode_row(episode_id="episode-1"):
    now = datetime.now(timezone.utc)
    return {
        "episode_id": episode_id,
        "project_id": "project-1",
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


def aggregate_attachments():
    now = datetime.now(timezone.utc)
    return [
        [
            {
                "message_id": 11,
                "session_id": "session-1",
                "message_position": 0,
                "attached_at": now,
            }
        ],
        [
            {
                "entity_id": 2,
                "source_message_count": 1,
                "first_seen_at": now,
                "last_seen_at": now,
            }
        ],
        [{"relationship_id": "project-1:2:3", "source_message_count": 1}],
    ]


def card_attachments():
    now = datetime.now(timezone.utc)
    return [
        [
            {
                "entity_id": 2,
                "source_message_count": 1,
                "first_seen_at": now,
                "last_seen_at": now,
            }
        ],
        [{"relationship_id": "project-1:2:3", "source_message_count": 1}],
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
async def test_project_episode_hydrates_aggregate_with_normalized_visible_scope():
    client = RecordingPostgresClient(
        fetch_one_results=[episode_row()],
        fetch_all_results=aggregate_attachments(),
    )

    episode = await EpisodeReader(client).get_project_episode(
        " episode-1 ",
        user_name=" ada ",
        project_id=" project-1 ",
        visible_project_ids=[" project-1 ", "project-2", "project-1"],
    )

    assert episode is not None
    assert episode.new_developments == ["Episode tables are available."]
    assert episode.messages[0].message_id == 11
    assert episode.entities[0].first_seen_at is not None
    assert client.calls[0][2] == (
        "episode-1",
        ["project-1", "project-2"],
        "ada",
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_project_episode_searches_use_project_scope_and_stored_indexes():
    client = RecordingPostgresClient(
        fetch_all_results=[[episode_row()], *card_attachments()]
    )

    episodes = await EpisodeReader(client).search_project_episodes(
        " episodic memory ",
        user_name="ada",
        project_id="project-1",
        visible_project_ids=["project-1"],
        limit=4,
    )

    assert [episode.episode_id for episode in episodes] == ["episode-1"]
    query, params = client.calls[0][1], client.calls[0][2]
    assert "e.search_tsvector @@ terms.query" in query
    assert "ts_rank_cd(e.search_tsvector, terms.query)" in query
    assert params == ("episodic memory", ["project-1"], "ada", 4)


@pytest.mark.storage
@pytest.mark.no_network
async def test_project_entity_and_semantic_queries_use_project_scope():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [{**episode_row(), "similarity": 0.86}],
            *card_attachments(),
            [episode_row()],
            *card_attachments(),
        ]
    )
    reader = EpisodeReader(client)

    semantic_matches = await reader.search_project_episodes_by_embedding(
        [0.1] * 1024,
        user_name="ada",
        project_id="project-1",
        visible_project_ids=["project-1"],
        limit=3,
        score_threshold=0.5,
    )
    episodes = await reader.get_project_episodes_for_entities(
        [2],
        user_name="ada",
        project_id="project-1",
        visible_project_ids=["project-1"],
        limit=3,
    )

    assert [(episode.episode_id, score) for episode, score in semantic_matches] == [
        ("episode-1", 0.86)
    ]
    assert [episode.episode_id for episode in episodes] == ["episode-1"]
    semantic_query, semantic_params = client.calls[0][1], client.calls[0][2]
    assert "e.embedding <=> %s::vector" in semantic_query
    assert semantic_params[1:3] == (["project-1"], "ada")
    entity_query, entity_params = client.calls[3][1], client.calls[3][2]
    assert "COUNT(DISTINCT ee.entity_id) AS entity_overlap" in entity_query
    assert entity_params == (["project-1"], "ada", [2], 3)


@pytest.mark.storage
@pytest.mark.no_network
async def test_project_episode_source_messages_require_current_project_visibility():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "message_id": 11,
                    "session_id": "session-1",
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

    messages = await reader.get_project_episode_source_messages(
        "episode-1",
        user_name="ada",
        project_id="project-1",
        visible_project_ids=["project-1"],
    )

    assert messages[0]["content"] == "Build the storage slice first."
    assert client.calls[0][2] == ("episode-1", ["project-1"], "ada")
    with pytest.raises(ValueError, match="include project_id"):
        await reader.get_recent_project_episodes(
            user_name="ada",
            project_id="project-1",
            visible_project_ids=["project-2"],
            limit=1,
        )


@pytest.mark.storage
@pytest.mark.no_network
async def test_project_episode_queries_short_circuit_empty_inputs_and_reject_bad_scores():
    client = RecordingPostgresClient()
    reader = EpisodeReader(client)

    assert await reader.get_recent_project_episodes(
        user_name="ada", project_id="project-1", limit=0
    ) == []
    assert await reader.search_project_episodes(
        "   ", user_name="ada", project_id="project-1", limit=3
    ) == []
    assert await reader.search_project_episodes(
        "history", user_name="ada", project_id="project-1", limit=0
    ) == []
    assert await reader.search_project_episodes_by_embedding(
        [0.1], user_name="ada", project_id="project-1", limit=0
    ) == []
    assert await reader.get_project_episodes_for_entities(
        [], user_name="ada", project_id="project-1", limit=3
    ) == []
    assert await reader.get_project_episodes_for_entities(
        [1], user_name="ada", project_id="project-1", limit=0
    ) == []
    with pytest.raises(ValueError, match="score_threshold"):
        await reader.search_project_episodes_by_embedding(
            [0.1],
            user_name="ada",
            project_id="project-1",
            limit=1,
            score_threshold=1.1,
        )

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_project_episode_reader_isolates_user_and_visible_project_scopes(
    real_postgres_client,
):
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
    visible = await reader.get_project_episode(
        "episode-1", user_name="ada", project_id="project-1"
    )
    assert visible is not None
    assert [message.message_id for message in visible.messages] == [101]

    assert await reader.get_project_episode(
        "episode-2",
        user_name="ada",
        project_id="project-1",
        visible_project_ids=["project-1"],
    ) is None
    shared = await reader.get_project_episode(
        "episode-2",
        user_name="ada",
        project_id="project-1",
        visible_project_ids=["project-1", "project-2"],
    )
    assert shared is not None
    assert await reader.get_project_episode(
        "episode-1",
        user_name="bob",
        project_id="project-1",
        visible_project_ids=["project-1"],
    ) is None

    recent = await reader.get_recent_project_episodes(
        user_name="ada",
        project_id="project-1",
        visible_project_ids=["project-1"],
        limit=10,
    )
    assert [episode.episode_id for episode in recent] == ["episode-1"]

    search_matches = await reader.search_project_episodes(
        "visible",
        user_name="ada",
        project_id="project-1",
        visible_project_ids=["project-1"],
        limit=10,
    )
    assert [episode.episode_id for episode in search_matches] == ["episode-1"]
    assert await reader.get_project_episode_source_messages(
        "episode-2",
        user_name="ada",
        project_id="project-1",
        visible_project_ids=["project-1"],
    ) == []
