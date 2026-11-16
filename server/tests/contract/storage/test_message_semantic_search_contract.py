"""Storage contract for semantic exchange- and Episode-to-message retrieval."""

import json

import pytest

from core.knowledge.db.readers.message_reader import MessageReader
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.storage
@pytest.mark.no_network
async def test_semantic_message_search_preserves_scope_and_projection_boundary():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [{"message_id": 7, "score": 0.82, "session_id": "session-2"}]
        ]
    )
    vector = [0.25] * 768

    rows = await MessageReader(client).search_semantic_message_sources(
        vector,
        user_name="ada",
        session_ids=["session-1", "session-2"],
        visible_project_ids=["project-1", "project-2"],
        limit=9,
        threshold=0.4,
    )

    assert rows == [(7, 0.82, "session-2")]
    call = client.calls[0]
    assert call[0] == "fetch_all"
    assert "FROM public.exchange_embeddings" in call[1]
    assert "JOIN public.episode_messages" in call[1]
    assert call[1].count("message.lifecycle_state = 'sealed'") == 2
    assert call[1].count("session.status = 'open'") == 2
    assert call[2] == {
        "vector": json.dumps(vector),
        "user_name": "ada",
        "session_ids": ["session-1", "session-2"],
        "project_ids": ["project-1", "project-2"],
        "threshold": 0.4,
        "limit": 9,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_semantic_message_search_skips_empty_visibility_without_database_read():
    client = RecordingPostgresClient()

    rows = await MessageReader(client).search_semantic_message_sources(
        [0.25] * 768,
        user_name="ada",
        session_ids=[],
        visible_project_ids=["project-1"],
    )

    assert rows == []
    assert client.calls == []


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_semantic_episode_search_returns_only_visible_open_messages(
    real_postgres_client,
):
    matching = [1.0] + [0.0] * 767
    await real_postgres_client.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id, status)
        VALUES
            ('session-open', 'ada', 'project-1', 'open'),
            ('session-closed', 'ada', 'project-1', 'deleted');

        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content
        ) VALUES
            ('ada', 'session-open', 101, 'project-1', 'user', 'Visible evidence'),
            ('ada', 'session-closed', 102, 'project-1', 'user', 'Hidden evidence');
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.episodes (
            episode_id, project_id, summary, embedding
        ) VALUES ('episode-semantic', 'project-1', 'Deployment uses violet', %s::vector)
        """,
        (json.dumps(matching),),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.episode_messages (
            episode_id, project_id, session_id, message_id, message_position
        ) VALUES
            ('episode-semantic', 'project-1', 'session-open', 101, 0),
            ('episode-semantic', 'project-1', 'session-closed', 102, 1)
        """
    )

    rows = await MessageReader(
        real_postgres_client
    ).search_semantic_message_sources(
        matching,
        user_name="ada",
        session_ids=["session-open", "session-closed"],
        visible_project_ids=["project-1"],
        threshold=0.9,
    )

    assert rows == [(101, pytest.approx(1.0), "session-open")]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_closed_exchanges_are_searchable_without_an_episode(real_postgres_client):
    from core.knowledge.db.writers.message_writer import MessageWriter

    client = real_postgres_client
    await client.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id, status)
        VALUES ('session-1', 'ada', 'project-1', 'open');

        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            lifecycle_state, exchange_state, exchange_outcome,
            exchange_closed_at_ms, user_msg_id
        ) VALUES
            ('ada', 'session-1', 201, 'project-1', 'user', 'Which queue?',
             'sealed', 'closed', 'assistant_final', 10, NULL),
            ('ada', 'session-1', 202, 'project-1', 'assistant', 'Use the durable queue.',
             'sealed', 'open', NULL, NULL, 201),
            ('ada', 'session-1', 203, 'project-1', 'user', 'Still running',
             'editable', 'open', NULL, NULL, NULL),
            ('ada', 'session-1', 204, 'project-1', 'user', 'Cancelled ask',
             'sealed', 'closed', 'cancelled', 11, NULL);
        """
    )
    reader = MessageReader(client)
    pending = await reader.get_exchanges_without_embeddings(
        user_name="ada", project_id="project-1", limit=10
    )
    assert [(row["user_message_id"], row["assistant_content"]) for row in pending] == [
        (201, "Use the durable queue.")
    ]

    matching = [0.0, 1.0] + [0.0] * 766
    writer = MessageWriter(client)
    rows = [{**pending[0], "embedding": matching}]
    assert await writer.write_exchange_embeddings(project_id="project-1", rows=rows) == 1
    assert await writer.write_exchange_embeddings(project_id="project-1", rows=rows) == 0
    assert await reader.get_exchanges_without_embeddings(
        user_name="ada", project_id="project-1", limit=10
    ) == []

    found = await reader.search_semantic_message_sources(
        matching,
        user_name="ada",
        session_ids=["session-1"],
        visible_project_ids=["project-1"],
        threshold=0.9,
    )

    assert sorted(message_id for message_id, _score, _session in found) == [201, 202]
