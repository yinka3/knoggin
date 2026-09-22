"""Storage contract for semantic Episode-to-message candidate retrieval."""

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
    vector = [0.25] * 1024

    rows = await MessageReader(client).search_semantic_episode_sources(
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
    assert "JOIN public.episode_messages" in call[1]
    assert "message.lifecycle_state = 'sealed'" in call[1]
    assert "session.status = 'open'" in call[1]
    assert call[2] == (
        json.dumps(vector),
        "ada",
        ["session-1", "session-2"],
        ["project-1", "project-2"],
        json.dumps(vector),
        0.4,
        9,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_semantic_message_search_skips_empty_visibility_without_database_read():
    client = RecordingPostgresClient()

    rows = await MessageReader(client).search_semantic_episode_sources(
        [0.25] * 1024,
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
async def test_semantic_episode_source_search_returns_only_visible_open_messages(
    real_postgres_client,
):
    matching = [1.0] + [0.0] * 1023
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
    ).search_semantic_episode_sources(
        matching,
        user_name="ada",
        session_ids=["session-open", "session-closed"],
        visible_project_ids=["project-1"],
        threshold=0.9,
    )

    assert rows == [(101, pytest.approx(1.0), "session-open")]
