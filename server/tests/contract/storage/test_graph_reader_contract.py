import pytest

from core.knowledge.db.readers.graph_reader import GraphReader
from core.knowledge.db.readers.message_reader import MessageReader
from tests.fixtures.fakes import RecordingPostgresClient


def _message(message_id: int, timestamp: int | None) -> dict:
    return {
        "id": message_id,
        "user_name": "ada",
        "session_id": "session-1",
        "role": "user",
        "content": f"message {message_id}",
        "timestamp": timestamp,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_surrounding_messages_use_strict_timestamp_and_message_id_bounds():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [_message(3, 200)],
            [_message(2, 200), _message(1, 100)],
            [_message(4, 200), _message(5, 300)],
        ]
    )

    messages = await GraphReader(client).get_surrounding_messages(
        3,
        user_name="ada",
        session_id="session-1",
        visible_project_ids=["project-1"],
        forward=2,
        target_total=5,
    )

    assert [message["id"] for message in messages] == [1, 2, 3, 4, 5]
    _, back_query, back_params = client.calls[1]
    _, forward_query, forward_params = client.calls[2]
    assert "timestamp_ms = %s AND message_id < %s" in back_query
    assert "ORDER BY timestamp_ms DESC NULLS FIRST, message_id DESC" in back_query
    assert "timestamp_ms = %s AND message_id > %s" in forward_query
    assert "ORDER BY timestamp_ms ASC NULLS LAST, message_id ASC" in forward_query
    assert back_params[:6] == (200, 200, 3, 200, 200, 3)
    assert forward_params[:6] == (200, 200, 3, 200, 200, 3)


@pytest.mark.storage
@pytest.mark.no_network
async def test_discovery_surrounding_context_requires_open_sealed_history():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [_message(3, 200)],
            [_message(2, 100)],
            [_message(4, 300)],
        ]
    )

    await GraphReader(client).get_surrounding_messages(
        3,
        user_name="ada",
        session_id="session-1",
        visible_project_ids=["project-1"],
        discoverable_only=True,
    )

    for _, query, _ in client.calls:
        assert "lifecycle_state = 'sealed'" in query
        assert "status = 'open'" in query


@pytest.mark.storage
@pytest.mark.no_network
async def test_explicit_message_hydration_does_not_apply_discovery_lifecycle_filters():
    client = RecordingPostgresClient(fetch_all_results=[[_message(3, 200)]])

    messages = await GraphReader(client).get_messages_by_ids(
        [3],
        user_name="ada",
        session_ids=["deleted-session"],
        visible_project_ids=["project-1"],
    )

    assert [message["id"] for message in messages] == [3]
    _, query, _ = client.calls[0]
    assert "lifecycle_state = 'sealed'" not in query
    assert "status = 'open'" not in query


@pytest.mark.storage
@pytest.mark.no_network
async def test_fts_discovery_requires_open_sealed_history():
    client = RecordingPostgresClient(fetch_all_results=[[]])

    assert await MessageReader(client).search_fts(
        "release plan",
        user_name="ada",
        session_ids=["session-1"],
        visible_project_ids=["project-1"],
    ) == []

    _, query, _ = client.calls[0]
    assert "m.lifecycle_state = 'sealed'" in query
    assert "s.status = 'open'" in query


@pytest.mark.storage
@pytest.mark.no_network
async def test_recent_project_messages_uses_an_exclusive_cursor():
    client = RecordingPostgresClient(fetch_all_results=[[_message(6, 600)]])

    await GraphReader(client).get_recent_project_messages(
        "ada",
        "project-1",
        limit=10,
        before_message_id=7,
    )

    _, query, params = client.calls[0]
    assert "AND message_id < %s" in query
    assert params == ("ada", "project-1", 7, 10)


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_surrounding_messages_do_not_repeat_same_timestamp_rows(
    real_postgres_client,
):
    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content, timestamp_ms
        )
        VALUES
            ('ada', 'session-1', 1, 'project-1', 'user', 'one', 100),
            ('ada', 'session-1', 2, 'project-1', 'user', 'two', 200),
            ('ada', 'session-1', 3, 'project-1', 'user', 'three', 200),
            ('ada', 'session-1', 4, 'project-1', 'user', 'four', 200),
            ('ada', 'session-1', 5, 'project-1', 'user', 'five', 300)
        """
    )

    messages = await GraphReader(real_postgres_client).get_surrounding_messages(
        3,
        user_name="ada",
        session_id="session-1",
        visible_project_ids=["project-1"],
        forward=2,
        target_total=5,
    )

    assert [message["id"] for message in messages] == [1, 2, 3, 4, 5]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_discovery_excludes_unsealed_and_deleted_session_history_but_provenance_remains_readable(
    real_postgres_client,
):
    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id, status)
        VALUES
            ('open-session', 'ada', 'project-1', 'open'),
            ('deleted-session', 'ada', 'project-1', 'deleted')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, lifecycle_state
        ) VALUES
            ('ada', 'open-session', 101, 'project-1', 'user',
             'needle sealed', 100, 'sealed'),
            ('ada', 'open-session', 102, 'project-1', 'assistant',
             'needle editable', 101, 'editable'),
            ('ada', 'open-session', 103, 'project-1', 'assistant',
             'needle superseded', 102, 'superseded'),
            ('ada', 'deleted-session', 104, 'project-1', 'user',
             'needle deleted session', 103, 'sealed')
        """
    )

    message_reader = MessageReader(real_postgres_client)
    matches = await message_reader.search_fts(
        "needle",
        user_name="ada",
        session_ids=["open-session", "deleted-session"],
        visible_project_ids=["project-1"],
    )
    assert [(message_id, session_id) for message_id, _, session_id in matches] == [
        (101, "open-session")
    ]

    graph_reader = GraphReader(real_postgres_client)
    context = await graph_reader.get_surrounding_messages(
        101,
        user_name="ada",
        session_id="open-session",
        visible_project_ids=["project-1"],
        discoverable_only=True,
    )
    assert [message["id"] for message in context] == [101]

    retained = await graph_reader.get_messages_by_ids(
        [104],
        user_name="ada",
        session_ids=["deleted-session"],
        visible_project_ids=["project-1"],
    )
    assert [message["id"] for message in retained] == [104]
