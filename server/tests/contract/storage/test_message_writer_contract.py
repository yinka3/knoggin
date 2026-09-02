import pytest

from core.knowledge.db.writers.message_writer import MessageWriter
from tests.fixtures.fakes import RecordingPostgresClient

MESSAGE_SQL_PARAMS = (
    "ada",
    "session-1",
    7,
    "project-1",
    "user",
    "hello graph",
    7,
    "{}",
    123456,
    "sealed",
    None,
    None,
    1,
    None,
    None,
    "excluded",
    None,
    None,
    None,
)


@pytest.mark.storage
@pytest.mark.no_network
async def test_message_writer_saves_canonical_rows():
    client = RecordingPostgresClient(fetch_one_results=[{"message_id": 7}])
    writer = MessageWriter(client)

    saved = await writer.save_message_logs(
        [
            {
                "id": 7,
                "content": "hello graph",
                "role": "user",
                "user_name": "ada",
                "session_id": "session-1",
                "project_id": "project-1",
                "timestamp": 123456,
            }
        ]
    )

    assert saved is True
    assert len(client.calls) == 1
    canonical_call = client.calls[0]
    assert canonical_call[0] == "execute"
    assert "INSERT INTO messages" in canonical_call[1]
    assert "ON CONFLICT (user_name, session_id, message_id)" in canonical_call[1]
    assert "messages.content = EXCLUDED.content" in canonical_call[1]
    assert canonical_call[2] == MESSAGE_SQL_PARAMS
    assert client.transaction_enters == 1
    assert client.transaction_exits == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_message_writer_empty_list_skips_db():
    client = RecordingPostgresClient()
    assert await MessageWriter(client).save_message_logs([]) is True
    assert client.calls == []
    assert client.transaction_enters == 0


@pytest.mark.storage
@pytest.mark.no_network
async def test_message_writer_rejects_missing_scope_without_execute():
    client = RecordingPostgresClient()

    with pytest.raises(ValueError, match="missing required scope fields"):
        await MessageWriter(client).save_message_logs(
            [
                {
                    "id": 7,
                    "content": "hello graph",
                    "role": "user",
                    "user_name": "ada",
                    "session_id": "session-1",
                }
            ]
        )

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_message_writer_rejects_conflicting_canonical_message_payload():
    client = RecordingPostgresClient(fetch_one_results=[None])

    with pytest.raises(RuntimeError, match="Canonical message ID collision"):
        await MessageWriter(client).save_message_logs(
            [
                {
                    "id": 7,
                    "content": "different payload",
                    "role": "assistant",
                    "user_name": "ada",
                    "session_id": "session-1",
                    "project_id": "project-1",
                    "timestamp": 123456,
                }
            ]
        )

    assert len(client.calls) == 1
