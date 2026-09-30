from unittest.mock import AsyncMock

import pytest

from core.knowledge.db.readers.message_reader import MessageReader


async def test_history_returns_recent_window_in_durable_order():
    client = AsyncMock()
    client.fetch_all.return_value = [
        {"message_id": 9, "timestamp": 100, "metadata": "invalid"},
        {"message_id": 8, "timestamp": 100, "metadata": '{"source": "saved"}'},
    ]
    rows = await MessageReader(client).get_session_history(
        user_name="alice", session_id="session", limit=2, up_to_msg_id=9
    )
    assert [row["message_id"] for row in rows] == [8, 9]
    assert rows[0]["metadata"] == {"source": "saved"}
    assert rows[1]["metadata"] == {}
    query, params = client.fetch_all.call_args.args
    assert "ORDER BY message.message_id DESC LIMIT" in query
    assert "message.lifecycle_state <> 'superseded'" in query
    assert "session.project_id = message.project_id" in query
    assert params == {"user_name": "alice", "session_id": "session", "limit": 2, "up_to_msg_id": 9}


@pytest.mark.parametrize("limit", [0, -1, True, 10001, "2"])
async def test_history_rejects_invalid_window_before_reading(limit):
    client = AsyncMock()
    with pytest.raises(ValueError):
        await MessageReader(client).get_session_history(user_name="alice", session_id="session", limit=limit)
    client.fetch_all.assert_not_called()
