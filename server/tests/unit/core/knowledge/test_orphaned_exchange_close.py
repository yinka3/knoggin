import pytest

from core.knowledge.db.writers.message_lifecycle_writer import MessageLifecycleWriter


class FakeClient:
    async def fetch_all(self, _query, _params):
        return [
            {"project_id": "p1", "session_id": "s1", "message_id": 1},
            {"project_id": "p1", "session_id": "s1", "message_id": 2},
            {"project_id": "p1", "session_id": "s2", "message_id": 3},
        ]


@pytest.mark.unit
@pytest.mark.no_network
async def test_one_unclosable_exchange_does_not_stop_startup_cleanup():
    writer = MessageLifecycleWriter(FakeClient(), message_writer=None)
    attempted = []

    async def close_user_exchange(**kwargs):
        attempted.append(kwargs["user_message_id"])
        if kwargs["user_message_id"] == 2:
            raise ValueError("User exchange is already closed as completed, not failed")

    writer.close_user_exchange = close_user_exchange

    assert await writer.close_orphaned_open_exchanges(user_name="ada") == 2
    assert attempted == [1, 2, 3]
