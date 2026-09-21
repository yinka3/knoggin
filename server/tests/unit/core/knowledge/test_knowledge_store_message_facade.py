import pytest

from core.knowledge.store import KnowledgeStore


@pytest.mark.unit
@pytest.mark.no_network
async def test_message_facade_routes_scoped_reads_to_the_message_reader():
    class MessageReader:
        def __init__(self):
            self.calls = []

        async def get_visible_session_ids(self, **kwargs):
            self.calls.append(("sessions", kwargs))
            return ["session-1"]

        async def get_message_text(self, message_id, **kwargs):
            self.calls.append(("text", message_id, kwargs))
            return "message"

        async def get_messages_by_ids(self, ids, **kwargs):
            self.calls.append(("messages", ids, kwargs))
            return [{"id": ids[0]}]

        async def get_recent_project_messages(self, *args, **kwargs):
            self.calls.append(("recent", args, kwargs))
            return [{"id": 8}]

        async def get_surrounding_messages(self, message_id, **kwargs):
            self.calls.append(("surrounding", message_id, kwargs))
            return [{"id": message_id}]

    reader = MessageReader()
    store = KnowledgeStore.__new__(KnowledgeStore)
    store._message_reader = reader

    scope = {"user_name": "ada", "visible_project_ids": ["project-1"]}
    assert await store.get_visible_session_ids(**scope) == ["session-1"]
    assert await store.get_message_text(7, session_id="session-1", **scope) == "message"
    assert await store.get_messages_by_ids(
        [7], session_ids=["session-1"], **scope
    ) == [{"id": 7}]
    assert await store.get_recent_project_messages("ada", "project-1", 3, 7) == [
        {"id": 8}
    ]
    assert await store.get_surrounding_messages(
        7,
        session_id="session-1",
        forward=2,
        target_total=6,
        discoverable_only=True,
        **scope,
    ) == [{"id": 7}]

    assert reader.calls == [
        ("sessions", scope),
        ("text", 7, {**scope, "session_id": "session-1"}),
        (
            "messages",
            [7],
            {**scope, "session_ids": ["session-1"]},
        ),
        ("recent", ("ada", "project-1", 3), {"before_message_id": 7}),
        (
            "surrounding",
            7,
            {
                **scope,
                "session_id": "session-1",
                "forward": 2,
                "target_total": 6,
                "discoverable_only": True,
            },
        ),
    ]
