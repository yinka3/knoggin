import pytest

from core.agent.tools.search import SearchTools


def make_search_tool(knowledge_store=None):
    tool = SearchTools()
    tool.knowledge_store = knowledge_store or object()
    tool.user_name = "ada"
    tool.session_id = "session-1"
    tool.readable_project_ids = ["project-1"]
    return tool


@pytest.mark.no_network
async def test_surrounding_context_uses_durable_scoped_messages():
    class FakeKnowledgeStore:
        def __init__(self):
            self.calls = []

        async def get_surrounding_messages(
            self,
            msg_id,
            *,
            user_name,
            session_id,
            visible_project_ids,
            forward,
            target_total,
        ):
            self.calls.append(
                (
                    msg_id,
                    forward,
                    target_total,
                    user_name,
                    session_id,
                    visible_project_ids,
                )
            )
            return [
                {
                    "id": 4,
                    "role": "assistant",
                    "content": "before",
                    "timestamp": 1767261600000,
                },
                {
                    "id": 5,
                    "role": "user",
                    "content": "hit",
                    "timestamp": 1767261660000,
                },
            ]

    knowledge_store = FakeKnowledgeStore()
    tool = make_search_tool(knowledge_store=knowledge_store)

    context = await tool._get_surrounding_context(
        "msg_5", forward=1, target_total=3, session_id="session-2"
    )

    assert knowledge_store.calls == [
        (5, 1, 3, "ada", "session-2", ["project-1"])
    ]
    assert [item["id"] for item in context] == ["msg_4", "msg_5"]
    assert context[1]["is_hit"] is True


@pytest.mark.no_network
async def test_search_messages_drops_results_without_hit_context():
    tool = make_search_tool()
    tool.search_cfg = {"default_message_limit": 8}

    async def search(query, limit):
        return [("msg_7", 0.5, "session-1")]

    async def context_without_hit(msg_id, session_id=None):
        return [{"id": "msg_6", "content": "nearby", "role": "user"}]

    tool._search_messages = search
    tool._get_surrounding_context = context_without_hit

    assert await tool.search_messages("query") == []


@pytest.mark.no_network
async def test_hydrate_evidence_uses_explicit_and_default_scope():
    class FakeKnowledgeStore:
        def __init__(self):
            self.calls = []

        async def get_messages_by_ids(
            self,
            message_ids,
            *,
            user_name,
            session_ids,
            visible_project_ids,
        ):
            self.calls.append(
                (message_ids, user_name, session_ids, visible_project_ids)
            )
            message_id = message_ids[0]
            return [
                {
                    "id": message_id,
                    "user_name": user_name,
                    "session_id": session_ids[0],
                    "content": (
                        "explicit scoped message"
                        if user_name == "grace"
                        else "default scoped message"
                    ),
                    "timestamp": 1767261600000 + message_id,
                }
            ]

    knowledge_store = FakeKnowledgeStore()
    tool = make_search_tool(knowledge_store=knowledge_store)

    evidence = await tool._hydrate_evidence(
        [
            {"user_name": "grace", "session_id": "session-9", "message_id": 7},
            8,
            "turn_2",
            {"message_id": "msg_nope"},
        ]
    )

    assert evidence == [
        {
            "id": "msg_7",
            "user_name": "grace",
            "session_id": "session-9",
            "message": "explicit scoped message",
            "timestamp": "2026-01-01T10:00:00.007000+00:00",
        },
        {
            "id": "msg_8",
            "user_name": "ada",
            "session_id": "session-1",
            "message": "default scoped message",
            "timestamp": "2026-01-01T10:00:00.008000+00:00",
        },
    ]
    assert knowledge_store.calls == [
        ([7], "grace", ["session-9"], ["project-1"]),
        ([8], "ada", ["session-1"], ["project-1"]),
    ]
