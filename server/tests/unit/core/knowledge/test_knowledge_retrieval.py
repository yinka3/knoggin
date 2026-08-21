from types import SimpleNamespace

import pytest

from core.agent.tools.registry import Tools
from core.knowledge.retrieval import KnowledgeRetrieval
from tests.fixtures.fakes import FakeRedis


class _UnavailableRedis:
    def pipeline(self):
        raise RuntimeError("redis unavailable")

    async def zrank(self, *_args):
        raise RuntimeError("redis unavailable")


class _Postgres:
    async def fetch_all(self, _query, _params):
        return [{"session_id": "session-1"}, {"session_id": "session-2"}]


@pytest.mark.no_network
async def test_message_context_uses_durable_fallback_when_redis_is_unavailable():
    class Store:
        async def search_messages_fts(self, *_args, **_kwargs):
            return [(7, 1.0, "session-2")]

        async def get_surrounding_messages(self, message_id, **kwargs):
            assert message_id == 7
            assert kwargs["visible_project_ids"] == ["project-1", "project-2"]
            return [
                {
                    "id": 7,
                    "role": "user",
                    "content": "Durable project memory",
                    "timestamp": 1_700_000_000_000,
                }
            ]

    retrieval = KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1", "project-2"],
        user_name="ada",
        entities=SimpleNamespace(),
        embedding_service=SimpleNamespace(),
        knowledge_store=Store(),
        postgres=_Postgres(),
        redis=_UnavailableRedis(),
        search_config={"fts_limit": 10},
    )

    results = await retrieval.search_messages(
        "project memory", session_id="session-1"
    )

    assert results[0]["session_id"] == "session-2"
    assert results[0]["message"] == "Durable project memory"


@pytest.mark.no_network
async def test_entity_search_hydrates_visible_relationship_evidence():
    class Store:
        async def search_entity(self, query, **kwargs):
            assert query == "Ada"
            assert kwargs["visible_project_ids"] == ["project-1", "project-2"]
            return [{"name": "Ada", "top_connections": [{"evidence_refs": [9]}]}]

        async def get_messages_by_ids(self, message_ids, **kwargs):
            assert message_ids == [9]
            assert kwargs["visible_project_ids"] == ["project-1", "project-2"]
            return [
                {
                    "id": 9,
                    "user_name": "ada",
                    "session_id": "session-1",
                    "content": "Observed relationship",
                    "timestamp": 1_700_000_000_000,
                }
            ]

    retrieval = KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1", "project-2"],
        user_name="ada",
        entities=SimpleNamespace(),
        embedding_service=SimpleNamespace(),
        knowledge_store=Store(),
        postgres=_Postgres(),
        redis=_UnavailableRedis(),
    )

    results = await retrieval.search_entities("Ada", session_id="session-1")

    assert results[0]["top_connections"][0]["evidence"] == [
        {
            "id": "msg_9",
            "user_name": "ada",
            "session_id": "session-1",
            "message": "Observed relationship",
            "timestamp": "2023-11-14T22:13:20+00:00",
        }
    ]


@pytest.mark.no_network
async def test_agent_memory_tools_delegate_to_project_scoped_retrieval():
    class Retrieval:
        def __init__(self):
            self.calls = []

        async def search_messages(self, query, *, session_id, limit):
            self.calls.append((query, session_id, limit))
            return [{"id": "msg_7"}]

    retrieval = Retrieval()
    entities = SimpleNamespace(
        embedding_service=SimpleNamespace(),
        project_id="project-1",
        readable_project_ids=["project-1"],
    )
    tools = Tools(
        user_name="ada",
        entities=entities,
        session_id="session-1",
        knowledge_retrieval=retrieval,
        knowledge_store=SimpleNamespace(),
        postgres=SimpleNamespace(),
        redis=FakeRedis(),
    )
    try:
        assert await tools.search_messages("project memory", limit=3) == [
            {"id": "msg_7"}
        ]
    finally:
        await tools.close()

    assert retrieval.calls == [("project memory", "session-1", 3)]


@pytest.mark.no_network
async def test_episode_reads_receive_the_directional_readable_project_scope():
    class Store:
        def __init__(self):
            self.calls = []

        async def get_recent_project_episodes(self, **kwargs):
            self.calls.append(kwargs)
            return []

    store = Store()
    retrieval = KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1", "project-2"],
        user_name="ada",
        entities=SimpleNamespace(),
        embedding_service=SimpleNamespace(),
        knowledge_store=store,
        postgres=_Postgres(),
        redis=_UnavailableRedis(),
    )

    result = await retrieval.read_recent_episodes(session_id="session-1")

    assert result["resolution"] == "recent"
    assert store.calls == [
        {
            "user_name": "ada",
            "project_id": "project-1",
            "limit": 2,
            "visible_project_ids": ["project-1", "project-2"],
        }
    ]
