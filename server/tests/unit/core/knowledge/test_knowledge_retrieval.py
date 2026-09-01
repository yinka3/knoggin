from types import SimpleNamespace

import pytest

from common.exceptions import ToolExecutionError
from core.agent.tools.registry import Tools
from core.knowledge.retrieval import KnowledgeRetrieval


class _Postgres:
    async def fetch_all(self, _query, _params):
        return [{"session_id": "session-1"}, {"session_id": "session-2"}]


@pytest.mark.no_network
async def test_message_context_uses_durable_storage():
    class Store:
        async def search_messages_fts(self, *_args, **_kwargs):
            return [(7, 1.0, "session-2")]

        async def get_surrounding_messages(self, message_id, **kwargs):
            assert message_id == 7
            assert kwargs["visible_project_ids"] == ["project-1", "project-2"]
            assert kwargs["discoverable_only"] is True
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
async def test_hot_topic_context_is_compact_or_hydrated_by_retrieval_mode():
    class Store:
        def __init__(self):
            self.calls = []

        async def get_hot_topic_context_with_messages(self, topics, **kwargs):
            self.calls.append((topics, kwargs))
            return {
                "Identity": {
                    "entities": [{"name": "Ada"}],
                    "message_refs": [
                        {
                            "user_name": "ada",
                            "session_id": "session-1",
                            "message_id": 7,
                        }
                    ],
                }
            }

        async def get_messages_by_ids(self, message_ids, **kwargs):
            assert message_ids == [7]
            assert kwargs["visible_project_ids"] == ["project-1", "project-2"]
            return [
                {
                    "id": 7,
                    "user_name": "ada",
                    "session_id": "session-1",
                    "content": "Identity evidence",
                    "timestamp": 1_700_000_000_000,
                }
            ]

    store = Store()
    retrieval = KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1", "project-2"],
        user_name="ada",
        entities=SimpleNamespace(),
        embedding_service=SimpleNamespace(),
        knowledge_store=store,
        postgres=_Postgres(),
    )

    compact = await retrieval.get_hot_topic_context(
        ["Identity"], session_id="session-1", slim=True
    )
    hydrated = await retrieval.get_hot_topic_context(
        ["Identity"], session_id="session-1", slim=False
    )

    assert store.calls == [
        (
            ["Identity"],
            {"msg_limit": 5, "visible_project_ids": ["project-1", "project-2"]},
        ),
        (
            ["Identity"],
            {"msg_limit": 5, "visible_project_ids": ["project-1", "project-2"]},
        ),
    ]
    assert compact == {"Identity": {"entities": [{"name": "Ada"}], "messages": []}}
    assert hydrated == {
        "Identity": {
            "entities": [{"name": "Ada"}],
            "messages": [
                {
                    "id": "msg_7",
                    "user_name": "ada",
                    "session_id": "session-1",
                    "message": "Identity evidence",
                    "timestamp": "2023-11-14T22:13:20+00:00",
                }
            ],
        }
    }


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
    )
    try:
        assert await tools.search_messages("project memory", limit=3) == [
            {"id": "msg_7"}
        ]
    finally:
        await tools.close()

    assert retrieval.calls == [("project memory", "session-1", 3)]


@pytest.mark.no_network
async def test_recent_episode_tool_passes_its_session_to_retrieval():
    class Retrieval:
        def __init__(self):
            self.calls = []

        async def read_recent_episodes(self, *, session_id, limit):
            self.calls.append((session_id, limit))
            return {"resolution": "recent", "results": []}

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
    )
    try:
        assert await tools.read_recent_episodes(limit=3) == {
            "resolution": "recent",
            "results": [],
        }
    finally:
        await tools.close()

    assert retrieval.calls == [("session-1", 3)]


@pytest.mark.no_network
async def test_exact_entity_episode_lookup_uses_the_store_contract_without_session_id():
    class Entities:
        async def get_id(self, name):
            assert name == "Ada"
            return 2

        async def get_profile(self, _entity_id):
            return None

    class Store:
        def __init__(self):
            self.calls = []

        async def get_project_episodes_for_entities(
            self,
            entity_ids,
            *,
            user_name,
            project_id,
            limit,
            visible_project_ids,
        ):
            self.calls.append(
                {
                    "entity_ids": entity_ids,
                    "user_name": user_name,
                    "project_id": project_id,
                    "limit": limit,
                    "visible_project_ids": visible_project_ids,
                }
            )
            return []

    store = Store()
    retrieval = KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1"],
        user_name="ada",
        entities=Entities(),
        embedding_service=SimpleNamespace(),
        knowledge_store=store,
        postgres=_Postgres(),
    )

    result = await retrieval.episode_check(
        "What did Ada decide?", session_id="session-1", entity_name="Ada"
    )

    assert result["resolution"] == "exact"
    assert store.calls == [
        {
            "entity_ids": [2],
            "user_name": "ada",
            "project_id": "project-1",
            "limit": 5,
            "visible_project_ids": ["project-1"],
        }
    ]


@pytest.mark.no_network
async def test_message_discovery_session_scope_only_includes_open_sessions():
    class Postgres:
        def __init__(self):
            self.query = ""

        async def fetch_all(self, query, _params):
            self.query = query
            return [{"session_id": "open-session"}]

    postgres = Postgres()
    retrieval = KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1"],
        user_name="ada",
        entities=SimpleNamespace(),
        embedding_service=SimpleNamespace(),
        knowledge_store=SimpleNamespace(),
        postgres=postgres,
    )

    assert await retrieval._get_visible_session_ids() == ["open-session"]
    assert "status = 'open'" in postgres.query


@pytest.mark.no_network
async def test_topic_context_tool_normalizes_topics_and_rejects_inactive_ones():
    class Domain:
        active_topics = ("Work", "Finance")

        @staticmethod
        def normalize_topic(topic):
            return {"work": "Work", "career": "Work", "finance": "Finance"}.get(
                topic.strip().casefold()
            )

    class Retrieval:
        def __init__(self):
            self.calls = []

        async def get_hot_topic_context(self, topics, *, session_id, slim):
            self.calls.append((topics, session_id, slim))
            return {
                topic: {"entities": [{"name": topic}], "messages": []}
                for topic in topics
            }

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
        compiled_domain=Domain(),
        knowledge_retrieval=retrieval,
        knowledge_store=SimpleNamespace(),
        postgres=SimpleNamespace(),
    )
    try:
        assert await tools.load_topic_context(["career", "Finance", "Work"]) == {
            "Work": {"entities": [{"name": "Work"}], "messages": []},
            "Finance": {"entities": [{"name": "Finance"}], "messages": []},
        }
        with pytest.raises(ToolExecutionError, match="Unknown or inactive"):
            await tools.load_topic_context(["Work", "Unknown"])
    finally:
        await tools.close()

    assert retrieval.calls == [(["Work", "Finance"], "session-1", False)]


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
