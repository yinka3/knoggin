from types import SimpleNamespace

import pytest

from common.exceptions import ToolExecutionError
from core.agent.run import AgentIdentity, AgentRun, AgentRunLimits
from core.agent.tools.registry import Tools, install_tool_runtime
from core.knowledge.retrieval import KnowledgeRetrieval
from tests.fixtures.agent_retrieval_scenarios import MESSAGE_RETRIEVAL_SCENARIOS


@pytest.mark.no_network
async def test_message_context_uses_durable_storage():
    class Store:
        async def get_visible_session_ids(self, **kwargs):
            assert kwargs == {
                "user_name": "ada",
                "visible_project_ids": ["project-1", "project-2"],
            }
            return ["session-1", "session-2"]

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
        search_config={"fts_limit": 10},
    )

    results = await retrieval.search_messages(
        "project memory", session_id="session-1"
    )

    assert results[0]["session_id"] == "session-2"
    assert results[0]["message"] == "Durable project memory"


@pytest.mark.no_network
@pytest.mark.parametrize(
    "scenario",
    MESSAGE_RETRIEVAL_SCENARIOS,
    ids=lambda scenario: scenario.name,
)
async def test_message_search_fuses_lexical_and_semantic_episode_sources(scenario):
    class Store:
        async def get_visible_session_ids(self, **_kwargs):
            return ["session-1", "session-2"]

        async def search_messages_fts(self, _query, **_kwargs):
            return list(scenario.lexical_hits)

        async def search_messages_semantic(self, _embedding, **_kwargs):
            return list(scenario.semantic_hits)

        async def get_messages_by_ids(self, message_ids, **kwargs):
            return [
                {
                    "id": message_id,
                    "user_name": "ada",
                    "session_id": kwargs["session_ids"][0],
                    "role": "user",
                    "content": f"evidence-{message_id}",
                    "timestamp": 1_700_000_000_000,
                }
                for message_id in message_ids
            ]

        async def get_surrounding_messages(self, message_id, **kwargs):
            return [
                {
                    "id": message_id,
                    "role": "user",
                    "content": f"evidence-{message_id}",
                    "timestamp": 1_700_000_000_000,
                    "session_id": kwargs["session_id"],
                }
            ]

    class Embeddings:
        async def encode_query(self, _query):
            return [0.1] * 1024

        async def rerank(self, _query, candidates):
            return [float(len(candidates) - index) for index, _ in enumerate(candidates)]

    retrieval = KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1", "project-2"],
        user_name="ada",
        entities=SimpleNamespace(),
        embedding_service=Embeddings(),
        knowledge_store=Store(),
    )

    results = await retrieval.search_messages(
        scenario.query,
        session_id="session-1",
        limit=8,
    )

    assert tuple(int(result["id"].removeprefix("msg_")) for result in results) == (
        scenario.expected_message_ids
    )


@pytest.mark.no_network
async def test_message_search_keeps_all_candidates_when_reranker_is_incomplete():
    class Store:
        async def get_visible_session_ids(self, **_kwargs):
            return ["session-1"]

        async def search_messages_fts(self, _query, **_kwargs):
            return [(1, 1.0, "session-1"), (2, 0.5, "session-1")]

        async def search_messages_semantic(self, _embedding, **_kwargs):
            return []

        async def get_messages_by_ids(self, message_ids, **_kwargs):
            return [
                {
                    "id": message_id,
                    "session_id": "session-1",
                    "content": f"evidence-{message_id}",
                }
                for message_id in message_ids
            ]

    class Embeddings:
        async def encode_query(self, _query):
            return [0.1] * 1024

        async def rerank(self, _query, _candidates):
            return [0.9]

    retrieval = KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1"],
        user_name="ada",
        entities=SimpleNamespace(),
        embedding_service=Embeddings(),
        knowledge_store=Store(),
    )

    results = await retrieval._search_messages("query", session_id="session-1", k=8)

    assert [result[0] for result in results] == ["msg_1", "msg_2"]


@pytest.mark.no_network
async def test_message_search_propagates_lexical_storage_failure():
    failure = RuntimeError("lexical storage unavailable")

    class Store:
        async def get_visible_session_ids(self, **_kwargs):
            return ["session-1"]

        async def search_messages_fts(self, _query, **_kwargs):
            raise failure

        async def search_messages_semantic(self, _embedding, **_kwargs):
            return []

    class Embeddings:
        async def encode_query(self, _query):
            return [0.1] * 1024

    retrieval = KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1"],
        user_name="ada",
        entities=SimpleNamespace(),
        embedding_service=Embeddings(),
        knowledge_store=Store(),
    )

    with pytest.raises(RuntimeError, match="lexical storage unavailable") as caught:
        await retrieval._search_messages("query", session_id="session-1", k=8)

    assert caught.value is failure


@pytest.mark.no_network
async def test_entity_search_returns_stable_identity_and_project_contexts():
    class Store:
        async def search_entity(self, query, **kwargs):
            assert query == "Ada"
            assert kwargs["visible_project_ids"] == ["project-1", "project-2"]
            return [
                {
                    "entity_id": 9,
                    "canonical_name": "Ada Lovelace",
                    "aliases": ["Ada"],
                    "contexts": [
                        {
                            "project_id": "project-1",
                            "entity_type": "person",
                            "topic": "Identity",
                            "last_mentioned_ms": 1_700_000_000_000,
                        }
                    ],
                }
            ]

    retrieval = KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1", "project-2"],
        user_name="ada",
        entities=SimpleNamespace(),
        embedding_service=SimpleNamespace(),
        knowledge_store=Store(),
    )

    results = await retrieval.search_entities("Ada")

    assert results == [
        {
            "entity_id": 9,
            "canonical_name": "Ada Lovelace",
            "aliases": ["Ada"],
            "contexts": [
                {
                    "project_id": "project-1",
                    "entity_type": "person",
                    "topic": "Identity",
                    "last_mentioned_ms": 1_700_000_000_000,
                }
            ],
        }
    ]


@pytest.mark.no_network
@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
async def test_connection_retrieval_rejects_invalid_limits_before_entity_lookup(limit):
    class Entities:
        async def get_profile(self, _entity_id):
            raise AssertionError("invalid limits must fail before storage access")

    retrieval = KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1"],
        user_name="ada",
        entities=Entities(),
        embedding_service=SimpleNamespace(),
        knowledge_store=SimpleNamespace(),
    )

    with pytest.raises(ValueError, match="positive integer"):
        await retrieval.get_connections(9, session_id="session-1", limit=limit)


@pytest.mark.no_network
@pytest.mark.parametrize("hours", [0, -1, True, 1.5])
async def test_activity_retrieval_rejects_invalid_hours_before_entity_lookup(hours):
    class Entities:
        async def get_profile(self, _entity_id):
            raise AssertionError("invalid hours must fail before entity lookup")

    retrieval = KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1"],
        user_name="ada",
        entities=Entities(),
        embedding_service=SimpleNamespace(),
        knowledge_store=SimpleNamespace(),
    )

    with pytest.raises(ValueError, match="positive integer"):
        await retrieval.get_recent_activity(
            9, session_id="session-1", hours=hours
        )


@pytest.mark.no_network
@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
async def test_message_and_entity_search_reject_invalid_limits(limit):
    retrieval = KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1"],
        user_name="ada",
        entities=SimpleNamespace(),
        embedding_service=SimpleNamespace(),
        knowledge_store=SimpleNamespace(),
    )

    with pytest.raises(ValueError, match="positive integer"):
        await retrieval.search_messages(
            "query", session_id="session-1", limit=limit
        )
    with pytest.raises(ValueError, match="positive integer"):
        await retrieval.search_entities("query", limit=limit)


@pytest.mark.no_network
@pytest.mark.parametrize("query", ["", "   ", None])
async def test_message_entity_and_episode_search_reject_blank_queries(query):
    retrieval = KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1"],
        user_name="ada",
        entities=SimpleNamespace(),
        embedding_service=SimpleNamespace(),
        knowledge_store=SimpleNamespace(),
    )

    with pytest.raises(ValueError, match="non-blank string"):
        await retrieval.search_messages(query, session_id="session-1")
    with pytest.raises(ValueError, match="non-blank string"):
        await retrieval.search_entities(query)
    with pytest.raises(ValueError, match="non-blank string"):
        await retrieval.episode_check(query, session_id="session-1")


@pytest.mark.no_network
async def test_hot_topic_context_hydrates_current_project_entity_mentions():
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
    )

    hydrated = await retrieval.get_hot_topic_context(
        ["Identity"], session_id="session-1"
    )

    assert store.calls == [
        (
            ["Identity"],
            {"msg_limit": 5, "project_id": "project-1"},
        ),
    ]
    assert hydrated == {
        "Identity": {
            "entities": [{"name": "Ada"}],
            "messages": [
                {
                    "id": "msg_7",
                        "user_name": "ada",
                        "session_id": "session-1",
                        "role": "assistant",
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
async def test_run_graph_limit_controls_the_relationship_retrieval_query():
    class Retrieval:
        def __init__(self):
            self.calls = []

        async def get_connections(self, entity_id, *, session_id, limit):
            self.calls.append((entity_id, session_id, limit))
            return [{"relationship_id": "r-1"}]

    entities = SimpleNamespace(
        embedding_service=SimpleNamespace(),
        project_id="project-1",
        readable_project_ids=["project-1"],
    )
    retrieval = Retrieval()
    tools = Tools(
        user_name="ada",
        entities=entities,
        session_id="session-1",
        knowledge_retrieval=retrieval,
        knowledge_store=SimpleNamespace(),
        postgres=SimpleNamespace(),
    )
    run = AgentRun.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        user_query="Show connections",
        run_id="run-1",
        agent=AgentIdentity(
            config=SimpleNamespace(id="agent-1"),
            name="STELLA",
            persona="",
        ),
        limits=AgentRunLimits(max_accumulated_graph=2),
    )
    try:
        install_tool_runtime(tools, run.tool_runtime, {})
        assert await tools.get_connections(7) == [{"relationship_id": "r-1"}]
    finally:
        await tools.close()

    assert retrieval.calls == [(7, "session-1", 2)]


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
        async def get_profile(self, entity_id):
            assert entity_id == 2
            return SimpleNamespace(canonical_name="Ada Lovelace")

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
    )

    result = await retrieval.episode_check(
        "What did Ada decide?", session_id="session-1", entity_id=2
    )

    assert result["resolution"] == "exact"
    assert result["results"][0]["entity_id"] == 2
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
async def test_message_discovery_reads_visible_sessions_through_store_boundary():
    class Store:
        def __init__(self):
            self.calls = []

        async def get_visible_session_ids(self, **kwargs):
            self.calls.append(kwargs)
            return ["open-session"]

        async def search_messages_fts(self, _query, **kwargs):
            assert kwargs["session_ids"] == ["open-session"]
            return []

    store = Store()
    retrieval = KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1"],
        user_name="ada",
        entities=SimpleNamespace(),
        embedding_service=SimpleNamespace(),
        knowledge_store=store,
    )

    assert await retrieval.search_messages("plan", session_id="session-1") == []
    assert store.calls == [
        {"user_name": "ada", "visible_project_ids": ["project-1"]}
    ]


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

        async def get_hot_topic_context(self, topics, *, session_id):
            self.calls.append((topics, session_id))
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

    assert retrieval.calls == [(["Work", "Finance"], "session-1")]


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
