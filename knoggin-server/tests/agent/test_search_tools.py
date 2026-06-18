import pytest

from infrastructure.redis_client import RedisKeys
from knoggin_server.agent.tools.search import SearchTools
from tests.fixtures.fakes import FakeRedis


@pytest.mark.no_network
async def test_search_messages_uses_fts_candidates_and_reranks_without_vectors():
    class FakeGraphClient:
        def __init__(self):
            self.fts_calls = []

        async def search_messages_fts(
            self,
            query,
            limit,
            user_name,
            session_ids=None,
            project_ids=None,
        ):
            self.fts_calls.append(
                (query, limit, user_name, session_ids, project_ids)
            )
            return [
                (1, 0.5, "session-1"),
                (2, 1.0, "session-1"),
            ]

    class FakeEmbeddingService:
        async def encode_single(self, query):
            raise AssertionError("message vector search should not run")

        async def rerank(self, query, texts):
            assert texts == ["second message", "first message"]
            return [0.9, 0.1]

    graph_client = FakeGraphClient()
    tool = SearchTools()
    tool.graph_client = graph_client
    tool.embedding_service = FakeEmbeddingService()
    tool.search_cfg = {"fts_limit": 10, "rerank_candidates": 10}
    tool.user_name = "ada"
    tool.session_id = "session-1"

    async def visible_session_ids():
        return ["session-1"]

    async def hydrate(refs):
        return [
            {
                "session_id": ref["session_id"],
                "id": f"msg_{ref['message_id']}",
                "message": (
                    "first message"
                    if ref["message_id"] == 1
                    else "second message"
                ),
            }
            for ref in refs
        ]

    tool._get_visible_session_ids = visible_session_ids
    tool._hydrate_evidence = hydrate

    results = await tool._search_messages("query", 2)

    assert graph_client.fts_calls == [
        ("query", 10, "ada", ["session-1"], None)
    ]
    assert results == [
        ("msg_2", 0.9, "session-1"),
        ("msg_1", 0.1, "session-1"),
    ]


@pytest.mark.no_network
async def test_search_messages_empty_fts_skips_rerank():
    class FakeGraphClient:
        async def search_messages_fts(
            self,
            query,
            limit,
            user_name,
            session_ids=None,
            project_ids=None,
        ):
            return []

    class FakeEmbeddingService:
        async def rerank(self, query, texts):
            raise AssertionError("rerank should not run without FTS candidates")

    tool = SearchTools()
    tool.graph_client = FakeGraphClient()
    tool.embedding_service = FakeEmbeddingService()
    tool.search_cfg = {"fts_limit": 10}
    tool.user_name = "ada"
    tool.session_id = "session-1"
    tool.readable_project_ids = None

    assert await tool._search_messages("query", 5) == []


@pytest.mark.no_network
async def test_search_messages_rerank_failure_falls_back_to_normalized_fts_scores():
    class FakeGraphClient:
        async def search_messages_fts(
            self,
            query,
            limit,
            user_name,
            session_ids=None,
            project_ids=None,
        ):
            return [
                (1, 5.0, "session-1"),
                (2, 10.0, "session-1"),
                (3, 2.5, "session-2"),
            ]

    class FakeEmbeddingService:
        async def rerank(self, query, texts):
            raise RuntimeError("reranker unavailable")

    tool = SearchTools()
    tool.graph_client = FakeGraphClient()
    tool.embedding_service = FakeEmbeddingService()
    tool.search_cfg = {"fts_limit": 10, "rerank_candidates": 10}
    tool.user_name = "ada"
    tool.session_id = "session-1"
    tool.readable_project_ids = None

    async def visible_session_ids():
        return ["session-1", "session-2"]

    async def hydrate(refs):
        return [
            {
                "session_id": ref["session_id"],
                "id": f"msg_{ref['message_id']}",
                "message": f"message {ref['message_id']}",
            }
            for ref in refs
        ]

    tool._get_visible_session_ids = visible_session_ids
    tool._hydrate_evidence = hydrate

    results = await tool._search_messages("query", 2)

    assert results == [
        ("msg_2", 1.0, "session-1"),
        ("msg_1", 0.5, "session-1"),
    ]


@pytest.mark.no_network
async def test_search_messages_uses_readable_project_scope_directly():
    class FakeGraphClient:
        def __init__(self):
            self.calls = []

        async def search_messages_fts(
            self,
            query,
            limit,
            user_name,
            session_ids=None,
            project_ids=None,
        ):
            self.calls.append(
                (query, limit, user_name, session_ids, project_ids)
            )
            return []

    graph_client = FakeGraphClient()
    redis = FakeRedis()
    await redis.sadd(RedisKeys.project_sessions("ada", "project-1"), "session-2")
    await redis.sadd(RedisKeys.project_sessions("ada", "project-1"), "session-3")
    await redis.sadd(RedisKeys.project_sessions("ada", "project-2"), "session-4")

    tool = SearchTools()
    tool.redis = redis
    tool.graph_client = graph_client
    tool.embedding_service = object()
    tool.search_cfg = {"fts_limit": 10}
    tool.user_name = "ada"
    tool.session_id = "session-1"
    tool.readable_project_ids = ["project-1", "project-2"]

    assert await tool._search_messages("query", 5) == []

    _, _, user_name, session_ids, project_ids = graph_client.calls[0]
    assert user_name == "ada"
    assert session_ids is None
    assert project_ids == ["project-1", "project-2"]


@pytest.mark.no_network
async def test_search_messages_public_result_preserves_hit_session_id():
    tool = SearchTools()
    tool.redis = FakeRedis()
    tool.search_cfg = {"default_message_limit": 8}
    tool.user_name = "ada"
    tool.session_id = "session-1"

    async def search(query, limit):
        assert query == "project note"
        assert limit == 8
        return [("msg_7", 0.75, "session-2")]

    async def surrounding_context(msg_id, session_id=None):
        assert msg_id == "msg_7"
        assert session_id == "session-2"
        return [
            {
                "id": "msg_7",
                "role": "user",
                "content": "Relevant project note",
                "timestamp": "2026-01-01T10:00:00+00:00",
                "is_hit": True,
            }
        ]

    tool._search_messages = search
    tool._get_surrounding_context = surrounding_context

    results = await tool.search_messages("project note")

    assert results == [
        {
            "id": "msg_7",
            "user_name": "ada",
            "session_id": "session-2",
            "role": "user",
            "message": "Relevant project note",
            "timestamp": "2026-01-01T10:00:00+00:00",
            "score": 0.75,
            "context": [
                {
                    "id": "msg_7",
                    "role": "user",
                    "content": "Relevant project note",
                    "timestamp": "2026-01-01T10:00:00+00:00",
                    "is_hit": True,
                }
            ],
        }
    ]
