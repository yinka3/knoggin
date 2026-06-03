import pytest

from knoggin_server.agent.tools.search import SearchTools


@pytest.mark.no_network
async def test_search_messages_uses_fts_candidates_and_reranks_without_vectors():
    class FakeGraphClient:
        def __init__(self):
            self.fts_calls = []

        async def search_messages_fts(self, query, limit, user_name, session_ids):
            self.fts_calls.append((query, limit, user_name, session_ids))
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

    assert graph_client.fts_calls == [("query", 10, "ada", ["session-1"])]
    assert results == [
        ("msg_2", 0.9, "session-1"),
        ("msg_1", 0.1, "session-1"),
    ]
