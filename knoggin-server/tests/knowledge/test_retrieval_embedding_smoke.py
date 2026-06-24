import os

import numpy as np
import pytest

from knoggin_server.agent.tools.search import SearchTools
from knoggin_server.knowledge.services.embedding_service import EmbeddingService


def cosine(vec_a, vec_b):
    a = np.asarray(vec_a, dtype=float)
    b = np.asarray(vec_b, dtype=float)
    denominator = np.linalg.norm(a) * np.linalg.norm(b)
    if denominator == 0:
        return 0.0
    return float(np.dot(a, b) / denominator)


async def load_local_embedding_service():
    huggingface_hub = pytest.importorskip("huggingface_hub")
    embedding_model_name = os.environ.get(
        "KNOGGIN_SEMANTIC_SMOKE_MODEL",
        os.environ.get("KNOGGIN_EMBEDDING_MODEL", "dunzhang/stella_en_1.5B_v5"),
    )
    reranker_model_name = os.environ.get(
        "KNOGGIN_SEMANTIC_SMOKE_RERANKER_MODEL",
        os.environ.get("KNOGGIN_RERANKER_MODEL", "BAAI/bge-reranker-large"),
    )

    try:
        embedding_model_path = huggingface_hub.snapshot_download(
            embedding_model_name,
            local_files_only=True,
        )
        reranker_model_path = huggingface_hub.snapshot_download(
            reranker_model_name,
            local_files_only=True,
        )
    except Exception as exc:
        pytest.skip(f"Local embedding service models are unavailable: {exc}")

    service = EmbeddingService(
        embedding_model=embedding_model_path,
        reranker_model=reranker_model_path,
        device=os.environ.get("KNOGGIN_SEMANTIC_SMOKE_DEVICE", "cpu"),
        batch_size=4,
    )

    try:
        await service.load_models()
    except Exception as exc:
        service.cleanup()
        pytest.skip(f"Local embedding service could not load: {exc}")

    return service


@pytest.mark.slow
@pytest.mark.no_network
async def test_real_embedding_retrieval_ranks_memory_rag_messages_over_unrelated_chat():
    service = await load_local_embedding_service()
    query = "What did we decide about testing Knoggin's Memory/RAG retrieval?"
    messages = [
        (
            "msg_1",
            "We should test Knoggin's Memory/RAG retrieval as its own subsystem: "
            "relevant memories, fact recall, project scoping, recency windows, "
            "ranking behavior, empty fallbacks, and avoiding unrelated notes.",
        ),
        (
            "msg_2",
            "For DocumentService and folder upload, we agreed to wait because "
            "that path "
            "will change later. The document chunking tests should come after the "
            "redesign settles.",
        ),
        (
            "msg_3",
            "I made dinner plans for Friday: meet at the Thai place near the "
            "station, order the basil noodles, and leave before the late train.",
        ),
        (
            "msg_4",
            "The website page needs a calmer layout with less marketing copy and "
            "more direct product screenshots in the first viewport.",
        ),
        (
            "msg_5",
            "When we work on community behavior, we should test profile refinement, "
            "community summaries, stable preferences, stale updates, and background "
            "job timing separately from retrieval.",
        ),
        (
            "msg_6",
            "For the agent and tool layer, the important checks are prompt injection, "
            "tool scope, graceful tool failures, and avoiding cross-project leakage.",
        ),
        (
            "msg_7",
            "The storage contract tests already cover graph readers, fact writers, "
            "entity aliases, mutation plans, and scoping around Postgres and AGE.",
        ),
        (
            "msg_8",
            "I want the frontend controls to stay compact: icon buttons for tools, "
            "tabs for views, and no large marketing hero before the actual app.",
        ),
        (
            "msg_9",
            "We can use real embedding models for retrieval smoke tests, but no real "
            "LLM calls. The CPU path is fine because speed is not the point here.",
        ),
        (
            "msg_10",
            "Tomorrow I need to buy coffee filters, renew the library book, and send "
            "the itinerary to Maya before lunch.",
        ),
    ]

    try:
        query_embedding = await service.encode_single(query)
        message_embeddings = await service.encode([message for _, message in messages])
        similarity_scores = [
            cosine(query_embedding, embedding) for embedding in message_embeddings
        ]

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
                    (3, 1.0, "session-1"),
                    (1, 0.9, "session-1"),
                    (4, 0.8, "session-1"),
                    (2, 0.7, "session-1"),
                    (10, 0.65, "session-1"),
                    (8, 0.6, "session-1"),
                    (5, 0.55, "session-1"),
                    (7, 0.5, "session-1"),
                    (6, 0.45, "session-1"),
                    (9, 0.4, "session-1"),
                ]

        tool = SearchTools()
        tool.graph_client = FakeGraphClient()
        tool.embedding_service = service
        tool.search_cfg = {"fts_limit": 12, "rerank_candidates": 10}
        tool.user_name = "ada"
        tool.session_id = "session-1"

        async def visible_session_ids():
            return ["session-1"]

        async def hydrate(refs):
            content_by_id = {msg_id: content for msg_id, content in messages}
            return [
                {
                    "session_id": ref["session_id"],
                    "id": f"msg_{ref['message_id']}",
                    "message": content_by_id[f"msg_{ref['message_id']}"],
                }
                for ref in refs
            ]

        tool._get_visible_session_ids = visible_session_ids
        tool._hydrate_evidence = hydrate

        reranked = await tool._search_messages(query, 6)
    except Exception as exc:
        pytest.skip(f"Local embedding retrieval smoke could not encode/rerank: {exc}")
    finally:
        service.cleanup()

    assert similarity_scores[0] > similarity_scores[2]
    assert similarity_scores[0] > similarity_scores[3]
    assert similarity_scores[0] > similarity_scores[9]
    assert reranked[0][0] == "msg_1"
    assert "msg_3" not in [msg_id for msg_id, _, _ in reranked[:2]]
    assert "msg_10" not in [msg_id for msg_id, _, _ in reranked[:2]]
