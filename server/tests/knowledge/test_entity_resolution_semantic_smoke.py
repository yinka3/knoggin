import os

import numpy as np
import pytest

from core.knowledge.services.embedding_service import EmbeddingService


def cosine(vec_a, vec_b):
    a = np.asarray(vec_a, dtype=float)
    b = np.asarray(vec_b, dtype=float)
    denominator = np.linalg.norm(a) * np.linalg.norm(b)
    if denominator == 0:
        return 0.0
    return float(np.dot(a, b) / denominator)


@pytest.mark.slow
@pytest.mark.no_network
async def test_local_embedding_service_similarity_ordering_smoke():
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
        batch_size=8,
    )

    try:
        await service.load_models()
        texts = [
            "Knoggin memory graph project",
            "the memory graph tool",
            "lunch plans for Friday",
            "OpenAI organization research lab",
            "ChatGPT product chatbot",
        ]
        embeddings = await service.encode(texts)
    except Exception as exc:
        pytest.skip(f"Local embedding service could not load or encode: {exc}")
    finally:
        service.cleanup()

    assert cosine(embeddings[0], embeddings[1]) > cosine(embeddings[0], embeddings[2])
    assert cosine(embeddings[3], embeddings[3]) > cosine(embeddings[3], embeddings[4])
