"""Shared local-embedding helpers used by retrieval smoke tests."""

import os

import numpy as np
import pytest

from core.knowledge.embedding_service import EmbeddingService


def cosine(vec_a, vec_b):
    a = np.asarray(vec_a, dtype=float)
    b = np.asarray(vec_b, dtype=float)
    denominator = np.linalg.norm(a) * np.linalg.norm(b)
    if denominator == 0:
        return 0.0
    return float(np.dot(a, b) / denominator)


async def load_local_embedding_service():
    """Load configured local embedding and reranker artifacts or skip the smoke test."""
    huggingface_hub = pytest.importorskip("huggingface_hub")
    embedding_model_name = os.environ.get(
        "SEMANTIC_SMOKE_MODEL",
        os.environ.get("EMBEDDING_MODEL", "BAAI/bge-base-en-v1.5"),
    )
    reranker_model_name = os.environ.get(
        "SEMANTIC_SMOKE_RERANKER_MODEL",
        os.environ.get("RERANKER_MODEL", "BAAI/bge-reranker-base"),
    )

    def download_local(model_name: str, revision_env: str) -> str:
        kwargs = {"local_files_only": True}
        revision = os.environ.get(revision_env)
        if revision:
            kwargs["revision"] = revision
        return huggingface_hub.snapshot_download(model_name, **kwargs)

    try:
        embedding_model_path = download_local(
            embedding_model_name,
            "SEMANTIC_SMOKE_MODEL_REVISION",
        )
        reranker_model_path = download_local(
            reranker_model_name,
            "SEMANTIC_SMOKE_RERANKER_REVISION",
        )
    except Exception as exc:
        if os.environ.get("REQUIRE_LOCAL_MODELS") == "1":
            pytest.fail(
                f"Required local embedding smoke artifacts are unavailable: {exc}"
            )
        pytest.skip(f"Local embedding service models are unavailable: {exc}")

    service = EmbeddingService(
        embedding_model=embedding_model_path,
        reranker_model=reranker_model_path,
        device=os.environ.get("SEMANTIC_SMOKE_DEVICE", "cpu"),
        batch_size=4,
    )

    try:
        await service.load_models()
    except Exception:
        service.cleanup()
        raise

    return service
