"""Shared local-embedding helpers used by retrieval smoke tests."""

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


async def load_local_embedding_service(*, include_nli: bool = False):
    """Load configured local model artifacts or skip the smoke test.

    The NLI artifact is opt-in because ordinary retrieval only needs the
    embedder and reranker. The pre-release model lane opts in so a missing NLI
    artifact fails that lane instead of silently reducing its coverage.
    """
    huggingface_hub = pytest.importorskip("huggingface_hub")
    embedding_model_name = os.environ.get(
        "KNOGGIN_SEMANTIC_SMOKE_MODEL",
        os.environ.get("KNOGGIN_EMBEDDING_MODEL", "dunzhang/stella_en_1.5B_v5"),
    )
    reranker_model_name = os.environ.get(
        "KNOGGIN_SEMANTIC_SMOKE_RERANKER_MODEL",
        os.environ.get("KNOGGIN_RERANKER_MODEL", "BAAI/bge-reranker-large"),
    )
    nli_model_name = os.environ.get(
        "KNOGGIN_SEMANTIC_SMOKE_NLI_MODEL",
        os.environ.get("KNOGGIN_NLI_MODEL", "cross-encoder/nli-MiniLM2-L6-H768"),
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
            "KNOGGIN_SEMANTIC_SMOKE_MODEL_REVISION",
        )
        reranker_model_path = download_local(
            reranker_model_name,
            "KNOGGIN_SEMANTIC_SMOKE_RERANKER_REVISION",
        )
        nli_model_path = (
            download_local(
                nli_model_name,
                "KNOGGIN_SEMANTIC_SMOKE_NLI_REVISION",
            )
            if include_nli
            else None
        )
    except Exception as exc:
        if os.environ.get("KNOGGIN_REQUIRE_LOCAL_MODELS") == "1":
            pytest.fail(
                f"Required local embedding smoke artifacts are unavailable: {exc}"
            )
        pytest.skip(f"Local embedding service models are unavailable: {exc}")

    service = EmbeddingService(
        embedding_model=embedding_model_path,
        reranker_model=reranker_model_path,
        nli_model=nli_model_path or nli_model_name,
        device=os.environ.get("KNOGGIN_SEMANTIC_SMOKE_DEVICE", "cpu"),
        batch_size=4,
    )

    try:
        await service.load_models()
    except Exception:
        service.cleanup()
        raise

    return service
