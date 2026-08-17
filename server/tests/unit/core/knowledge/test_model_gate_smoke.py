"""Pre-release smoke coverage for the configured local model stack."""

import asyncio
import math
import os

import numpy as np
import pytest

from core.knowledge.services.embedding_service import EmbeddingService
from tests.unit.core.knowledge.test_retrieval_embedding_smoke import (
    load_local_embedding_service,
)


@pytest.mark.slow
@pytest.mark.model
@pytest.mark.no_network
async def test_cached_model_stack_concurrency_chunking_reload_and_classification():
    service = await load_local_embedding_service(include_nli=True)
    try:
        assert isinstance(service, EmbeddingService)
        assert service.embedding_backend in {"onnx", "torch"}
        if service.embedding_backend == "onnx":
            assert service.onnx_provider
            expected_provider = os.environ.get(
                "KNOGGIN_SEMANTIC_SMOKE_EXPECTED_PROVIDER"
            )
            if expected_provider:
                assert service.onnx_provider == expected_provider
        assert service.embedding_dim > 0

        concurrent_batches = [
            ["Knoggin stores durable episodic memory."] * 2,
            ["Knoggin retrieves grounded context."] * 2,
        ]
        concurrent_embeddings = await asyncio.gather(
            *(service.encode(batch) for batch in concurrent_batches)
        )
        assert [len(batch) for batch in concurrent_embeddings] == [2, 2]
        assert all(
            len(vector) == service.embedding_dim
            for batch in concurrent_embeddings
            for vector in batch
        )

        texts = [
            f"A bounded embedding batch item about memory graph state {index}."
            for index in range(service.batch_size * 2 + 1)
        ]
        embeddings = await service.encode(texts)
        assert len(embeddings) == len(texts)
        assert all(len(vector) == service.embedding_dim for vector in embeddings)
        single_embedding = await service.encode_single(texts[0])
        np.testing.assert_allclose(
            single_embedding,
            embeddings[0],
            rtol=1e-5,
            atol=1e-6,
        )

        rerank_scores = await service.rerank(
            "memory graph",
            ["memory graph state", "weather forecast"],
        )
        assert len(rerank_scores) == 2
        assert all(math.isfinite(score) for score in rerank_scores)

        judgments = await service.classify_text_pairs(
            [
                ("The cat sat on the mat.", "An animal sat on a mat."),
                ("The cat sat on the mat.", "The cat was not on the mat."),
            ]
        )
        assert len(judgments) == 2
        assert {judgment.label for judgment in judgments} <= {
            "entailment",
            "contradiction",
            "neutral",
        }
        for judgment in judgments:
            assert judgment.scores
            assert math.isclose(sum(judgment.scores.values()), 1.0, rel_tol=1e-5)

        provider = service.onnx_provider
        service.cleanup()
        assert service._embedder is None
        assert service._reranker is None
        assert service._nli is None

        await service.load_models()
        assert service.embedding_dim == len(await service.encode_single(texts[0]))
        if service.embedding_backend == "onnx":
            assert service.onnx_provider == provider
    finally:
        service.cleanup()
