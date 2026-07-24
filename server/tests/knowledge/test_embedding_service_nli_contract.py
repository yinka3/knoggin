import numpy as np
import pytest

import core.knowledge.services.embedding_service as embedding_module
from core.knowledge.services.embedding_service import (
    EmbeddingService,
    TextPairClassification,
)


class FakeConfig:
    id2label = {0: "entailment", 1: "neutral", 2: "contradiction"}


class FakeModel:
    config = FakeConfig()


class FakeCrossEncoder:
    init_calls = []
    predict_calls = []
    model = FakeModel()

    def __init__(self, model_name, *, device=None, model_kwargs=None, backend="torch"):
        self.init_calls.append(
            {
                "model_name": model_name,
                "device": device,
                "model_kwargs": model_kwargs,
                "backend": backend,
            }
        )

    def predict(self, pairs):
        self.predict_calls.append(list(pairs))
        return np.asarray(
            [
                [0.1, 0.2, 3.0],
                [2.5, 0.4, 0.1],
            ],
            dtype=float,
        )


@pytest.mark.no_network
@pytest.mark.parametrize(
    ("platform", "available", "expected"),
    [
        (
            "darwin",
            ["CoreMLExecutionProvider", "CPUExecutionProvider"],
            "CoreMLExecutionProvider",
        ),
        (
            "win32",
            ["DmlExecutionProvider", "CPUExecutionProvider"],
            "DmlExecutionProvider",
        ),
        (
            "linux",
            ["CUDAExecutionProvider", "CPUExecutionProvider"],
            "CUDAExecutionProvider",
        ),
        ("linux", ["CPUExecutionProvider"], "CPUExecutionProvider"),
    ],
)
def test_embedding_service_auto_selects_available_onnx_provider(
    monkeypatch, platform, available, expected
):
    monkeypatch.setattr(embedding_module.sys, "platform", platform)
    monkeypatch.setattr(
        embedding_module.ort,
        "get_available_providers",
        lambda: available,
    )

    service = EmbeddingService(embedding_backend="onnx")

    assert service.onnx_provider == expected


@pytest.mark.no_network
def test_embedding_service_rejects_unavailable_explicit_onnx_provider(monkeypatch):
    monkeypatch.setattr(
        embedding_module.ort,
        "get_available_providers",
        lambda: ["CPUExecutionProvider"],
    )
    monkeypatch.setenv("KNOGGIN_ONNX_PROVIDER", "CoreMLExecutionProvider")

    with pytest.raises(ValueError, match="is unavailable"):
        EmbeddingService(embedding_backend="onnx")


@pytest.mark.no_network
async def test_embedding_service_classifies_text_pairs_with_lazy_cross_encoder(
    monkeypatch,
):
    FakeCrossEncoder.init_calls = []
    FakeCrossEncoder.predict_calls = []
    monkeypatch.setattr(embedding_module, "CrossEncoder", FakeCrossEncoder)
    service = EmbeddingService(
        nli_model="custom/nli",
        device="cpu",
        batch_size=4,
        embedding_backend="onnx",
    )

    assert FakeCrossEncoder.init_calls == []

    judgments = await service.classify_text_pairs(
        [
            ("Alice uses Notion.", "Alice does not use Notion."),
            ("Alice uses Notion.", "Alice uses Notion."),
        ]
    )

    assert FakeCrossEncoder.init_calls == [
        {
            "model_name": "custom/nli",
            "device": "cpu",
            "model_kwargs": {"provider": "CPUExecutionProvider"},
            "backend": "onnx",
        }
    ]
    assert FakeCrossEncoder.predict_calls == [
        [
            ("Alice uses Notion.", "Alice does not use Notion."),
            ("Alice uses Notion.", "Alice uses Notion."),
        ]
    ]
    assert [judgment.label for judgment in judgments] == [
        "contradiction",
        "entailment",
    ]
    assert all(isinstance(judgment, TextPairClassification) for judgment in judgments)
    assert judgments[0].scores["contradiction"] > judgments[0].scores["neutral"]
    assert judgments[1].scores["entailment"] > judgments[1].scores["neutral"]


@pytest.mark.no_network
async def test_embedding_service_reuses_loaded_nli_model(monkeypatch):
    FakeCrossEncoder.init_calls = []
    FakeCrossEncoder.predict_calls = []
    monkeypatch.setattr(embedding_module, "CrossEncoder", FakeCrossEncoder)
    service = EmbeddingService(
        nli_model="custom/nli",
        device="cpu",
        embedding_backend="onnx",
    )

    await service.classify_text_pairs([("A", "B")])
    await service.classify_text_pairs([("C", "D")])

    assert len(FakeCrossEncoder.init_calls) == 1
    assert FakeCrossEncoder.predict_calls == [[("A", "B")], [("C", "D")]]


@pytest.mark.no_network
async def test_embedding_service_loads_embedder_before_lazy_onnx_reranker(
    monkeypatch,
):
    sentence_transformer_calls = []

    class FakeSentenceTransformer:
        def __init__(self, model_name, **kwargs):
            sentence_transformer_calls.append(
                {"model_name": model_name, **kwargs}
            )

        def get_sentence_embedding_dimension(self):
            return 1024

    FakeCrossEncoder.init_calls = []
    monkeypatch.setattr(
        embedding_module,
        "SentenceTransformer",
        FakeSentenceTransformer,
    )
    monkeypatch.setattr(embedding_module, "CrossEncoder", FakeCrossEncoder)

    service = EmbeddingService(
        embedding_model="custom/embedder",
        reranker_model="custom/reranker",
        embedding_backend="onnx",
        device="cpu",
    )
    await service.load_models()

    assert service.embedding_backend == "onnx"
    assert sentence_transformer_calls == [
        {
            "model_name": "custom/embedder",
            "trust_remote_code": True,
            "device": "cpu",
            "model_kwargs": {"provider": "CPUExecutionProvider"},
            "config_kwargs": {
                "use_memory_efficient_attention": False,
                "unpad_inputs": False,
            },
            "backend": "onnx",
        }
    ]
    assert FakeCrossEncoder.init_calls == []

    await service.load_reranker()

    assert FakeCrossEncoder.init_calls == [
        {
            "model_name": "custom/reranker",
            "device": "cpu",
            "model_kwargs": {"provider": "CPUExecutionProvider"},
            "backend": "onnx",
        }
    ]
