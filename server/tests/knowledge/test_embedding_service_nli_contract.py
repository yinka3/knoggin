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

    def __init__(self, model_name, *, device=None, model_kwargs=None):
        self.init_calls.append(
            {
                "model_name": model_name,
                "device": device,
                "model_kwargs": model_kwargs,
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
            "model_kwargs": {"torch_dtype": embedding_module.torch.float16},
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
    service = EmbeddingService(nli_model="custom/nli", device="cpu")

    await service.classify_text_pairs([("A", "B")])
    await service.classify_text_pairs([("C", "D")])

    assert len(FakeCrossEncoder.init_calls) == 1
    assert FakeCrossEncoder.predict_calls == [[("A", "B")], [("C", "D")]]
