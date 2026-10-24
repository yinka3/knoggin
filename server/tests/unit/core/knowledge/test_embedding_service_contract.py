import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from huggingface_hub import snapshot_download
from transformers import AutoTokenizer

import core.knowledge.embedding_service as embedding_module
from core.knowledge.embedding_service import EmbeddingService


class FakeCrossEncoder:
    init_calls = []

    def __init__(self, model_name, *, device=None, model_kwargs=None, backend="torch"):
        self.init_calls.append(
            {
                "model_name": model_name,
                "device": device,
                "model_kwargs": model_kwargs,
                "backend": backend,
            }
        )

@pytest.fixture
def inline_fake_model_work(monkeypatch):
    """Keep fake model contract tests independent of the thread executor."""

    async def run_inline(self, operation, **_kwargs):
        return operation()

    monkeypatch.setattr(EmbeddingService, "_run_blocking", run_inline)


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
    monkeypatch.setenv("ONNX_PROVIDER", "CoreMLExecutionProvider")

    with pytest.raises(ValueError, match="is unavailable"):
        EmbeddingService(embedding_backend="onnx")


@pytest.mark.no_network
async def test_embedding_service_loads_embedder_before_lazy_onnx_reranker(
    monkeypatch,
    inline_fake_model_work,
):
    sentence_transformer_calls = []

    class FakeSentenceTransformer:
        def __init__(self, model_name, **kwargs):
            sentence_transformer_calls.append({"model_name": model_name, **kwargs})

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


@pytest.mark.no_network
async def test_embedding_service_runs_pooled_onnx_sentence_export_directly(
    monkeypatch, tmp_path
):
    model_path = tmp_path / "embedding-model"
    onnx_path = model_path / "onnx" / "model.onnx"
    onnx_path.parent.mkdir(parents=True)
    onnx_path.write_bytes(b"fake")

    class FakeTokenizer:
        @classmethod
        def from_pretrained(cls, model_name, **kwargs):
            assert model_name == str(model_path)
            assert kwargs == {"trust_remote_code": True}
            return cls()

        def __call__(self, texts, **kwargs):
            assert texts == ["first", "second"]
            assert kwargs == {
                "padding": True,
                "truncation": True,
                "return_tensors": "np",
            }
            return {
                "input_ids": np.asarray([[1, 2], [3, 4]], dtype=np.int64),
                "attention_mask": np.ones((2, 2), dtype=np.int64),
            }

    class FakeSession:
        def __init__(self, model_name, *, providers):
            assert model_name == str(onnx_path)
            assert providers == ["CPUExecutionProvider"]

        def get_inputs(self):
            return [
                type("Input", (), {"name": "input_ids"})(),
                type("Input", (), {"name": "attention_mask"})(),
            ]

        def get_outputs(self):
            return [
                type(
                    "Output",
                    (),
                    {"name": "sentence_embedding", "shape": ["batch", 3]},
                )()
            ]

        def run(self, output_names, inputs):
            assert output_names == ["sentence_embedding"]
            assert set(inputs) == {"input_ids", "attention_mask"}
            return [
                np.asarray(
                    [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
                    dtype=np.float32,
                )
            ]

    monkeypatch.setattr(embedding_module, "AutoTokenizer", FakeTokenizer)
    monkeypatch.setattr(embedding_module.ort, "InferenceSession", FakeSession)
    service = EmbeddingService(
        embedding_model=str(model_path),
        embedding_backend="onnx",
        device="cpu",
    )

    await service.load_models()

    assert service.embedding_dim == 3
    np.testing.assert_allclose(
        await service.encode(["first", "second"]),
        [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
    )


@pytest.mark.no_network
async def test_torch_embedding_rejects_overlong_inputs_before_encoding(
    inline_fake_model_work,
):
    class FakeTokenizer:
        def num_special_tokens_to_add(self, *, pair):
            assert pair is False
            return 1

        def encode(self, value, *, add_special_tokens, truncation):
            assert add_special_tokens is True
            assert truncation is False
            return list(range(len(value.split()) + 1))

    class FakeEmbedder:
        tokenizer = FakeTokenizer()
        max_seq_length = 4

        def __init__(self):
            self.calls = []

        def encode(self, texts):
            self.calls.append(texts)
            return np.asarray([[0.1, 0.2, 0.3] for _ in texts], dtype=np.float32)

    service = EmbeddingService(embedding_backend="torch")
    embedder = FakeEmbedder()
    service._embedder = embedder

    with pytest.raises(ValueError, match=r"Embedding input 1.*5 > 4 tokens"):
        await service.encode(["one two three four"])
    with pytest.raises(ValueError, match=r"Embedding input 2.*5 > 4 tokens"):
        await service.encode(["short", "one two three four"])
    with pytest.raises(ValueError, match=r"Embedding input 1.*5 > 4 tokens"):
        await service.encode_single("one two three four")
    assert embedder.calls == []
    vectors = await service.encode(["short"])
    np.testing.assert_allclose(vectors, [[0.1, 0.2, 0.3]])
    assert embedder.calls == [["short"]]


@pytest.mark.no_network
def test_episode_split_keeps_every_character_and_each_chunk_within_token_limit():
    class FakeTokenizer:
        def encode(self, value, *, add_special_tokens, truncation):
            assert add_special_tokens is True
            assert truncation is False
            return [0, *value.encode("utf-8"), 1]

    class FakeEmbedder:
        tokenizer = FakeTokenizer()
        max_seq_length = 12

    service = EmbeddingService(embedding_backend="torch")
    service._embedder = FakeEmbedder()
    text = "A short phrase. " * 10 + "🌍" * 10

    chunks = service.split_for_embedding(text)

    assert len(chunks) > 1
    assert "".join(chunks) == text
    assert all(
        len(service._embedder.tokenizer.encode(
            chunk, add_special_tokens=True, truncation=False
        )) <= 12
        for chunk in chunks
    )


@pytest.mark.no_network
def test_embedding_split_requires_a_usable_tokenizer_limit():
    service = EmbeddingService(embedding_backend="torch")
    assert not service.has_token_limit
    with pytest.raises(RuntimeError, match="not loaded"):
        service.split_for_embedding("text")

    service._embedder = SimpleNamespace(tokenizer=None, max_seq_length=4)
    assert not service.has_token_limit
    with pytest.raises(RuntimeError, match="tokenizer limit"):
        service.split_for_embedding("text")


@pytest.mark.no_network
def test_embedding_split_rejects_a_character_larger_than_the_model_limit():
    class FakeTokenizer:
        def encode(self, value, *, add_special_tokens, truncation):
            return [0] * (len(value) + 2)

    service = EmbeddingService(embedding_backend="torch")
    service._embedder = SimpleNamespace(tokenizer=FakeTokenizer(), max_seq_length=2)
    assert service.has_token_limit
    assert service.split_for_embedding("") == [""]
    with pytest.raises(ValueError, match="One character exceeds"):
        service.split_for_embedding("x")


@pytest.mark.no_network
async def test_embedding_short_ascii_input_uses_safe_token_bound(
    inline_fake_model_work,
):
    class FakeTokenizer:
        def __init__(self):
            self.calls = []

        def num_special_tokens_to_add(self, *, pair):
            return 2

        def encode(self, value, *, add_special_tokens, truncation):
            self.calls.append(value)
            return [0] * (len(value) + 2)

    tokenizer = FakeTokenizer()
    service = EmbeddingService(embedding_backend="torch")
    service._embedder = SimpleNamespace(
        tokenizer=tokenizer,
        max_seq_length=6,
        encode=lambda values: np.asarray([[1.0] for _ in values]),
    )

    await service.encode(["abc", "abcd"])

    assert tokenizer.calls == []


@pytest.mark.no_network
def test_cached_stella_tokenizer_splits_policy_valid_episode_without_truncation(
    monkeypatch,
):
    monkeypatch.setenv("HF_HUB_OFFLINE", "1")
    monkeypatch.setenv("TRANSFORMERS_OFFLINE", "1")
    try:
        path = Path(snapshot_download(
            "dunzhang/stella_en_1.5B_v5", local_files_only=True
        ))
        tokenizer = AutoTokenizer.from_pretrained(
            str(path), local_files_only=True, trust_remote_code=True
        )
    except (OSError, ValueError):
        pytest.skip("Stella tokenizer is not fully cached")
    config = json.loads(
        (path / "sentence_bert_config.json").read_text(encoding="utf-8")
    )
    limit = config["max_seq_length"]
    service = EmbeddingService(embedding_backend="torch")
    service._embedder = SimpleNamespace(tokenizer=tokenizer, max_seq_length=limit)
    assert service.has_token_limit
    text = "Summary:\n" + "Decision 42. " * 250
    assert len(text) < 4000
    assert len(tokenizer.encode(text, add_special_tokens=True, truncation=False)) > limit

    chunks = service.split_for_embedding(text)

    assert len(chunks) > 1
    assert "".join(chunks) == text
    assert all(
        len(tokenizer.encode(chunk, add_special_tokens=True, truncation=False))
        <= limit
        for chunk in chunks
    )


@pytest.mark.no_network
async def test_direct_onnx_uses_sentence_transformer_limit_before_encoding(
    monkeypatch, tmp_path
):
    model_path = tmp_path / "embedding-model"
    onnx_path = model_path / "onnx" / "model.onnx"
    onnx_path.parent.mkdir(parents=True)
    onnx_path.write_bytes(b"fake")
    (model_path / "sentence_bert_config.json").write_text(
        '{"max_seq_length": 4}', encoding="utf-8"
    )

    class FakeTokenizer:
        model_max_length = 32_768

        @classmethod
        def from_pretrained(cls, *_args, **_kwargs):
            return cls()

        def num_special_tokens_to_add(self, *, pair):
            assert pair is False
            return 1

        def encode(self, value, *, add_special_tokens, truncation):
            assert add_special_tokens is True
            assert truncation is False
            return list(range(len(value.split()) + 1))

        def __call__(self, *_args, **_kwargs):
            raise AssertionError("Overlong text reached the ONNX tokenizer")

    class FakeSession:
        def __init__(self, *_args, **_kwargs):
            pass

        def get_inputs(self):
            return []

        def get_outputs(self):
            return [
                type("Output", (), {"name": "sentence_embedding", "shape": ["batch", 3]})()
            ]

        def run(self, *_args, **_kwargs):
            raise AssertionError("Overlong text reached the ONNX model")

    monkeypatch.setattr(embedding_module, "AutoTokenizer", FakeTokenizer)
    monkeypatch.setattr(embedding_module.ort, "InferenceSession", FakeSession)
    service = EmbeddingService(
        embedding_model=str(model_path),
        embedding_backend="onnx",
        device="cpu",
    )
    await service.load_models()

    assert service._embedder.max_seq_length == 4
    with pytest.raises(ValueError, match=r"Embedding input 1.*5 > 4 tokens"):
        await service.encode(["one two three four"])


@pytest.mark.no_network
async def test_stella_retrieval_query_uses_document_search_instruction(monkeypatch):
    service = EmbeddingService(
        embedding_model="dunzhang/stella_en_1.5B_v5",
        embedding_backend="torch",
        device="cpu",
    )
    calls = []

    async def encode_single(text):
        calls.append(text)
        return [0.1] * 1024

    monkeypatch.setattr(service, "encode_single", encode_single)

    await service.encode_query("What changed in the project?")

    assert calls == [
        "Instruct: Given a web search query, retrieve relevant passages that "
        "answer the query.\nQuery: What changed in the project?"
    ]


@pytest.mark.no_network
async def test_bge_retrieval_query_uses_document_search_instruction(monkeypatch):
    service = EmbeddingService(
        embedding_model="BAAI/bge-base-en-v1.5",
        embedding_backend="onnx",
        device="cpu",
    )
    calls = []

    async def encode_single(text):
        calls.append(text)
        return [0.1] * 768

    monkeypatch.setattr(service, "encode_single", encode_single)

    await service.encode_query("How do random forests reduce variance?")

    assert calls == [
        "Represent this sentence for searching relevant passages: "
        "How do random forests reduce variance?"
    ]


@pytest.mark.no_network
async def test_unrecognized_retrieval_model_remains_unprompted(monkeypatch):
    service = EmbeddingService(
        embedding_model="local/custom-retriever",
        embedding_backend="torch",
        device="cpu",
    )
    calls = []

    async def encode_single(text):
        calls.append(text)
        return [0.1] * 1024

    monkeypatch.setattr(service, "encode_single", encode_single)

    await service.encode_query("project decision")

    assert calls == ["project decision"]
