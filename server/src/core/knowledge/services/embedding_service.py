import asyncio
import gc
import os
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import onnxruntime as ort
import torch
from huggingface_hub import snapshot_download
from loguru import logger
from sentence_transformers import CrossEncoder, SentenceTransformer
from transformers import AutoTokenizer

from infrastructure.model_work import ModelWorkCoordinator, ModelWorkPriority
from infrastructure.work_record import WorkRecord


class _DirectOnnxSentenceEmbedder:
    """Run ONNX exports that already return pooled sentence embeddings.

    Stella's ONNX export exposes a ``sentence_embedding`` output rather than
    the token-level ``last_hidden_state`` expected by SentenceTransformers'
    generic ONNX pipeline.  Keeping this small adapter behind the existing
    embedder interface lets those exports use the same service contract.
    """

    def __init__(
        self,
        model_path: Path,
        provider: str,
        *,
        session: ort.InferenceSession | None = None,
    ) -> None:
        self._model_path = model_path
        self._tokenizer = AutoTokenizer.from_pretrained(
            str(model_path),
            trust_remote_code=True,
        )
        self._session = session or ort.InferenceSession(
            str(model_path / "onnx" / "model.onnx"),
            providers=[provider],
        )
        outputs = self._session.get_outputs()
        self._output_names = [output.name for output in outputs]
        try:
            self._embedding_output_index = self._output_names.index(
                "sentence_embedding"
            )
        except ValueError as exc:
            raise ValueError(
                "Direct ONNX sentence embedder requires a "
                "'sentence_embedding' output"
            ) from exc

        output_shape = outputs[self._embedding_output_index].shape
        dimension = output_shape[-1] if output_shape else None
        if not isinstance(dimension, int) or dimension <= 0:
            raise ValueError(
                "Direct ONNX sentence embedder must expose a fixed embedding "
                f"dimension, got {output_shape!r}"
            )
        self._embedding_dimension = dimension

    def get_sentence_embedding_dimension(self) -> int:
        return self._embedding_dimension

    def encode(self, texts: List[str]) -> np.ndarray:
        features = self._tokenizer(
            texts,
            padding=True,
            truncation=True,
            return_tensors="np",
        )
        model_inputs = {}
        missing_inputs = []
        for model_input in self._session.get_inputs():
            if model_input.name not in features:
                missing_inputs.append(model_input.name)
                continue
            model_inputs[model_input.name] = np.asarray(features[model_input.name])
        if missing_inputs:
            raise ValueError(
                "Tokenizer did not provide ONNX inputs: "
                f"{', '.join(missing_inputs)}"
            )

        outputs = self._session.run(self._output_names, model_inputs)
        embeddings = np.asarray(
            outputs[self._embedding_output_index],
            dtype=np.float32,
        )
        if embeddings.ndim != 2 or embeddings.shape[0] != len(texts):
            raise RuntimeError(
                "ONNX sentence embedding output must have shape "
                f"(batch, dimension), got {embeddings.shape!r}"
            )
        return embeddings

    def close(self) -> None:
        self._session = None
        self._tokenizer = None


@dataclass(frozen=True)
class TextPairClassification:
    """Natural language inference result for a pair of texts."""

    premise: str
    hypothesis: str
    label: str
    scores: Dict[str, float] = field(default_factory=dict)


class EmbeddingService:
    """Embedding infrastructure for the knowledge graph."""

    BATCH_SIZE = 64
    supports_model_work_records = True
    _ONNX_PROVIDER_ALIASES = {
        "cpu": "CPUExecutionProvider",
        "coreml": "CoreMLExecutionProvider",
        "cuda": "CUDAExecutionProvider",
        "directml": "DmlExecutionProvider",
        "openvino": "OpenVINOExecutionProvider",
    }

    def __init__(
        self,
        embedding_model: str = "dunzhang/stella_en_1.5B_v5",
        reranker_model: str = "BAAI/bge-reranker-large",
        nli_model: str = (
            "MoritzLaurer/DeBERTa-v3-large-mnli-fever-anli-ling-wanli"
        ),
        device: str = None,
        batch_size: int = 32,
        model_work: ModelWorkCoordinator | None = None,
        embedding_backend: str | None = None,
    ):
        self.device = device or "cpu"
        self.batch_size = batch_size
        self._lock = threading.Lock()

        self._backend = (
            embedding_backend
            or os.getenv("KNOGGIN_EMBEDDING_BACKEND", "onnx")
        ).strip().lower()
        if self._backend not in {"torch", "onnx"}:
            raise ValueError(
                "KNOGGIN_EMBEDDING_BACKEND must be either 'torch' or 'onnx'"
            )

        self._embedder = None
        self._reranker = None
        self._nli = None
        self._embedding_dim = None
        self._config_kwargs = {}
        if str(self.device) == "cpu":
            self._config_kwargs["use_memory_efficient_attention"] = False
            self._config_kwargs["unpad_inputs"] = False
        self._torch_model_kwargs = {"torch_dtype": torch.float32}
        self._onnx_provider = (
            self._resolve_onnx_provider(
                os.getenv("KNOGGIN_ONNX_PROVIDER", "auto")
            )
            if self._backend == "onnx"
            else None
        )
        self._embedding_model_kwargs = (
            {"provider": self._onnx_provider}
            if self._backend == "onnx"
            else self._torch_model_kwargs
        )
        self._cross_encoder_model_kwargs = self._embedding_model_kwargs
        self._embedding_model = embedding_model
        self._reranker_model = reranker_model
        self._nli_model = nli_model
        self._model_work = model_work

        logger.info(
            "EmbeddingService initialized | "
            f"device={self.device} | batch_size={batch_size} | "
            f"backend={self._backend} | provider={self._onnx_provider or 'n/a'}"
        )

    @classmethod
    def _resolve_onnx_provider(cls, requested_provider: str) -> str:
        """Choose an installed execution provider, honoring explicit choices."""
        requested = requested_provider.strip()
        if not requested:
            raise ValueError("KNOGGIN_ONNX_PROVIDER must not be empty")
        available = set(ort.get_available_providers())
        if requested.lower() == "auto":
            if sys.platform == "darwin":
                preferred = ("CoreMLExecutionProvider", "CPUExecutionProvider")
            elif sys.platform == "win32":
                preferred = (
                    "CUDAExecutionProvider",
                    "DmlExecutionProvider",
                    "OpenVINOExecutionProvider",
                    "CPUExecutionProvider",
                )
            else:
                preferred = (
                    "CUDAExecutionProvider",
                    "OpenVINOExecutionProvider",
                    "CPUExecutionProvider",
                )
            provider = next(
                (candidate for candidate in preferred if candidate in available),
                None,
            )
            if provider is None:
                raise RuntimeError(
                    "No supported ONNX Runtime execution provider is installed; "
                    f"available providers: {sorted(available)}"
                )
            logger.info(
                "Selected ONNX Runtime provider "
                f"{provider} automatically from {sorted(available)}"
            )
            return provider

        provider = cls._ONNX_PROVIDER_ALIASES.get(
            requested.lower(), requested
        )
        if provider not in available:
            raise ValueError(
                f"KNOGGIN_ONNX_PROVIDER={provider!r} is unavailable; "
                f"available providers: {sorted(available)}"
            )
        return provider

    def set_model_work_coordinator(
        self, model_work: ModelWorkCoordinator
    ) -> None:
        self._model_work = model_work

    async def _run_blocking(
        self,
        operation,
        *,
        name: str,
        priority: ModelWorkPriority,
        work_kind: str,
        parent_work_record: WorkRecord | None = None,
    ):
        if parent_work_record is not None and not isinstance(
            parent_work_record, WorkRecord
        ):
            raise TypeError("parent_work_record must be a WorkRecord")
        if self._model_work is not None:
            work_record = None
            if parent_work_record is not None:
                work_record = WorkRecord.for_model_operation(
                    work_kind,
                    parent_work_record.scope,
                    parent_id=parent_work_record.id,
                    priority=parent_work_record.priority,
                )
            return await self._model_work.run_blocking(
                operation,
                priority=priority,
                name=name,
                work_record=work_record,
                parent_work_record=parent_work_record,
            )
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, operation)

    async def load_models(self):
        """Load the always-needed sentence embedding model."""
        if self._embedder:
            return

        # ONNX Runtime sessions for pooled exports must be created and used on
        # the event-loop thread.  In this environment, handing that session to
        # the generic executor can strand the asyncio future after inference.
        if self._backend == "onnx":
            direct_embedder = self._load_direct_onnx_embedder()
            if direct_embedder is not None:
                self._embedder = direct_embedder
                self._embedding_dim = (
                    direct_embedder.get_sentence_embedding_dimension()
                )
                logger.info(
                    "Loaded direct pooled ONNX embedding model from "
                    f"{direct_embedder._model_path} | "
                    f"dims={self._embedding_dim}"
                )
                return

        await self._run_blocking(
            self._load_embedder_sync,
            name="embedding-model-load",
            priority=ModelWorkPriority.BACKGROUND,
            work_kind="model_load",
        )

        if self._embedder:
            self._embedding_dim = self._embedder.get_sentence_embedding_dimension()
            logger.info(
                f"Loaded embedding model on {self.device} | "
                f"dims={self._embedding_dim}"
            )
        else:
            logger.error("Failed to load embedder model")

    def _load_embedder_sync(self) -> None:
        with self._lock:
            if self._embedder is None:
                self._embedder = SentenceTransformer(
                    self._embedding_model,
                    trust_remote_code=True,
                    device=self.device,
                    model_kwargs=self._embedding_model_kwargs,
                    config_kwargs=self._config_kwargs,
                    backend=self._backend,
                )

    def _load_direct_onnx_embedder(self) -> _DirectOnnxSentenceEmbedder | None:
        """Use a cached pooled ONNX export when its output is self-contained."""
        if self._backend != "onnx":
            return None

        model_path = Path(self._embedding_model)
        if not model_path.is_dir():
            try:
                model_path = Path(
                    snapshot_download(
                        self._embedding_model,
                        local_files_only=True,
                    )
                )
            except Exception:
                # SentenceTransformer remains responsible for downloading and
                # loading models that are not already in the local cache.
                return None

        onnx_path = model_path / "onnx" / "model.onnx"
        if not onnx_path.is_file():
            return None

        session = ort.InferenceSession(
            str(onnx_path),
            providers=[self._onnx_provider],
        )
        output_names = {output.name for output in session.get_outputs()}
        if "sentence_embedding" not in output_names:
            return None
        return _DirectOnnxSentenceEmbedder(
            model_path,
            self._onnx_provider,
            session=session,
        )

    async def load_reranker(
        self,
        *,
        priority: ModelWorkPriority = ModelWorkPriority.FOREGROUND,
    ) -> None:
        """Load the optional cross-encoder only when reranking is needed."""
        if self._reranker is not None:
            return
        await self._run_blocking(
            self._load_reranker_sync,
            name="reranker-model-load",
            priority=priority,
            work_kind="model_load",
        )

    def _load_reranker_sync(self) -> None:
        with self._lock:
            if self._reranker is None:
                self._reranker = CrossEncoder(
                    self._reranker_model,
                    device=self.device,
                    model_kwargs=self._cross_encoder_model_kwargs,
                    backend=self._backend,
                )
                logger.info(f"Loaded reranker model on {self.device}")

    async def load_nli_model(
        self,
        *,
        priority: ModelWorkPriority = ModelWorkPriority.FOREGROUND,
    ) -> None:
        """Load NLI only for workflows that need evidence classification."""
        if self._nli is not None:
            return
        await self._run_blocking(
            self._load_nli_model_sync,
            name="nli-model-load",
            priority=priority,
            work_kind="nli",
        )

    def _load_nli_model_sync(self) -> None:
        with self._lock:
            if self._nli is None:
                self._nli = CrossEncoder(
                    self._nli_model,
                    device=self.device,
                    model_kwargs=self._cross_encoder_model_kwargs,
                    backend=self._backend,
                )
                logger.info(f"Loaded NLI model on {self.device}")

    @property
    def embedding_dim(self) -> int:
        """Dynamically determined dimension of the loaded embedding model."""
        if self._embedding_dim is None:
            return 1024
        return self._embedding_dim

    @property
    def nli_model(self) -> str:
        """Configured NLI model name. Loaded only when NLI support is wired in."""
        return self._nli_model

    @property
    def embedding_backend(self) -> str:
        """Backend used by the sentence embedding model."""
        return self._backend

    @property
    def onnx_provider(self) -> str | None:
        """The resolved ONNX Runtime provider, or None for the Torch backend."""
        return self._onnx_provider

    async def encode(
        self,
        texts: List[str],
        *,
        parent_work_record: WorkRecord | None = None,
    ) -> List[List[float]]:
        """Batch encode texts to vectors with chunking for large inputs (async)."""
        if not texts:
            return []

        if isinstance(self._embedder, _DirectOnnxSentenceEmbedder):
            return self._encode_sync(texts)

        return await self._run_blocking(
            lambda: self._encode_sync(texts),
            name="embedding-encode",
            priority=ModelWorkPriority.BACKGROUND,
            work_kind="embedding",
            parent_work_record=parent_work_record,
        )

    def _encode_sync(self, texts: List[str]) -> List[List[float]]:
        if not self._embedder:
            raise RuntimeError("Embedder not loaded. Call load_models() first.")
        if len(texts) <= self.batch_size:
            with self._lock:
                return self._embedder.encode(texts).astype(np.float32).tolist()

        all_embeddings = []
        for i in range(0, len(texts), self.batch_size):
            chunk = texts[i : i + self.batch_size]
            with self._lock:
                embeddings = self._embedder.encode(chunk)
            all_embeddings.append(embeddings)

        return np.vstack(all_embeddings).astype(np.float32).tolist()

    async def encode_single(self, text: str) -> List[float]:
        """Encode single text, returns list for JSON serialization (async)."""
        if isinstance(self._embedder, _DirectOnnxSentenceEmbedder):
            return self._encode_single_sync(text)

        return await self._run_blocking(
            lambda: self._encode_single_sync(text),
            name="embedding-encode-single",
            priority=ModelWorkPriority.FOREGROUND,
            work_kind="embedding",
        )

    def _encode_single_sync(self, text: str) -> List[float]:
        if not self._embedder:
            raise RuntimeError("Embedder not loaded. Call load_models() first.")
        with self._lock:
            embedding = self._embedder.encode([text])[0]
        return embedding.astype(np.float32).tolist()

    async def rerank(
        self, query: str, candidates: List[str], batch_size: int = None
    ) -> List[float]:
        """Score query-candidate pairs via cross-encoder (async)."""
        if not candidates:
            return []
        await self.load_reranker()
        return await self._run_blocking(
            lambda: self._rerank_sync(query, candidates, batch_size),
            name="embedding-rerank",
            priority=ModelWorkPriority.FOREGROUND,
            work_kind="rerank",
        )

    def _rerank_sync(
        self, query: str, candidates: List[str], batch_size: int = None
    ) -> List[float]:
        if not self._reranker:
            raise RuntimeError("Reranker failed to load")
        batch_size = batch_size or self.batch_size
        pairs = [(query, c) for c in candidates]

        if len(pairs) <= batch_size:
            with self._lock:
                scores = self._reranker.predict(pairs)
            return [float(s) for s in scores]

        all_scores = []
        for i in range(0, len(pairs), batch_size):
            chunk = pairs[i : i + batch_size]
            with self._lock:
                scores = self._reranker.predict(chunk)
            all_scores.extend(float(s) for s in scores)

        return all_scores

    async def classify_text_pairs(
        self,
        pairs: List[Tuple[str, str]],
        batch_size: int = None,
    ) -> List[TextPairClassification]:
        """Classify text pairs as entailment, contradiction, or neutral."""
        if not pairs:
            return []
        await self.load_nli_model()
        return await self._run_blocking(
            lambda: self._classify_text_pairs_sync(pairs, batch_size),
            name="embedding-nli",
            priority=ModelWorkPriority.FOREGROUND,
            work_kind="nli",
        )

    def _classify_text_pairs_sync(
        self,
        pairs: List[Tuple[str, str]],
        batch_size: int = None,
    ) -> List[TextPairClassification]:
        if self._nli is None:
            raise RuntimeError("NLI model failed to load")

        batch_size = batch_size or self.batch_size
        judgments = []
        labels = self._nli_labels()

        for i in range(0, len(pairs), batch_size):
            chunk = pairs[i : i + batch_size]
            with self._lock:
                raw_scores = self._nli.predict(chunk)

            for pair, raw_score in zip(chunk, raw_scores):
                scores = self._nli_score_map(raw_score, labels)
                label = max(scores, key=scores.get)
                judgments.append(
                    TextPairClassification(
                        premise=pair[0],
                        hypothesis=pair[1],
                        label=label,
                        scores=scores,
                    )
                )

        return judgments

    def _nli_labels(self) -> List[str]:
        config = getattr(getattr(self._nli, "model", None), "config", None)
        id2label = getattr(config, "id2label", None)
        if id2label:
            return [
                str(id2label[index]).casefold()
                for index in sorted(id2label)
            ]
        return ["contradiction", "entailment", "neutral"]

    @staticmethod
    def _nli_score_map(raw_score, labels: List[str]) -> Dict[str, float]:
        scores = np.asarray(raw_score, dtype=float)
        if scores.ndim == 0:
            scores = np.asarray([float(scores)])
        shifted = scores - np.max(scores)
        exp_scores = np.exp(shifted)
        denominator = float(exp_scores.sum())
        probabilities = (
            exp_scores / denominator if denominator else np.zeros_like(exp_scores)
        )

        return {
            labels[index] if index < len(labels) else f"label_{index}": float(score)
            for index, score in enumerate(probabilities)
        }

    def cleanup(self):
        """Explicitly free model memory."""
        if hasattr(self, "_embedder") and self._embedder is not None:
            close = getattr(self._embedder, "close", None)
            if close is not None:
                close()
            del self._embedder
            self._embedder = None

        if hasattr(self, "_reranker") and self._reranker is not None:
            del self._reranker
            self._reranker = None

        if hasattr(self, "_nli") and self._nli is not None:
            del self._nli
            self._nli = None

        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        gc.collect()
        logger.info("EmbeddingService cleaned up")
