import asyncio
import gc
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

import numpy as np
import torch
from loguru import logger
from sentence_transformers import CrossEncoder, SentenceTransformer

from common.schema.contracts import EngineWorkUnit
from infrastructure.model_work import ModelWorkCoordinator, ModelWorkPriority


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
    supports_model_work_units = True

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
    ):
        self.device = device or "cpu"
        self.batch_size = batch_size
        self._lock = threading.Lock()

        self._embedder = None
        self._reranker = None
        self._nli = None
        self._embedding_dim = None
        self._config_kwargs = {}
        if str(self.device) == "cpu":
            self._config_kwargs["use_memory_efficient_attention"] = False
            self._config_kwargs["unpad_inputs"] = False
        self._model_Kwargs = {"torch_dtype": torch.float16}
        self._embedding_model = embedding_model
        self._reranker_model = reranker_model
        self._nli_model = nli_model
        self._model_work = model_work

        logger.info(
            "EmbeddingService initialized | "
            f"device={self.device} | batch_size={batch_size}"
        )

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
        parent_work_unit: EngineWorkUnit | None = None,
    ):
        if self._model_work is not None:
            work_unit = None
            if parent_work_unit is not None:
                work_unit = EngineWorkUnit.for_model_operation(
                    kind=work_kind,
                    scope=parent_work_unit.scope,
                    parent_work_unit_id=parent_work_unit.id,
                    priority=parent_work_unit.priority,
                )
            return await self._model_work.run_blocking(
                operation,
                priority=priority,
                name=name,
                work_unit=work_unit,
                parent_work_unit=parent_work_unit,
            )
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, operation)

    async def load_models(self):
        """Async initialization for heavy ML models."""
        if self._embedder and self._reranker:
            return

        def _load_models():
            embedder = SentenceTransformer(
                self._embedding_model,
                trust_remote_code=True,
                device=self.device,
                model_kwargs=self._model_Kwargs,
                config_kwargs=self._config_kwargs,
            )
            reranker = CrossEncoder(
                self._reranker_model,
                device=self.device,
                model_kwargs=self._model_Kwargs,
            )
            return embedder, reranker

        self._embedder, self._reranker = await self._run_blocking(
            _load_models,
            name="embedding-model-load",
            priority=ModelWorkPriority.BACKGROUND,
            work_kind="model_load",
        )

        if self._embedder:
            self._embedding_dim = self._embedder.get_sentence_embedding_dimension()
            logger.info(f"Loaded models on {self.device} | dims={self._embedding_dim}")
        else:
            logger.error("Failed to load embedder model")

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

    async def encode(
        self,
        texts: List[str],
        *,
        parent_work_unit: EngineWorkUnit | None = None,
    ) -> List[List[float]]:
        """Batch encode texts to vectors with chunking for large inputs (async)."""
        if not texts:
            return []

        return await self._run_blocking(
            lambda: self._encode_sync(texts),
            name="embedding-encode",
            priority=ModelWorkPriority.BACKGROUND,
            work_kind="embedding",
            parent_work_unit=parent_work_unit,
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
            raise RuntimeError("Reranker not loaded. Call load_models() first.")
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
            with self._lock:
                if self._nli is None:
                    self._nli = CrossEncoder(
                        self._nli_model,
                        device=self.device,
                        model_kwargs=self._model_Kwargs,
                    )
                    logger.info(f"Loaded NLI model on {self.device}")

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
