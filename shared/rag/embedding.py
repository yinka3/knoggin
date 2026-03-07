import gc
import threading
import numpy as np
import torch
from sentence_transformers import SentenceTransformer, CrossEncoder
from typing import List
from loguru import logger


class EmbeddingService:
    """Embedding infrastructure for the knowledge graph."""
    
    EMBEDDING_DIM = 1024
    BATCH_SIZE = 64  # Process in chunks to avoid OOM
    
    def __init__(
        self,
        embedding_model: str = 'dunzhang/stella_en_400M_v5',
        reranker_model: str = 'BAAI/bge-reranker-base',
        device: str = None,
        batch_size: int = 64
    ):
        self.device = device or 'cpu'
        self.batch_size = batch_size
        self._lock = threading.Lock()
        
        config_kwargs = {}
        if str(self.device) == "cpu":
            # The stella_en_400M_v5 model requires these config overrides 
            # to disable memory efficient attention (xformers) on CPU
            config_kwargs["use_memory_efficient_attention"] = False
            config_kwargs["unpad_inputs"] = False

        self._embedder = SentenceTransformer(
            embedding_model, 
            trust_remote_code=True, 
            device=self.device,
            model_kwargs={"torch_dtype": torch.float16},
            config_kwargs=config_kwargs
        )
        self._reranker = CrossEncoder(reranker_model, device=self.device)
        
        logger.info(f"EmbeddingService initialized | device={self.device} | batch_size={batch_size}")
    
    async def encode(self, texts: List[str]) -> List[List[float]]:
        """Batch encode texts to vectors with chunking for large inputs (async)."""
        if not texts:
            return []
        
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._encode_sync, texts)
        
    def _encode_sync(self, texts: List[str]) -> List[List[float]]:
        if len(texts) <= self.batch_size:
            with self._lock:
                return self._embedder.encode(texts).astype(np.float32).tolist()

        all_embeddings = []
        for i in range(0, len(texts), self.batch_size):
            chunk = texts[i:i + self.batch_size]
            with self._lock:
                embeddings = self._embedder.encode(chunk)
            all_embeddings.append(embeddings)
        
        return np.vstack(all_embeddings).astype(np.float32).tolist()
    
    async def encode_single(self, text: str) -> List[float]:
        """Encode single text, returns list for JSON serialization (async)."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._encode_single_sync, text)

    def _encode_single_sync(self, text: str) -> List[float]:
        with self._lock:
            embedding = self._embedder.encode([text])[0]
        return embedding.astype(np.float32).tolist()
    
    async def rerank(self, query: str, candidates: List[str], batch_size: int = None) -> List[float]:
        """Score query-candidate pairs via cross-encoder (async)."""
        if not candidates:
            return []
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._rerank_sync, query, candidates, batch_size)
        
    def _rerank_sync(self, query: str, candidates: List[str], batch_size: int = None) -> List[float]:
        batch_size = batch_size or self.batch_size
        pairs = [(query, c) for c in candidates]
        
        if len(pairs) <= batch_size:
            with self._lock:
                scores = self._reranker.predict(pairs)
            return [float(s) for s in scores]

        all_scores = []
        for i in range(0, len(pairs), batch_size):
            chunk = pairs[i:i + batch_size]
            with self._lock:
                scores = self._reranker.predict(chunk)
            all_scores.extend(float(s) for s in scores)
        
        return all_scores
    
    def cleanup(self):
        """Explicitly free model memory."""
        if hasattr(self, '_embedder') and self._embedder is not None:
            del self._embedder
            self._embedder = None
        
        if hasattr(self, '_reranker') and self._reranker is not None:
            del self._reranker
            self._reranker = None
        
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        gc.collect()
        logger.info("EmbeddingService cleaned up")