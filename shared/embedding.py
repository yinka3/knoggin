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
        self._embedder = SentenceTransformer(
            embedding_model, 
            trust_remote_code=True, 
            device=self.device,
            model_kwargs={"torch_dtype": torch.float16}
        )
        self._reranker = CrossEncoder(reranker_model, device=self.device)
        
        logger.info(f"EmbeddingService initialized | device={self.device} | batch_size={batch_size}")
    
    def encode(self, texts: List[str]) -> List[List[float]]:
        """Batch encode texts to vectors with chunking for large inputs."""
        if not texts:
            return []
        
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
    
    def encode_single(self, text: str) -> List[float]:
        """Encode single text, returns list for JSON serialization."""

        with self._lock:
            embedding = self._embedder.encode([text])[0]
        return embedding.astype(np.float32).tolist()
    
    def rerank(self, query: str, candidates: List[str], batch_size: int = None) -> List[float]:
        """Score query-candidate pairs via cross-encoder."""
        if not candidates:
            return []
        
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