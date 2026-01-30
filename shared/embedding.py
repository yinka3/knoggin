import gc
import numpy as np
import torch
from sentence_transformers import SentenceTransformer, CrossEncoder
from typing import List
from loguru import logger


class EmbeddingService:
    """Embedding infrastructure for the knowledge graph."""
    
    EMBEDDING_DIM = 1024
    
    def __init__(
        self,
        embedding_model: str = 'dunzhang/stella_en_400M_v5',
        reranker_model: str = 'BAAI/bge-reranker-base',
        device: str = None
    ):
        self.device = device or ('cuda' if torch.cuda.is_available() else 'cpu')
        
        self._embedder = SentenceTransformer(
            embedding_model, 
            trust_remote_code=True, 
            device=self.device,
            model_kwargs={"torch_dtype": torch.float16}
        )
        self._reranker = CrossEncoder(reranker_model, device=self.device)
        
        logger.info(f"EmbeddingService initialized | device={self.device}")
    
    def encode(self, texts: List[str]) -> np.ndarray:
        """Batch encode texts to vectors."""
        if not texts:
            return np.array([])
        return self._embedder.encode(texts).astype(np.float32)
    
    def encode_single(self, text: str) -> List[float]:
        """Encode single text, returns list for JSON serialization."""
        return self.encode([text])[0].tolist()
    
    def rerank(self, query: str, candidates: List[str]) -> List[float]:
        """Score query-candidate pairs via cross-encoder."""
        if not candidates:
            return []
        pairs = [(query, c) for c in candidates]
        return self._reranker.predict(pairs).tolist()
    
    def cleanup(self):
        """Explicitly free model memory."""
        del self._embedder
        del self._reranker
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        gc.collect()
        logger.info("EmbeddingService cleaned up")