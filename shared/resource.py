
from concurrent.futures import ThreadPoolExecutor

from gliner import GLiNER
from loguru import logger
import spacy
import torch
from transformers import pipeline
from db.store import MemGraphStore
from log.llm_trace import get_trace_logger
from main.embedding import EmbeddingService
from main.redisclient import AsyncRedisClient
from main.service import LLMService


class ResourceManager:
    _instance = None
    
    def __init__(self):
        self.store = None
        self.embedding = None
        self.redis = None
        self.llm_service = None
        self.executor = None
        self.gliner = None
        self.spacy = None
        self.emotion_classifier = None
        pass

    @classmethod
    async def initialize(cls) -> "ResourceManager":

        if cls._instance is not None:
            return cls._instance
        
        instance = cls()

        device_id = "cuda" if torch.cuda.is_available() else "cpu"
        device = torch.device(device_id)
        instance.executor = ThreadPoolExecutor(max_workers=4)
        instance.store = MemGraphStore()
        instance.redis = AsyncRedisClient().get_client()
        instance.llm_service = LLMService(trace_logger=get_trace_logger()) # maybe move api env here instead
        instance.embedding = EmbeddingService(device=device)

        exclude = ["ner", "lemmatizer", "attribute_ruler"]
        nlp = spacy.load("en_core_web_md", exclude=exclude)
        nlp.add_pipe("doc_cleaner")
        logger.info("Loaded en_core_web_md (CPU)")
        instance.spacy = nlp

        model = GLiNER.from_pretrained("urchade/gliner_large-v2.1")
        model.to(device)
        logger.info("Loaded GLiNER large-v2.1")
        instance.gliner = model

        instance.emotion_classifier = pipeline(
            "text-classification",
            model="j-hartmann/emotion-english-distilroberta-base",
            top_k=None,
            device=device_id
        )
        logger.info("Loaded Emotion Model")
        cls._instance = instance
        return instance

    