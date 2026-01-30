
from concurrent.futures import ThreadPoolExecutor
from gliner import GLiNER
from loguru import logger
import redis
import spacy
import torch
from db.store import MemGraphStore
from log.llm_trace import get_trace_logger
from main.embedding import EmbeddingService
from shared.redisclient import AsyncRedisClient
from main.service import LLMService

class ResourceManager:
    _instance = None
    
    def __init__(self):
        self.store: MemGraphStore = None
        self.embedding: EmbeddingService = None
        self.redis: redis.Redis = None
        self.llm_service: LLMService = None
        self.executor: ThreadPoolExecutor = None
        self.gliner: GLiNER = None
        self.spacy: spacy.Language = None

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
        instance.llm_service = LLMService(trace_logger=get_trace_logger())
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

        cls._instance = instance
        return instance

    