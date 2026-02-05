
import asyncio
from concurrent.futures import ThreadPoolExecutor
from gliner import GLiNER
from loguru import logger
import redis.asyncio as aioredis
import spacy
import torch
from db.store import MemGraphStore
from log.llm_trace import get_trace_logger
from shared.config import get_config_value
from shared.embedding import EmbeddingService
from shared.redisclient import AsyncRedisClient
from shared.service import LLMService

class ResourceManager:
    _instance = None
    _init_lock: asyncio.Lock = None
    
    def __init__(self):
        self.store: MemGraphStore = None
        self.embedding: EmbeddingService = None
        self.redis: aioredis.Redis = None
        self.llm_service: LLMService = None
        self.executor: ThreadPoolExecutor = None
        self.gliner: GLiNER = None
        self.spacy: spacy.Language = None

    @classmethod
    async def initialize(cls) -> "ResourceManager":
        if cls._init_lock is None:
            cls._init_lock = asyncio.Lock()
        
        async with cls._init_lock:
            if cls._instance is not None:
                return cls._instance
        
            instance = cls()

            try:
                device_id = "cuda" if torch.cuda.is_available() else "cpu"
                device = torch.device(device_id)
                
                instance.executor = ThreadPoolExecutor(max_workers=4)
                instance.store = MemGraphStore()
                instance.redis = await AsyncRedisClient.get_instance()
                
                llm_config = get_config_value("llm", {})
                instance.llm_service = LLMService(
                    api_key=llm_config.get("api_key"),
                    reasoning_model=llm_config.get("reasoning_model", "google/gemini-2.5-flash"),
                    agent_model=llm_config.get("agent_model", "google/gemini-3-flash-preview"),
                    trace_logger=get_trace_logger()
                )
                
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
                logger.info("ResourceManager initialization complete")
                return instance
                
            except Exception as e:
                logger.error(f"ResourceManager initialization failed: {e}")
                await instance._cleanup_partial()
                raise

    async def _cleanup_partial(self):
        """Clean up partially initialized resources."""
        if self.executor:
            self.executor.shutdown(wait=False)
            self.executor = None
        
        try:
            await AsyncRedisClient.close_redis()
        except Exception:
            pass
        self.redis = None
        
        if self.store:
            try:
                self.store.close()
            except Exception:
                pass
            self.store = None
        
        if self.embedding:
            try:
                self.embedding.cleanup()
            except Exception:
                pass
            self.embedding = None
        
        self.gliner = None
        self.spacy = None
        
        logger.info("Cleaned up partial ResourceManager initialization")
    
    async def shutdown(self):
        """Release all managed resources."""
        if self.executor:
            self.executor.shutdown(wait=True)
        
        await AsyncRedisClient.close_redis()
        
        if self.store:
            self.store.close()
        
        if self.embedding:
            self.embedding.cleanup()
        
        logger.info("ResourceManager shutdown complete")
        
        self.__class__._instance = None

    