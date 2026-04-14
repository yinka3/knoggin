import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, List, TYPE_CHECKING

import torch
import spacy
import chromadb
from gliner import GLiNER
from loguru import logger
import redis.asyncio as aioredis

from db.community_store import CommunityStore
from db.store import MemGraphStore
from log.llm_trace import get_trace_logger
from common.config.base import get_config
from common.rag.embedding import EmbeddingService
from common.mcp.client import MCPClientManager
from common.infra.redis import AsyncRedisClient
from common.services.llm_service import LLMService
from common.errors.agent import DependencyError, ConfigurationError

class ResourceManager:
    _instance: Optional['ResourceManager'] = None
    _lock = None
    
    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock
    
    def __init__(self):
        self.store = None
        self.embedding = None
        self.redis = None
        self.llm_service = None
        self.executor = None
        self.gliner = None
        self.spacy = None
        self.chroma = None
        self.mcp_manager = None
        self.active_resolver = None
        self.community_store = None

    @classmethod
    async def initialize(cls, num_workers: int = 4) -> 'ResourceManager':
        """Initialize all resources concurrently."""
        async with cls._get_lock():
            if cls._instance is not None:
                return cls._instance

            instance = cls()
            
            try:
                use_gpu = os.getenv("KNOGGIN_GPU", "false").lower() == "true"
                if use_gpu and torch.cuda.is_available():
                    device = torch.device("cuda")
                    logger.info("GPU enabled — CUDA")
                elif use_gpu and hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
                    device = torch.device("mps")
                    logger.info("GPU enabled — MPS")
                else:
                    device = torch.device("cpu")

                instance.executor = ThreadPoolExecutor(max_workers=num_workers)
                instance.store = MemGraphStore()
                instance.redis = await AsyncRedisClient.get_instance()
                
                config = get_config()
                llm_config = config.llm
                instance.llm_service = LLMService(
                    api_key=llm_config.api_key,
                    agent_model=llm_config.agent_model,
                    extraction_model=llm_config.extraction_model,
                    merge_model=llm_config.merge_model,
                    trace_logger=get_trace_logger(),
                    redis_client=instance.redis
                )
                instance.embedding = EmbeddingService(device=device)

                async def load_spacy():
                    exclude = ["ner", "lemmatizer", "attribute_ruler"]
                    loop = asyncio.get_running_loop()
                    nlp = await loop.run_in_executor(None, lambda: spacy.load("en_core_web_md", exclude=exclude))
                    nlp.add_pipe("doc_cleaner")
                    instance.spacy = nlp
                    logger.info("Loaded spacy model")

                async def load_gliner():
                    loop = asyncio.get_running_loop()
                    model = await loop.run_in_executor(None, lambda: GLiNER.from_pretrained("urchade/gliner_large-v2.1"))
                    model.to(device)
                    instance.gliner = model
                    logger.info("Loaded GLiNER model")
                
                try:
                    await asyncio.gather(
                        instance.llm_service.load_tokenizer(),
                        instance.embedding.load_models(),
                        load_spacy(),
                        load_gliner()
                    )
                except Exception as e:
                    logger.critical(f"Global resource initialization failed: {e}")
                    raise DependencyError(
                        f"Failed to initialize one or more critical resources: {e}",
                        details={"original_error": str(e)}
                    )

                chroma_path = os.path.join(os.getenv("CONFIG_DIR", "./config"), "chroma_db")
                instance.chroma = chromadb.PersistentClient(path=chroma_path)
                
                mcp_config = config.mcp
                instance.mcp_manager = await MCPClientManager.create(mcp_config.model_dump())
                
                await instance.store.initialize()
                instance.community_store = instance.store.community

                cls._instance = instance
                logger.info("ResourceManager initialization complete")
                return instance
                
            except Exception as e:
                logger.error(f"ResourceManager initialization failed: {e}")
                await instance._cleanup_partial()
                if not isinstance(e, (DependencyError, ConfigurationError)):
                    raise DependencyError(f"Unexpected error during initialization: {e}")
                raise

    async def _cleanup_partial(self):
        """Clean up partially initialized resources."""
        if self.executor:
            self.executor.shutdown(wait=False)
        if self.redis:
            await AsyncRedisClient.close_redis()
        if self.store:
            await self.store.close()
        if self.embedding:
            self.embedding.cleanup()
        if self.mcp_manager:
            await self.mcp_manager.shutdown()
        self.gliner = None
        self.spacy = None
        self.chroma = None
        logger.info("Cleaned up partial ResourceManager initialization")
    
    async def shutdown(self):
        """Release all managed resources."""
        async with self.__class__._get_lock():
            if self.executor:
                self.executor.shutdown(wait=True)
            await AsyncRedisClient.close_redis()
            if self.store:
                await self.store.close()
            if self.embedding:
                self.embedding.cleanup()
            if self.mcp_manager:
                await self.mcp_manager.shutdown()
            if self.llm_service:
                await self.llm_service.close()
            self.gliner = None
            self.spacy = None
            self.chroma = None
            logger.info("ResourceManager shutdown complete")
            self.__class__._instance = None