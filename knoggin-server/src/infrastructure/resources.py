import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

import redis.asyncio as aioredis
import spacy
import torch
from gliner import GLiNER
from loguru import logger

from common.conf.manager import ConfigManager
from common.exceptions import ConfigurationError, DependencyError
from infrastructure.graph_client import GraphClient
from infrastructure.llm_client import LLMService
from infrastructure.redis_client import AsyncRedisClient
from knoggin.knowledge.services.embedding_service import EmbeddingService
from knoggin.knowledge.services.entity_service import EntityManager
from log.llm_trace import get_trace_logger


class ResourceManager:
    _instance: Optional["ResourceManager"] = None
    _lock = None

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    @classmethod
    def get(cls) -> "ResourceManager":
        if cls._instance is None:
            raise RuntimeError("ResourceManager not initialized")
        return cls._instance

    def __init__(self):

        self.graph_client: Optional[GraphClient] = None
        self.embedding: Optional[EmbeddingService] = None
        self.redis: Optional[aioredis.Redis] = None
        self.llm_service: Optional[LLMService] = None
        self.executor: Optional[ThreadPoolExecutor] = None
        self.gliner: Optional[GLiNER] = None
        self.spacy: Optional[Any] = None
        self.active_entities: Optional[EntityManager] = None

    @classmethod
    async def initialize(cls, num_workers: int = 4) -> "ResourceManager":
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
                elif (
                    use_gpu
                    and hasattr(torch.backends, "mps")
                    and torch.backends.mps.is_available()
                ):
                    device = torch.device("mps")
                    logger.info("GPU enabled — MPS")
                else:
                    device = torch.device("cpu")

                instance.executor = ThreadPoolExecutor(max_workers=num_workers)

                dsn = os.environ.get("DATABASE_URL")
                if not dsn:
                    raise ConfigurationError(
                        "DATABASE_URL environment variable is not set"
                    )
                instance.graph_client = GraphClient(dsn=dsn)
                instance.redis = await AsyncRedisClient.get_instance()

                config = ConfigManager.get().config
                llm_config = config.llm
                instance.llm_service = LLMService(
                    api_key=llm_config.api_key,
                    agent_model=llm_config.agent_model,
                    extraction_model=llm_config.extraction_model,
                    merge_model=llm_config.merge_model,
                    trace_logger=get_trace_logger(),
                    redis_client=instance.redis,
                )
                instance.embedding = EmbeddingService(device=device)

                async def load_spacy():
                    exclude = ["ner", "lemmatizer", "attribute_ruler"]
                    loop = asyncio.get_running_loop()
                    processor = await loop.run_in_executor(
                        None, lambda: spacy.load("en_core_web_md", exclude=exclude)
                    )
                    processor.add_pipe("doc_cleaner")
                    instance.spacy = processor
                    logger.info("Loaded spacy model")

                async def load_gliner():
                    loop = asyncio.get_running_loop()
                    model = await loop.run_in_executor(
                        None,
                        lambda: GLiNER.from_pretrained("urchade/gliner_large-v2.1"),
                    )
                    model.to(device)
                    instance.gliner = model
                    logger.info("Loaded GLiNER model")

                try:
                    await asyncio.gather(
                        instance.llm_service.load_tokenizer(),
                        instance.embedding.load_models(),
                        load_spacy(),
                        load_gliner(),
                    )
                except Exception as e:
                    logger.critical(f"Global resource initialization failed: {e}")
                    raise DependencyError(
                        f"Failed to initialize one or more critical resources: {e}",
                        details={"original_error": str(e)},
                    )

                await instance.graph_client.connect()

                instance.active_entities = EntityManager(
                    graph_client=instance.graph_client,
                    embedding_service=instance.embedding,
                )
                cls._instance = instance
                logger.info("ResourceManager initialization complete")
                return instance

            except Exception as e:
                logger.error(f"ResourceManager initialization failed: {e}")
                await instance._teardown(wait=False)
                if not isinstance(e, (DependencyError, ConfigurationError)):
                    raise DependencyError(
                        f"Unexpected error during initialization: {e}"
                    )
                raise

    async def _teardown(self, wait: bool = True):
        """Internal helper to release all managed resources."""
        if self.executor:
            self.executor.shutdown(wait=wait)

        await AsyncRedisClient.close_redis()

        if self.graph_client:
            await self.graph_client.close()
            self.graph_client = None
        if self.embedding:
            self.embedding.cleanup()
        if self.llm_service:
            await self.llm_service.close()

        self.gliner = None
        self.spacy = None
        self.redis = None
        self.graph_client = None

    async def shutdown(self):
        """Release all managed resources."""
        async with self.__class__._get_lock():
            await self._teardown(wait=True)
            logger.info("ResourceManager shutdown complete")
            self.__class__._instance = None
