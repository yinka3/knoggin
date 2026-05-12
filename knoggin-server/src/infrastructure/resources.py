import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

import chromadb
import redis.asyncio as aioredis
import spacy
import torch
from gliner import GLiNER
from loguru import logger

from common.conf.base import get_config
from common.errors.exceptions import ConfigurationError, DependencyError
from common.mcp.client import MCPClientManager
from infrastructure.memgraph_client import MemgraphClient
from infrastructure.llm_client import LLMService
from infrastructure.redis_client import AsyncRedisClient
from knoggin.community.db.community_store import CommunityStore
from knoggin.knowledge.services.embedding_service import EmbeddingService
from knoggin.knowledge.services.entity_service import EntityManager
from knoggin.knowledge.services.graph_builder_service import GraphBuilderService
from knoggin.knowledge.services.graph_search_service import GraphSearchService
from log.llm_trace import get_trace_logger


class ResourceManager:
    _instance: Optional["ResourceManager"] = None
    _lock = None

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    def __init__(self):
        self.memgraph: Optional[MemgraphClient] = None
        self.embedding: Optional[EmbeddingService] = None
        self.redis: Optional[aioredis.Redis] = None
        self.llm_service: Optional[LLMService] = None
        self.executor: Optional[ThreadPoolExecutor] = None
        self.gliner: Optional[GLiNER] = None
        self.spacy: Optional[Any] = None
        self.chroma: Optional[Any] = None
        self.mcp_manager: Optional[MCPClientManager] = None
        self.active_entities: Optional[EntityManager] = None
        self.community_store: Optional[CommunityStore] = None
        self.graph_search: Optional[GraphSearchService] = None
        self.graph_builder: Optional[GraphBuilderService] = None

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
                instance.memgraph = MemgraphClient()
                instance.redis = await AsyncRedisClient.get_instance()

                config = get_config()
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

                chroma_path = os.path.join(
                    os.getenv("CONFIG_DIR", "./config"), "chroma_db"
                )
                instance.chroma = chromadb.PersistentClient(path=chroma_path)

                mcp_config = config.mcp
                instance.mcp_manager = await MCPClientManager.create(
                    mcp_config.model_dump()
                )

                await instance.memgraph.initialize()
                instance.community_store = instance.memgraph.community
                instance.active_entities = EntityManager(
                    memgraph=instance.memgraph,
                    embedding_service=instance.embedding
                )
                instance.graph_search = GraphSearchService(
                    memgraph=instance.memgraph,
                    embedding_service=instance.embedding
                )
                instance.graph_builder = GraphBuilderService(
                    memgraph=instance.memgraph,
                    embedding_service=instance.embedding,
                    redis=instance.redis,
                    entities_manager=instance.active_entities
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

        if self.memgraph:
            await self.memgraph.close()
        if self.embedding:
            self.embedding.cleanup()
        if self.mcp_manager:
            await self.mcp_manager.shutdown()
        if self.llm_service:
            await self.llm_service.close()

        self.gliner = None
        self.spacy = None
        self.chroma = None
        self.redis = None
        self.memgraph = None

    async def shutdown(self):
        """Release all managed resources."""
        async with self.__class__._get_lock():
            await self._teardown(wait=True)
            logger.info("ResourceManager shutdown complete")
            self.__class__._instance = None
