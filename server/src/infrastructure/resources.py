import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from typing import Any, Optional

import redis.asyncio as aioredis
import spacy
import torch
from dotenv import load_dotenv
from gliner import GLiNER
from loguru import logger

from common.conf.manager import ConfigManager
from common.exceptions import ConfigurationError, DependencyError
from common.schema.settings import RedisConnectionSettings
from common.utils.coordination_log import configure_coordination_log
from common.utils.events import CommunityEventEmitter
from core.knowledge.services.embedding_service import EmbeddingService
from infrastructure.background_work import BackgroundWorkCoordinator
from infrastructure.knowledge_store import KnowledgeStore
from infrastructure.llm_client import LLMService
from infrastructure.model_work import ModelWorkCoordinator, ModelWorkPriority
from infrastructure.postgres_client import PostgresClient
from infrastructure.redis_client import AsyncRedisClient
from infrastructure.resource_profile import ResourceProfile
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

        self.knowledge_store: Optional[KnowledgeStore] = None
        self.postgres: Optional[PostgresClient] = None
        self.embedding: Optional[EmbeddingService] = None
        self.redis_manager: Optional[AsyncRedisClient] = None
        self.redis: Optional[aioredis.Redis] = None
        self.llm_service: Optional[LLMService] = None
        self.executor: Optional[ThreadPoolExecutor] = None
        self.background_work: Optional[BackgroundWorkCoordinator] = None
        self.model_work: Optional[ModelWorkCoordinator] = None
        self.resource_profile: Optional[ResourceProfile] = None
        self.gliner: Optional[GLiNER] = None
        self.spacy: Optional[Any] = None
        self.config_unsubscribers: list[Any] = []

    @classmethod
    async def initialize(cls, num_workers: int | None = None) -> "ResourceManager":
        """Initialize all resources concurrently."""
        load_dotenv()
        async with cls._get_lock():
            if cls._instance is not None:
                return cls._instance

            instance = cls()

            try:
                resource_profile = ResourceProfile.from_environment()
                if num_workers is not None:
                    resource_profile = replace(
                        resource_profile,
                        worker_count=num_workers,
                    )
                instance.resource_profile = resource_profile
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

                instance.executor = ThreadPoolExecutor(
                    max_workers=resource_profile.worker_count
                )
                instance.background_work = BackgroundWorkCoordinator(
                    max_concurrency=resource_profile.background_job_workers,
                    max_queued_per_project=int(
                        os.getenv("KNOGGIN_BACKGROUND_QUEUE_PER_PROJECT", "8")
                    ),
                    max_queued_global=int(
                        os.getenv("KNOGGIN_BACKGROUND_QUEUE_GLOBAL", "64")
                    ),
                )
                await instance.background_work.start()
                instance.model_work = ModelWorkCoordinator(
                    instance.executor,
                    foreground_concurrency=resource_profile.foreground_model_workers,
                    background_concurrency=resource_profile.background_model_workers,
                    foreground_timeout_seconds=float(
                        os.getenv("KNOGGIN_FOREGROUND_MODEL_TIMEOUT_SECONDS", "30")
                    ),
                )
                await instance.model_work.start()

                dsn = os.environ.get("DATABASE_URL")
                if not dsn:
                    raise ConfigurationError(
                        "DATABASE_URL environment variable is not set"
                    )
                redis_settings = RedisConnectionSettings.from_env()
                instance.redis_manager = AsyncRedisClient(redis_settings)
                instance.redis = await instance.redis_manager.connect()
                CommunityEventEmitter.get().bind_redis(instance.redis)

                config = ConfigManager.get().config
                configure_coordination_log(config.developer_settings.coordination_log)
                instance.config_unsubscribers.append(
                    ConfigManager.get().subscribe(
                        configure_coordination_log,
                        "developer_settings.coordination_log",
                    )
                )
                llm_config = config.llm
                trace_logger = (
                    get_trace_logger()
                    if os.getenv("KNOGGIN_LLM_TRACE", "false").lower() == "true"
                    else None
                )
                instance.llm_service = LLMService(
                    api_key=llm_config.api_key,
                    agent_model=llm_config.agent_model,
                    extraction_model=llm_config.extraction_model,
                    merge_model=llm_config.merge_model,
                    base_url=llm_config.base_url,
                    trace_logger=trace_logger,
                )
                instance.config_unsubscribers.append(
                    ConfigManager.get().subscribe(
                        instance.llm_service.update_settings, "llm"
                    )
                )
                embedding_model = os.getenv(
                    "KNOGGIN_EMBEDDING_MODEL", "dunzhang/stella_en_1.5B_v5"
                )
                reranker_model = os.getenv(
                    "KNOGGIN_RERANKER_MODEL", "BAAI/bge-reranker-large"
                )
                nli_model = os.getenv(
                    "KNOGGIN_NLI_MODEL",
                    "MoritzLaurer/DeBERTa-v3-large-mnli-fever-anli-ling-wanli",
                )
                instance.embedding = EmbeddingService(
                    embedding_model=embedding_model,
                    reranker_model=reranker_model,
                    nli_model=nli_model,
                    device=device,
                    batch_size=resource_profile.embedding_batch_size,
                )
                if hasattr(instance.embedding, "set_model_work_coordinator"):
                    instance.embedding.set_model_work_coordinator(instance.model_work)
                instance.postgres = PostgresClient(dsn=dsn)
                instance.knowledge_store = KnowledgeStore(
                    postgres_client=instance.postgres,
                    embedding_service=instance.embedding,
                )

                async def load_spacy():
                    exclude = ["ner", "lemmatizer", "attribute_ruler"]
                    processor = await instance.model_work.run_blocking(
                        lambda: spacy.load("en_core_web_md", exclude=exclude),
                        priority=ModelWorkPriority.BACKGROUND,
                        name="spacy-model-load",
                    )
                    processor.add_pipe("doc_cleaner")
                    instance.spacy = processor
                    logger.info("Loaded spacy model")

                async def load_gliner():
                    model = await instance.model_work.run_blocking(
                        lambda: GLiNER.from_pretrained("urchade/gliner_large-v2.1"),
                        priority=ModelWorkPriority.BACKGROUND,
                        name="gliner-model-load",
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

                await instance.postgres.connect()
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
        for unsubscribe in self.config_unsubscribers:
            unsubscribe()
        self.config_unsubscribers.clear()

        if self.background_work:
            await self.background_work.shutdown()
            self.background_work = None

        if self.model_work:
            await self.model_work.shutdown()
            self.model_work = None

        if self.executor:
            self.executor.shutdown(wait=wait)
            self.executor = None

        if self.redis is not None:
            CommunityEventEmitter.get().unbind_redis(self.redis)
        if self.redis_manager is not None:
            await self.redis_manager.close()
            self.redis_manager = None
        self.redis = None

        if self.postgres:
            await self.postgres.close()
        self.postgres = None
        if self.embedding:
            self.embedding.cleanup()
        if self.llm_service:
            await self.llm_service.close()

        self.gliner = None
        self.spacy = None
        self.knowledge_store = None

    def work_snapshot(self) -> dict[str, object]:
        """Return local scheduler health without requiring an API endpoint."""
        return {
            "background_work": (
                self.background_work.snapshot() if self.background_work else None
            ),
            "model_work": self.model_work.snapshot() if self.model_work else None,
        }

    async def shutdown(self):
        """Release all managed resources."""
        async with self.__class__._get_lock():
            await self._teardown(wait=True)
            logger.info("ResourceManager shutdown complete")
            self.__class__._instance = None
