"""Application-owned runtime dependencies and their lifecycle."""

from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from inspect import isawaitable
from typing import Any, Callable, Optional, Protocol, cast

import spacy
import torch
from dotenv import load_dotenv
from loguru import logger

from common.conf.manager import ConfigManager
from common.exceptions import ConfigurationError, DependencyError
from common.utils.coordination_log import configure_coordination_log
from common.utils.lifecycle import settle_owned_task
from core.ingestion.vp01 import GLiNER25VP01Adapter
from core.knowledge.db.embedding_rebuilder import EmbeddingRebuilder
from core.knowledge.services.embedding_service import EmbeddingService
from core.knowledge.store import KnowledgeStore
from infrastructure.background_work import BackgroundWorkCoordinator
from infrastructure.llm_client import LLMService
from infrastructure.model_work import ModelWorkCoordinator, ModelWorkPriority
from infrastructure.postgres_client import PostgresClient
from infrastructure.resource_profile import ResourceProfile
from log.llm_trace import get_trace_logger


@dataclass(frozen=True, slots=True)
class RuntimeResourceShutdownFailure:
    """One cleanup failure collected while other resources continue stopping."""

    phase: str
    error: Exception


class RuntimeResourcesShutdownError(RuntimeError):
    """Raised after every runtime-resource cleanup phase has been attempted."""

    def __init__(self, failures: tuple[RuntimeResourceShutdownFailure, ...]) -> None:
        self.failures = failures
        phases = ", ".join(failure.phase for failure in failures)
        super().__init__(f"Runtime resource shutdown failed in phase(s): {phases}")


class ReadyRuntimeResources(Protocol):
    """Non-optional view available after ``RuntimeResources.create()`` succeeds."""

    knowledge_store: KnowledgeStore
    postgres: PostgresClient
    embedding: EmbeddingService
    llm_service: LLMService
    executor: ThreadPoolExecutor
    background_work: BackgroundWorkCoordinator
    model_work: ModelWorkCoordinator
    resource_profile: ResourceProfile
    vp01: GLiNER25VP01Adapter
    spacy: Any

    async def get_vp01(self, language: str) -> GLiNER25VP01Adapter: ...


class RuntimeResources:
    """The explicit container of dependencies owned by an application runtime."""

    def __init__(self) -> None:
        self.knowledge_store: Optional[KnowledgeStore] = None
        self.postgres: Optional[PostgresClient] = None
        self.embedding: Optional[EmbeddingService] = None
        self.llm_service: Optional[LLMService] = None
        self.executor: Optional[ThreadPoolExecutor] = None
        self.background_work: Optional[BackgroundWorkCoordinator] = None
        self.model_work: Optional[ModelWorkCoordinator] = None
        self.resource_profile: Optional[ResourceProfile] = None
        self.vp01: Optional[GLiNER25VP01Adapter] = None
        self._vp01_by_language: dict[str, GLiNER25VP01Adapter] = {}
        self._vp01_load_lock = asyncio.Lock()
        self._vp01_device: str | None = None
        self.spacy: Optional[Any] = None
        self.config_unsubscribers: list[Any] = []
        self._started = False
        self._closing = False
        self._shutdown_lock = asyncio.Lock()
        self._shutdown_complete = False
        self._shutdown_error: RuntimeResourcesShutdownError | None = None

    @classmethod
    async def create(cls, num_workers: int | None = None) -> "RuntimeResources":
        """Create fully initialized resources without registering global state."""

        instance = cls()
        try:
            await instance._start(num_workers=num_workers)
        except BaseException as exc:
            logger.error(f"Runtime resource initialization failed: {exc}")
            cleanup = asyncio.create_task(instance._teardown(wait=True))
            cleanup_failures = await settle_owned_task(cleanup)
            for failure in cleanup_failures:
                logger.error(
                    "Runtime resource startup cleanup failed in {}: {}",
                    failure.phase,
                    failure.error,
                )
            if cleanup_failures:
                # Keep failed handles reachable through the primary startup error.
                exc.cleanup_owner = instance
            if not isinstance(exc, Exception) or isinstance(exc, (DependencyError, ConfigurationError)):
                raise
            error = DependencyError(
                f"Unexpected error during runtime resource initialization: {exc}"
            )
            if cleanup_failures:
                error.cleanup_owner = instance
            raise error from exc

        instance._started = True
        logger.info("Runtime resources initialized")
        return instance

    async def _start(self, *, num_workers: int | None) -> None:
        device = self._configure_startup(num_workers=num_workers)
        await self._start_work_coordinators()
        await self._connect_datastores()
        self._construct_shared_services(device=device)
        await self._load_heavyweight_models(device=device)

    def require_ready(self) -> ReadyRuntimeResources:
        """Return the complete resource view guaranteed by successful startup."""

        if not self._started or self._closing or self._shutdown_complete:
            raise RuntimeError("Runtime resources are not ready")
        return cast(ReadyRuntimeResources, self)

    async def get_vp01(self, language: str) -> GLiNER25VP01Adapter:
        """Return the domain-selected local VP-01 model, loading multi on demand."""

        if language not in {"en", "multilingual"}:
            raise ValueError("VP-01 language must be 'en' or 'multilingual'")
        if self.model_work is None:
            raise RuntimeError("VP-01 model work coordinator is unavailable")
        existing = self._vp01_by_language.get(language)
        if existing is not None:
            return existing
        async with self._vp01_load_lock:
            existing = self._vp01_by_language.get(language)
            if existing is not None:
                return existing
            model = await self.model_work.run_blocking(
                lambda: GLiNER25VP01Adapter.load(
                    language=language,
                    device=self._vp01_device,
                ),
                priority=ModelWorkPriority.BACKGROUND,
                name=f"vp01-{language}-model-load",
            )
            self._vp01_by_language[language] = model
            if language == "en":
                self.vp01 = model
            logger.info("Loaded GLiNER2.5 {} VP-01 model", language)
            return model

    def _configure_startup(self, *, num_workers: int | None) -> torch.device:
        load_dotenv()
        resource_profile = ResourceProfile.from_environment()
        if num_workers is not None:
            resource_profile = replace(resource_profile, worker_count=num_workers)
        self.resource_profile = resource_profile

        use_gpu = os.getenv("KNOGGIN_GPU", "false").lower() == "true"
        if use_gpu and torch.cuda.is_available():
            logger.info("GPU enabled — CUDA")
            return torch.device("cuda")
        if (
            use_gpu
            and hasattr(torch.backends, "mps")
            and torch.backends.mps.is_available()
        ):
            logger.info("GPU enabled — MPS")
            return torch.device("mps")
        return torch.device("cpu")

    async def _start_work_coordinators(self) -> None:
        if self.resource_profile is None:
            raise RuntimeError("Runtime resource profile was not initialized")

        self.executor = ThreadPoolExecutor(max_workers=self.resource_profile.worker_count)
        self.background_work = BackgroundWorkCoordinator(
            max_concurrency=self.resource_profile.background_job_workers,
            max_queued_global=int(os.getenv("KNOGGIN_BACKGROUND_QUEUE_GLOBAL", "64")),
        )
        await self.background_work.start()
        self.model_work = ModelWorkCoordinator(
            self.executor,
            foreground_concurrency=self.resource_profile.foreground_model_workers,
            background_concurrency=self.resource_profile.background_model_workers,
            max_queued_foreground=int(
                os.getenv("KNOGGIN_MODEL_QUEUE_FOREGROUND", "32")
            ),
            max_queued_background=int(
                os.getenv("KNOGGIN_MODEL_QUEUE_BACKGROUND", "64")
            ),
            foreground_timeout_seconds=float(
                os.getenv("KNOGGIN_FOREGROUND_MODEL_TIMEOUT_SECONDS", "30")
            ),
        )
        await self.model_work.start()

    async def _connect_datastores(self) -> None:
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            raise ConfigurationError("DATABASE_URL environment variable is not set")

        self.postgres = PostgresClient(dsn=dsn)
        await self.postgres.connect()


    def _construct_shared_services(self, *, device: torch.device) -> None:
        if self.postgres is None or self.model_work is None or self.resource_profile is None:
            raise RuntimeError("Runtime datastore and worker dependencies are unavailable")

        config_manager = ConfigManager.get()
        config = config_manager.config

        def configure_runtime_coordination_log(settings):
            configure_coordination_log(
                settings.model_copy(
                    update={"path": str(config_manager.resolve_path(settings.path))}
                )
            )

        configure_runtime_coordination_log(
            config.developer_settings.coordination_log
        )
        self.config_unsubscribers.append(
            config_manager.subscribe(
                configure_runtime_coordination_log,
                "developer_settings.coordination_log",
            )
        )
        llm_config = config.llm
        trace_logger = (
            get_trace_logger()
            if os.getenv("KNOGGIN_LLM_TRACE", "false").lower() == "true"
            else None
        )
        self.llm_service = LLMService(
            api_key=llm_config.api_key,
            agent_model=llm_config.agent_model,
            extraction_model=llm_config.extraction_model,
            merge_model=llm_config.merge_model,
            spending_budget=llm_config.spending_budget,
            base_url=llm_config.base_url,
            trace_logger=trace_logger,
            postgres_client=self.postgres,
        )
        self.config_unsubscribers.append(
            config_manager.subscribe(self.llm_service.update_settings, "llm")
        )
        self.embedding = EmbeddingService(
            embedding_model=os.getenv(
                "KNOGGIN_EMBEDDING_MODEL", "dunzhang/stella_en_1.5B_v5"
            ),
            reranker_model=os.getenv("KNOGGIN_RERANKER_MODEL", "BAAI/bge-reranker-large"),
            nli_model=os.getenv(
                "KNOGGIN_NLI_MODEL",
                "MoritzLaurer/DeBERTa-v3-large-mnli-fever-anli-ling-wanli",
            ),
            device=device,
            batch_size=self.resource_profile.embedding_batch_size,
        )
        if hasattr(self.embedding, "set_model_work_coordinator"):
            self.embedding.set_model_work_coordinator(self.model_work)
        self.knowledge_store = KnowledgeStore(
            postgres_client=self.postgres,
            embedding_service=self.embedding,
        )

    async def _load_heavyweight_models(self, *, device: torch.device) -> None:
        if self.llm_service is None or self.embedding is None or self.model_work is None:
            raise RuntimeError("Runtime model dependencies are unavailable")
        self._vp01_device = str(device)

        async def load_spacy() -> None:
            processor = await self.model_work.run_blocking(
                lambda: spacy.blank("en"),
                priority=ModelWorkPriority.BACKGROUND,
                name="spacy-blank-en-init",
            )
            self.spacy = processor
            logger.info("Initialized blank English spaCy tokenizer for alias matching")

        async def load_vp01() -> None:
            await self.get_vp01("en")

        loaders = asyncio.gather(
                self.llm_service.load_tokenizer(),
                self.embedding.load_models(),
                load_spacy(),
                load_vp01(),
                return_exceptions=True,
            )
        try:
            results = await asyncio.shield(loaders)
            for result in results:
                if isinstance(result, BaseException):
                    raise result
            await EmbeddingRebuilder(self.postgres, self.embedding).ensure_configuration()
        except asyncio.CancelledError:
            await settle_owned_task(loaders)
            raise
        except Exception as exc:
            logger.critical(f"Global resource initialization failed: {exc}")
            raise DependencyError(
                f"Failed to initialize one or more critical resources: {exc}",
                details={"original_error": str(exc)},
            ) from exc

    async def _teardown(
        self,
        *,
        wait: bool,
    ) -> tuple[RuntimeResourceShutdownFailure, ...]:
        """Attempt every owned cleanup phase and return any failures."""

        failures: list[RuntimeResourceShutdownFailure] = []

        self._closing = True

        async def attempt(phase: str, callback: Callable[[], object]) -> bool:
            try:
                result = callback()
                if isawaitable(result):
                    await result
                return True
            except Exception as exc:
                logger.exception(f"Runtime resource cleanup failed: {phase}")
                failures.append(RuntimeResourceShutdownFailure(phase=phase, error=exc))
                return False

        for index, unsubscribe in enumerate(tuple(self.config_unsubscribers), start=1):
            if await attempt(f"configuration unsubscribe {index}", unsubscribe):
                self.config_unsubscribers.remove(unsubscribe)

        background_work = self.background_work
        if background_work is not None:
            if await attempt("background work", background_work.shutdown):
                self.background_work = None
        if self.background_work is not None:
            return tuple(failures)

        model_work = self.model_work
        if model_work is not None:
            if await attempt("model work", model_work.shutdown):
                self.model_work = None
        if self.model_work is not None or self.config_unsubscribers:
            return tuple(failures)

        executor = self.executor
        if executor is not None:
            if await attempt("executor", lambda: executor.shutdown(wait=wait)):
                self.executor = None
        if self.executor is not None:
            return tuple(failures)

        postgres = self.postgres
        if postgres is not None:
            if await attempt("PostgreSQL", postgres.close):
                self.postgres = None

        embedding = self.embedding
        if embedding is not None:
            if await attempt("embedding", embedding.cleanup):
                self.embedding = None

        llm_service = self.llm_service
        if llm_service is not None:
            if await attempt("LLM client", llm_service.close):
                self.llm_service = None

        if failures:
            return tuple(failures)
        self.vp01 = None
        self._vp01_by_language.clear()
        self._vp01_device = None
        self.spacy = None
        self.knowledge_store = None
        self.resource_profile = None
        return tuple(failures)

    def work_snapshot(self) -> dict[str, object]:
        """Return local scheduler health without requiring an API endpoint."""

        pool_snapshot = getattr(getattr(self, "postgres", None), "pool_snapshot", None)
        return {
            "postgres": pool_snapshot() if callable(pool_snapshot) else None,
            "background_work": (
                self.background_work.snapshot() if self.background_work else None
            ),
            "model_work": self.model_work.snapshot() if self.model_work else None,
        }

    async def shutdown(self) -> None:
        """Serialize cleanup, retaining unsuccessful owners for a later retry."""

        async with self._shutdown_lock:
            cleanup = asyncio.create_task(self._shutdown_locked())
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                try:
                    await settle_owned_task(cleanup)
                except Exception:
                    logger.exception("Resource cleanup failed while caller was cancelled")
                raise

    async def _shutdown_locked(self) -> None:

        if self._shutdown_complete:
            return

        self._closing = True
        self._started = False
        failures = await self._teardown(wait=True)
        if failures:
            error = RuntimeResourcesShutdownError(failures)
            self._shutdown_error = error
            raise error
        self._shutdown_error = None
        self._shutdown_complete = True
        logger.info("Runtime resources shut down")
