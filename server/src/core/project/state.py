import asyncio
import os
from typing import Any, Optional

import redis.asyncio as aioredis
from loguru import logger

from common.conf.domain_config import CompiledDomain, DomainConfig
from common.scoping import require_scope_value, require_visible_project_ids
from core.ingestion.services.processor import TextProcessor
from core.knowledge.documents import DocumentService
from core.knowledge.entity.resolver import EntityResolver
from core.knowledge.services.embedding_service import EmbeddingService
from core.project.domain_config_store import DomainActivation, DomainConfigStore
from core.project.workspace_service import ProjectWorkspaceService
from infrastructure.background_work import BackgroundWorkCoordinator
from infrastructure.job.scheduler import Scheduler
from infrastructure.postgres_client import PostgresClient
from infrastructure.resource_profile import ResourceProfile


class ProjectState:
    """
    Holds the runtime shared resources for a Project.
    """

    COMMUNITY_TASK_SHUTDOWN_TIMEOUT = 30.0

    def __init__(
        self,
        project_id: str,
        entities: EntityResolver,
        pipeline: TextProcessor,
        scheduler: Scheduler,
        user_name: str,
        redis_client: aioredis.Redis,
        readable_project_ids: list[str],
        postgres_client: PostgresClient,
        embedding_service: EmbeddingService,
        domain_config: DomainConfig,
        batch_processor: Optional[Any] = None,
        background_work: Optional[BackgroundWorkCoordinator] = None,
        domain_config_store: Optional[DomainConfigStore] = None,
    ):
        self.project_id = require_scope_value(
            project_id,
            "project_id",
            "ProjectState",
        )
        self.readable_project_ids = require_visible_project_ids(
            readable_project_ids,
            "ProjectState",
        )
        if not isinstance(domain_config, DomainConfig):
            raise TypeError("ProjectState requires a DomainConfig")
        self.entities = entities
        self.pipeline = pipeline
        self.scheduler = scheduler
        self.user_name = user_name
        self.redis_client = redis_client
        self.postgres_client = postgres_client
        self.embedding_service = embedding_service
        self.batch_processor = batch_processor
        self.domain_config_store = domain_config_store or DomainConfigStore(
            postgres_client
        )
        self.domain_config = domain_config
        self.compiled_domain: CompiledDomain = domain_config.compile()
        self._domain_config_lock = asyncio.Lock()
        resource_profile = ResourceProfile.from_environment()
        self.document_service = DocumentService(
            project_id=project_id,
            postgres_client=postgres_client,
            embedding_service=embedding_service,
            background_work=background_work,
            document_rerank_enabled=os.getenv("KNOGGIN_DOCUMENT_RERANK_ENABLED", "true")
            .strip()
            .lower()
            in {"1", "true", "yes", "on"},
            document_rerank_candidates=int(
                os.getenv("KNOGGIN_DOCUMENT_RERANK_CANDIDATES", "15")
            ),
            workspace_prepare_concurrency=(
                resource_profile.workspace_prepare_concurrency
            ),
        )
        self.workspace_service = ProjectWorkspaceService(self.document_service)

        self.episode_job: Optional[Any] = None
        self._community_task: Optional[asyncio.Task] = None
        self.active_runtime_sessions_count = 0
        self.config_unsubscribers: list[Any] = []
        self._shutdown_lock = asyncio.Lock()
        self._closed = False

    async def record_session_activity(self):
        """Record user activity against the project-level scheduler."""
        await self.scheduler.record_activity()

    def add_config_unsubscriber(self, unsubscribe):
        self.config_unsubscribers.append(unsubscribe)

    def track_community_task(self, task: asyncio.Task) -> None:
        """Associate the project's one long-running AAC task with its runtime."""
        self._community_task = task
        task.add_done_callback(self._clear_community_task)

    def _clear_community_task(self, task: asyncio.Task) -> None:
        if self._community_task is task:
            self._community_task = None

    async def _stop_community_task(self) -> None:
        task = self._community_task
        self._community_task = None
        if task is None or task.done():
            return

        logger.info(f"Cancelling AAC discussion for {self.project_id}")
        task.cancel()
        try:
            await asyncio.wait_for(
                asyncio.gather(task, return_exceptions=True),
                timeout=self.COMMUNITY_TASK_SHUTDOWN_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.warning(
                f"Timed out waiting for AAC discussion shutdown for {self.project_id}"
            )

    async def shutdown(self):
        """Cleanly shuts down project-level background resources."""
        async with self._shutdown_lock:
            if self._closed:
                return

            logger.info(f"Shutting down ProjectState resources for {self.project_id}")
            failures = []
            unsubscribers = self.config_unsubscribers
            self.config_unsubscribers = []
            for unsubscribe in unsubscribers:
                try:
                    unsubscribe()
                except Exception as exc:
                    logger.exception(
                        f"Project configuration cleanup failed for {self.project_id}"
                    )
                    failures.append(exc)

            for phase, shutdown in (
                ("community", self._stop_community_task),
                ("documents", self.document_service.shutdown),
                ("scheduler", self.scheduler.stop if self.scheduler else None),
            ):
                if shutdown is None:
                    continue
                try:
                    await shutdown()
                except Exception as exc:
                    logger.exception(
                        f"Project shutdown phase failed for {self.project_id}: {phase}"
                    )
                    failures.append(exc)

            self._closed = True
            if failures:
                raise RuntimeError(
                    f"ProjectState shutdown failed for {self.project_id}"
                ) from failures[0]
        # EntityResolver and others don't have explicit shutdown methods,
        # but they will be garbage collected.

    async def load_domain_config(self) -> DomainConfig:
        """Load the active domain and install its immutable runtime snapshot."""
        async with self._domain_config_lock:
            config = await self.domain_config_store.load(
                self.user_name,
                self.project_id,
            )
            if config is None:
                raise RuntimeError(
                    "Project domain configuration is required before runtime use"
                )
            self.domain_config = config
            self.compiled_domain = config.compile()
            self._install_compiled_domain(self.compiled_domain)
        return config

    def _install_compiled_domain(self, compiled_domain: CompiledDomain) -> None:
        """Fan one immutable domain snapshot into future ingestion admission."""

        for component in (self.batch_processor, self.pipeline):
            setter = getattr(component, "set_compiled_domain", None)
            if setter is not None:
                setter(compiled_domain)

    async def capture_domain(self) -> CompiledDomain:
        """Return a stable domain snapshot for one admitted runtime operation."""
        async with self._domain_config_lock:
            return self.compiled_domain

    def validate_domain_config(self, candidate):
        """Validate a complete candidate without touching project state."""

        from core.project.domain_config_operations import validate_domain_config

        return validate_domain_config(candidate)

    def preview_domain_config(self, candidate):
        """Preview a complete candidate against the loaded active config."""

        from core.project.domain_config_operations import preview_domain_config

        return preview_domain_config(self.domain_config, candidate)

    async def activate_domain_candidate(self, candidate, *, expected_version: int):
        """Run candidate validation and guarded activation as one workflow."""

        from core.project.domain_config_operations import DomainConfigOperations

        return await DomainConfigOperations.activate(
            self,
            candidate,
            expected_version=expected_version,
        )

    async def activate_domain_config(
        self,
        candidate: DomainConfig,
        *,
        expected_version: int,
    ) -> DomainActivation:
        """Persist and install a complete domain configuration atomically.

        The candidate is compiled before persistence. The project lock protects
        runtime readers from observing an intermediate snapshot; the store's
        row lock and revision check protect the durable value from lost updates.
        """
        from core.project.domain_config_operations import parse_candidate

        candidate = parse_candidate(candidate)
        async with self._domain_config_lock:
            activation = await self.domain_config_store.activate(
                user_name=self.user_name,
                project_id=self.project_id,
                candidate=candidate,
                expected_version=expected_version,
            )
            self.domain_config = activation.config
            self.compiled_domain = activation.compiled
            self._install_compiled_domain(activation.compiled)
            return activation
