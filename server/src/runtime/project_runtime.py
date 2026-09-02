import asyncio
from typing import Any, Optional

from loguru import logger

from common.conf.domain_config import CompiledDomain, DomainConfig
from common.scoping import require_scope_value, require_visible_project_ids
from core.ingestion.text_processor import TextProcessor
from core.knowledge.documents import DocumentService
from core.knowledge.entity.resolver import EntityResolver
from core.project.domain_config_store import DomainActivation, DomainConfigStore
from infrastructure.background_work import BackgroundWorkCoordinator
from infrastructure.job.scheduler import Scheduler


class ProjectRuntime:
    """
    Holds the runtime shared resources for a Project.
    """

    def __init__(
        self,
        project_id: str,
        entities: EntityResolver,
        knowledge_retrieval: Any,
        text_processor: TextProcessor,
        scheduler: Scheduler,
        user_name: str,
        readable_project_ids: list[str],
        domain_config: DomainConfig,
        document_service: DocumentService,
        domain_config_store: DomainConfigStore,
        ingestion_pipeline: Optional[Any] = None,
        background_work: Optional[BackgroundWorkCoordinator] = None,
    ):
        self.project_id = require_scope_value(
            project_id,
            "project_id",
            "ProjectRuntime",
        )
        self.readable_project_ids = require_visible_project_ids(
            readable_project_ids,
            "ProjectRuntime",
        )
        if not isinstance(domain_config, DomainConfig):
            raise TypeError("ProjectRuntime requires a DomainConfig")
        self.entities = entities
        self.knowledge_retrieval = knowledge_retrieval
        self.text_processor = text_processor
        self.scheduler = scheduler
        self.user_name = user_name
        self.ingestion_pipeline = ingestion_pipeline
        self.background_work = background_work
        self.domain_config_store = domain_config_store
        self.domain_config = domain_config
        self.compiled_domain: CompiledDomain = domain_config.compile()
        self._domain_config_lock = asyncio.Lock()
        self.document_service = document_service

        self.episode_job: Optional[Any] = None
        self.config_unsubscribers: list[Any] = []
        self._closed = False

    def add_config_unsubscriber(self, unsubscribe):
        self.config_unsubscribers.append(unsubscribe)

    async def shutdown(self):
        """Stop admission, then release every project-owned runtime resource."""
        if self._closed:
            return

        logger.info(f"Shutting down ProjectRuntime resources for {self.project_id}")
        failures = []

        for phase, shutdown in (
            ("scheduler", self.scheduler.stop if self.scheduler else None),
            ("document indexing", self.document_service.indexer.shutdown),
            (
                "background work",
                self._cancel_owned_background_work,
            ),
        ):
            if shutdown is None:
                continue
            try:
                result = shutdown()
                if result is not None:
                    await result
            except Exception as exc:
                logger.exception(
                    f"Project shutdown phase failed for {self.project_id}: {phase}"
                )
                failures.append(exc)

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

        self._closed = True
        if failures:
            raise RuntimeError(
                f"ProjectRuntime shutdown failed for {self.project_id}"
            ) from failures[0]
        # EntityResolver and others don't have explicit shutdown methods,
        # but they will be garbage collected.

    async def _cancel_owned_background_work(self) -> None:
        """Cancel only document-index and scheduler work owned by this runtime."""
        if self.background_work is None:
            return
        owners = {f"project:{self.project_id}:document-index"}
        if self.scheduler is not None:
            owners.update(
                f"project:{self.project_id}:{name}"
                for name in self.scheduler.registered_job_names
            )
        for owner in sorted(owners):
            await self.background_work.cancel_owner(owner)

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

        for component in (self.ingestion_pipeline, self.text_processor):
            setter = getattr(component, "set_compiled_domain", None)
            if setter is not None:
                setter(compiled_domain)

    async def capture_domain(self) -> CompiledDomain:
        """Return a stable domain snapshot for one admitted runtime operation."""
        async with self._domain_config_lock:
            return self.compiled_domain

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
