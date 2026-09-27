"""Top-level application runtime and ordered shutdown ownership."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from loguru import logger

from common.conf.manager import ConfigManager
from common.utils.lifecycle import settle_owned_task
from common.utils.time_utils import get_now
from core.agent.orchestrator import AgentOrchestrator
from core.agent.services.agent_manager import AgentManager
from core.community.runtime import AACRuntime
from core.health.service import RuntimeHealthService
from core.knowledge.documents import ProjectFilesystemFactory
from core.project.project_manager import ProjectManager
from core.session.session_manager import SessionManager
from runtime.resources import RuntimeResources


@dataclass(frozen=True, slots=True)
class ShutdownFailure:
    """One phase failure collected while remaining shutdown phases continue."""

    phase: str
    error: Exception


class ApplicationShutdownError(RuntimeError):
    """Raised only after every top-level shutdown phase has been attempted."""

    def __init__(self, failures: tuple[ShutdownFailure, ...]) -> None:
        self.failures = failures
        phases = ", ".join(failure.phase for failure in failures)
        super().__init__(f"Application shutdown failed in phase(s): {phases}")


@dataclass(slots=True)
class ApplicationRuntime:
    """The root owner of shared resources, projects, sessions, and health."""

    config_manager: ConfigManager
    resources: RuntimeResources
    projects: ProjectManager
    sessions: SessionManager
    agent_manager: AgentManager
    agent_orchestrator: AgentOrchestrator
    aac_runtime: AACRuntime
    health_service: RuntimeHealthService = field(init=False)
    started_at: datetime = field(init=False)
    _shutdown_complete: bool = field(init=False, default=False, repr=False)
    _shutdown_lock: asyncio.Lock = field(init=False, default_factory=asyncio.Lock, repr=False)
    _completed_shutdown_phases: set[str] = field(init=False, default_factory=set, repr=False)
    _shutdown_error: ApplicationShutdownError | None = field(
        init=False,
        default=None,
        repr=False,
    )

    def __post_init__(self) -> None:
        self.started_at = get_now()
        self.health_service = RuntimeHealthService(
            resources=self.resources,
            projects=self.projects,
            sessions=self.sessions,
            started_at=self.started_at,
        )
        self.sessions.attach_health_service(self.health_service)

    @classmethod
    async def start(
        cls,
        *,
        user_name: str,
        config_dir: str | Path,
        num_workers: int | None = None,
    ) -> "ApplicationRuntime":
        """Build the canonical runtime whose shutdown owns every live layer."""

        config_manager = ConfigManager.initialize(config_dir)
        resources = await RuntimeResources.create(num_workers=num_workers)
        try:
            knowledge_store = resources.knowledge_store
            if knowledge_store is None:
                raise RuntimeError("Runtime resources did not initialize KnowledgeStore")
            await knowledge_store.ensure_identity_entity(
                user_name,
                config_manager.config.user_aliases,
            )
            projects = ProjectManager(
                resources=resources,
                user_name=user_name,
                filesystem_factory=ProjectFilesystemFactory(
                    config_manager.resolve_path(
                        config_manager.config.developer_settings.documents.project_library_root
                    )
                ),
                config_manager=config_manager,
            )
            await projects.start()
            agent_manager = AgentManager(resources, user_name)
            await agent_manager.ensure_default_agent()
            agent_orchestrator = AgentOrchestrator(
                agent_manager,
                config_manager=config_manager,
                entity_maintenance_service=projects.entity_maintenance_service,
                project_maintenance_service=projects.maintenance_service,
            )
            sessions = SessionManager(
                resources=resources,
                user_name=user_name,
                project_manager=projects,
                agent_orchestrator=agent_orchestrator,
                config_manager=config_manager,
            )
            aac_runtime = await AACRuntime.create(
                user_name=user_name,
                resources=resources,
                agent_manager=agent_manager,
                config_provider=ConfigManager,
            )
            await aac_runtime.start()
            return cls(
                config_manager=config_manager,
                resources=resources,
                projects=projects,
                sessions=sessions,
                agent_manager=agent_manager,
                agent_orchestrator=agent_orchestrator,
                aac_runtime=aac_runtime,
            )
        except Exception:
            if "aac_runtime" in locals() and aac_runtime is not None:
                try:
                    await aac_runtime.shutdown()
                except Exception:
                    logger.exception("AAC runtime cleanup failed during application startup")
            if "projects" in locals() and projects is not None:
                try:
                    await projects.shutdown()
                except Exception:
                    logger.exception("Project manager cleanup failed during application startup")
            try:
                await resources.shutdown()
            except Exception:
                logger.exception("Runtime resource cleanup failed during application startup")
            raise

    async def shutdown(self) -> None:
        """Retry failed cleanup without closing dependencies of live consumers."""

        async with self._shutdown_lock:
            cleanup = asyncio.create_task(self._shutdown_locked())
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                try:
                    await settle_owned_task(cleanup)
                except Exception:
                    logger.exception("Application cleanup failed while caller was cancelled")
                raise

    async def _shutdown_locked(self) -> None:

        if self._shutdown_complete:
            return

        self.health_service.mark_closing()
        failures: list[ShutdownFailure] = []
        for phase, owner in (
            ("aac", self.aac_runtime),
            ("sessions", self.sessions),
            ("projects", self.projects),
            ("resources", self.resources),
        ):
            if phase in self._completed_shutdown_phases:
                continue
            if phase == "projects" and "sessions" not in self._completed_shutdown_phases:
                continue
            if phase == "resources" and not {"aac", "sessions", "projects"}.issubset(
                self._completed_shutdown_phases
            ):
                continue
            try:
                logger.info(f"Application shutdown phase started: {phase}")
                await owner.shutdown()
                self._completed_shutdown_phases.add(phase)
                logger.info(f"Application shutdown phase completed: {phase}")
            except Exception as exc:
                logger.exception(f"Application shutdown phase failed: {phase}")
                failures.append(ShutdownFailure(phase=phase, error=exc))

        if failures:
            error = ApplicationShutdownError(tuple(failures))
            self._shutdown_error = error
            raise error from failures[0].error
        self._shutdown_error = None
        self._shutdown_complete = True

    def application_port(self, *, default_domain_config=None):
        """Return the public application adapter for this live runtime."""

        from runtime.api_port import ApplicationRuntimePort

        return ApplicationRuntimePort(
            self,
            default_domain_config=default_domain_config,
        )
