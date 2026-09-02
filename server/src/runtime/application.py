"""Top-level application runtime and ordered shutdown ownership."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from loguru import logger

from common.conf.manager import ConfigManager
from common.utils.time_utils import get_now
from core.agent.orchestrator import AgentOrchestrator
from core.agent.services.agent_manager import AgentManager
from core.community.runtime import AACRuntime
from core.health.service import RuntimeHealthService
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

    resources: RuntimeResources
    projects: ProjectManager
    sessions: SessionManager
    agent_manager: AgentManager
    agent_orchestrator: AgentOrchestrator
    aac_runtime: AACRuntime
    health_service: RuntimeHealthService = field(init=False)
    started_at: datetime = field(init=False)
    _shutdown_complete: bool = field(init=False, default=False, repr=False)
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
        num_workers: int | None = None,
    ) -> "ApplicationRuntime":
        """Build the canonical runtime whose shutdown owns every live layer."""

        resources = await RuntimeResources.create(num_workers=num_workers)
        try:
            knowledge_store = resources.knowledge_store
            if knowledge_store is None:
                raise RuntimeError("Runtime resources did not initialize KnowledgeStore")
            await knowledge_store.ensure_identity_entity(
                user_name,
                ConfigManager.get().config.user_aliases,
            )
            projects = ProjectManager(resources=resources, user_name=user_name)
            await projects.start()
            agent_manager = AgentManager(resources, user_name)
            await agent_manager.ensure_default_agent()
            agent_orchestrator = AgentOrchestrator(
                agent_manager,
                config_provider=ConfigManager,
            )
            sessions = SessionManager(
                resources=resources,
                user_name=user_name,
                project_manager=projects,
                agent_orchestrator=agent_orchestrator,
            )
            aac_runtime = await AACRuntime.create(
                user_name=user_name,
                resources=resources,
                agent_manager=agent_manager,
                config_provider=ConfigManager,
            )
            await aac_runtime.start()
            return cls(
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
        """Release application-owned work in the only safe dependency order."""

        if self._shutdown_complete:
            if self._shutdown_error is not None:
                raise self._shutdown_error
            return

        self.health_service.mark_closing()
        failures: list[ShutdownFailure] = []
        for phase, owner in (
            ("aac", self.aac_runtime),
            ("sessions", self.sessions),
            ("projects", self.projects),
            ("resources", self.resources),
        ):
            try:
                logger.info(f"Application shutdown phase started: {phase}")
                await owner.shutdown()
                logger.info(f"Application shutdown phase completed: {phase}")
            except Exception as exc:
                logger.exception(f"Application shutdown phase failed: {phase}")
                failures.append(ShutdownFailure(phase=phase, error=exc))

        self._shutdown_complete = True
        if failures:
            error = ApplicationShutdownError(tuple(failures))
            self._shutdown_error = error
            raise error from failures[0].error

    def application_port(self, *, default_domain_config=None):
        """Return the public application adapter for this live runtime."""

        from runtime.api_port import ApplicationRuntimePort

        return ApplicationRuntimePort(
            self,
            default_domain_config=default_domain_config,
        )
