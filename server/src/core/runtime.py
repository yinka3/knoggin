"""Top-level application runtime and ordered shutdown ownership."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from loguru import logger

from common.utils.time_utils import get_now
from core.health.service import RuntimeHealthService
from core.project.project_manager import ProjectManager
from core.session.session_manager import SessionManager
from runtime.resources import ResourceManager


class ShutdownOwner(Protocol):
    """A top-level runtime component with explicit asynchronous cleanup."""

    async def shutdown(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ShutdownFailure:
    """One phase failure collected while the remaining shutdown phases continue."""

    phase: str
    error: Exception


class ApplicationShutdownError(RuntimeError):
    """Raised only after every top-level shutdown phase has been attempted."""

    def __init__(self, failures: tuple[ShutdownFailure, ...]) -> None:
        self.failures = failures
        phases = ", ".join(failure.phase for failure in failures)
        super().__init__(f"Application shutdown failed in phase(s): {phases}")


class ApplicationShutdownCoordinator:
    """Own the one ordered, idempotent application shutdown sequence.

    Sessions release their project-runtime leases.  Any remaining project state
    must then stop its own jobs before shared storage, model, and worker
    resources are released.  Each phase is attempted even if an earlier one
    fails, so a partial shutdown never intentionally leaks later resources.
    """

    def __init__(
        self,
        *,
        sessions: ShutdownOwner,
        projects: ShutdownOwner,
        resources: ShutdownOwner,
    ) -> None:
        self._sessions = sessions
        self._projects = projects
        self._resources = resources
        self._lock = asyncio.Lock()
        self._shutdown_task: asyncio.Task[None] | None = None

    async def shutdown(self) -> None:
        """Run shutdown once; concurrent callers join the same cleanup task."""

        async with self._lock:
            if self._shutdown_task is None:
                self._shutdown_task = asyncio.create_task(
                    self._shutdown(),
                    name="application-shutdown",
                )
            shutdown_task = self._shutdown_task

        await asyncio.shield(shutdown_task)

    async def _shutdown(self) -> None:
        failures: list[ShutdownFailure] = []
        for phase, owner in (
            ("sessions", self._sessions),
            ("projects", self._projects),
            ("resources", self._resources),
        ):
            try:
                logger.info(f"Application shutdown phase started: {phase}")
                await owner.shutdown()
                logger.info(f"Application shutdown phase completed: {phase}")
            except Exception as exc:
                logger.exception(f"Application shutdown phase failed: {phase}")
                failures.append(ShutdownFailure(phase=phase, error=exc))

        if failures:
            raise ApplicationShutdownError(tuple(failures)) from failures[0].error


@dataclass(slots=True)
class ApplicationRuntime:
    """The application's root owner of shared resources, projects, and sessions."""

    resources: ResourceManager
    projects: ProjectManager
    sessions: SessionManager
    shutdown_coordinator: ApplicationShutdownCoordinator = field(init=False)
    health_service: RuntimeHealthService = field(init=False)
    started_at: datetime = field(init=False)

    def __post_init__(self) -> None:
        self.started_at = get_now()
        self.health_service = RuntimeHealthService(
            resources=self.resources,
            projects=self.projects,
            active_sessions=getattr(self.sessions, "active_sessions", {}),
            started_at=self.started_at,
        )
        # Session-created agent tools reach the application-owned service
        # through the shared resource graph without taking ownership of it.
        try:
            self.resources.health_service = self.health_service
        except AttributeError:
            pass
        self.shutdown_coordinator = ApplicationShutdownCoordinator(
            sessions=self.sessions,
            projects=self.projects,
            resources=self.resources,
        )

    @classmethod
    async def start(
        cls,
        *,
        user_name: str,
        num_workers: int | None = None,
    ) -> "ApplicationRuntime":
        """Build the canonical runtime whose shutdown owns every live layer."""

        resources = await ResourceManager.initialize(num_workers=num_workers)
        projects = ProjectManager(resources=resources, user_name=user_name)
        sessions = SessionManager(
            resources=resources,
            user_name=user_name,
            active_sessions={},
            project_manager=projects,
        )
        return cls(resources=resources, projects=projects, sessions=sessions)

    async def shutdown(self) -> None:
        """Release application-owned work in the only safe dependency order."""

        self.health_service.mark_closing()
        await self.shutdown_coordinator.shutdown()
