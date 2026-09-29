"""Application-owned deterministic maintenance trigger."""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

from core.knowledge.db.writers.artifact_retention_writer import ArtifactRetentionWriter
from infrastructure.background_work import BackgroundWorkCoordinator


class ApplicationMaintenanceScheduler:
    """Run cheap global maintenance preflight independently of project leases.

    Entity identity is user-global and may need inspection while every
    ``ProjectRuntime`` is unloaded.  This small trigger owns only the
    deterministic preflight and bounded artifact expiry; a future bounded model
    pass can be admitted without coupling work to a loaded project scheduler.
    """

    def __init__(
        self,
        *,
        maintenance_service: Any,
        user_name: str,
        artifact_retention_writer: ArtifactRetentionWriter | None = None,
        background_work: BackgroundWorkCoordinator | None = None,
        interval_seconds: float = 300.0,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self.maintenance_service = maintenance_service
        self.user_name = user_name
        self.artifact_retention_writer = artifact_retention_writer
        self.background_work = background_work
        self.interval_seconds = float(interval_seconds)
        self._task: asyncio.Task | None = None
        self._stopping = False
        self._last_result: dict[str, Any] | None = None
        self._last_artifact_deleted_count: int | None = None
        self._last_artifact_sweep_succeeded: bool | None = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def run_once(self) -> dict[str, Any]:
        """Execute one deterministic preflight and retain its bounded result."""
        if self.background_work is None:
            result = await self.maintenance_service.preflight(user_name=self.user_name)
        else:
            result = await self.background_work.submit(
                "__application__",
                lambda: self.maintenance_service.preflight(user_name=self.user_name),
                name="entity-maintenance-preflight",
                owner="application:entity-maintenance",
                coalesce_key="entity-maintenance-preflight",
            )
        self._last_result = dict(result)
        return self._last_result

    async def purge_expired_artifacts(self) -> int:
        """Run one expiry batch even with all project runtimes unloaded."""
        if self.artifact_retention_writer is None:
            return 0
        self._last_artifact_deleted_count = None
        self._last_artifact_sweep_succeeded = False
        if self.background_work is None:
            count = await self.artifact_retention_writer.purge_expired_artifacts(user_name=self.user_name)
        else:
            count = await self.background_work.submit(
                "__application__",
                lambda: self.artifact_retention_writer.purge_expired_artifacts(user_name=self.user_name),
                name="artifact-retention-purge",
                owner="application:artifact-retention",
                coalesce_key="artifact-retention-purge",
            )
        self._last_artifact_deleted_count = count
        self._last_artifact_sweep_succeeded = True
        return count

    async def start(self) -> None:
        """Start the application-owned trigger if it is not already running."""
        if self.running:
            return
        self._stopping = False
        self._task = asyncio.create_task(
            self._run_loop(),
            name=f"application-maintenance:{self.user_name}",
        )

    async def _run_loop(self) -> None:
        try:
            while not self._stopping:
                try:
                    await self.purge_expired_artifacts()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.error("Artifact retention sweep failed; retrying next interval")
                try:
                    await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Application maintenance preflight failed")
                await asyncio.sleep(self.interval_seconds)
        except asyncio.CancelledError:
            raise

    async def stop(self) -> None:
        """Stop this trigger and cancel only its application-owned queue work."""
        self._stopping = True
        task, self._task = self._task, None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if self.background_work is not None:
            await self.background_work.cancel_owner("application:entity-maintenance")
            await self.background_work.cancel_owner("application:artifact-retention")

    def snapshot(self) -> dict[str, Any]:
        return {
            "running": self.running,
            "interval_seconds": self.interval_seconds,
            "last_result": self._last_result,
            "artifact_retention": {
                "last_deleted_count": self._last_artifact_deleted_count,
                "last_sweep_succeeded": self._last_artifact_sweep_succeeded,
            },
        }
