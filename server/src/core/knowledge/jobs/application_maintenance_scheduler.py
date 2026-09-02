"""Application-owned deterministic maintenance trigger."""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

from infrastructure.background_work import BackgroundWorkCoordinator


class ApplicationMaintenanceScheduler:
    """Run cheap global maintenance preflight independently of project leases.

    Entity identity is user-global and may need inspection while every
    ``ProjectRuntime`` is unloaded.  This small trigger owns only the
    deterministic preflight; a future bounded model pass can be admitted from
    its result without coupling that work to a project scheduler.
    """

    def __init__(
        self,
        *,
        maintenance_service: Any,
        user_name: str,
        background_work: BackgroundWorkCoordinator | None = None,
        interval_seconds: float = 300.0,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self.maintenance_service = maintenance_service
        self.user_name = user_name
        self.background_work = background_work
        self.interval_seconds = float(interval_seconds)
        self._task: asyncio.Task | None = None
        self._stopping = False
        self._last_result: dict[str, Any] | None = None

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

    def snapshot(self) -> dict[str, Any]:
        return {
            "running": self.running,
            "interval_seconds": self.interval_seconds,
            "last_result": self._last_result,
        }
