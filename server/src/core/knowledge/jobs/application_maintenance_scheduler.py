"""Application-owned deterministic maintenance trigger."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

from loguru import logger

from common.conf.manager import ConfigManager
from core.knowledge.db.writers.artifact_retention_writer import ArtifactRetentionWriter
from infrastructure.background_work import BackgroundWorkCoordinator


class ApplicationMaintenanceScheduler:
    """Run cheap global maintenance preflight independently of project leases.

    Entity identity is user-global and may need inspection while every
    ``ProjectRuntime`` is unloaded.  This small trigger owns only the
    deterministic preflight and bounded artifact expiry. When the preflight
    finds duplicate candidates, a bounded model pass reviews them at most once
    per `jobs.duplicate_review.interval_hours`.
    """

    def __init__(
        self,
        *,
        maintenance_service: Any,
        user_name: str,
        artifact_retention_writer: ArtifactRetentionWriter | None = None,
        project_file_cleanup: Callable[[], Awaitable[None]] | None = None,
        background_work: BackgroundWorkCoordinator | None = None,
        config_manager: ConfigManager | None = None,
        interval_seconds: float = 300.0,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self.maintenance_service = maintenance_service
        self.user_name = user_name
        self.artifact_retention_writer = artifact_retention_writer
        self.project_file_cleanup = project_file_cleanup
        self.background_work = background_work
        self._config_manager = config_manager
        self.interval_seconds = float(interval_seconds)
        self._last_duplicate_review_at: float | None = None
        self._last_duplicate_review: dict[str, Any] | None = None
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

    async def review_duplicates_if_due(self) -> dict[str, Any] | None:
        """Run the duplicate review when enabled, due, and the last preflight
        found candidates; None when it did not run."""

        settings = (self._config_manager or ConfigManager.get()).config.developer_settings.jobs.duplicate_review
        due = (
            self._last_duplicate_review_at is None
            or time.monotonic() - self._last_duplicate_review_at >= settings.interval_hours * 3600
        )
        if not (settings.enabled and due and (self._last_result or {}).get("llm_required")):
            return None
        self._last_duplicate_review_at = time.monotonic()

        def review():
            return self.maintenance_service.review_duplicates(max_pairs=settings.max_pairs_per_run)

        if self.background_work is None:
            result = await review()
        else:
            result = await self.background_work.submit(
                "__application__", review,
                name="entity-duplicate-review",
                owner="application:entity-maintenance",
                coalesce_key="entity-duplicate-review",
            )
        logger.info("Duplicate entity review: {}", result)
        self._last_duplicate_review = dict(result)
        return self._last_duplicate_review

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
                if self.project_file_cleanup is not None:
                    try:
                        await self.project_file_cleanup()
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception("Pending project file cleanup failed")
                try:
                    await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Application maintenance preflight failed")
                try:
                    await self.review_duplicates_if_due()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Duplicate entity review failed")
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
            "last_duplicate_review": self._last_duplicate_review,
            "artifact_retention": {
                "last_deleted_count": self._last_artifact_deleted_count,
                "last_sweep_succeeded": self._last_artifact_sweep_succeeded,
            },
        }
