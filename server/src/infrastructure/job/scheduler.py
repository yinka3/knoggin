import asyncio
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

from loguru import logger

from common.utils.diagnostic_context import diagnostic_scope
from common.utils.events import emit
from common.utils.time_utils import get_now, get_now_unix
from infrastructure.background_work import (
    BackgroundWorkCoordinator,
    BackgroundWorkRejected,
)
from infrastructure.job.base import BaseJob, JobContext, JobResult


@dataclass(frozen=True, slots=True)
class ScheduledJobPolicy:
    """Execution rules captured when one scheduled job is admitted."""

    execution_timeout_seconds: float


class Scheduler:
    """Run one project's jobs with local cadence and bounded execution."""

    CHECK_INTERVAL = 30
    JOB_EXECUTION_TIMEOUT = 300
    SHUTDOWN_TIMEOUT = 30

    def __init__(
        self,
        user_name: str,
        project_id: str,
        *,
        background_work: Optional[BackgroundWorkCoordinator] = None,
    ):
        self.user_name = user_name
        self.project_id = project_id
        self.background_work = background_work
        self._jobs: Dict[str, BaseJob] = {}
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self._job_admission_locks: Dict[str, asyncio.Lock] = {}
        self._monitor_task: Optional[asyncio.Task] = None
        self._is_running = False
        self._admissions_closed = False
        self._started_at: datetime | None = None
        self._stopped_at: datetime | None = None
        self._last_successful_runs: dict[str, float] = {}
        self._job_runs: dict[str, dict[str, object]] = {}
        self._recent_outcomes: deque[dict[str, object]] = deque(maxlen=20)

    @property
    def running(self) -> bool:
        return self._is_running

    def register(self, job: BaseJob) -> "Scheduler":
        """Register one project-owned job before scheduler startup."""

        if self._admissions_closed:
            raise RuntimeError("Scheduler is stopping and cannot register jobs")
        if job.name in self._jobs:
            raise ValueError(f"Job '{job.name}' is already registered")
        self._jobs[job.name] = job
        logger.info(f"Registered job: {job.name}")
        return self

    def _capture_job_policy(self) -> ScheduledJobPolicy:
        timeout = self.JOB_EXECUTION_TIMEOUT
        if not isinstance(timeout, (int, float)) or timeout <= 0:
            raise ValueError("JOB_EXECUTION_TIMEOUT must be positive")
        return ScheduledJobPolicy(execution_timeout_seconds=float(timeout))

    async def _build_context(self) -> JobContext:
        return JobContext(
            user_name=self.user_name,
            project_id=self.project_id,
        )

    async def start(self) -> None:
        """Start local job admission and the monitoring loop."""

        if self._is_running:
            return
        if self._admissions_closed:
            raise RuntimeError("Scheduler has stopped permanently")

        self._is_running = True
        self._started_at = get_now()
        self._stopped_at = None
        try:
            await self._check_jobs()
            self._monitor_task = asyncio.create_task(
                self._monitor_loop(),
                name=f"scheduler:{self.user_name}:{self.project_id}",
            )
        except BaseException:
            self._is_running = False
            self._monitor_task = None
            raise
        await self._safe_emit(
            self.project_id,
            "job",
            "scheduler_started",
            {"jobs": list(self._jobs.keys())},
        )
        logger.info(
            "Scheduler started with {} jobs: {}",
            len(self._jobs),
            list(self._jobs.keys()),
        )

    async def stop(self) -> None:
        """Stop admission and cancel unfinished scheduler-owned tasks."""

        if (
            self._admissions_closed
            and self._monitor_task is None
            and not self._running_tasks
        ):
            return

        self._is_running = False
        self._admissions_closed = True
        self._stopped_at = get_now()

        if self._monitor_task is not None:
            self._monitor_task.cancel()
            await asyncio.gather(self._monitor_task, return_exceptions=True)
            self._monitor_task = None

        active_tasks = [
            task for task in self._running_tasks.values() if not task.done()
        ]
        if active_tasks:
            _, pending = await asyncio.wait(
                active_tasks,
                timeout=self.SHUTDOWN_TIMEOUT,
            )
            if pending:
                logger.warning(
                    "Cancelling {} jobs after shutdown timeout", len(pending)
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        self._running_tasks.clear()
        await self._safe_emit(self.project_id, "job", "scheduler_stopped", {})
        logger.info("Scheduler stopped")

    async def _check_jobs(self) -> None:
        """Evaluate all jobs against their local trigger and cadence policy."""

        if not self._is_running:
            return
        ctx = await self._build_context()
        for job_name, job in self._jobs.items():
            try:
                await self._schedule_if_due(job_name, job, ctx)
            except Exception as exc:
                logger.exception("Job {} check failed: {}", job_name, exc)

    async def _monitor_loop(self) -> None:
        while self._is_running:
            try:
                await asyncio.sleep(self.CHECK_INTERVAL)
                await self._check_jobs()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Scheduler monitor loop error: {}", exc)

    async def _schedule_if_due(
        self,
        job_name: str,
        job: BaseJob,
        ctx: JobContext,
    ) -> None:
        admission_lock = self._job_admission_locks.setdefault(job_name, asyncio.Lock())
        async with admission_lock:
            if self._admissions_closed:
                return
            current_task = self._running_tasks.get(job_name)
            if current_task is not None and not current_task.done():
                logger.debug("Skipping {}: previous run still active", job_name)
                return
            if not job.enabled:
                return

            trigger_due = await job.should_run(ctx)
            cadence_due = False if trigger_due else self._is_cadence_due(job)
            if not trigger_due and not cadence_due:
                return

            policy = self._capture_job_policy()
            task = asyncio.create_task(
                self._execute_job(job, ctx, policy=policy),
                name=f"job:{self.user_name}:{self.project_id}:{job_name}",
            )
            self._job_runs[job_name] = {
                "name": job_name,
                "state": "queued",
                "queued_at": get_now(),
                "started_at": None,
                "finished_at": None,
                "execution_timeout_seconds": policy.execution_timeout_seconds,
            }
            self._running_tasks[job_name] = task
            task.add_done_callback(
                lambda completed, name=job_name: self._cleanup_task(name, completed)
            )

    async def _execute_job(
        self,
        job: BaseJob,
        ctx: JobContext,
        *,
        policy: Optional[ScheduledJobPolicy] = None,
    ) -> JobResult | None:
        """Run one job directly or through the shared bounded queue."""

        policy = policy or self._capture_job_policy()
        if self.background_work is not None:
            try:
                return await self.background_work.submit(
                    ctx.project_id,
                    lambda: self._execute_job_now(job, ctx, policy=policy),
                    name=job.name,
                    coalesce_key=f"job:{job.name}",
                )
            except BackgroundWorkRejected as exc:
                logger.warning(exc.message)
                self._finish_job_run(job.name, "failed")
                await self._safe_emit(
                    ctx.project_id,
                    "job",
                    "admission_rejected",
                    {"name": job.name, **exc.details},
                )
                return JobResult(success=False, summary=exc.message)
        return await self._execute_job_now(job, ctx, policy=policy)

    async def _execute_job_now(
        self,
        job: BaseJob,
        ctx: JobContext,
        *,
        policy: ScheduledJobPolicy,
    ) -> JobResult | None:
        with diagnostic_scope(user_name=ctx.user_name, project_id=ctx.project_id):
            return await self._execute_job_now_scoped(job, ctx, policy=policy)

    async def _execute_job_now_scoped(
        self,
        job: BaseJob,
        ctx: JobContext,
        *,
        policy: ScheduledJobPolicy,
    ) -> JobResult | None:
        logger.info("Executing job: {}", job.name)
        self._mark_job_running(job.name, policy)
        try:
            await self._safe_emit(ctx.project_id, "job", "started", {"name": job.name})
            async with asyncio.timeout(policy.execution_timeout_seconds):
                result = await job.execute(ctx)
            if result.success:
                self._record_cadence_success(job)
            state = "completed" if result.success else "failed"
            self._finish_job_run(job.name, state)
            await self._safe_emit(
                ctx.project_id,
                "job",
                "completed" if result.success else "failed",
                {
                    "name": job.name,
                    "success": result.success,
                    "summary": result.summary,
                },
            )
            if result.summary:
                logger.info("Job {}: {}", job.name, result.summary)
            return result
        except TimeoutError:
            self._finish_job_run(job.name, "timed_out")
            logger.error(
                "Job {} timed out after {}s",
                job.name,
                policy.execution_timeout_seconds,
            )
            await self._safe_emit(ctx.project_id, "job", "timeout", {"name": job.name})
        except asyncio.CancelledError:
            self._finish_job_run(job.name, "cancelled")
            logger.info("Job {} cancelled", job.name)
            await self._safe_emit(ctx.project_id, "job", "cancelled", {"name": job.name})
            raise
        except Exception as exc:
            self._finish_job_run(job.name, "failed")
            await self._safe_emit(
                ctx.project_id,
                "job",
                "failed",
                {"name": job.name, "error": str(exc)},
            )
            logger.exception("Job {} execution failed: {}", job.name, exc)
        return None

    def _mark_job_running(
        self,
        job_name: str,
        policy: ScheduledJobPolicy,
    ) -> None:
        run = self._job_runs.setdefault(
            job_name,
            {"name": job_name, "queued_at": get_now(), "finished_at": None},
        )
        run.update(
            {
                "state": "running",
                "started_at": get_now(),
                "execution_timeout_seconds": policy.execution_timeout_seconds,
            }
        )

    def _finish_job_run(self, job_name: str, state: str) -> None:
        run = self._job_runs.setdefault(
            job_name,
            {"name": job_name, "queued_at": get_now()},
        )
        finished_at = get_now()
        run["state"] = state
        run["finished_at"] = finished_at
        started_at = run.get("started_at") or run.get("queued_at")
        elapsed = (
            (finished_at - started_at).total_seconds()
            if isinstance(started_at, datetime)
            else None
        )
        if isinstance(elapsed, (int, float)) and elapsed >= 0:
            run["elapsed_seconds"] = round(elapsed, 3)
        outcome = {
            "name": str(job_name)[:100],
            "state": state,
            "finished_at": finished_at,
        }
        if isinstance(run.get("started_at"), type(finished_at)):
            outcome["started_at"] = run["started_at"]
        if "elapsed_seconds" in run:
            outcome["elapsed_seconds"] = run["elapsed_seconds"]
        self._recent_outcomes.append(outcome)

    def snapshot_for_health(self) -> dict[str, object]:
        """Return a bounded local scheduler projection for health reporting."""

        now = get_now()
        active_jobs: list[dict[str, object]] = []
        queued_jobs = 0
        running_jobs = 0
        stalled_jobs = 0
        for name, task in self._running_tasks.items():
            if task.done():
                continue
            run = self._job_runs.get(name) or {
                "name": name,
                "state": "queued",
                "queued_at": now,
            }
            state = run.get("state", "queued")
            reference = run.get("started_at") or run.get("queued_at")
            elapsed = (
                (now - reference).total_seconds()
                if isinstance(reference, datetime)
                else 0.0
            )
            timeout = run.get("execution_timeout_seconds")
            is_stalled = (
                state in {"queued", "running"}
                and isinstance(timeout, (int, float))
                and elapsed > timeout
            )
            if is_stalled:
                state = "stalled"
                stalled_jobs += 1
            elif state == "queued":
                queued_jobs += 1
            elif state == "running":
                running_jobs += 1
            active_jobs.append(
                {
                    "name": str(name)[:100],
                    "state": state,
                    "started_at": run.get("started_at"),
                    "queued_at": run.get("queued_at"),
                    "elapsed_seconds": round(max(elapsed, 0.0), 3),
                    "execution_timeout_seconds": timeout,
                }
            )

        successful = [
            outcome.get("finished_at")
            for outcome in self._recent_outcomes
            if outcome.get("state") == "completed"
        ]
        return {
            "state": "running" if self._is_running else "stopped",
            "started_at": self._started_at,
            "stopped_at": self._stopped_at,
            "uptime_seconds": (
                max((now - self._started_at).total_seconds(), 0.0)
                if self._is_running and self._started_at is not None
                else 0.0
            ),
            "registered_jobs": [str(name)[:100] for name in sorted(self._jobs)[:100]],
            "enabled_jobs": [
                str(name)[:100]
                for name in sorted(name for name, job in self._jobs.items() if job.enabled)[
                    :100
                ]
            ],
            "active_jobs": active_jobs[:20],
            "queued_jobs": queued_jobs,
            "running_jobs": running_jobs,
            "stalled_jobs": stalled_jobs,
            "recent_outcomes": list(self._recent_outcomes),
            "recent_failed_jobs": sum(
                outcome.get("state") in {"failed", "timed_out"}
                for outcome in self._recent_outcomes
            ),
            "last_successful_job_at": max(successful) if successful else None,
        }

    def health_snapshot(self) -> dict[str, object]:
        return self.snapshot_for_health()

    def _is_cadence_due(self, job: BaseJob) -> bool:
        cadence_seconds = getattr(job, "cadence_seconds", None)
        if cadence_seconds is None:
            return False
        if cadence_seconds <= 0:
            raise ValueError(
                f"Job '{job.name}' cadence must be greater than zero seconds"
            )

        last_run = self._last_successful_runs.get(job.name)
        if last_run is None:
            if getattr(job, "run_immediately_on_first_check", False):
                return True
            self._last_successful_runs[job.name] = get_now_unix()
            return False
        return get_now_unix() - last_run >= cadence_seconds

    def _record_cadence_success(self, job: BaseJob) -> None:
        if getattr(job, "cadence_seconds", None) is not None:
            self._last_successful_runs[job.name] = get_now_unix()

    async def _safe_emit(
        self,
        scope_id: str,
        component: str,
        event: str,
        data: dict,
    ) -> None:
        try:
            await emit(scope_id, component, event, data)
        except Exception as exc:
            logger.warning(
                "Failed to emit scheduler event '{}' for {}: {}",
                event,
                scope_id,
                exc,
            )

    def _cleanup_task(self, job_name: str, task: asyncio.Task) -> None:
        """Observe task failures and remove completed work from local tracking."""

        run = self._job_runs.get(job_name)
        if not task.cancelled():
            try:
                error = task.exception()
            except Exception as exc:
                logger.error("Could not inspect job task {}: {}", job_name, exc)
            else:
                if error is not None:
                    if not run or run.get("state") not in {
                        "failed",
                        "timed_out",
                        "cancelled",
                    }:
                        self._finish_job_run(job_name, "failed")
                    logger.error("Unhandled job task failure for {}: {}", job_name, error)
        elif not run or run.get("state") not in {
            "failed",
            "timed_out",
            "cancelled",
        }:
            self._finish_job_run(job_name, "cancelled")
        if self._running_tasks.get(job_name) is task:
            del self._running_tasks[job_name]
