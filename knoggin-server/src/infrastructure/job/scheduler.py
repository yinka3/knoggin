import asyncio
import uuid
from typing import Dict, Optional

import redis.asyncio as aioredis
from loguru import logger

from common.utils.events import emit
from common.utils.time_utils import get_now, get_now_iso, get_now_unix, parse_iso_time
from infrastructure.job.base import BaseJob, JobContext
from infrastructure.redis_client import RedisKeys


class Scheduler:
    """
    Generic job scheduler with inactivity-based triggering.
    Jobs register themselves and define their own trigger conditions.
    """

    CHECK_INTERVAL = 30
    JOB_EXECUTION_TIMEOUT = 300
    SHUTDOWN_TIMEOUT = 30
    LEASE_GRACE_SECONDS = 60
    _RELEASE_LEASE_SCRIPT = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    end
    return 0
    """

    def __init__(
        self,
        user_name: str,
        project_id: str,
        redis: aioredis.Redis,
    ):
        self.user_name = user_name
        self.project_id = project_id
        self.redis = redis
        self._jobs: Dict[str, BaseJob] = {}
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self._monitor_task: Optional[asyncio.Task] = None
        self._is_running = False

    @property
    def running(self) -> bool:
        return self._is_running

    def register(self, job: BaseJob) -> "Scheduler":
        """Register a job. Returns self for chaining."""
        if job.name in self._jobs:
            raise ValueError(f"Job '{job.name}' is already registered")
        self._jobs[job.name] = job
        logger.info(f"Registered job: {job.name}")
        return self

    async def _build_context(self) -> JobContext:
        idle_seconds = await self._get_idle_seconds()
        return JobContext(
            user_name=self.user_name,
            project_id=self.project_id,
            idle_seconds=idle_seconds,
        )

    async def start(self):
        """Start the scheduler loop."""
        if self._is_running:
            return

        self._is_running = True
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
            f"Scheduler started with {len(self._jobs)} jobs: {list(self._jobs.keys())}"
        )

    async def stop(self):
        """Stop monitoring and give active jobs one shared shutdown deadline."""
        if (
            not self._is_running
            and self._monitor_task is None
            and not self._running_tasks
        ):
            return

        self._is_running = False

        if self._monitor_task:
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
                    f"Cancelling {len(pending)} jobs after shutdown timeout"
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        self._running_tasks.clear()
        await self._safe_emit(self.project_id, "job", "scheduler_stopped", {})

        logger.info("Scheduler stopped")

    async def record_activity(self):
        """Record user activity timestamp. Call on each user message."""
        await self.redis.set(
            RedisKeys.project_last_activity(self.user_name, self.project_id),
            get_now_iso(),
        )

    async def _get_idle_seconds(self) -> float:
        """Calculate seconds since last user activity."""
        last_activity = await self.redis.get(
            RedisKeys.project_last_activity(self.user_name, self.project_id)
        )
        if not last_activity:
            return 0.0
        last_ts = parse_iso_time(last_activity)
        if not last_ts:
            return 0.0
        return (get_now() - last_ts).total_seconds()

    async def _check_jobs(self) -> None:
        """Evaluate every registered job against its normal trigger policy."""
        ctx = await self._build_context()
        for job_name, job in self._jobs.items():
            try:
                await self._schedule_if_due(job_name, job, ctx)
            except Exception as exc:
                logger.exception(f"Job {job_name} check failed: {exc}")

    async def _monitor_loop(self):
        """Main loop - check jobs periodically."""
        while self._is_running:
            try:
                await asyncio.sleep(self.CHECK_INTERVAL)
                if self._is_running:
                    await self._check_jobs()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception(f"Scheduler monitor loop error: {exc}")

    async def _schedule_if_due(
        self,
        job_name: str,
        job: BaseJob,
        ctx: JobContext,
    ) -> None:
        current_task = self._running_tasks.get(job_name)
        if current_task and not current_task.done():
            logger.debug(f"Skipping {job_name}: previous run still active")
            return
        if not job.enabled:
            return

        lease = await self._acquire_lease(ctx, job)
        if lease is None:
            return
        lease_key, lease_token = lease

        try:
            trigger_due = await job.should_run(ctx)
            cadence_due = (
                False if trigger_due else await self._is_cadence_due(job, ctx)
            )
            if not trigger_due and not cadence_due:
                await self._release_lease(lease_key, lease_token)
                return
            task = asyncio.create_task(
                self._execute_job(
                    job,
                    ctx,
                    lease_key=lease_key,
                    lease_token=lease_token,
                ),
                name=f"job:{self.user_name}:{self.project_id}:{job_name}",
            )
        except BaseException:
            await self._release_lease(lease_key, lease_token)
            raise

        self._running_tasks[job_name] = task
        task.add_done_callback(
            lambda completed, name=job_name: self._cleanup_task(name, completed)
        )

    async def _execute_job(
        self,
        job: BaseJob,
        ctx: JobContext,
        *,
        lease_key: Optional[str] = None,
        lease_token: Optional[str] = None,
    ):
        """Execute a single job with error handling."""
        if lease_key is None or lease_token is None:
            lease = await self._acquire_lease(ctx, job)
            if lease is None:
                return None
            lease_key, lease_token = lease

        logger.info(f"Executing job: {job.name}")
        try:
            await self._safe_emit(
                ctx.project_id,
                "job",
                "started",
                {"name": job.name},
            )
            result = await asyncio.wait_for(
                job.execute(ctx), timeout=self.JOB_EXECUTION_TIMEOUT
            )
            if result.success:
                await self._record_cadence_success(job, ctx)
            event = "completed" if result.success else "failed"
            await self._safe_emit(
                ctx.project_id,
                "job",
                event,
                {
                    "name": job.name,
                    "success": result.success,
                    "summary": result.summary,
                },
            )

            if result.summary:
                logger.info(f"Job {job.name}: {result.summary}")
            return result
        except TimeoutError:
            logger.error(
                f"Job {job.name} timed out after {self.JOB_EXECUTION_TIMEOUT}s"
            )
            await self._safe_emit(
                ctx.project_id,
                "job",
                "timeout",
                {"name": job.name},
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._safe_emit(
                ctx.project_id,
                "job",
                "failed",
                {"name": job.name, "error": str(exc)},
            )
            logger.exception(f"Job {job.name} execution failed: {exc}")
        finally:
            await self._release_lease(lease_key, lease_token)

        return None

    async def _is_cadence_due(self, job: BaseJob, ctx: JobContext) -> bool:
        cadence_seconds = getattr(job, "cadence_seconds", None)
        if cadence_seconds is None:
            return False
        if cadence_seconds <= 0:
            raise ValueError(
                f"Job '{job.name}' cadence must be greater than zero seconds"
            )

        last_run_key = RedisKeys.job_last_run(
            job.name,
            ctx.user_name,
            ctx.project_id,
        )
        last_run = await self.redis.get(last_run_key)
        if last_run is None:
            if getattr(job, "run_immediately_on_first_check", False):
                return True
            await self.redis.set(last_run_key, get_now_unix())
            return False

        try:
            elapsed = get_now_unix() - float(last_run)
        except (TypeError, ValueError):
            logger.warning(
                f"Job {job.name} has an invalid cadence timestamp; running now"
            )
            return True
        return elapsed >= cadence_seconds

    async def _record_cadence_success(
        self,
        job: BaseJob,
        ctx: JobContext,
    ) -> None:
        if getattr(job, "cadence_seconds", None) is None:
            return
        await self.redis.set(
            RedisKeys.job_last_run(job.name, ctx.user_name, ctx.project_id),
            get_now_unix(),
        )

    async def _acquire_lease(
        self,
        ctx: JobContext,
        job: BaseJob,
    ) -> Optional[tuple[str, str]]:
        lease_key = RedisKeys.job_lease(ctx.user_name, ctx.project_id, job.name)
        lease_token = uuid.uuid4().hex
        lease_seconds = self.JOB_EXECUTION_TIMEOUT + self.LEASE_GRACE_SECONDS
        acquired = await self.redis.set(
            lease_key,
            lease_token,
            ex=lease_seconds,
            nx=True,
        )
        if not acquired:
            logger.debug(f"Skipping {job.name}: execution lease is already held")
            return None
        return lease_key, lease_token

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
                f"Failed to emit scheduler event '{event}' for {scope_id}: {exc}"
            )

    async def _release_lease(self, lease_key: str, lease_token: str) -> None:
        try:
            await self.redis.eval(
                self._RELEASE_LEASE_SCRIPT,
                1,
                lease_key,
                lease_token,
            )
        except Exception as exc:
            logger.warning(f"Failed to release job lease {lease_key}: {exc}")

    def _cleanup_task(self, job_name: str, task: asyncio.Task):
        """Observe task failures and remove the completed task from tracking."""
        if not task.cancelled():
            try:
                error = task.exception()
            except Exception as exc:
                logger.error(f"Could not inspect job task {job_name}: {exc}")
            else:
                if error is not None:
                    logger.error(f"Unhandled job task failure for {job_name}: {error}")
        if self._running_tasks.get(job_name) is task:
            del self._running_tasks[job_name]
