import asyncio
from datetime import datetime, timezone
from typing import Dict, Optional

from loguru import logger
import redis.asyncio as aioredis
from common.utils.events import emit
from common.infra.redis import RedisKeys
from jobs.base import BaseJob, JobContext


class Scheduler:
    """
    Generic job scheduler with inactivity-based triggering.
    Jobs register themselves and define their own trigger conditions.
    """
    
    CHECK_INTERVAL = 30
    JOB_EXECUTION_TIMEOUT = 300
    
    def __init__(self, user_name: str, session_id: str, redis: aioredis.Redis, resources=None):
        self.user_name = user_name
        self.session_id = session_id
        self.redis = redis
        self.resources = resources
        self._jobs: Dict[str, BaseJob] = {}
        self._last_runs: Dict[str, datetime] = {}
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self._monitor_task: Optional[asyncio.Task] = None
        self._is_running = False
    
    def register(self, job: BaseJob) -> "Scheduler":
        """Register a job. Returns self for chaining."""
        self._jobs[job.name] = job
        logger.info(f"Registered job: {job.name}")
        return self
    
    async def _build_context(self) -> JobContext:
        idle_seconds = await self._get_idle_seconds()
        return JobContext(
            user_name=self.user_name,
            session_id=self.session_id,
            redis=self.redis,
            idle_seconds=idle_seconds,
            resources=self.resources
        )
    
    async def start(self):
        """Start the scheduler loop."""
        self._is_running = True
        
        await self._run_pending_checks()
        
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        await emit(
            self.session_id,
            "job",
            "scheduler_started",
            {"jobs": list(self._jobs.keys())}
        )
        logger.info(f"Scheduler started with {len(self._jobs)} jobs: {list(self._jobs.keys())}")
    
    async def stop(self):
        """Graceful shutdown - notify all jobs."""
        self._is_running = False
        
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        for task in list(self._running_tasks.values()):
            if not task.done():
                try:
                    await asyncio.wait_for(task, timeout=30.0)
                except asyncio.TimeoutError:
                    logger.warning("Job timed out during shutdown, cancelling task")
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
        
        ctx = await self._build_context()
        for job in self._jobs.values():
            try:
                await job.on_shutdown(ctx)
            except Exception as e:
                logger.error(f"Job {job.name} shutdown failed: {e}")
        
        await emit(self.session_id, "job", "scheduler_stopped", {})
        
        logger.info("Scheduler stopped")
    
    async def record_activity(self):
        """Record user activity timestamp. Call on each user message."""
        await self.redis.set(
            RedisKeys.last_activity(self.user_name, self.session_id), 
            datetime.now(timezone.utc).isoformat()
        )
    
    
    async def _get_idle_seconds(self) -> float:
        """Calculate seconds since last user activity."""
        last_activity = await self.redis.get(RedisKeys.last_activity(self.user_name, self.session_id))
        if not last_activity:
            return 0.0
        last_ts = datetime.fromisoformat(last_activity)
        return (datetime.now(timezone.utc) - last_ts).total_seconds()
    
    async def _run_pending_checks(self):
        """Check for work pending from previous session."""
        ctx = await self._build_context()
        
        for job_name, job in self._jobs.items():
            if not getattr(job, 'enabled', True):
                continue
            pending_key = RedisKeys.job_pending(self.user_name, self.session_id, job_name)
            if await self.redis.get(pending_key):
                logger.info(f"Found pending work for job: {job_name}")
                await self.redis.delete(pending_key)
                await self._execute_job(job, ctx)
    
    async def _monitor_loop(self):
        """Main loop - check jobs periodically."""
        while self._is_running:
            try:
                await asyncio.sleep(self.CHECK_INTERVAL)
                
                ctx = await self._build_context()
                
                for job_name, job in self._jobs.items():
                    ctx.last_run = self._last_runs.get(job_name)
                    
                    try:
                        current_task = self._running_tasks.get(job_name)
                        if current_task and not current_task.done():
                            logger.debug(f"Skipping {job_name}: previous run still active.")
                            continue

                        if not getattr(job, 'enabled', True):
                            continue
                        if await job.should_run(ctx):
                            task = asyncio.create_task(self._execute_job(job, ctx))
                            self._running_tasks[job_name] = task
                            task.add_done_callback(lambda t, name=job_name: self._cleanup_task(name, t))

                    except Exception as e:
                        logger.error(f"Job {job_name} check failed: {e}")
            except Exception as e:
                logger.error(f"Scheduler monitor loop error: {e}")
    
    async def _execute_job(self, job: BaseJob, ctx: JobContext):
        """Execute a single job with error handling."""
        logger.info(f"Executing job: {job.name}")
        await emit(ctx.session_id, "job", "started", {"name": job.name})
        try:
            result = await asyncio.wait_for(
                job.execute(ctx), 
                timeout=self.JOB_EXECUTION_TIMEOUT
            )
            await emit(ctx.session_id, "job", "completed", {
                "name": job.name,
                "success": result.success,
                "summary": result.summary
            })
            self._last_runs[job.name] = datetime.now(timezone.utc)
            
            if result.summary:
                logger.info(f"Job {job.name}: {result.summary}")
            
            if result.reschedule_seconds:
                task = asyncio.create_task(self._delayed_run(job, result.reschedule_seconds))
                self._running_tasks[job.name] = task
                task.add_done_callback(lambda t, name=job.name: self._cleanup_task(name, t))
        
        except asyncio.TimeoutError:
            logger.error(f"Job {job.name} timed out after {self.JOB_EXECUTION_TIMEOUT}s")
            await emit(ctx.session_id, "job", "timeout", {"name": job.name})
            
        except Exception as e:
            await emit(ctx.session_id, "job", "failed", {
                "name": job.name,
                "error": str(e)
            })
            logger.error(f"Job {job.name} execution failed: {e}")
    
    async def _delayed_run(self, job: BaseJob, delay: float):
        """Run a job after a delay."""
        await asyncio.sleep(delay)
        if self._is_running:
            ctx = await self._build_context()
            await self._execute_job(job, ctx)
    
    def _cleanup_task(self, job_name: str, task: asyncio.Task):
        """Remove finished task from tracking."""
        if self._running_tasks.get(job_name) is task:
            del self._running_tasks[job_name]