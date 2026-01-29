import json
import time
from loguru import logger
from jobs.base import BaseJob, JobContext, JobResult
from shared.redisclient import RedisKeys

class DLQReplayJob(BaseJob):
    """
    Periodically checks the Dead Letter Queue.
    - Retries 'transient' errors (network blips).
    - Parks 'fatal' errors (code bugs) so they don't loop forever.
    """
    
    INTERVAL = 60
    BATCH_SIZE = 50
    MAX_ATTEMPTS = 2
    
    TRANSIENT_ERRORS = [
        # Network
        "ConnectionError",
        "TimeoutError",
        "socket.timeout",
        "Connection refused",
        "ECONNRESET",
        
        # HTTP
        "Service Unavailable",  # 503
        "Bad Gateway",          # 502
        "Gateway Timeout",      # 504
        "rate limit",           # 429
        "Too Many Requests",    # 429
        
        # Redis
        "BusyLoadingError",
        
        # OpenRouter
        "overloaded",
    ]

    @property
    def name(self) -> str:
        return "dlq_auto_replay"

    async def should_run(self, ctx: JobContext) -> bool:
        last_run_key = RedisKeys.job_last_run(self.name, ctx.user_name, ctx.session_id)
        last_run_ts = await ctx.redis.get(last_run_key)
        
        if not last_run_ts:
            # First run — set timestamp, skip this cycle
            await ctx.redis.set(last_run_key, time.time())
            return False
        
        elapsed = time.time() - float(last_run_ts)
        return elapsed >= self.INTERVAL

    async def execute(self, ctx: JobContext) -> JobResult:
        dlq_key = RedisKeys.dlq(ctx.user_name, ctx.session_id)
        park_key = RedisKeys.dlq_parked(ctx.user_name, ctx.session_id)
        buffer_key = RedisKeys.buffer(ctx.user_name, ctx.session_id)
        
        queue_len = await ctx.redis.llen(dlq_key)
        if queue_len == 0:
            return JobResult(success=True, summary="DLQ empty")

        processed = 0
        retried = 0
        parked = 0
        
        for _ in range(min(queue_len, self.BATCH_SIZE)):
            raw_item = await ctx.redis.lpop(dlq_key)
            if not raw_item:
                break
                
            processed += 1
            entry = json.loads(raw_item)
            error_msg = str(entry.get("error", ""))
            messages = entry.get("messages", [])
            attempt = entry.get("attempt", 1)
            try:
                entry = json.loads(raw_item)
                error_msg = str(entry.get("error", ""))
                messages = entry.get("messages", [])
                
                is_transient = any(t in error_msg for t in self.TRANSIENT_ERRORS)
                
                if is_transient:
                    for msg in messages:
                        await ctx.redis.rpush(buffer_key, json.dumps(msg))
                    retried += 1
                    logger.info(f"Auto-healing DLQ item: {error_msg} -> Requeued")
                elif attempt < self.MAX_ATTEMPTS:
                    entry["attempt"] = attempt + 1
                    entry["last_retry"] = time.time()
                    await ctx.redis.rpush(dlq_key, json.dumps(entry))
                    retried += 1
                    logger.info(f"Retry {attempt + 1}/{self.MAX_ATTEMPTS}: {error_msg}")
                else:
                    entry["parked_at"] = time.time()
                    await ctx.redis.rpush(park_key, json.dumps(entry))
                    parked += 1
                    logger.warning(f"Parked after {attempt} attempts: {error_msg}")
            except Exception as e:
                logger.error(f"Failed to process DLQ item: {e}")
                await ctx.redis.rpush(park_key, raw_item)
                parked += 1

        summary = f"Processed {processed}: {retried} retried, {parked} parked"
        return JobResult(success=True, summary=summary)