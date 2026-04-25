import json
import time
from typing import Callable, Awaitable, Optional
from loguru import logger
from jobs.base import BaseJob, JobContext, JobResult
from core.entity_resolver import EntityResolver
from core.batch_processor import BatchProcessor, BatchResult
from common.utils.events import emit
from common.infra.redis import RedisKeys
import redis.asyncio as aioredis

class DLQReplayJob(BaseJob):
    """
    Periodically checks the Dead Letter Queue with stage-aware retry:
    - graph_write: Cheap retry, just write (no LLM cost)
    - processing: Full reprocess with stored context (LLM cost)
    """
    
    TRANSIENT_ERRORS = [
        # Network
        "ConnectionError",
        "TimeoutError",
        "socket.timeout",
        "Connection refused",
        "ECONNRESET",
        
        # HTTP
        "Service Unavailable",
        "Bad Gateway",
        "Gateway Timeout",
        "rate limit",
        "Too Many Requests",
        
        # Redis
        "BusyLoadingError",
        
        # OpenRouter
        "overloaded",

        # Memgraph
        "serialization error",
        "conflicting transactions",
        "Cannot get shared access",
        "Cannot get unique access", 
        "Cannot get read only access",
        "Storage access timeout",
        "access timeout",
        "TransientError",
    ]

    def __init__(
        self, 
        ent_resolver: EntityResolver,
        processor: BatchProcessor,
        write_to_graph: Callable[[BatchResult], Awaitable[tuple[bool, Optional[str]]]],
        redis_client: aioredis.Redis,
        interval: int = 60, 
        batch_size: int = 50, 
        max_attempts: int = 3
    ):
        self.ent_resolver = ent_resolver
        self.processor = processor
        self.write_to_graph = write_to_graph
        self.redis = redis_client
        self.interval = interval
        self.batch_size = batch_size
        self.max_attempts = max_attempts

    @property
    def name(self) -> str:
        return "dlq_auto_replay"

    async def should_run(self, ctx: JobContext) -> bool:
        last_run_key = RedisKeys.job_last_run(self.name, ctx.user_name, ctx.session_id)
        last_run_ts = await self.redis.get(last_run_key)
        
        if not last_run_ts:
            await self.redis.set(last_run_key, time.time())
            return False
        
        try:
            elapsed = time.time() - float(last_run_ts)
        except ValueError:
            await self.redis.set(last_run_key, time.time())
            return False
        
        return elapsed >= self.interval

    def _is_transient(self, error: str) -> bool:
        return any(t.lower() in error.lower() for t in self.TRANSIENT_ERRORS)

    def _validate_batch_result(self, result: BatchResult) -> BatchResult:
        """Filter out entity IDs that no longer exist in resolver."""
        valid_ids = [
            eid for eid in result.entity_ids 
            if eid in self.ent_resolver.entity_profiles
        ]
        
        removed_count = len(result.entity_ids) - len(valid_ids)
        if removed_count > 0:
            logger.warning(f"DLQ: Filtered {removed_count} stale entity IDs")
            valid_set = set(valid_ids)
            result.entity_ids = valid_ids
            result.new_entity_ids &= valid_set
            result.alias_updated_ids &= valid_set
        
        return result

    async def _retry_graph_write(self, entry: dict, ctx: JobContext) -> bool:
        """Retry just the graph write — no LLM cost."""

        if self.write_to_graph is None:
            logger.error("DLQ: write_to_graph callback not configured, cannot retry")
            return False
    
        try:
            result = BatchResult.from_dict(entry["batch_result"])
            result = self._validate_batch_result(result)
            
            if not result.entity_ids:
                logger.warning("DLQ: No valid entities left after validation, skipping")
                return True  # Consider it handled
            
            success, _ = await self.write_to_graph(result)
            
            if success:
                logger.info(f"DLQ: Graph write retry succeeded for {len(result.entity_ids)} entities")
                await emit(ctx.session_id, "job", "dlq_graph_write_success", {
                    "entity_count": len(result.entity_ids)
                })
            
            return success
            
        except Exception as e:
            logger.error(f"DLQ graph write retry failed: {e}")
            return False

    async def _retry_message_log(self, entry: dict, ctx: JobContext) -> bool:
        """Retry saving message logs and subsequently the graph write."""
        import asyncio
        try:
            messages = entry.get("messages", [])
            if not messages:
                logger.warning("DLQ: No messages in entry, skipping message log retry")
                return True
                
            batch_result_dict = entry.get("batch_result")
            if not batch_result_dict:
                logger.error("DLQ: No batch_result mapped for message_log retry. Falling back to full processing.")
                return await self._retry_processing(entry, ctx)
                
            result = BatchResult.from_dict(batch_result_dict)
            result = self._validate_batch_result(result)
            
            batch = [
                {
                    "id": msg['id'],
                    "content": msg.get('message', msg.get('content', '')),
                    "role": msg.get('role', 'user'),
                    "timestamp": msg.get('timestamp', ''),
                    "embedding": result.message_embeddings.get(msg['id'], [])
                }
                for msg in messages
            ]
            
            await asyncio.wait_for(
                self.processor.store.save_message_logs(batch),
                timeout=30.0
            )
            logger.info(f"DLQ: Message log retry succeeded for {len(messages)} messages")
            
            has_writes = bool(
                result.extraction_result or 
                result.new_entity_ids or 
                result.alias_updated_ids or
                result.alias_updates
            )
            
            if has_writes:
                success, err = await self.write_to_graph(result)
                if not success:
                    logger.error(f"DLQ: Message log succeeded, but paired graph write failed: {err}")
                    return False
                    
            return True

        except asyncio.TimeoutError:
            logger.error("DLQ message log retry timed out")
            return False
        except Exception as e:
            logger.error(f"DLQ message log retry failed: {e}")
            return False

    async def _retry_processing(self, entry: dict, ctx: JobContext) -> bool:
        """Full reprocess with stored context — LLM cost."""

        if self.write_to_graph is None:
            logger.error("DLQ: write_to_graph callback not configured, cannot retry")
            return False
        
        try:
            messages = entry.get("messages", [])
            session_text = entry.get("session_text", "")
            
            if not messages:
                logger.warning("DLQ: No messages in entry, skipping")
                return True
            
            result = await self.processor.run(messages, session_text)
            
            if not result.success:
                logger.warning(f"DLQ: Reprocessing failed: {result.error}")
                return False
            
            has_writes = bool(
                result.extraction_result or
                result.new_entity_ids or
                result.alias_updated_ids or
                result.alias_updates
            )
            if has_writes:
                success, err = await self.write_to_graph(result)
                if not success:
                    logger.warning(f"DLQ: Reprocessing succeeded but graph write failed: {err}")
                    return False
            
            logger.info(f"DLQ: Full reprocess succeeded for {len(messages)} messages")
            await emit(ctx.session_id, "job", "dlq_reprocess_success", {
                "msg_count": len(messages),
                "entity_count": len(result.entity_ids)
            })
            
            return True
            
        except Exception as e:
            logger.error(f"DLQ reprocessing failed: {e}")
            return False

    async def execute(self, ctx: JobContext) -> JobResult:
        dlq_key = RedisKeys.dlq(ctx.user_name, ctx.session_id)
        park_key = RedisKeys.dlq_parked(ctx.user_name, ctx.session_id)
        
        queue_len = await self.redis.llen(dlq_key)
        if queue_len == 0:
            await self.redis.set(RedisKeys.job_last_run(self.name, ctx.user_name, ctx.session_id), time.time())
            return JobResult(success=True, summary="DLQ empty")
        
        await emit(ctx.session_id, "job", "dlq_processing", {
            "queue_length": queue_len,
            "batch_size": min(queue_len, self.batch_size)
        })

        batch_size = min(queue_len, self.batch_size)

        processed = 0
        retried = 0
        parked = 0
        consecutive_failures = 0
        MAX_CONSECUTIVE_FAILURES = 5
        
        for _ in range(batch_size):
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                logger.error(f"DLQ circuit breaker triggered: {consecutive_failures} consecutive failures. Halting batch.")
                break
                
            raw_item = await self.redis.lpop(dlq_key)
            if not raw_item:
                break
                
            processed += 1
            
            try:
                entry = json.loads(raw_item)
                error_msg = str(entry.get("error", ""))
                attempt = entry.get("attempt", 1)
                stage = entry.get("stage", "processing")
                
                is_transient = self._is_transient(error_msg)
                
                if is_transient and attempt < self.max_attempts:
                    if stage == "graph_write":
                        success = await self._retry_graph_write(entry, ctx)
                    elif stage == "message_log":
                        success = await self._retry_message_log(entry, ctx)
                    else:
                        success = await self._retry_processing(entry, ctx)
                    
                    if success:
                        retried += 1
                        consecutive_failures = 0
                        await emit(ctx.session_id, "job", "dlq_retry_success", {
                            "stage": stage,
                            "attempt": attempt
                        })
                    else:
                        consecutive_failures += 1
                        entry["attempt"] = attempt + 1
                        await self.redis.rpush(dlq_key, json.dumps(entry))
                        logger.info(f"DLQ: Retry failed, re-queued (attempt {attempt + 1}/{self.max_attempts})")
                        await emit(ctx.session_id, "job", "dlq_retry_failed", {
                            "stage": stage,
                            "attempt": attempt + 1,
                            "max_attempts": self.max_attempts
                        })
                else:
                    consecutive_failures = 0
                    await self.redis.rpush(park_key, raw_item)
                    parked += 1
                    
                    reason = "max_attempts_exceeded" if attempt >= self.max_attempts else "fatal_error"
                    logger.warning(f"DLQ: Parked entry ({reason}): {error_msg[:100]}")
                    await emit(ctx.session_id, "job", "dlq_parked", {
                        "reason": reason,
                        "error": error_msg[:200],
                        "attempt": attempt
                    })
                    
            except json.JSONDecodeError:
                consecutive_failures = 0
                logger.error("DLQ: Corrupt entry, parking")
                await self.redis.rpush(park_key, raw_item)
                parked += 1
            except Exception as e:
                consecutive_failures += 1
                logger.error(f"DLQ: Unexpected error: {e}")
                await self.redis.rpush(park_key, raw_item)
                parked += 1
        
        await self.redis.set(RedisKeys.job_last_run(self.name, ctx.user_name, ctx.session_id), time.time())
        
        summary = f"Processed {processed}: {retried} retried, {parked} parked"
        logger.info(f"DLQ job complete: {summary}")
        
        await emit(ctx.session_id, "job", "dlq_complete", {
            "processed": processed,
            "retried": retried,
            "parked": parked
        })
        
        return JobResult(success=True, summary=summary)

    def update_settings(self, interval: int = None, batch_size: int = None, max_attempts: int = None):
        if interval is not None:
            self.interval = interval
        if batch_size is not None:
            self.batch_size = batch_size
        if max_attempts is not None:
            self.max_attempts = max_attempts
        logger.info(f"DLQReplayJob updated: interval={self.interval}, batch_size={self.batch_size}, max_attempts={self.max_attempts}")