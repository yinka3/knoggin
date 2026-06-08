import asyncio
import json
from typing import Awaitable, Callable, Optional

import redis.asyncio as aioredis
from loguru import logger

from common.schema.contracts import BatchResult, EngineScope, EngineWorkUnit
from common.utils.events import emit
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import get_now_unix
from infrastructure.job.base import BaseJob, JobContext, JobResult
from infrastructure.redis_client import RedisKeys
from knoggin_server.ingestion.services.pipeline_service import (
    BatchProcessor,
)
from knoggin_server.knowledge.services.entity_service import EntityManager


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
        # GraphClient
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
        entities: EntityManager,
        processor: BatchProcessor,
        write_to_graph: Callable[[BatchResult], Awaitable[tuple[bool, Optional[str]]]],
        redis_client: aioredis.Redis,
        interval: int = 60,
        batch_size: int = 50,
        max_attempts: int = 3,
    ):
        self.entities = entities
        self.processor = processor
        if self.processor.graph_client is None:
            raise ValueError("DLQReplayJob requires a BatchProcessor with graph_client")
        self.write_to_graph = write_to_graph
        self.redis = redis_client
        self.interval = interval
        self.batch_size = batch_size
        self.max_attempts = max_attempts

    @property
    def name(self) -> str:
        return "dlq_auto_replay"

    async def should_run(self, ctx: JobContext) -> bool:
        last_run_key = RedisKeys.job_last_run(self.name, ctx.user_name, ctx.project_id)
        last_run_ts = await self.redis.get(last_run_key)

        if not last_run_ts:
            await self.redis.set(last_run_key, get_now_unix())
            return False

        try:
            elapsed = get_now_unix() - float(last_run_ts)
        except ValueError:
            await self.redis.set(last_run_key, get_now_unix())
            return False

        return elapsed >= self.interval

    def _is_transient(self, error: str) -> bool:
        return any(t.lower() in error.lower() for t in self.TRANSIENT_ERRORS)

    def _resolve_replay_scope(
        self,
        entry: dict,
        ctx: JobContext,
        batch_result: Optional[BatchResult] = None,
    ) -> EngineScope:
        session_id = entry.get("session_id")
        if not session_id:
            raise ValueError("DLQ entry missing required session_id")

        return EngineScope(
            user_name=entry.get("user_name") or ctx.user_name,
            session_id=session_id,
            project_id=entry.get("project_id")
            or getattr(self.entities, "project_id", None)
            or ctx.project_id,
        )

    def _replay_work_unit(
        self,
        entry: dict,
        ctx: JobContext,
        batch_result: Optional[BatchResult] = None,
    ) -> EngineWorkUnit:
        scope = self._resolve_replay_scope(entry, ctx, batch_result)
        return EngineWorkUnit.for_dlq_replay(
            scope=scope,
            stage=entry.get("stage", "unknown"),
            attempt=entry.get("attempt", 1),
        )

    def _refresh_replay_scope(
        self,
        replay_unit: EngineWorkUnit,
        entry: dict,
        ctx: JobContext,
        batch_result: BatchResult,
    ) -> None:
        replay_unit.scope = self._resolve_replay_scope(entry, ctx, batch_result)
        batch_result.scope = replay_unit.scope
        if batch_result.work_unit:
            batch_result.work_unit.scope = replay_unit.scope

    def _attach_replay_unit(
        self,
        batch_result: Optional[BatchResult],
        replay_unit: EngineWorkUnit,
    ) -> None:
        if batch_result and batch_result.work_unit:
            batch_result.work_unit.metadata["dlq_replay_work_unit"] = (
                replay_unit.model_dump(mode="json")
            )

    async def _emit_replay_unit_finished(
        self, ctx: JobContext, replay_unit: EngineWorkUnit
    ) -> None:
        await emit(
            ctx.project_id,
            "job",
            "dlq_work_unit_finished",
            replay_unit.model_dump(mode="json"),
            verbose_only=True,
        )

    def _validate_batch_result(self, result: BatchResult) -> BatchResult:
        """
        Filter out stale entity IDs (e.g. phantom entities purged after a failed write),
        forcing the DLQ to fall back to a safer full reprocessing retry.
        """
        valid_ids = [
            eid for eid in result.entity_ids if eid in self.entities.entity_profiles
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

        replay_unit = self._replay_work_unit(entry, ctx)
        replay_unit.mark_running()
        result = None

        try:
            result = BatchResult.from_dict(entry["batch_result"])
            self._refresh_replay_scope(replay_unit, entry, ctx, result)
            if result.work_unit:
                result.work_unit.trace.attempt = entry.get("attempt", 1)
            result = self._validate_batch_result(result)

            if not result.entity_ids:
                logger.warning("DLQ: No valid entities left after validation, skipping")
                replay_unit.mark_skipped("No valid entities left")
                self._attach_replay_unit(result, replay_unit)
                await self._emit_replay_unit_finished(ctx, replay_unit)
                return True  # Consider it handled

            success, _ = await self.write_to_graph(result)

            if success:
                replay_unit.mark_succeeded("Graph write retry succeeded")
                self._attach_replay_unit(result, replay_unit)
                await self._emit_replay_unit_finished(ctx, replay_unit)
                logger.info(
                    "DLQ: Graph write retry succeeded for "
                    f"{len(result.entity_ids)} entities"
                )
                await emit(
                    ctx.project_id,
                    "job",
                    "dlq_graph_write_success",
                    {"entity_count": len(result.entity_ids)},
                )

            if not success:
                replay_unit.mark_failed("Graph write retry failed")
                self._attach_replay_unit(result, replay_unit)
                await self._emit_replay_unit_finished(ctx, replay_unit)

            return success

        except Exception as e:
            logger.error(f"DLQ graph write retry failed: {e}")
            replay_unit.mark_failed(str(e))
            self._attach_replay_unit(result, replay_unit)
            await self._emit_replay_unit_finished(ctx, replay_unit)
            return False

    async def _retry_message_log(self, entry: dict, ctx: JobContext) -> bool:
        """Retry saving message logs and subsequently the graph write."""
        replay_unit = self._replay_work_unit(entry, ctx)
        replay_unit.mark_running()
        result = None

        try:
            messages = entry.get("messages", [])
            if not messages:
                logger.warning("DLQ: No messages in entry, skipping message log retry")
                replay_unit.mark_skipped("No messages")
                await self._emit_replay_unit_finished(ctx, replay_unit)
                return True

            batch_result_dict = entry.get("batch_result")
            if not batch_result_dict:
                logger.error(
                    "DLQ: No batch_result mapped for message_log retry. "
                    "Falling back to full processing."
                )
                return await self._retry_processing(entry, ctx)

            result = BatchResult.from_dict(batch_result_dict)
            self._refresh_replay_scope(replay_unit, entry, ctx, result)
            if result.work_unit:
                result.work_unit.trace.attempt = entry.get("attempt", 1)
            result = self._validate_batch_result(result)
            replay_scope = replay_unit.scope

            batch = [
                {
                    "id": msg["id"],
                    "content": msg.get("message", msg.get("content", "")),
                    "role": msg.get("role", "user"),
                    "user_name": replay_scope.user_name,
                    "session_id": replay_scope.session_id,
                    "project_id": replay_scope.project_id,
                    "timestamp": msg.get("timestamp", ""),
                }
                for msg in messages
            ]

            await asyncio.wait_for(
                self.processor.graph_client.save_message_logs(batch), timeout=30.0
            )
            logger.info(
                f"DLQ: Message log retry succeeded for {len(messages)} messages"
            )

            has_writes = result.has_graph_writes()

            if has_writes:
                success, err = await self.write_to_graph(result)
                if not success:
                    logger.error(
                        "DLQ: Message log succeeded, but paired graph write "
                        f"failed: {err}"
                    )
                    replay_unit.mark_failed(err or "Graph write failed")
                    self._attach_replay_unit(result, replay_unit)
                    await self._emit_replay_unit_finished(ctx, replay_unit)
                    return False

            replay_unit.mark_succeeded("Message log retry succeeded")
            self._attach_replay_unit(result, replay_unit)
            await self._emit_replay_unit_finished(ctx, replay_unit)
            return True

        except asyncio.TimeoutError:
            logger.error("DLQ message log retry timed out")
            replay_unit.mark_failed("Message log retry timed out")
            self._attach_replay_unit(result, replay_unit)
            await self._emit_replay_unit_finished(ctx, replay_unit)
            return False
        except Exception as e:
            logger.error(f"DLQ message log retry failed: {e}")
            replay_unit.mark_failed(str(e))
            self._attach_replay_unit(result, replay_unit)
            await self._emit_replay_unit_finished(ctx, replay_unit)
            return False

    async def _retry_processing(self, entry: dict, ctx: JobContext) -> bool:
        """Full reprocess with stored context — LLM cost."""

        if self.write_to_graph is None:
            logger.error("DLQ: write_to_graph callback not configured, cannot retry")
            return False

        replay_unit = self._replay_work_unit(entry, ctx)
        replay_unit.mark_running()
        result = None

        try:
            messages = entry.get("messages", [])
            session_text = entry.get("session_text", "")

            if not messages:
                logger.warning("DLQ: No messages in entry, skipping")
                replay_unit.mark_skipped("No messages")
                await self._emit_replay_unit_finished(ctx, replay_unit)
                return True

            result = await self.processor.run(
                messages, session_text, session_id=replay_unit.scope.session_id
            )
            self._refresh_replay_scope(replay_unit, entry, ctx, result)
            if result.work_unit:
                result.work_unit.trace.attempt = entry.get("attempt", 1)

            if not result.success:
                logger.warning(f"DLQ: Reprocessing failed: {result.error}")
                replay_unit.mark_failed(result.error or "Reprocessing failed")
                self._attach_replay_unit(result, replay_unit)
                await self._emit_replay_unit_finished(ctx, replay_unit)
                return False

            has_writes = result.has_graph_writes()
            if has_writes:
                success, err = await self.write_to_graph(result)
                if not success:
                    logger.warning(
                        f"DLQ: Reprocessing succeeded but graph write failed: {err}"
                    )
                    replay_unit.mark_failed(err or "Graph write failed")
                    self._attach_replay_unit(result, replay_unit)
                    await self._emit_replay_unit_finished(ctx, replay_unit)
                    return False

            logger.info(f"DLQ: Full reprocess succeeded for {len(messages)} messages")
            replay_unit.mark_succeeded("Full reprocess succeeded")
            self._attach_replay_unit(result, replay_unit)
            await self._emit_replay_unit_finished(ctx, replay_unit)
            await emit(
                ctx.project_id,
                "job",
                "dlq_reprocess_success",
                {"msg_count": len(messages), "entity_count": len(result.entity_ids)},
            )

            return True

        except Exception as e:
            logger.error(f"DLQ reprocessing failed: {e}")
            replay_unit.mark_failed(str(e))
            self._attach_replay_unit(result, replay_unit)
            await self._emit_replay_unit_finished(ctx, replay_unit)
            return False

    async def execute(self, ctx: JobContext) -> JobResult:
        dlq_key = RedisKeys.dlq(ctx.user_name, ctx.project_id)
        park_key = RedisKeys.dlq_parked(ctx.user_name, ctx.project_id)

        queue_len = await self.redis.llen(dlq_key)
        if queue_len == 0:
            await self.redis.set(
                RedisKeys.job_last_run(self.name, ctx.user_name, ctx.project_id),
                get_now_unix(),
            )
            return JobResult(success=True, summary="DLQ empty")

        await emit(
            ctx.project_id,
            "job",
            "dlq_processing",
            {"queue_length": queue_len, "batch_size": min(queue_len, self.batch_size)},
        )

        batch_size = min(queue_len, self.batch_size)

        processed = 0
        retried = 0
        parked = 0
        consecutive_failures = 0
        MAX_CONSECUTIVE_FAILURES = 5

        for _ in range(batch_size):
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                logger.error(
                    "DLQ circuit breaker triggered: "
                    f"{consecutive_failures} consecutive failures. Halting batch."
                )
                break

            raw_item = await self.redis.lpop(dlq_key)
            if not raw_item:
                break

            processed += 1

            try:
                entry = safe_json_loads(raw_item)
                if not entry or not isinstance(entry, dict):
                    consecutive_failures = 0
                    logger.error("DLQ: Corrupt entry, parking")
                    await self.redis.rpush(park_key, raw_item)
                    parked += 1
                    continue

                error_msg = str(entry.get("error", ""))
                attempt = entry.get("attempt", 1)
                stage = entry.get("stage", "processing")

                if not entry.get("session_id"):
                    consecutive_failures = 0
                    await self.redis.rpush(park_key, raw_item)
                    parked += 1
                    logger.error("DLQ: Malformed entry missing session_id, parking")
                    await emit(
                        ctx.project_id,
                        "job",
                        "dlq_parked",
                        {
                            "reason": "missing_session_id",
                            "error": error_msg[:200],
                            "attempt": attempt,
                        },
                    )
                    continue

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
                        await emit(
                            ctx.project_id,
                            "job",
                            "dlq_retry_success",
                            {"stage": stage, "attempt": attempt},
                        )
                    else:
                        consecutive_failures += 1
                        entry["attempt"] = attempt + 1
                        await self.redis.rpush(dlq_key, json.dumps(entry))
                        logger.info(
                            "DLQ: Retry failed, re-queued "
                            f"(attempt {attempt + 1}/{self.max_attempts})"
                        )
                        await emit(
                            ctx.project_id,
                            "job",
                            "dlq_retry_failed",
                            {
                                "stage": stage,
                                "attempt": attempt + 1,
                                "max_attempts": self.max_attempts,
                            },
                        )
                else:
                    consecutive_failures = 0
                    await self.redis.rpush(park_key, raw_item)
                    parked += 1

                    reason = (
                        "max_attempts_exceeded"
                        if attempt >= self.max_attempts
                        else "fatal_error"
                    )
                    logger.warning(f"DLQ: Parked entry ({reason}): {error_msg[:100]}")
                    await emit(
                        ctx.project_id,
                        "job",
                        "dlq_parked",
                        {
                            "reason": reason,
                            "error": error_msg[:200],
                            "attempt": attempt,
                        },
                    )

            except Exception as e:
                consecutive_failures += 1
                logger.error(f"DLQ: Unexpected error: {e}")
                await self.redis.rpush(park_key, raw_item)
                parked += 1

        await self.redis.set(
            RedisKeys.job_last_run(self.name, ctx.user_name, ctx.project_id),
            get_now_unix(),
        )

        summary = f"Processed {processed}: {retried} retried, {parked} parked"
        logger.info(f"DLQ job complete: {summary}")

        await emit(
            ctx.project_id,
            "job",
            "dlq_complete",
            {"processed": processed, "retried": retried, "parked": parked},
        )

        return JobResult(success=True, summary=summary)

    def update_settings(
        self,
        interval: int = None,
        interval_seconds: int = None,
        batch_size: int = None,
        max_attempts: int = None,
    ):
        new_interval = interval_seconds if interval_seconds is not None else interval
        if new_interval is not None:
            self.interval = new_interval
        if batch_size is not None:
            self.batch_size = batch_size
        if max_attempts is not None:
            self.max_attempts = max_attempts
        logger.info(
            "DLQReplayJob updated: "
            f"interval={self.interval}, "
            f"batch_size={self.batch_size}, "
            f"max_attempts={self.max_attempts}"
        )
