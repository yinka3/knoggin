import asyncio
import json
from typing import Awaitable, Callable, Optional

import redis.asyncio as aioredis
from loguru import logger

from common.schema.contracts import BatchResult, EngineScope, EngineWorkUnit
from common.schema.settings import DLQSettings
from common.utils.events import emit
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import get_now_unix
from infrastructure.job.base import BaseJob, JobContext, JobResult
from infrastructure.redis_client import RedisKeys
from knoggin_server.ingestion.dlq_state import (
    DLQ_STATUS_COMPLETED,
    DLQ_STATUS_PARKED,
    DLQ_STATUS_PROCESSING,
    DLQ_STATUS_QUEUED,
    TERMINAL_DLQ_STATUSES,
    compute_dlq_id,
    ensure_dlq_id,
    serialize_dlq_entry,
)
from knoggin_server.ingestion.services.pipeline_service import (
    IngestionPipeline,
)
from knoggin_server.knowledge.entity.resolver import EntityResolver


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
        # KnowledgeStore
        "serialization error",
        "conflicting transactions",
        "Cannot get shared access",
        "Cannot get unique access",
        "Cannot get read only access",
        "Storage access timeout",
        "access timeout",
        "TransientError",
    ]
    CLAIM_TTL_SECONDS = 15 * 60

    def __init__(
        self,
        entities: EntityResolver,
        processor: IngestionPipeline,
        write_to_graph: Callable[[BatchResult], Awaitable[tuple[bool, Optional[str]]]],
        redis_client: aioredis.Redis,
        interval: int = 60,
        batch_size: int = 50,
        max_attempts: int = 3,
    ):
        self.entities = entities
        self.processor = processor
        if self.processor.knowledge_store is None:
            raise ValueError(
                "DLQReplayJob requires a IngestionPipeline with knowledge_store"
            )
        self.write_to_graph = write_to_graph
        self.redis = redis_client
        self.interval = interval
        self.batch_size = batch_size
        self.max_attempts = max_attempts

    @property
    def name(self) -> str:
        return "dlq_auto_replay"

    @property
    def cadence_seconds(self) -> float:
        return self.interval

    async def should_run(self, ctx: JobContext) -> bool:
        return False

    def _is_transient(self, error: str) -> bool:
        return any(t.lower() in error.lower() for t in self.TRANSIENT_ERRORS)

    async def remark_dirty_entities(
        self, user_name: str, project_id: str, entity_ids: list[int | str]
    ) -> int:
        if not entity_ids:
            return 0
        return await self.redis.sadd(
            RedisKeys.dirty_entities(user_name, project_id),
            *[str(entity_id) for entity_id in entity_ids],
        )

    async def remark_merge_candidates(
        self, user_name: str, project_id: str, entity_ids: list[int | str]
    ) -> int:
        if not entity_ids:
            return 0
        return await self.redis.sadd(
            RedisKeys.merge_queue(user_name, project_id),
            *[str(entity_id) for entity_id in entity_ids],
        )

    async def requeue_parked_dlq_item(
        self, user_name: str, project_id: str, dlq_id: str
    ) -> bool:
        park_key = RedisKeys.dlq_parked(user_name, project_id)
        dlq_key = RedisKeys.dlq(user_name, project_id)
        state_key = RedisKeys.dlq_state(user_name, project_id)
        if await self.redis.hget(state_key, dlq_id) != DLQ_STATUS_PARKED:
            return False

        raw_items = await self.redis.lrange(park_key, 0, -1)
        for raw_item in raw_items:
            entry = self._decode_entry(raw_item)
            if not entry or ensure_dlq_id(entry) != dlq_id:
                continue
            await self.redis.lrem(park_key, 1, raw_item)
            await self.redis.hset(state_key, dlq_id, DLQ_STATUS_QUEUED)
            await self.redis.rpush(dlq_key, serialize_dlq_entry(entry))
            return True
        return False

    def _decode_entry(self, raw_item: str) -> Optional[dict]:
        entry = safe_json_loads(raw_item)
        return entry if isinstance(entry, dict) else None

    def _corrupt_dlq_id(self, raw_item: str) -> str:
        return compute_dlq_id(
            {
                "user_name": "corrupt",
                "project_id": "corrupt",
                "session_id": raw_item,
                "stage": "corrupt",
            }
        )

    async def _requeue_abandoned_claims(self, ctx: JobContext) -> int:
        processing_key = RedisKeys.dlq_processing(ctx.user_name, ctx.project_id)
        dlq_key = RedisKeys.dlq(ctx.user_name, ctx.project_id)
        state_key = RedisKeys.dlq_state(ctx.user_name, ctx.project_id)
        claims_key = RedisKeys.dlq_claims(ctx.user_name, ctx.project_id)
        now = get_now_unix()
        requeued = 0

        for raw_item in await self.redis.lrange(processing_key, 0, -1):
            entry = self._decode_entry(raw_item)
            dlq_id = ensure_dlq_id(entry) if entry else self._corrupt_dlq_id(raw_item)
            claim = self._decode_entry(await self.redis.hget(claims_key, dlq_id) or "")
            claimed_at = float(claim.get("claimed_at", 0)) if claim else 0
            state = await self.redis.hget(state_key, dlq_id)
            if state in TERMINAL_DLQ_STATUSES:
                await self.redis.lrem(processing_key, 1, raw_item)
                continue
            if claim and now - claimed_at <= self.CLAIM_TTL_SECONDS:
                continue

            await self.redis.lrem(processing_key, 1, raw_item)
            await self.redis.hdel(claims_key, dlq_id)
            await self.redis.hset(state_key, dlq_id, DLQ_STATUS_QUEUED)
            await self.redis.rpush(
                dlq_key, serialize_dlq_entry(entry) if entry else raw_item
            )
            requeued += 1
        return requeued

    async def _claim_next(
        self, ctx: JobContext
    ) -> tuple[Optional[dict], Optional[str], Optional[str]]:
        dlq_key = RedisKeys.dlq(ctx.user_name, ctx.project_id)
        processing_key = RedisKeys.dlq_processing(ctx.user_name, ctx.project_id)
        state_key = RedisKeys.dlq_state(ctx.user_name, ctx.project_id)
        claims_key = RedisKeys.dlq_claims(ctx.user_name, ctx.project_id)

        while True:
            raw_item = await self.redis.lmove(
                dlq_key, processing_key, "LEFT", "RIGHT"
            )
            if not raw_item:
                return None, None, None

            entry = self._decode_entry(raw_item)
            dlq_id = ensure_dlq_id(entry) if entry else self._corrupt_dlq_id(raw_item)
            state = await self.redis.hget(state_key, dlq_id)
            if state in TERMINAL_DLQ_STATUSES or state == DLQ_STATUS_PROCESSING:
                await self.redis.lrem(processing_key, 1, raw_item)
                continue

            await self.redis.hset(state_key, dlq_id, DLQ_STATUS_PROCESSING)
            await self.redis.hset(
                claims_key,
                dlq_id,
                json.dumps(
                    {
                        "claimed_at": get_now_unix(),
                        "job": self.name,
                        "project_id": ctx.project_id,
                    },
                    sort_keys=True,
                ),
            )
            return entry, raw_item, dlq_id

    async def _ack_completed(
        self, ctx: JobContext, raw_item: str, dlq_id: str
    ) -> None:
        await self.redis.lrem(
            RedisKeys.dlq_processing(ctx.user_name, ctx.project_id), 1, raw_item
        )
        await self.redis.hdel(
            RedisKeys.dlq_claims(ctx.user_name, ctx.project_id), dlq_id
        )
        await self.redis.hset(
            RedisKeys.dlq_state(ctx.user_name, ctx.project_id),
            dlq_id,
            DLQ_STATUS_COMPLETED,
        )

    async def _requeue_claimed(
        self, ctx: JobContext, raw_item: str, entry: dict, dlq_id: str
    ) -> None:
        await self.redis.lrem(
            RedisKeys.dlq_processing(ctx.user_name, ctx.project_id), 1, raw_item
        )
        await self.redis.hdel(
            RedisKeys.dlq_claims(ctx.user_name, ctx.project_id), dlq_id
        )
        await self.redis.hset(
            RedisKeys.dlq_state(ctx.user_name, ctx.project_id),
            dlq_id,
            DLQ_STATUS_QUEUED,
        )
        await self.redis.rpush(
            RedisKeys.dlq(ctx.user_name, ctx.project_id),
            serialize_dlq_entry(entry),
        )

    async def _park_claimed(
        self,
        ctx: JobContext,
        raw_item: str,
        entry: Optional[dict],
        dlq_id: str,
    ) -> bool:
        processing_key = RedisKeys.dlq_processing(ctx.user_name, ctx.project_id)
        park_key = RedisKeys.dlq_parked(ctx.user_name, ctx.project_id)
        state_key = RedisKeys.dlq_state(ctx.user_name, ctx.project_id)
        claims_key = RedisKeys.dlq_claims(ctx.user_name, ctx.project_id)
        already_parked = await self.redis.hget(state_key, dlq_id) == DLQ_STATUS_PARKED

        await self.redis.lrem(processing_key, 1, raw_item)
        await self.redis.hdel(claims_key, dlq_id)
        if already_parked:
            return False

        await self.redis.hset(state_key, dlq_id, DLQ_STATUS_PARKED)
        await self.redis.rpush(
            park_key, serialize_dlq_entry(entry) if entry else raw_item
        )
        return True

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
            eid for eid in result.entity_ids if self.entities.has_cached_entity(eid)
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
                    {
                        "user_name": ctx.user_name,
                        "project_id": ctx.project_id,
                        "dlq_key": RedisKeys.dlq(ctx.user_name, ctx.project_id),
                        "dlq_id": entry.get("dlq_id"),
                        "stage": entry.get("stage", "graph_write"),
                        "attempt": entry.get("attempt", 1),
                        "entity_ids": result.entity_ids,
                        "entity_count": len(result.entity_ids),
                    },
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
                self.processor.knowledge_store.save_message_logs(batch), timeout=30.0
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
                {
                    "user_name": ctx.user_name,
                    "project_id": ctx.project_id,
                    "dlq_key": RedisKeys.dlq(ctx.user_name, ctx.project_id),
                    "dlq_id": entry.get("dlq_id"),
                    "stage": entry.get("stage", "processing"),
                    "attempt": entry.get("attempt", 1),
                    "message_ids": [msg.get("id") for msg in messages],
                    "msg_count": len(messages),
                    "entity_ids": result.entity_ids,
                    "entity_count": len(result.entity_ids),
                },
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
        await self._requeue_abandoned_claims(ctx)

        queue_len = await self.redis.llen(dlq_key)
        if queue_len == 0:
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

            entry, raw_item, dlq_id = await self._claim_next(ctx)
            if not raw_item or not dlq_id:
                break

            processed += 1

            try:
                if not entry or not isinstance(entry, dict):
                    consecutive_failures = 0
                    logger.error("DLQ: Corrupt entry, parking")
                    did_park = await self._park_claimed(
                        ctx, raw_item, entry, dlq_id
                    )
                    parked += int(did_park)
                    continue

                ensure_dlq_id(entry)
                error_msg = str(entry.get("error", ""))
                attempt = entry.get("attempt", 1)
                stage = entry.get("stage", "processing")

                if not entry.get("session_id"):
                    consecutive_failures = 0
                    did_park = await self._park_claimed(
                        ctx, raw_item, entry, dlq_id
                    )
                    parked += int(did_park)
                    logger.error("DLQ: Malformed entry missing session_id, parking")
                    await emit(
                        ctx.project_id,
                        "job",
                        "dlq_parked",
                        {
                            "user_name": ctx.user_name,
                            "project_id": ctx.project_id,
                            "park_key": park_key,
                            "dlq_id": dlq_id,
                            "reason": "missing_session_id",
                            "error": error_msg[:200],
                            "attempt": attempt,
                            "stage": stage,
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
                            {
                                "user_name": ctx.user_name,
                                "project_id": ctx.project_id,
                                "dlq_key": dlq_key,
                                "dlq_id": dlq_id,
                                "stage": stage,
                                "attempt": attempt,
                            },
                        )
                        await self._ack_completed(ctx, raw_item, dlq_id)
                    else:
                        consecutive_failures += 1
                        entry["attempt"] = attempt + 1
                        await self._requeue_claimed(ctx, raw_item, entry, dlq_id)
                        logger.info(
                            "DLQ: Retry failed, re-queued "
                            f"(attempt {attempt + 1}/{self.max_attempts})"
                        )
                        await emit(
                            ctx.project_id,
                            "job",
                            "dlq_retry_failed",
                            {
                                "user_name": ctx.user_name,
                                "project_id": ctx.project_id,
                                "dlq_key": dlq_key,
                                "dlq_id": dlq_id,
                                "stage": stage,
                                "attempt": attempt + 1,
                                "max_attempts": self.max_attempts,
                            },
                        )
                else:
                    consecutive_failures = 0
                    did_park = await self._park_claimed(
                        ctx, raw_item, entry, dlq_id
                    )
                    parked += int(did_park)

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
                            "user_name": ctx.user_name,
                            "project_id": ctx.project_id,
                            "park_key": park_key,
                            "dlq_id": dlq_id,
                            "reason": reason,
                            "error": error_msg[:200],
                            "attempt": attempt,
                            "stage": stage,
                        },
                    )

            except Exception as e:
                consecutive_failures += 1
                logger.error(f"DLQ: Unexpected error: {e}")
                did_park = await self._park_claimed(ctx, raw_item, entry, dlq_id)
                parked += int(did_park)

        summary = f"Processed {processed}: {retried} retried, {parked} parked"
        logger.info(f"DLQ job complete: {summary}")

        await emit(
            ctx.project_id,
            "job",
            "dlq_complete",
            {"processed": processed, "retried": retried, "parked": parked},
        )

        return JobResult(success=True, summary=summary)

    def update_settings(self, settings: DLQSettings) -> None:
        self.interval = settings.interval_seconds
        self.batch_size = settings.batch_size
        self.max_attempts = settings.max_attempts
        logger.info(
            "DLQReplayJob updated: "
            f"interval={self.interval}, "
            f"batch_size={self.batch_size}, "
            f"max_attempts={self.max_attempts}"
        )
