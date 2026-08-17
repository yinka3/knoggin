import asyncio
import json
from typing import Awaitable, Callable, Optional

import redis.asyncio as aioredis
from loguru import logger

from common.schema.ingestion.contracts import ExecutionScope
from common.schema.settings import DLQSettings, IngestionSettings
from common.utils.events import emit
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import get_now_unix
from core.ingestion.batch import IngestionBatch, IngestionMilestone
from core.ingestion.checkpoint import commit_ingestion_checkpoint
from core.ingestion.dlq_payload import DLQPayload
from core.ingestion.dlq_state import (
    DLQ_STATUS_COMPLETED,
    DLQ_STATUS_PARKED,
    DLQ_STATUS_PROCESSING,
    DLQ_STATUS_QUEUED,
    TERMINAL_DLQ_STATUSES,
    compute_dlq_id,
    ensure_dlq_id,
    serialize_dlq_entry,
)
from core.ingestion.policy import IngestionPolicy
from core.ingestion.services.pipeline_service import (
    IngestionPipeline,
)
from core.knowledge.entity.resolver import EntityResolver
from infrastructure.job.base import BaseJob, JobContext, JobResult
from infrastructure.redis_client import RedisKeys
from infrastructure.work_record import WorkRecord


class DLQReplayJob(BaseJob):
    """
    Replays failed ingestion work from the Redis-backed dead letter queue.

    The job is cadence-driven and uses explicit Redis state to claim, requeue,
    complete, or park DLQ entries. Replay is stage-aware:
    - graph_write: retry graph persistence only, avoiding LLM work.
    - message_log: retry message-log persistence, then graph persistence.
    - candidate_suggestions: retry suggestions, then graph persistence.
    - checkpoint: retry only the final durable checkpoint.
    - processing: rerun the ingestion pipeline with stored messages/context.

    Failed transient retries are requeued until max attempts is reached; malformed
    or terminal entries are parked for manual inspection.
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
        write_to_graph: Callable[
            [IngestionBatch], Awaitable[tuple[bool, Optional[str]]]
        ],
        redis_client: aioredis.Redis,
        settings: DLQSettings,
    ):
        self.entities = entities
        self.processor = processor
        if self.processor.knowledge_store is None:
            raise ValueError(
                "DLQReplayJob requires a IngestionPipeline with knowledge_store"
            )
        self.write_to_graph = write_to_graph
        self.redis = redis_client
        self.update_settings(settings)

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

    async def remark_merge_candidates(
        self, user_name: str, project_id: str, entity_ids: list[int | str]
    ) -> int:
        if not entity_ids:
            return 0
        return await self.redis.sadd(
            RedisKeys.merge_queue(user_name, project_id),
            *[str(entity_id) for entity_id in entity_ids],
        )

    async def remark_dirty_entities(
        self, user_name: str, project_id: str, entity_ids: list[int | str]
    ) -> int:
        if not entity_ids:
            return 0
        return await self.redis.sadd(
            RedisKeys.dirty_entities(user_name, project_id),
            *[str(entity_id) for entity_id in entity_ids],
        )

    async def requeue_parked_dlq_item(
        self, user_name: str, project_id: str, dlq_id: str
    ) -> bool:
        park_key = RedisKeys.dlq_parked(user_name, project_id)
        dlq_key = RedisKeys.dlq(user_name, project_id)
        state_key = RedisKeys.dlq_state(user_name, project_id)
        entry = None
        parked_raw_item = None
        state = await self.redis.hget(state_key, dlq_id)
        if state is not None and state != DLQ_STATUS_PARKED:
            return False
        raw_items = await self.redis.lrange(park_key, 0, -1)
        for raw_item in raw_items:
            candidate = self._decode_entry(raw_item)
            if candidate and ensure_dlq_id(candidate) == dlq_id:
                entry = candidate
                parked_raw_item = raw_item
                break
        if entry is None:
            entry = await self.processor.knowledge_store.get_parked_dlq_item(
                dlq_id=dlq_id,
                user_name=user_name,
                project_id=project_id,
            )
        if entry is None:
            return False

        # Make the user-visible workflow transition durable before touching
        # Redis. A process failure can then be recovered from Postgres rather
        # than leaving an apparently parked item that has already been queued.
        if not await self.processor.knowledge_store.mark_parked_dlq_item_requeued(
            dlq_id=dlq_id,
            user_name=user_name,
            project_id=project_id,
        ):
            return False

        if parked_raw_item is not None:
            await self.redis.lrem(park_key, 1, parked_raw_item)

        queued_raw_item = serialize_dlq_entry(entry)
        try:
            await self.redis.rpush(dlq_key, queued_raw_item)
            await self.redis.hset(state_key, dlq_id, DLQ_STATUS_QUEUED)
            return True
        except Exception as exc:
            logger.error("DLQ requeue did not reach Redis: {}", exc)

        # Do not strand a queue item whose durable subject is still parked.
        await self.redis.lrem(dlq_key, 1, queued_raw_item)
        await self.redis.hset(state_key, dlq_id, DLQ_STATUS_PARKED)
        # Restore the durable parked state and reopen its review. The payload
        # is still the source of truth if Redis is unavailable.
        await self.processor.knowledge_store.park_dlq_item(
            dlq_id=dlq_id,
            user_name=user_name,
            project_id=project_id,
            entry=entry,
        )
        if parked_raw_item is not None:
            await self.redis.rpush(park_key, parked_raw_item)
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

    async def _recover_durable_requeues(self, ctx: JobContext) -> int:
        """Rebuild Redis queue entries after a crash between durable and Redis work."""
        recovered = 0
        state_key = RedisKeys.dlq_state(ctx.user_name, ctx.project_id)
        dlq_key = RedisKeys.dlq(ctx.user_name, ctx.project_id)
        for entry in await self.processor.knowledge_store.get_requeued_dlq_items(
            user_name=ctx.user_name, project_id=ctx.project_id
        ):
            dlq_id = ensure_dlq_id(entry)
            if await self.redis.hget(state_key, dlq_id) is not None:
                continue
            queued_entries = await self.redis.lrange(dlq_key, 0, -1)
            if any(
                candidate
                and ensure_dlq_id(candidate) == dlq_id
                for candidate in (self._decode_entry(raw) for raw in queued_entries)
            ):
                await self.redis.hset(state_key, dlq_id, DLQ_STATUS_QUEUED)
                continue
            await self.redis.rpush(dlq_key, serialize_dlq_entry(entry))
            await self.redis.hset(state_key, dlq_id, DLQ_STATUS_QUEUED)
            recovered += 1
        return recovered

    async def _claim_next(
        self, ctx: JobContext
    ) -> tuple[Optional[dict], Optional[str], Optional[str]]:
        dlq_key = RedisKeys.dlq(ctx.user_name, ctx.project_id)
        processing_key = RedisKeys.dlq_processing(ctx.user_name, ctx.project_id)
        state_key = RedisKeys.dlq_state(ctx.user_name, ctx.project_id)
        claims_key = RedisKeys.dlq_claims(ctx.user_name, ctx.project_id)

        while True:
            raw_item = await self.redis.lmove(dlq_key, processing_key, "LEFT", "RIGHT")
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

    async def _ack_completed(self, ctx: JobContext, raw_item: str, dlq_id: str) -> None:
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
        await self.redis.zadd(
            RedisKeys.dlq_completed(ctx.user_name, ctx.project_id),
            {dlq_id: get_now_unix()},
        )
        await self.processor.knowledge_store.mark_parked_dlq_item_completed_if_requeued(
            dlq_id=dlq_id,
            user_name=ctx.user_name,
            project_id=ctx.project_id,
        )

    async def _prune_completed_state(self, ctx: JobContext) -> int:
        """Remove expired completed-state dedup markers, never queued work."""
        cutoff = get_now_unix() - self.completed_state_retention_seconds
        completed_key = RedisKeys.dlq_completed(ctx.user_name, ctx.project_id)
        expired_ids = await self.redis.zrange(
            completed_key,
            "-inf",
            cutoff,
            byscore=True,
        )
        if not expired_ids:
            return 0

        await self.redis.hdel(
            RedisKeys.dlq_state(ctx.user_name, ctx.project_id),
            *expired_ids,
        )
        await self.redis.zrem(completed_key, *expired_ids)
        return len(expired_ids)

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

        durable_entry = dict(entry or {})
        durable_entry.update(
            {
                "dlq_id": dlq_id,
                "user_name": durable_entry.get("user_name") or ctx.user_name,
                "project_id": durable_entry.get("project_id") or ctx.project_id,
                "raw_item": raw_item,
            }
        )
        await self.processor.knowledge_store.park_dlq_item(
            dlq_id=dlq_id,
            user_name=ctx.user_name,
            project_id=ctx.project_id,
            entry=durable_entry,
        )

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
        batch: Optional[IngestionBatch] = None,
    ) -> ExecutionScope:
        session_id = entry.get("session_id")
        if not session_id:
            raise ValueError("DLQ entry missing required session_id")

        return ExecutionScope(
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
        batch: Optional[IngestionBatch] = None,
    ) -> WorkRecord:
        scope = self._resolve_replay_scope(entry, ctx, batch)
        return WorkRecord.for_dlq_replay(
            scope=scope,
            stage=entry.get("stage", "unknown"),
            attempt=entry.get("attempt", 1),
        )

    def _refresh_replay_scope(
        self,
        replay_unit: WorkRecord,
        entry: dict,
        ctx: JobContext,
        batch: IngestionBatch,
    ) -> None:
        replay_unit.scope = self._resolve_replay_scope(entry, ctx, batch)
        batch.scope = replay_unit.scope
        batch.work_unit.scope = replay_unit.scope

    def _attach_replay_unit(
        self,
        batch: Optional[IngestionBatch],
        replay_unit: WorkRecord,
    ) -> None:
        if batch is not None:
            batch.work_unit.metadata["dlq_replay_work_record"] = replay_unit.snapshot()

    async def _emit_replay_unit_finished(
        self, ctx: JobContext, replay_unit: WorkRecord
    ) -> None:
        terminal_status = replay_unit.require_terminal_status()
        await emit(
            ctx.project_id,
            "job",
            "dlq_work_unit_finished",
            {
                **replay_unit.snapshot(),
                "terminal_outcome": terminal_status.value,
            },
            verbose_only=True,
        )

    @staticmethod
    def _set_replay_attempt(result: IngestionBatch, attempt: int) -> None:
        result.work_unit.attempt = attempt

    @staticmethod
    def _hydrate_replay_batch(payload_data: dict) -> IngestionBatch:
        payload = DLQPayload.model_validate(payload_data)
        return payload.to_ingestion_batch()

    def _validate_replay_batch(self, result: IngestionBatch) -> IngestionBatch:
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

    async def _write_replay_graph_if_needed(
        self, result: IngestionBatch
    ) -> Optional[str]:
        if IngestionMilestone.GRAPH_COMMITTED in result.milestones:
            return None
        if not result.has_graph_writes():
            return None
        if self.write_to_graph is None:
            return "write_to_graph callback not configured"

        success, err = await self.write_to_graph(result)
        if success:
            return None
        return err or "Graph write failed"

    async def _commit_replay_checkpoint(self, batch: IngestionBatch) -> None:
        """Finish a replay after graph persistence without repeating graph work."""

        if IngestionMilestone.CHECKPOINT_COMMITTED in batch.milestones:
            return
        if IngestionMilestone.GRAPH_COMMITTED not in batch.milestones:
            raise ValueError("Checkpoint replay requires a graph-committed batch")
        if not batch.messages:
            raise ValueError("Checkpoint replay requires batch messages")
        await commit_ingestion_checkpoint(self.redis, batch)
        batch.mark_checkpoint_committed()

    async def _retry_graph_write(self, entry: dict, ctx: JobContext) -> bool:
        """Retry just the graph write — no LLM cost."""

        if self.write_to_graph is None:
            logger.error("DLQ: write_to_graph callback not configured, cannot retry")
            return False

        replay_unit = self._replay_work_unit(entry, ctx)
        replay_unit.mark_running()
        result = None

        try:
            result = self._hydrate_replay_batch(entry["batch_result"])
            self._refresh_replay_scope(replay_unit, entry, ctx, result)
            self._set_replay_attempt(result, entry.get("attempt", 1))
            result = self._validate_replay_batch(result)

            graph_error = await self._write_replay_graph_if_needed(result)
            success = graph_error is None

            if success:
                await self._commit_replay_checkpoint(result)
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
                logger.error(f"DLQ graph write retry failed: {graph_error}")
                replay_unit.mark_failed(graph_error or "Graph write retry failed")
                self._attach_replay_unit(result, replay_unit)
                await self._emit_replay_unit_finished(ctx, replay_unit)

            return success

        except Exception as e:
            logger.error(f"DLQ graph write retry failed: {e}")
            replay_unit.mark_failed(str(e))
            self._attach_replay_unit(result, replay_unit)
            await self._emit_replay_unit_finished(ctx, replay_unit)
            return False
        finally:
            if result is not None:
                result.release()

    async def _retry_message_log(self, entry: dict, ctx: JobContext) -> bool:
        """Retry saving message logs and subsequently the graph write."""
        replay_unit = self._replay_work_unit(entry, ctx)
        replay_unit.mark_running()
        result = None

        try:
            batch_result_dict = entry.get("batch_result")
            if not batch_result_dict:
                logger.error(
                    "DLQ: No batch_result mapped for message_log retry. "
                    "Falling back to full processing."
                )
                return await self._retry_processing(entry, ctx)

            result = self._hydrate_replay_batch(batch_result_dict)
            self._refresh_replay_scope(replay_unit, entry, ctx, result)
            self._set_replay_attempt(result, entry.get("attempt", 1))
            result = self._validate_replay_batch(result)
            messages = result.messages or entry.get("messages", [])
            if not messages:
                logger.warning("DLQ: No messages in entry, skipping message log retry")
                replay_unit.mark_skipped("No messages")
                await self._emit_replay_unit_finished(ctx, replay_unit)
                return True
            await self._save_message_logs_for_replay(result, messages)

            await self._save_candidate_suggestions_for_replay(result)

            graph_error = await self._write_replay_graph_if_needed(result)
            if graph_error:
                logger.error(
                    "DLQ: Message log succeeded, but paired graph write "
                    f"failed: {graph_error}"
                )
                replay_unit.mark_failed(graph_error)
                self._attach_replay_unit(result, replay_unit)
                await self._emit_replay_unit_finished(ctx, replay_unit)
                return False

            await self._commit_replay_checkpoint(result)

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
        finally:
            if result is not None:
                result.release()

    async def _retry_candidate_suggestions(self, entry: dict, ctx: JobContext) -> bool:
        """Retry suggestions, then finish the remaining durable lifecycle."""

        replay_unit = self._replay_work_unit(entry, ctx)
        replay_unit.mark_running()
        result = None
        try:
            result = self._hydrate_replay_batch(entry["batch_result"])
            self._refresh_replay_scope(replay_unit, entry, ctx, result)
            self._set_replay_attempt(result, entry.get("attempt", 1))
            result = self._validate_replay_batch(result)

            await self._save_candidate_suggestions_for_replay(result)
            graph_error = await self._write_replay_graph_if_needed(result)
            if graph_error:
                raise RuntimeError(graph_error)
            await self._commit_replay_checkpoint(result)

            replay_unit.mark_succeeded("Candidate suggestion retry succeeded")
            self._attach_replay_unit(result, replay_unit)
            await self._emit_replay_unit_finished(ctx, replay_unit)
            return True
        except Exception as exc:
            logger.error(f"DLQ candidate suggestion retry failed: {exc}")
            replay_unit.mark_failed(str(exc))
            self._attach_replay_unit(result, replay_unit)
            await self._emit_replay_unit_finished(ctx, replay_unit)
            return False
        finally:
            if result is not None:
                result.release()

    async def _save_candidate_suggestions_for_replay(
        self, result: IngestionBatch
    ) -> None:
        if IngestionMilestone.CANDIDATE_SUGGESTIONS_HANDLED in result.milestones:
            return
        if not result.candidate_suggestions:
            result.mark_candidate_suggestions_handled()
            return
        if result.scope is None:
            logger.warning("Skipping candidate suggestion replay without batch scope")
            return
        try:
            await self.processor.knowledge_store.save_candidate_suggestions(
                result.scope,
                result.candidate_suggestions,
            )
        except Exception as e:
            logger.error(f"Failed to save replay candidate suggestions: {e}")
            await emit(
                result.scope.project_id,
                "job",
                "candidate_suggestions_save_failed",
                {
                    "user_name": result.scope.user_name,
                    "project_id": result.scope.project_id,
                    "session_id": result.scope.session_id,
                    "suggestion_count": len(result.candidate_suggestions),
                    "error": str(e),
                    "source": "dlq_replay",
                },
            )
            raise
        result.mark_candidate_suggestions_handled()

    async def _save_message_logs_for_replay(
        self, result: IngestionBatch, messages: list[dict]
    ) -> None:
        """Persist logs exactly once before advancing replay durability."""

        if IngestionMilestone.MESSAGE_LOGS_HANDLED in result.milestones:
            return
        if result.scope is None:
            raise ValueError("Message log replay requires batch scope")
        batch = [
            {
                "id": msg["id"],
                "content": msg.get("message", msg.get("content", "")),
                "role": msg.get("role", "user"),
                "user_name": result.scope.user_name,
                "session_id": result.scope.session_id,
                "project_id": result.scope.project_id,
                "timestamp": msg.get("timestamp", ""),
            }
            for msg in messages
        ]
        await asyncio.wait_for(
            self.processor.knowledge_store.save_message_logs(batch), timeout=30.0
        )
        result.mark_message_logs_handled()
        logger.info("DLQ: Message log replay succeeded for %s messages", len(messages))

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
            batch_payload = entry.get("batch_result")

            if not messages:
                logger.warning("DLQ: No messages in entry, skipping")
                replay_unit.mark_skipped("No messages")
                await self._emit_replay_unit_finished(ctx, replay_unit)
                return True
            if not batch_payload:
                raise ValueError("DLQ processing replay requires a batch policy")

            policy = IngestionPolicy.from_dict(
                DLQPayload.model_validate(batch_payload).policy
            )

            result = self.processor.open_batch(
                messages,
                session_text,
                session_id=replay_unit.scope.session_id,
                policy=policy,
            )
            await self.processor.process(result)
            self._refresh_replay_scope(replay_unit, entry, ctx, result)
            self._set_replay_attempt(result, entry.get("attempt", 1))

            if not result.success:
                logger.warning(f"DLQ: Reprocessing failed: {result.error}")
                replay_unit.mark_failed(result.error or "Reprocessing failed")
                self._attach_replay_unit(result, replay_unit)
                await self._emit_replay_unit_finished(ctx, replay_unit)
                return False

            await self._save_message_logs_for_replay(result, result.messages)
            await self._save_candidate_suggestions_for_replay(result)

            graph_error = await self._write_replay_graph_if_needed(result)
            if graph_error:
                logger.warning(
                    f"DLQ: Reprocessing succeeded but graph write failed: {graph_error}"
                )
                replay_unit.mark_failed(graph_error)
                self._attach_replay_unit(result, replay_unit)
                await self._emit_replay_unit_finished(ctx, replay_unit)
                return False

            await self._commit_replay_checkpoint(result)

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
        finally:
            if result is not None:
                result.release()

    async def _retry_checkpoint(self, entry: dict, ctx: JobContext) -> bool:
        """Retry only the final durable checkpoint after a graph commit."""

        replay_unit = self._replay_work_unit(entry, ctx)
        replay_unit.mark_running()
        result = None
        try:
            result = self._hydrate_replay_batch(entry["batch_result"])
            self._refresh_replay_scope(replay_unit, entry, ctx, result)
            self._set_replay_attempt(result, entry.get("attempt", 1))
            if IngestionMilestone.GRAPH_COMMITTED not in result.milestones:
                raise ValueError("Checkpoint DLQ payload is missing graph commit")
            await self._commit_replay_checkpoint(result)
            replay_unit.mark_succeeded("Checkpoint retry succeeded")
            self._attach_replay_unit(result, replay_unit)
            await self._emit_replay_unit_finished(ctx, replay_unit)
            return True
        except Exception as exc:
            replay_unit.mark_failed(str(exc))
            self._attach_replay_unit(result, replay_unit)
            await self._emit_replay_unit_finished(ctx, replay_unit)
            return False
        finally:
            if result is not None:
                result.release()

    async def execute(self, ctx: JobContext) -> JobResult:
        dlq_key = RedisKeys.dlq(ctx.user_name, ctx.project_id)
        park_key = RedisKeys.dlq_parked(ctx.user_name, ctx.project_id)
        await self._requeue_abandoned_claims(ctx)
        recovered_requeues = await self._recover_durable_requeues(ctx)
        pruned_completed = await self._prune_completed_state(ctx)

        queue_len = await self.redis.llen(dlq_key)
        if queue_len == 0:
            return JobResult(
                success=True,
                summary=(
                    ("DLQ empty" if not recovered_requeues else f"Recovered {recovered_requeues} durable requeues")
                    if not pruned_completed
                    else f"DLQ empty; pruned {pruned_completed} completed markers"
                ),
            )

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
                    did_park = await self._park_claimed(ctx, raw_item, entry, dlq_id)
                    parked += int(did_park)
                    continue

                ensure_dlq_id(entry)
                error_msg = str(entry.get("error", ""))
                attempt = entry.get("attempt", 1)
                stage = entry.get("stage", "processing")

                if not entry.get("session_id"):
                    consecutive_failures = 0
                    did_park = await self._park_claimed(ctx, raw_item, entry, dlq_id)
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
                    elif stage == "candidate_suggestions":
                        success = await self._retry_candidate_suggestions(entry, ctx)
                    elif stage == "checkpoint":
                        success = await self._retry_checkpoint(entry, ctx)
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
                    did_park = await self._park_claimed(ctx, raw_item, entry, dlq_id)
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
        self.completed_state_retention_seconds = (
            settings.completed_state_retention_hours * 3600
        )
        logger.info(
            "DLQReplayJob updated: "
            f"interval={self.interval}, "
            f"batch_size={self.batch_size}, "
            f"max_attempts={self.max_attempts}, "
            "completed_state_retention_hours="
            f"{settings.completed_state_retention_hours}"
        )
