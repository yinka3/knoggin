import asyncio
from datetime import datetime
from typing import Awaitable, Callable, Dict, List, Optional, assert_never

import redis.asyncio as aioredis
from loguru import logger

from common.schema.settings import IngestionSettings
from common.utils.diagnostic_context import diagnostic_scope
from common.utils.events import emit, emit_sync
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import get_now
from core.ingestion.batch import IngestionBatch, IngestionMessage
from core.ingestion.checkpoint import commit_ingestion_checkpoint
from core.ingestion.ports import IngestionPersistence
from core.ingestion.services.pipeline_service import IngestionPipeline
from infrastructure.redis_client import RedisKeys
from infrastructure.work_record import WorkStatus


class IngestionWorker:
    """
    Drains buffered session messages and coordinates persistence around ingestion.

    IngestionWorker reads queued messages from Redis, builds session context, runs
    IngestionPipeline, saves message logs, persists candidate suggestions, writes
    graph mutations, updates processing checkpoints, and routes failed batches to
    the DLQ.

    This class is infrastructure coordination, not entity extraction logic. It
    handles operational failure boundaries around Redis, message-log persistence,
    graph writes, and DLQ retry behavior while keeping successfully processed
    batches moving through the ingestion system.
    """

    def __init__(
        self,
        user_name: str,
        session_id: str,
        knowledge_store: IngestionPersistence,
        processor: IngestionPipeline,
        redis: aioredis.Redis,
        get_session_context: Callable[[int, Optional[int]], Awaitable[List[Dict]]],
        write_to_graph: Callable[
            [IngestionBatch], Awaitable[tuple[bool, Optional[str]]]
        ],
        settings: IngestionSettings,
    ):

        self.user_name = user_name
        self.session_id = session_id
        self.knowledge_store = knowledge_store
        self.processor = processor
        self.redis = redis

        # callbacks
        self.get_session_context = get_session_context

        self.write_to_graph = write_to_graph

        self._wake_event = asyncio.Event()
        self._shutdown_requested = False
        self._task: Optional[asyncio.Task] = None
        self._flush_future = None
        # Health telemetry is deliberately local and synchronous.  It must not
        # perform Redis reads or inspect the batch payload while the worker is
        # running.
        self._health_state = "not_started"
        self._health_started_at: datetime | None = None
        self._health_current_batch_size = 0
        self._health_current_batch_started_at: datetime | None = None
        self._health_last_success_at: datetime | None = None
        self._health_last_failure_category: str | None = None
        self._health_last_failure_at: datetime | None = None
        self._health_consecutive_failures = 0
        self.update_settings(settings)

    @property
    def _buffer_key(self) -> str:
        return RedisKeys.buffer(self.user_name, self.session_id)

    @property
    def _checkpoint_key(self) -> str:
        return RedisKeys.checkpoint(self.user_name, self.session_id)

    def start(self):
        if self._task is not None:
            logger.warning("IngestionWorker already running")
            return

        self._shutdown_requested = False
        self._health_state = "running"
        self._health_started_at = get_now()
        self._health_consecutive_failures = 0
        self._task = asyncio.create_task(self._run())
        self._task.add_done_callback(self._on_task_done)

        emit_sync(
            self.session_id,
            "pipeline",
            "consumer_started",
            {
                "batch_size": self.batch_size,
                "checkpoint_interval": self.checkpoint_interval,
            },
        )

    async def stop(self):
        if self._task is None:
            logger.warning("IngestionWorker not running")
            return

        logger.info("Stopping IngestionWorker...")
        self._shutdown_requested = True
        self._health_state = "draining"
        self._wake_event.set()  # wake if waiting

        try:
            await self._task
        except asyncio.CancelledError:
            pass

        self._task = None
        if self._health_state != "failed":
            self._health_state = "stopped"
        await emit(self.session_id, "pipeline", "consumer_stopped", {})

    def signal(self):
        self._wake_event.set()

    def _on_task_done(self, task: asyncio.Task):
        if task.cancelled():
            logger.info("IngestionWorker task cancelled")
            return

        if exc := task.exception():
            self._record_health_failure("worker_task")
            self._health_state = "failed"
            logger.error(f"IngestionWorker task failed: {exc}")

    def health_snapshot(self) -> dict[str, object]:
        """Return bounded local worker state without touching Redis."""

        def iso(value: datetime | None) -> str | None:
            return value.isoformat() if value is not None else None

        return {
            "state": getattr(self, "_health_state", "not_started"),
            "started_at": iso(getattr(self, "_health_started_at", None)),
            "current_batch_size": getattr(self, "_health_current_batch_size", 0),
            "current_batch_started_at": iso(
                getattr(self, "_health_current_batch_started_at", None)
            ),
            "last_success_at": iso(getattr(self, "_health_last_success_at", None)),
            "last_failure_category": getattr(
                self, "_health_last_failure_category", None
            ),
            "last_failure_at": iso(getattr(self, "_health_last_failure_at", None)),
            "consecutive_failures": getattr(
                self, "_health_consecutive_failures", 0
            ),
            "batch_size": getattr(self, "batch_size", 0),
            "batch_timeout_seconds": getattr(self, "batch_timeout", None),
            "checkpoint_interval": getattr(self, "checkpoint_interval", 0),
        }

    def _record_health_failure(self, category: str) -> None:
        self._health_last_failure_category = category[:100]
        self._health_last_failure_at = get_now()
        self._health_consecutive_failures += 1

    async def flush(self):
        """Force a partial drain of the buffer. Blocks until complete."""
        if self._task is None or self._task.done():
            return
        if self._flush_future is not None and not self._flush_future.done():
            await self._flush_future
            return

        future = asyncio.get_running_loop().create_future()
        self._flush_future = future
        self._wake_event.set()
        await future

    def _format_session_text(self, conversation: List[Dict]) -> str:
        lines = []
        for turn in conversation:
            content = turn["content"]
            lines.append(f"[{turn['role_label']}]: {content}")
        return "\n".join(lines)

    @staticmethod
    def _mark_batch_work_failed(batch: IngestionBatch, error: Exception | str) -> None:
        """Finish the parent record when its durable lifecycle cannot continue."""

        work = batch.work_unit
        match work.status:
            case WorkStatus.PENDING:
                work.mark_running()
                work.mark_failed(str(error))
            case WorkStatus.RUNNING:
                work.mark_failed(str(error))
            case WorkStatus.FAILED:
                return
            case WorkStatus.SUCCEEDED:
                raise RuntimeError("Cannot fail work that already succeeded")
            case WorkStatus.DEFERRED:
                raise RuntimeError("Cannot fail work that was deferred")
            case WorkStatus.SKIPPED:
                raise RuntimeError("Cannot fail work that was skipped")
            case WorkStatus.CANCELLED:
                return
            case unexpected:
                assert_never(unexpected)

    @staticmethod
    def _mark_batch_work_succeeded(batch: IngestionBatch) -> None:
        """Finish the parent record only after every required commit succeeds."""

        work = batch.work_unit
        match work.status:
            case WorkStatus.PENDING:
                work.mark_running()
            case WorkStatus.RUNNING:
                pass
            case WorkStatus.SUCCEEDED:
                return
            case WorkStatus.FAILED:
                raise RuntimeError("Cannot succeed work that already failed")
            case WorkStatus.DEFERRED:
                raise RuntimeError("Cannot succeed work that was deferred")
            case WorkStatus.SKIPPED:
                raise RuntimeError("Cannot succeed work that was skipped")
            case WorkStatus.CANCELLED:
                raise RuntimeError("Cannot succeed work that was cancelled")
            case unexpected:
                assert_never(unexpected)

        semantic_summary = work.metadata.get("semantic_summary")
        summary = "Durable ingestion commits completed"
        if semantic_summary:
            summary = f"{semantic_summary}; {summary}"
        work.mark_succeeded(summary)

    def update_settings(self, config: IngestionSettings):
        """Update settings dynamically while running."""
        self.settings = config
        self.batch_size = config.batch_size
        self.batch_debounce_seconds = config.batch_debounce_seconds
        self.batch_timeout = config.batch_timeout
        self.checkpoint_interval = config.checkpoint_interval
        self.session_window = config.session_window

        logger.info(
            "Consumer ingestion settings updated: "
            f"batch={self.batch_size}, debounce={self.batch_debounce_seconds}, "
            f"timeout={self.batch_timeout}"
        )

    async def _wait_for_batch(self) -> bool:
        """Wait briefly for a full batch. Returns whether partial work is due."""
        if self._shutdown_requested or self._flush_future is not None:
            return True

        deadline = asyncio.get_running_loop().time() + self.batch_debounce_seconds
        while True:
            if self._shutdown_requested or self._flush_future is not None:
                return True
            if await self.redis.llen(self._buffer_key) >= self.batch_size:
                return False

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return True

            try:
                await asyncio.wait_for(self._wake_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                return True
            self._wake_event.clear()

    async def _run(self):
        with logger.contextualize(
            user=self.user_name, session=self.session_id, component="IngestionWorker"
        ):
            logger.info(f"IngestionWorker started for {self.user_name}")

            error_count = 0
            while not self._shutdown_requested:
                timed_out = False
                try:
                    await asyncio.wait_for(
                        self._wake_event.wait(), timeout=self.batch_timeout
                    )
                except asyncio.TimeoutError:
                    timed_out = True

                self._wake_event.clear()
                try:
                    flush_partial = (
                        timed_out
                        or self._flush_future is not None
                        or self._shutdown_requested
                    )
                    if not flush_partial:
                        flush_partial = await self._wait_for_batch()
                    while True:
                        deferred_partial = await self._drain_buffer(
                            flush_partial=flush_partial
                        )
                        if not deferred_partial or self._shutdown_requested:
                            break
                        flush_partial = await self._wait_for_batch()
                    error_count = 0  # Reset on success
                except Exception as e:
                    error_count += 1
                    self._record_health_failure("worker_loop")
                    backoff = min(60, 2**error_count)
                    logger.error(
                        "IngestionWorker: Unexpected error during _drain_buffer: "
                        f"{e}. Backing off for {backoff}s..."
                    )
                    await asyncio.sleep(backoff)
                finally:
                    if self._flush_future and not self._flush_future.done():
                        self._flush_future.set_result(None)
                    self._flush_future = None

        logger.info("IngestionWorker shutting down, final drain...")
        try:
            await self._drain_buffer(flush_partial=True)

            logger.info("IngestionWorker shutdown complete")
        except Exception as e:
            self._record_health_failure("shutdown_drain")
            self._health_state = "failed"
            logger.error(f"IngestionWorker shutdown sequence failed: {e}")

    async def _drain_buffer(self, flush_partial: bool) -> bool:
        """Drain complete batches and report whether an undersized tail remains."""
        with logger.contextualize(
            user=self.user_name, session=self.session_id, component="IngestionWorker"
        ):
            batches_count = 0
            total_processed = 0
            all_msg_ids = []
            dlq_count = 0
            deferred_partial = False

            while True:
                buffer_len = await self.redis.llen(self._buffer_key)
                if buffer_len == 0:
                    break
                if not flush_partial and buffer_len < self.batch_size:
                    deferred_partial = True
                    break

                raw, messages = await self._read_buffer_batch()
                if not raw:
                    break

                if not messages:
                    await self.redis.ltrim(self._buffer_key, len(raw), -1)
                    continue

                self._health_current_batch_size = len(messages)
                self._health_current_batch_started_at = get_now()
                self._health_state = (
                    "draining"
                    if self._shutdown_requested or self._flush_future is not None
                    else "running"
                )
                batch_had_dlq = False

                conversation = await self.get_session_context(
                    self.session_window, messages[0]["id"]
                )
                session_text = self._format_session_text(conversation)

                batch = self.processor.open_batch(
                    messages,
                    session_text,
                    session_id=self.session_id,
                    policy=self.processor.capture_policy(self.settings),
                )
                try:
                    try:
                        await self._process_messages(batch)
                    except Exception as e:
                        logger.error(
                            f"Fatal error during IngestionPipeline computation: {e}"
                        )
                        batch.fail(f"Fatal exception: {e}")
                        self._mark_batch_work_failed(batch, batch.error or str(e))

                    if not batch.success:
                        self._record_health_failure("batch_processing")
                        dlq_success = await self.processor.move_to_dead_letter(
                            messages,
                            batch.error,
                            stage="processing",
                            session_text=session_text,
                            batch=batch,
                            session_id=self.session_id,
                        )
                        if not dlq_success:
                            logger.critical(
                                "DLQ write failed. Leaving "
                                f"{len(messages)} messages in buffer for retry."
                            )
                            await emit(
                                self.session_id,
                                "pipeline",
                                "dlq_write_failed",
                                {"msg_count": len(messages)},
                            )
                            break
                        batch_had_dlq = True
                        dlq_count += len(messages)
                    else:
                        (
                            can_continue,
                            dlq_written,
                        ) = await self._save_message_logs_or_dlq(batch)
                        if dlq_written:
                            self._record_health_failure("message_log")
                            batch_had_dlq = True
                            dlq_count += len(messages)
                        if not can_continue:
                            break
                        if dlq_written:
                            batches_count += 1
                            total_processed += len(messages)
                            all_msg_ids.extend([m["id"] for m in messages])
                            await self.redis.ltrim(self._buffer_key, len(raw), -1)
                            continue

                        (
                            can_continue,
                            dlq_written,
                        ) = await self._save_candidate_suggestions_or_dlq(batch)
                        if dlq_written:
                            self._record_health_failure("candidate_suggestions")
                            batch_had_dlq = True
                            dlq_count += len(messages)
                        if not can_continue:
                            break
                        if dlq_written:
                            batches_count += 1
                            total_processed += len(messages)
                            all_msg_ids.extend([m["id"] for m in messages])
                            await self.redis.ltrim(self._buffer_key, len(raw), -1)
                            continue

                        can_continue, dlq_written = await self._write_graph_or_dlq(
                            batch
                        )
                        if dlq_written:
                            self._record_health_failure("graph_write")
                            batch_had_dlq = True
                            dlq_count += len(messages)
                        if not can_continue:
                            break
                        if not dlq_written:
                            try:
                                await self._mark_batch_processed(batch)
                            except Exception as exc:
                                checkpoint_error = f"CHECKPOINT_COMMIT_FAILED: {exc}"
                                self._record_health_failure("checkpoint")
                                self._mark_batch_work_failed(batch, checkpoint_error)
                                dlq_success = await self.processor.move_to_dead_letter(
                                    batch.messages,
                                    checkpoint_error,
                                    stage="checkpoint",
                                    session_text=batch.session_text,
                                    batch=batch,
                                    session_id=self.session_id,
                                )
                                if not dlq_success:
                                    logger.critical(
                                        "DLQ write failed after checkpoint failure. "
                                        "Leaving messages in buffer."
                                    )
                                    break
                                batch_had_dlq = True
                                dlq_count += len(messages)

                    batches_count += 1
                    total_processed += len(messages)
                    all_msg_ids.extend([m["id"] for m in messages])

                    await self.redis.ltrim(self._buffer_key, len(raw), -1)
                    if not batch_had_dlq:
                        self._health_last_success_at = get_now()
                        self._health_consecutive_failures = 0
                finally:
                    batch.release()
                    self._health_current_batch_size = 0
                    self._health_current_batch_started_at = None
                    if self._health_state != "failed":
                        self._health_state = (
                            "draining" if self._shutdown_requested else "running"
                        )

            await emit(
                self.session_id,
                "pipeline",
                "drain_complete",
                {
                    "batches_processed": batches_count,
                    "total_messages": total_processed,
                    "msg_ids": all_msg_ids,
                    "dlq_count": dlq_count,
                    "partial_flush": flush_partial,
                },
            )
            return deferred_partial

    async def _read_buffer_batch(self) -> tuple[List, List[IngestionMessage]]:
        raw = await self.redis.lrange(self._buffer_key, 0, self.batch_size - 1)
        if not raw:
            await emit(self.session_id, "pipeline", "buffer_empty", {})
            return [], []

        await emit(self.session_id, "pipeline", "buffer_draining", {"queued": len(raw)})

        messages: List[IngestionMessage] = []
        invalid_count = 0
        for item in raw:
            parsed = safe_json_loads(item)
            if (
                not isinstance(parsed, dict)
                or "id" not in parsed
                or "message" not in parsed
            ):
                invalid_count += 1
                continue
            messages.append(parsed)

        if invalid_count:
            await emit(
                self.session_id,
                "pipeline",
                "buffer_invalid_entries",
                {"count": invalid_count},
            )

        return raw, messages

    async def _save_message_logs_or_dlq(
        self,
        batch: IngestionBatch,
    ) -> tuple[bool, bool]:
        with self._batch_diagnostic_scope(batch):
            return await self._save_message_logs_or_dlq_scoped(batch)

    async def _save_message_logs_or_dlq_scoped(
        self,
        batch: IngestionBatch,
    ) -> tuple[bool, bool]:
        message_rows = [
            {
                "id": msg["id"],
                "content": msg["message"],
                "role": msg.get("role", "user"),
                "user_name": self.user_name,
                "session_id": self.session_id,
                "project_id": self.processor.project_id,
                "timestamp": msg.get("timestamp", ""),
            }
            for msg in batch.messages
        ]
        try:
            await asyncio.wait_for(
                self.knowledge_store.save_message_logs(message_rows), timeout=30.0
            )
            batch.mark_message_logs_handled()
            return True, False
        except asyncio.CancelledError:
            batch.cancel_work("Ingestion cancelled while saving message logs")
            raise
        except Exception as e:
            failure = f"MESSAGE_LOG_SAVE_FAILED: {e}"
            self._mark_batch_work_failed(batch, failure)
            dlq_success = await self.processor.move_to_dead_letter(
                batch.messages,
                failure,
                stage="message_log",
                session_text=batch.session_text,
                batch=batch,
                session_id=self.session_id,
            )
            if not dlq_success:
                logger.critical(
                    "DLQ write failed after message log failure. "
                    "Leaving messages in buffer."
                )
                return False, False
            return True, True

    async def _save_candidate_suggestions_or_dlq(
        self,
        batch: IngestionBatch,
    ) -> tuple[bool, bool]:
        with self._batch_diagnostic_scope(batch):
            return await self._save_candidate_suggestions_or_dlq_scoped(batch)

    async def _save_candidate_suggestions_or_dlq_scoped(
        self, batch: IngestionBatch
    ) -> tuple[bool, bool]:
        """Persist suggestions or preserve the batch for stage-aware replay."""

        if not batch.candidate_suggestions:
            batch.mark_candidate_suggestions_handled()
            return True, False

        try:
            await asyncio.wait_for(
                self.knowledge_store.save_candidate_suggestions(
                    batch.scope,
                    batch.candidate_suggestions,
                ),
                timeout=30.0,
            )
            batch.mark_candidate_suggestions_handled()
            return True, False
        except asyncio.CancelledError:
            batch.cancel_work("Ingestion cancelled while saving candidate suggestions")
            raise
        except Exception as e:
            await emit(
                self.session_id,
                "pipeline",
                "candidate_suggestions_save_failed",
                {
                    "error": str(e),
                    "suggestion_count": len(batch.candidate_suggestions),
                },
            )
            failure = f"CANDIDATE_SUGGESTION_SAVE_FAILED: {type(e).__name__}: {e}"
            self._mark_batch_work_failed(batch, failure)
            dlq_success = await self.processor.move_to_dead_letter(
                batch.messages,
                failure,
                stage="candidate_suggestions",
                session_text=batch.session_text,
                batch=batch,
                session_id=self.session_id,
            )
            if not dlq_success:
                logger.critical(
                    "DLQ write failed after candidate suggestion failure. "
                    "Leaving messages in buffer."
                )
                return False, False
            return True, True

    async def _write_graph_or_dlq(
        self,
        batch: IngestionBatch,
    ) -> tuple[bool, bool]:
        with self._batch_diagnostic_scope(batch):
            return await self._write_graph_or_dlq_scoped(batch)

    async def _write_graph_or_dlq_scoped(
        self,
        batch: IngestionBatch,
    ) -> tuple[bool, bool]:
        try:
            graph_success, error_msg = await asyncio.wait_for(
                self.write_to_graph(batch),
                timeout=batch.policy.graph_write_timeout_seconds,
            )
        except asyncio.CancelledError:
            batch.cancel_work("Ingestion cancelled while writing graph data")
            raise
        except asyncio.TimeoutError:
            graph_success, error_msg = False, "GRAPH_WRITE_TIMEOUT"
        except Exception as e:
            graph_success, error_msg = False, str(e)

        if graph_success:
            return True, False

        await emit(
            self.session_id,
            "pipeline",
            "graph_write_failed",
            {"error": error_msg},
        )

        graph_error = error_msg or "GRAPH_WRITE_FAILED [unknown]"
        self._mark_batch_work_failed(batch, graph_error)
        dlq_success = await self.processor.move_to_dead_letter(
            batch.messages,
            graph_error,
            stage="graph_write",
            batch=batch,
            session_id=self.session_id,
        )
        if not dlq_success:
            logger.critical(
                "DLQ write failed after graph failure. Leaving "
                f"{len(batch.messages)} messages in buffer for retry."
            )
            await emit(
                self.session_id,
                "pipeline",
                "dlq_write_failed",
                {"msg_count": len(batch.messages)},
            )
            return False, False
        return True, True

    async def _process_messages(self, batch: IngestionBatch) -> None:
        """Run semantic processing for the batch owned by the drain loop."""

        try:
            await self.processor.process(batch)
        except asyncio.CancelledError:
            batch.cancel_work("Ingestion cancelled during semantic processing")
            raise

    async def _mark_batch_processed(self, batch: IngestionBatch) -> None:
        with self._batch_diagnostic_scope(batch):
            try:
                commit = await commit_ingestion_checkpoint(
                    self.redis,
                    batch,
                )
            except asyncio.CancelledError:
                batch.cancel_work("Ingestion cancelled while committing checkpoint")
                raise
            if commit.threshold_reached:
                try:
                    await emit(
                        self.session_id,
                        "pipeline",
                        "checkpoint_reached",
                        {"message_count": commit.count_before_reset},
                    )
                except Exception as exc:
                    logger.warning("Failed to emit checkpoint event: {}", exc)
            batch.mark_checkpoint_committed()
            self._mark_batch_work_succeeded(batch)

    @staticmethod
    def _batch_diagnostic_scope(batch: IngestionBatch):
        """Build a context manager without using diagnostics for routing."""

        return diagnostic_scope(
            user_name=batch.scope.user_name,
            project_id=batch.scope.project_id,
            session_id=batch.scope.session_id,
            ingestion_batch_id=batch.batch_id,
            work_id=batch.work_unit.id,
        )
