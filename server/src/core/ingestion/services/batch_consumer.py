import asyncio
from typing import Awaitable, Callable, Dict, List, Optional

import redis.asyncio as aioredis
from loguru import logger

from common.schema.settings import IngestionSettings
from common.utils.events import emit, emit_sync
from common.utils.json_utils import safe_json_loads
from core.ingestion.batch import IngestionBatch, IngestionMessage
from core.ingestion.checkpoint import commit_ingestion_checkpoint
from core.ingestion.services.pipeline_service import IngestionPipeline
from infrastructure.knowledge_store import KnowledgeStore
from infrastructure.redis_client import RedisKeys


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
        knowledge_store: KnowledgeStore,
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
        self._wake_event.set()  # wake if waiting

        try:
            await self._task
        except asyncio.CancelledError:
            pass

        self._task = None
        await emit(self.session_id, "pipeline", "consumer_stopped", {})

    def signal(self):
        self._wake_event.set()

    def _on_task_done(self, task: asyncio.Task):
        if task.cancelled():
            logger.info("IngestionWorker task cancelled")
            return

        if exc := task.exception():
            logger.error(f"IngestionWorker task failed: {exc}")

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

    def update_settings(self, config: IngestionSettings):
        """Update settings dynamically while running."""
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

                conversation = await self.get_session_context(
                    self.session_window, messages[0]["id"]
                )
                session_text = self._format_session_text(conversation)

                try:
                    batch = await self._process_messages(messages, session_text)
                except Exception as e:
                    logger.error(
                        f"Fatal error during IngestionPipeline computation: {e}"
                    )
                    batch = IngestionBatch.open(
                        user_name=self.user_name,
                        project_id=self.processor.project_id,
                        session_id=self.session_id,
                        messages=messages,
                        session_text=session_text,
                    )
                    batch.fail(f"Fatal exception: {e}")

                if not batch.success:
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
                    dlq_count += len(messages)
                else:
                    batch.set_checkpoint_policy(self.checkpoint_interval)
                    can_continue, dlq_written = await self._save_message_logs_or_dlq(batch)
                    if dlq_written:
                        dlq_count += len(messages)
                    if not can_continue:
                        break
                    if dlq_written:
                        batches_count += 1
                        total_processed += len(messages)
                        all_msg_ids.extend([m["id"] for m in messages])
                        await self.redis.ltrim(self._buffer_key, len(raw), -1)
                        continue

                    can_continue, dlq_written = (
                        await self._save_candidate_suggestions_or_dlq(batch)
                    )
                    if dlq_written:
                        dlq_count += len(messages)
                    if not can_continue:
                        break
                    if dlq_written:
                        batches_count += 1
                        total_processed += len(messages)
                        all_msg_ids.extend([m["id"] for m in messages])
                        await self.redis.ltrim(self._buffer_key, len(raw), -1)
                        continue

                    can_continue, dlq_written = await self._write_graph_or_dlq(batch)
                    if dlq_written:
                        dlq_count += len(messages)
                    if not can_continue:
                        break
                    if not dlq_written:
                        try:
                            await self._mark_batch_processed(batch)
                        except Exception as exc:
                            dlq_success = await self.processor.move_to_dead_letter(
                                batch.messages,
                                f"CHECKPOINT_COMMIT_FAILED: {exc}",
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
                            dlq_count += len(messages)

                batches_count += 1
                total_processed += len(messages)
                all_msg_ids.extend([m["id"] for m in messages])

                await self.redis.ltrim(self._buffer_key, len(raw), -1)

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

        await emit(
            self.session_id, "pipeline", "buffer_draining", {"queued": len(raw)}
        )

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
        except Exception as e:
            dlq_success = await self.processor.move_to_dead_letter(
                batch.messages,
                f"MESSAGE_LOG_SAVE_FAILED: {e}",
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
            dlq_success = await self.processor.move_to_dead_letter(
                batch.messages,
                "CANDIDATE_SUGGESTION_SAVE_FAILED: "
                f"{type(e).__name__}: {e}",
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
        try:
            graph_success, error_msg = await asyncio.wait_for(
                self.write_to_graph(batch), timeout=self.batch_timeout
            )
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

        dlq_success = await self.processor.move_to_dead_letter(
            batch.messages,
            error_msg or "GRAPH_WRITE_FAILED [unknown]",
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

    async def _process_messages(
        self, messages: List[Dict], session_text: str
    ) -> IngestionBatch:
        """Allocate and retain the aggregate through the worker lifecycle."""

        batch = self.processor.open_batch(
            messages, session_text, session_id=self.session_id
        )
        await self.processor.process(batch)
        return batch

    async def _mark_batch_processed(self, batch: IngestionBatch) -> None:
        commit = await commit_ingestion_checkpoint(
            self.redis,
            batch,
            checkpoint_interval=self.checkpoint_interval,
        )
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
