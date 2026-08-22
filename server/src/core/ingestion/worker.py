"""PostgreSQL queue worker; Redis only wakes an active consumer."""

import asyncio
from typing import Awaitable, Callable, Dict, List, Optional

from loguru import logger

from common.exceptions import (
    DependencyError,
    LLMBudgetExceededError,
    LLMProviderError,
    LLMResponseError,
    StorageError,
)
from common.schema.settings import IngestionSettings
from common.utils.events import emit, emit_sync
from common.utils.time_utils import get_now
from core.ingestion.batch import IngestionBatch
from core.ingestion.pipeline import IngestionPipeline
from core.ingestion.ports import IngestionPersistence
from infrastructure.work_record import WorkStatus


class IngestionWorker:
    """Claim sealed messages, process them, and commit through GraphWriter."""

    def __init__(
        self,
        user_name: str,
        session_id: str,
        knowledge_store: IngestionPersistence,
        processor: IngestionPipeline,
        get_session_context: Callable[[int, Optional[int]], Awaitable[List[Dict]]],
        write_to_graph: Callable[[IngestionBatch], Awaitable[object]],
        settings: IngestionSettings,
    ):
        self.user_name, self.session_id = user_name, session_id
        self.knowledge_store, self.processor = knowledge_store, processor
        self.get_session_context, self.write_to_graph = (
            get_session_context,
            write_to_graph,
        )
        self._wake_event, self._shutdown_requested, self._task = (
            asyncio.Event(),
            False,
            None,
        )
        self._flush_future = None
        self._health_state, self._health_started_at = "not_started", None
        self._health_current_batch_size = 0
        self._health_current_batch_started_at = None
        self._health_last_success_at = None
        self._health_last_failure_category = self._health_last_failure_at = None
        self._health_consecutive_failures = 0
        self.update_settings(settings)

    def update_settings(self, config: IngestionSettings) -> None:
        self.settings = config
        self.batch_size, self.batch_timeout = config.batch_size, config.batch_timeout
        self.session_window = config.session_window
        self.message_lifecycle_poll_seconds = config.message_lifecycle_poll_seconds
        self.ingestion_batch_settle_delay_seconds = (
            config.ingestion_batch_settle_delay_seconds
        )
        self.ingestion_max_attempts = config.ingestion_max_attempts

    def start(self) -> None:
        if self._task is not None:
            return
        self._shutdown_requested, self._health_state = False, "running"
        self._health_started_at = get_now()
        self._task = asyncio.create_task(self._run())
        self._task.add_done_callback(self._on_task_done)
        emit_sync(
            self.session_id,
            "pipeline",
            "consumer_started",
            {"batch_size": self.batch_size},
        )

    async def stop(self) -> None:
        if self._task is None:
            return
        self._shutdown_requested, self._health_state = True, "stopping"
        self._wake_event.set()
        await self._task
        self._task = None
        if self._health_state != "failed":
            self._health_state = "stopped"
        await emit(self.session_id, "pipeline", "consumer_stopped", {})

    def signal(self) -> None:
        self._wake_event.set()

    async def flush(self) -> None:
        if self._task is None or self._task.done():
            return
        if self._flush_future is None or self._flush_future.done():
            self._flush_future = asyncio.get_running_loop().create_future()
            self.signal()
        await self._flush_future

    def _on_task_done(self, task: asyncio.Task) -> None:
        if not task.cancelled() and (exc := task.exception()):
            self._record_failure("worker_task")
            self._health_state = "failed"
            logger.error("Ingestion worker task failed: {}", exc)

    def health_snapshot(self) -> dict[str, object]:
        def iso(value):
            return value.isoformat() if value else None

        return {
            "state": self._health_state,
            "started_at": iso(self._health_started_at),
            "current_batch_size": self._health_current_batch_size,
            "current_batch_started_at": iso(self._health_current_batch_started_at),
            "last_success_at": iso(self._health_last_success_at),
            "last_failure_category": self._health_last_failure_category,
            "last_failure_at": iso(self._health_last_failure_at),
            "consecutive_failures": self._health_consecutive_failures,
            "batch_size": self.batch_size,
            "batch_timeout_seconds": self.batch_timeout,
        }

    def _record_failure(self, category: str) -> None:
        self._health_last_failure_category, self._health_last_failure_at = (
            category,
            get_now(),
        )
        self._health_consecutive_failures += 1

    @staticmethod
    def _classify_failure(exc: Exception) -> tuple[bool, str, str]:
        code = str(getattr(exc, "code", "") or type(exc).__name__)
        if isinstance(exc, LLMResponseError | ValueError | TypeError):
            return False, "pipeline", code
        if isinstance(
            exc,
            StorageError
            | LLMProviderError
            | DependencyError
            | ConnectionError
            | TimeoutError
            | OSError,
        ):
            return True, "runtime", code
        return True, "runtime", code

    @staticmethod
    def _format_session_text(turns: List[Dict]) -> str:
        return "\n".join(f"[{turn['role_label']}]: {turn['content']}" for turn in turns)

    async def _run(self) -> None:
        while not self._shutdown_requested:
            try:
                await asyncio.wait_for(
                    self._wake_event.wait(), self.message_lifecycle_poll_seconds
                )
            except asyncio.TimeoutError:
                pass
            self._wake_event.clear()
            try:
                await self._drain_durable_queue()
            except Exception as exc:
                self._record_failure("worker_loop")
                logger.exception("Ingestion queue drain failed: {}", exc)
            finally:
                if self._flush_future is not None and not self._flush_future.done():
                    self._flush_future.set_result(None)
                self._flush_future = None

    async def _drain_durable_queue(self) -> None:
        store, project = self.knowledge_store, self.processor.project_id
        await store.seal_due_user_messages(
            user_name=self.user_name,
            project_id=project,
            session_id=self.session_id,
            settle_delay_seconds=self.ingestion_batch_settle_delay_seconds,
        )
        processed = 0
        while not self._shutdown_requested:
            claim = await store.claim_next_ingestion_batch(
                user_name=self.user_name,
                project_id=project,
                session_id=self.session_id,
                batch_size=self.batch_size,
            )
            if claim is None:
                break
            batch = None
            try:
                messages = claim.messages
                (
                    self._health_current_batch_size,
                    self._health_current_batch_started_at,
                ) = len(messages), get_now()
                context = await self.get_session_context(
                    self.session_window, messages[0]["id"]
                )
                batch = self.processor.open_batch(
                    messages,
                    self._format_session_text(context),
                    session_id=self.session_id,
                    policy=self.processor.capture_policy(),
                    batch_id=claim.batch_id,
                )
                await self.processor.process(batch)
                if not batch.success:
                    raise batch.failure or RuntimeError(
                        batch.error or "ingestion processing failed"
                    )
                await self.write_to_graph(batch)
                if batch.work_unit.status is WorkStatus.PENDING:
                    batch.work_unit.mark_running()
                if batch.work_unit.status is WorkStatus.RUNNING:
                    batch.work_unit.mark_succeeded("Durable ingestion commit completed")
                processed += 1
                self._health_last_success_at = get_now()
                self._health_consecutive_failures = 0
            except asyncio.CancelledError:
                await store.release_ingestion_claim(
                    user_name=self.user_name,
                    project_id=project,
                    session_id=self.session_id,
                    batch_id=claim.batch_id,
                    blocked=False,
                )
                raise
            except LLMBudgetExceededError:
                await store.release_ingestion_claim(
                    user_name=self.user_name,
                    project_id=project,
                    session_id=self.session_id,
                    batch_id=claim.batch_id,
                    blocked=False,
                )
                break
            except Exception as exc:
                self._record_failure("durable_batch")
                retryable, stage, code = self._classify_failure(exc)
                await store.fail_ingestion_claim(
                    user_name=self.user_name,
                    project_id=project,
                    session_id=self.session_id,
                    batch_id=claim.batch_id,
                    failure_stage=stage,
                    failure_code=code,
                    error_summary=str(exc),
                    retryable=retryable,
                    max_attempts=self.ingestion_max_attempts,
                )
                break
            finally:
                if batch is not None:
                    batch.release()
                self._health_current_batch_size = 0
                self._health_current_batch_started_at = None
        await emit(
            self.session_id,
            "pipeline",
            "drain_complete",
            {"batches_processed": processed},
        )
