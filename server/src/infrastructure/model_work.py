"""Bounded, prioritized execution for process-wide ML and NLP resources."""

import asyncio
import itertools
from concurrent.futures import Executor
from dataclasses import dataclass
from enum import IntEnum
from time import perf_counter
from typing import Awaitable, Callable, Optional, TypeVar

from common.schema.health import sanitize_health_details
from infrastructure.work_record import WorkRecord

ResultT = TypeVar("ResultT")


class ModelWorkPriority(IntEnum):
    """Lower values are scheduled before higher values."""

    FOREGROUND = 0
    BACKGROUND = 10


@dataclass
class QueuedModelTask:
    """Runtime-only queue payload owned by workflow telemetry."""

    future: asyncio.Future
    operation: Optional[Callable[[], Awaitable]]
    queued_at: float
    name: str
    work_record: Optional[WorkRecord] = None
    parent_work_record: Optional[WorkRecord] = None
    cancellation_summary: str = "Caller cancelled before execution"


class ModelWorkCoordinator:
    """Run ML/NLP work in bounded foreground and background execution lanes.

    The foreground lane is reserved for interactive agent requests.  Background
    work therefore cannot occupy its slot, even while it is running.
    """

    def __init__(
        self,
        executor: Executor,
        *,
        foreground_concurrency: int = 1,
        background_concurrency: int = 1,
        foreground_timeout_seconds: float | None = 30.0,
    ):
        if foreground_concurrency < 1 or background_concurrency < 1:
            raise ValueError("each model-work lane must have at least one worker")
        if foreground_timeout_seconds is not None and foreground_timeout_seconds <= 0:
            raise ValueError("foreground_timeout_seconds must be positive or None")
        self._executor = executor
        self._foreground_concurrency = foreground_concurrency
        self._background_concurrency = background_concurrency
        self._foreground_timeout_seconds = foreground_timeout_seconds
        self._queues = {
            ModelWorkPriority.FOREGROUND: asyncio.PriorityQueue(),
            ModelWorkPriority.BACKGROUND: asyncio.PriorityQueue(),
        }
        self._sequence = itertools.count()
        self._workers: list[asyncio.Task] = []
        self._closed = False
        self._in_flight_by_priority = {
            priority.name.lower(): 0 for priority in ModelWorkPriority
        }
        self._submitted = 0
        self._completed = 0
        self._failed = 0
        self._timed_out = 0
        self._wait_seconds_total = 0.0
        self._run_seconds_total = 0.0
        self._queued_by_priority = {
            priority.name.lower(): 0 for priority in ModelWorkPriority
        }
        self._work_by_name: dict[str, dict[str, float | int]] = {}

    async def start(self) -> None:
        if self._workers:
            return
        if self._closed:
            raise RuntimeError("ModelWorkCoordinator is closed")
        self._workers = [
            *[
                asyncio.create_task(
                    self._worker(ModelWorkPriority.FOREGROUND, index),
                    name=f"model-work:foreground:{index}",
                )
                for index in range(self._foreground_concurrency)
            ],
            *[
                asyncio.create_task(
                    self._worker(ModelWorkPriority.BACKGROUND, index),
                    name=f"model-work:background:{index}",
                )
                for index in range(self._background_concurrency)
            ],
        ]

    async def run_blocking(
        self,
        operation: Callable[[], ResultT],
        *,
        priority: ModelWorkPriority,
        name: str,
        work_record: Optional[WorkRecord] = None,
        parent_work_record: Optional[WorkRecord] = None,
        timeout_seconds: float | None = None,
    ) -> ResultT:
        loop = asyncio.get_running_loop()
        return await self.submit(
            lambda: loop.run_in_executor(self._executor, operation),
            priority=priority,
            name=name,
            work_record=work_record,
            parent_work_record=parent_work_record,
            timeout_seconds=timeout_seconds,
        )

    async def submit(
        self,
        operation: Callable[[], Awaitable[ResultT]],
        *,
        priority: ModelWorkPriority,
        name: str,
        work_record: Optional[WorkRecord] = None,
        parent_work_record: Optional[WorkRecord] = None,
        timeout_seconds: float | None = None,
    ) -> ResultT:
        if self._closed:
            raise RuntimeError("ModelWorkCoordinator is closed")
        self._require_work_record("work_record", work_record)
        self._require_work_record("parent_work_record", parent_work_record)
        await self.start()

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        queued_at = perf_counter()
        priority_name = priority.name.lower()
        self._submitted += 1
        self._queued_by_priority[priority_name] += 1
        metrics = self._work_by_name.setdefault(
            name,
            {
                "submitted": 0,
                "completed": 0,
                "failed": 0,
                "queued": 0,
                "wait_seconds_total": 0.0,
                "run_seconds_total": 0.0,
            },
        )
        metrics["submitted"] += 1
        metrics["queued"] += 1
        task = QueuedModelTask(
            future=future,
            operation=operation,
            queued_at=queued_at,
            name=name,
            work_record=work_record,
            parent_work_record=parent_work_record,
        )
        if work_record is not None:
            work_record.mark_queued()
        await self._queues[priority].put((int(priority), next(self._sequence), task))
        deadline = timeout_seconds
        if deadline is None and priority is ModelWorkPriority.FOREGROUND:
            deadline = self._foreground_timeout_seconds
        if deadline is None:
            return await future
        try:
            return await asyncio.wait_for(asyncio.shield(future), timeout=deadline)
        except asyncio.TimeoutError:
            if not future.done():
                task.cancellation_summary = "Foreground deadline exceeded before completion"
                future.cancel()
                self._timed_out += 1
            raise

    async def _worker(self, lane: ModelWorkPriority, index: int) -> None:
        queue = self._queues[lane]
        while True:
            priority_value, _, task = await queue.get()
            if task.operation is None:
                queue.task_done()
                return

            priority_name = ModelWorkPriority(priority_value).name.lower()
            self._queued_by_priority[priority_name] -= 1
            metrics = self._work_by_name[task.name]
            metrics["queued"] -= 1
            if task.future.cancelled():
                self._mark_cancelled(task, task.cancellation_summary)
                queue.task_done()
                continue

            self._in_flight_by_priority[priority_name] += 1
            wait_seconds = perf_counter() - task.queued_at
            self._wait_seconds_total += wait_seconds
            metrics["wait_seconds_total"] += wait_seconds
            if task.work_record is not None:
                task.work_record.mark_running()
            started_at = perf_counter()
            try:
                result = await task.operation()
            except BaseException as exc:
                self._failed += 1
                metrics["failed"] += 1
                if isinstance(exc, asyncio.CancelledError):
                    self._mark_cancelled(task, "Model work cancelled during execution")
                elif task.work_record is not None:
                    task.work_record.mark_failed(str(exc))
                    self._attach_child_summary(task)
                if not task.future.done():
                    task.future.set_exception(exc)
            else:
                self._completed += 1
                metrics["completed"] += 1
                if task.work_record is not None:
                    task.work_record.mark_succeeded()
                    self._attach_child_summary(task)
                if not task.future.done():
                    task.future.set_result(result)
            finally:
                run_seconds = perf_counter() - started_at
                self._run_seconds_total += run_seconds
                metrics["run_seconds_total"] += run_seconds
                self._in_flight_by_priority[priority_name] -= 1
                queue.task_done()

    @staticmethod
    def _attach_child_summary(task: QueuedModelTask) -> None:
        if task.work_record is not None and task.parent_work_record is not None:
            task.parent_work_record.add_model_work_summary(task.work_record)

    def _mark_cancelled(self, task: QueuedModelTask, summary: str) -> None:
        if task.work_record is not None:
            task.work_record.mark_cancelled(summary)
            self._attach_child_summary(task)

    @staticmethod
    def _require_work_record(name: str, value: Optional[WorkRecord]) -> None:
        if value is not None and not isinstance(value, WorkRecord):
            raise TypeError(f"{name} must be a WorkRecord")

    def snapshot(self) -> dict[str, object]:
        """Return cheap in-memory metrics suitable for future API instrumentation."""
        return {
            "foreground_concurrency": self._foreground_concurrency,
            "background_concurrency": self._background_concurrency,
            "foreground_timeout_seconds": self._foreground_timeout_seconds,
            "queued": sum(queue.qsize() for queue in self._queues.values()),
            "queued_by_priority": dict(self._queued_by_priority),
            "in_flight": sum(self._in_flight_by_priority.values()),
            "in_flight_by_priority": dict(self._in_flight_by_priority),
            "submitted": self._submitted,
            "completed": self._completed,
            "failed": self._failed,
            "timed_out": self._timed_out,
            "wait_seconds_total": self._wait_seconds_total,
            "run_seconds_total": self._run_seconds_total,
            "work_by_name": {
                name: dict(metrics) for name, metrics in self._work_by_name.items()
            },
        }

    def snapshot_for_health(self) -> dict[str, object]:
        """Return a bounded public projection of the internal metrics."""

        return sanitize_health_details(self.snapshot())

    async def shutdown(self) -> None:
        if self._closed:
            return
        self._closed = True

        for lane, queue in self._queues.items():
            while not queue.empty():
                priority_value, _, task = queue.get_nowait()
                if task.operation is not None and not task.future.done():
                    task.future.cancel()
                    priority_name = ModelWorkPriority(priority_value).name.lower()
                    self._queued_by_priority[priority_name] -= 1
                    self._work_by_name[task.name]["queued"] -= 1
                    self._mark_cancelled(task, "Coordinator shutdown before execution")
                queue.task_done()

            worker_count = (
                self._foreground_concurrency
                if lane is ModelWorkPriority.FOREGROUND
                else self._background_concurrency
            )
            for _ in range(worker_count):
                future = asyncio.get_running_loop().create_future()
                await queue.put(
                    (
                        99,
                        next(self._sequence),
                        QueuedModelTask(
                            future=future,
                            operation=None,
                            queued_at=perf_counter(),
                            name="shutdown",
                        ),
                    )
                )
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
