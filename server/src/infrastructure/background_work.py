"""Fair, project-aware scheduling for complete background operations."""

import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
from time import perf_counter
from typing import Awaitable, Callable, Deque, TypeVar

from common.exceptions import KnogginError

ResultT = TypeVar("ResultT")


@dataclass
class QueuedBackgroundWork:
    """In-memory work payload owned by a single project queue."""

    future: asyncio.Future
    operation: Callable[[], Awaitable]
    name: str
    submitted_at: float
    coalesce_key: str | None = None


class BackgroundWorkRejected(KnogginError):
    """Raised when bounded background work cannot be admitted."""

    def __init__(
        self,
        *,
        project_id: str,
        name: str,
        reason: str,
        limit: int,
        queued: int,
    ):
        super().__init__(
            f"Background work '{name}' for project '{project_id}' was rejected: "
            f"{reason}",
            code="background_work_rejected",
            details={
                "project_id": project_id,
                "name": name,
                "reason": reason,
                "limit": limit,
                "queued": queued,
            },
        )


class BackgroundWorkCoordinator:
    """Run background operations fairly across projects.

    A project gets at most one active operation. When that operation completes,
    the next waiting project gets a turn before the same project can run again.
    This coordinator intentionally schedules complete jobs; model-level resource
    limits remain the responsibility of ``ModelWorkCoordinator``.
    """

    def __init__(
        self,
        max_concurrency: int = 1,
        *,
        max_queued_per_project: int = 8,
        max_queued_global: int = 64,
    ):
        if min(max_concurrency, max_queued_per_project, max_queued_global) < 1:
            raise ValueError(
                "background-work concurrency and queue limits must be positive"
            )
        self._max_concurrency = max_concurrency
        self._max_queued_per_project = max_queued_per_project
        self._max_queued_global = max_queued_global
        self._project_queues: dict[str, Deque[QueuedBackgroundWork]] = defaultdict(
            deque
        )
        self._ready_projects: Deque[str] = deque()
        self._ready_project_ids: set[str] = set()
        self._active_project_ids: set[str] = set()
        self._coalesced_futures: dict[tuple[str, str], asyncio.Future] = {}
        self._condition = asyncio.Condition()
        self._workers: list[asyncio.Task] = []
        self._closed = False
        self._submitted = 0
        self._completed = 0
        self._failed = 0
        self._cancelled = 0
        self._coalesced = 0
        self._wait_seconds_total = 0.0
        self._wait_seconds_by_name: dict[str, float] = defaultdict(float)
        self._rejected_by_reason = {
            "global_queue_full": 0,
            "project_queue_full": 0,
        }

    async def start(self) -> None:
        if self._workers:
            return
        if self._closed:
            raise RuntimeError("BackgroundWorkCoordinator is closed")
        self._workers = [
            asyncio.create_task(self._worker(index), name=f"background-work:{index}")
            for index in range(self._max_concurrency)
        ]

    async def submit(
        self,
        project_id: str,
        operation: Callable[[], Awaitable[ResultT]],
        *,
        name: str,
        coalesce_key: str | None = None,
    ) -> ResultT:
        if not project_id:
            raise ValueError("project_id is required for background work")
        if self._closed:
            raise RuntimeError("BackgroundWorkCoordinator is closed")
        await self.start()

        loop = asyncio.get_running_loop()
        async with self._condition:
            self._prune_cancelled_work()
            future = self._get_coalesced_future(project_id, coalesce_key)
            if future is not None:
                self._coalesced += 1
            else:
                future = loop.create_future()
                work = QueuedBackgroundWork(
                    future=future,
                    operation=operation,
                    name=name,
                    submitted_at=perf_counter(),
                    coalesce_key=coalesce_key,
                )
                self._admit_work(project_id, work)
        return await asyncio.shield(future)

    def _get_coalesced_future(
        self,
        project_id: str,
        coalesce_key: str | None,
    ) -> asyncio.Future | None:
        if coalesce_key is None:
            return None
        future = self._coalesced_futures.get((project_id, coalesce_key))
        return future if future is not None and not future.done() else None

    def _admit_work(self, project_id: str, work: QueuedBackgroundWork) -> None:
        project_queued = len(self._project_queues.get(project_id, ()))
        if project_queued >= self._max_queued_per_project:
            self._rejected_by_reason["project_queue_full"] += 1
            raise BackgroundWorkRejected(
                project_id=project_id,
                name=work.name,
                reason="project_queue_full",
                limit=self._max_queued_per_project,
                queued=project_queued,
            )
        total_queued = self._queued_count()
        if total_queued >= self._max_queued_global:
            self._rejected_by_reason["global_queue_full"] += 1
            raise BackgroundWorkRejected(
                project_id=project_id,
                name=work.name,
                reason="global_queue_full",
                limit=self._max_queued_global,
                queued=total_queued,
            )
        self._project_queues[project_id].append(work)
        if work.coalesce_key is not None:
            self._coalesced_futures[(project_id, work.coalesce_key)] = work.future
        self._submitted += 1
        self._mark_project_ready(project_id)
        self._condition.notify()

    def _queued_count(self) -> int:
        return sum(len(queue) for queue in self._project_queues.values())

    def _prune_cancelled_work(self) -> None:
        for project_id, queue in list(self._project_queues.items()):
            kept = deque(work for work in queue if not work.future.cancelled())
            self._cancelled += len(queue) - len(kept)
            if kept:
                self._project_queues[project_id] = kept
            else:
                del self._project_queues[project_id]

    def _mark_project_ready(self, project_id: str) -> None:
        queue = self._project_queues.get(project_id)
        if (
            project_id not in self._active_project_ids
            and project_id not in self._ready_project_ids
            and queue
        ):
            self._ready_projects.append(project_id)
            self._ready_project_ids.add(project_id)

    async def _next_work(self) -> tuple[str, QueuedBackgroundWork] | None:
        async with self._condition:
            while True:
                while self._ready_projects:
                    project_id = self._ready_projects.popleft()
                    self._ready_project_ids.remove(project_id)
                    queue = self._project_queues.get(project_id)
                    if not queue:
                        continue
                    work = queue.popleft()
                    self._active_project_ids.add(project_id)
                    if not queue:
                        del self._project_queues[project_id]
                    return project_id, work
                if self._closed:
                    return None
                await self._condition.wait()

    async def _finish_project(
        self,
        project_id: str,
        work: QueuedBackgroundWork,
    ) -> None:
        async with self._condition:
            self._active_project_ids.discard(project_id)
            if work.coalesce_key is not None:
                key = (project_id, work.coalesce_key)
                if self._coalesced_futures.get(key) is work.future:
                    del self._coalesced_futures[key]
            self._mark_project_ready(project_id)
            self._condition.notify_all()

    async def _worker(self, index: int) -> None:
        while True:
            item = await self._next_work()
            if item is None:
                return
            project_id, work = item
            try:
                if work.future.cancelled():
                    self._cancelled += 1
                    continue
                wait_seconds = perf_counter() - work.submitted_at
                self._wait_seconds_total += wait_seconds
                self._wait_seconds_by_name[work.name] += wait_seconds
                result = await work.operation()
            except BaseException as exc:
                self._failed += 1
                if not work.future.done():
                    work.future.set_exception(exc)
            else:
                self._completed += 1
                if not work.future.done():
                    work.future.set_result(result)
            finally:
                await self._finish_project(project_id, work)

    def snapshot(self) -> dict[str, object]:
        """Return in-memory scheduling metrics for future instrumentation."""
        queued_by_project = {
            project_id: len(queue)
            for project_id, queue in self._project_queues.items()
        }
        return {
            "max_concurrency": self._max_concurrency,
            "max_queued_per_project": self._max_queued_per_project,
            "max_queued_global": self._max_queued_global,
            "queued": sum(queued_by_project.values()),
            "queued_by_project": queued_by_project,
            "ready_projects": list(self._ready_projects),
            "active_projects": sorted(self._active_project_ids),
            "submitted": self._submitted,
            "completed": self._completed,
            "failed": self._failed,
            "cancelled": self._cancelled,
            "coalesced": self._coalesced,
            "wait_seconds_total": self._wait_seconds_total,
            "wait_seconds_by_name": dict(self._wait_seconds_by_name),
            "rejected": sum(self._rejected_by_reason.values()),
            "rejected_by_reason": dict(self._rejected_by_reason),
        }

    async def shutdown(self) -> None:
        if self._closed:
            return
        self._closed = True
        async with self._condition:
            for queue in self._project_queues.values():
                for work in queue:
                    if not work.future.done():
                        work.future.cancel()
                        self._cancelled += 1
            self._project_queues.clear()
            self._ready_projects.clear()
            self._ready_project_ids.clear()
            self._coalesced_futures.clear()
            self._condition.notify_all()
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
