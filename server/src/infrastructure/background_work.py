"""One bounded global queue for non-model background operations."""

import asyncio
from collections import defaultdict, deque
from collections.abc import Mapping
from dataclasses import dataclass
from time import perf_counter
from typing import Awaitable, Callable, TypeVar

from common.exceptions import KnogginError
from common.schema.health import sanitize_health_details

ResultT = TypeVar("ResultT")


@dataclass
class QueuedBackgroundWork:
    """One globally queued operation tagged with its owning project."""

    project_id: str
    owner: str
    future: asyncio.Future
    operation: Callable[[], Awaitable]
    name: str
    submitted_at: float
    coalesce_key: str | None = None


class BackgroundWorkRejected(KnogginError):
    """Raised when the bounded global queue cannot admit more work."""

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
    """Run complete operations through one bounded global FIFO queue.

    ``project_id`` remains an ownership tag: a project runtime can cancel all
    queued and active work it owns during shutdown. It is not a fairness lane.
    Model scheduling remains owned by ``ModelWorkCoordinator``.
    """

    def __init__(self, max_concurrency: int = 1, *, max_queued_global: int = 64):
        if min(max_concurrency, max_queued_global) < 1:
            raise ValueError("background-work concurrency and queue limits must be positive")
        self._max_concurrency = max_concurrency
        self._max_queued_global = max_queued_global
        self._queue: deque[QueuedBackgroundWork] = deque()
        self._coalesced_futures: dict[tuple[str, str], asyncio.Future] = {}
        self._active_operations: dict[asyncio.Task, QueuedBackgroundWork] = {}
        self._condition = asyncio.Condition()
        self._start_lock = asyncio.Lock()
        self._workers: list[asyncio.Task] = []
        self._closed = False
        self._submitted = 0
        self._completed = 0
        self._failed = 0
        self._cancelled = 0
        self._coalesced = 0
        self._wait_seconds_total = 0.0
        self._wait_seconds_by_name: dict[str, float] = defaultdict(float)
        self._rejected_by_reason = {"global_queue_full": 0}

    async def start(self) -> None:
        if self._workers:
            return
        async with self._start_lock:
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
        owner: str | None = None,
        coalesce_key: str | None = None,
    ) -> ResultT:
        if not project_id:
            raise ValueError("project_id is required for background work")
        if self._closed:
            raise RuntimeError("BackgroundWorkCoordinator is closed")
        await self.start()

        loop = asyncio.get_running_loop()
        async with self._condition:
            if self._closed:
                raise RuntimeError("BackgroundWorkCoordinator is closed")
            future = self._get_coalesced_future(project_id, coalesce_key)
            if future is not None:
                self._coalesced += 1
            else:
                queued = len(self._queue)
                if queued >= self._max_queued_global:
                    self._rejected_by_reason["global_queue_full"] += 1
                    raise BackgroundWorkRejected(
                        project_id=project_id,
                        name=name,
                        reason="global_queue_full",
                        limit=self._max_queued_global,
                        queued=queued,
                    )
                future = loop.create_future()
                work = QueuedBackgroundWork(
                    project_id=project_id,
                    owner=owner or f"project:{project_id}:{name}",
                    future=future,
                    operation=operation,
                    name=name,
                    submitted_at=perf_counter(),
                    coalesce_key=coalesce_key,
                )
                self._queue.append(work)
                if coalesce_key is not None:
                    self._coalesced_futures[(project_id, coalesce_key)] = future
                self._submitted += 1
                self._condition.notify()
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

    async def _next_work(self) -> QueuedBackgroundWork | None:
        async with self._condition:
            while True:
                while self._queue:
                    work = self._queue.popleft()
                    if work.future.cancelled():
                        self._cancelled += 1
                        self._clear_coalesced_future(work)
                        continue
                    return work
                if self._closed:
                    return None
                await self._condition.wait()

    async def _worker(self, _index: int) -> None:
        while True:
            work = await self._next_work()
            if work is None:
                return
            operation_task = asyncio.create_task(
                work.operation(),
                name=f"background-operation:{work.project_id}:{work.name}",
            )
            self._active_operations[operation_task] = work
            try:
                wait_seconds = perf_counter() - work.submitted_at
                self._wait_seconds_total += wait_seconds
                self._wait_seconds_by_name[work.name] += wait_seconds
                result = await operation_task
            except asyncio.CancelledError:
                self._cancelled += 1
                if not work.future.done():
                    work.future.cancel()
            except Exception as exc:
                self._failed += 1
                if not work.future.done():
                    work.future.set_exception(exc)
            else:
                self._completed += 1
                if not work.future.done():
                    work.future.set_result(result)
            finally:
                self._active_operations.pop(operation_task, None)
                self._clear_coalesced_future(work)

    def _clear_coalesced_future(self, work: QueuedBackgroundWork) -> None:
        if work.coalesce_key is None:
            return
        key = (work.project_id, work.coalesce_key)
        if self._coalesced_futures.get(key) is work.future:
            del self._coalesced_futures[key]

    async def cancel_owner(self, owner: str) -> None:
        """Cancel and join one subsystem owner's work without broad project teardown."""
        if not isinstance(owner, str) or not owner.strip():
            raise ValueError("owner is required for background-work cancellation")
        normalized_owner = owner.strip()
        async with self._condition:
            retained: deque[QueuedBackgroundWork] = deque()
            while self._queue:
                work = self._queue.popleft()
                if work.owner != normalized_owner:
                    retained.append(work)
                    continue
                if not work.future.done():
                    work.future.cancel()
                    self._cancelled += 1
                self._clear_coalesced_future(work)
            self._queue = retained
            active = [
                task
                for task, work in self._active_operations.items()
                if work.owner == normalized_owner and not task.done()
            ]
            for task in active:
                task.cancel()
            self._condition.notify_all()

        current = asyncio.current_task()
        joinable = [task for task in active if task is not current]
        if joinable:
            await asyncio.gather(*joinable, return_exceptions=True)

    def snapshot(self) -> dict[str, object]:
        """Return global queue metrics and project ownership diagnostics."""

        queued_by_project: dict[str, int] = defaultdict(int)
        queued_categories_by_project: dict[str, list[str]] = defaultdict(list)
        for work in self._queue:
            queued_by_project[work.project_id] += 1
            if len(queued_categories_by_project[work.project_id]) < 20:
                queued_categories_by_project[work.project_id].append(work.name[:100])

        active_by_project: dict[str, list[str]] = defaultdict(list)
        active_by_owner: dict[str, list[str]] = defaultdict(list)
        for task, work in self._active_operations.items():
            if not task.done() and len(active_by_project[work.project_id]) < 20:
                active_by_project[work.project_id].append(work.name[:100])
            if not task.done() and len(active_by_owner[work.owner]) < 20:
                active_by_owner[work.owner].append(work.name[:100])

        return {
            "max_concurrency": self._max_concurrency,
            "max_queued_global": self._max_queued_global,
            "queued": len(self._queue),
            "queued_by_project": dict(queued_by_project),
            "queued_categories_by_project": dict(queued_categories_by_project),
            "active_by_project": dict(active_by_project),
            "active_by_owner": dict(active_by_owner),
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

    def snapshot_for_health(self, *, project_id: str | None = None) -> dict[str, object]:
        """Return health-safe queue metrics, optionally scoped by ownership tag."""

        snapshot = dict(self.snapshot())
        queued_by_project = snapshot.pop("queued_by_project", {})
        queued_categories = snapshot.pop("queued_categories_by_project", {})
        active_by_project = snapshot.pop("active_by_project", {})
        snapshot.pop("active_by_owner", None)
        if project_id is not None:
            snapshot["queued_for_project"] = (
                queued_by_project.get(project_id, 0)
                if isinstance(queued_by_project, Mapping)
                else 0
            )
            snapshot["queued_operation_categories"] = (
                queued_categories.get(project_id, [])
                if isinstance(queued_categories, Mapping)
                else []
            )
            snapshot["active_operation_categories"] = (
                active_by_project.get(project_id, [])
                if isinstance(active_by_project, Mapping)
                else []
            )
            snapshot["active_for_project"] = bool(
                snapshot["active_operation_categories"]
            )
        return sanitize_health_details(snapshot)

    async def shutdown(self) -> None:
        """Reject new work and cancel/join every queued or active operation."""

        if self._closed:
            return
        self._closed = True
        async with self._condition:
            while self._queue:
                work = self._queue.popleft()
                if not work.future.done():
                    work.future.cancel()
                    self._cancelled += 1
                self._clear_coalesced_future(work)
            active = [
                task for task in self._active_operations if not task.done()
            ]
            for task in active:
                task.cancel()
            self._condition.notify_all()

        current = asyncio.current_task()
        joinable = [task for task in active if task is not current]
        if joinable:
            await asyncio.gather(*joinable, return_exceptions=True)
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
