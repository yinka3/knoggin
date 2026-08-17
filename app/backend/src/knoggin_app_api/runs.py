"""In-memory run resources owned by the UI-specific FastAPI backend."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Literal
from uuid import uuid4

from loguru import logger

from knoggin import Knoggin, SourceProvenance, Turn, source_provenance_from_response


RunStatus = Literal[
    "queued",
    "running",
    "awaiting_input",
    "completed",
    "failed",
    "cancelled",
]
_TERMINAL_STATUSES = {"awaiting_input", "completed", "failed", "cancelled"}


@dataclass(frozen=True, slots=True)
class RunSnapshot:
    """Current state of one UI API run resource."""

    run_id: str
    session_id: str
    status: RunStatus
    created_at: datetime
    completed_at: datetime | None
    result: dict[str, Any] | None
    error: dict[str, Any] | None
    sources: tuple[SourceProvenance, ...] = ()


@dataclass(frozen=True, slots=True)
class RunEvent:
    """One UI API run event before its HTTP/SSE projection."""

    run_id: str
    session_id: str
    event: str
    sequence: int
    timestamp: datetime
    data: dict[str, Any]


@dataclass(slots=True)
class _RunRecord:
    run_id: str
    session_id: str
    idempotency_key: str | None = None
    task: asyncio.Task[None] | None = None
    status: RunStatus = "queued"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None
    sequence: int = 0
    events: list[RunEvent] = field(default_factory=list)
    subscribers: set[asyncio.Queue[RunEvent]] = field(default_factory=set)
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    sources: tuple[SourceProvenance, ...] = ()

    def remember(self, event: RunEvent) -> None:
        self.events.append(event)
        if len(self.events) > 256:
            del self.events[:-256]


class RunManager:
    """Adapt direct SDK streams into UI API run resources and SSE replay."""

    def __init__(self, knoggin: Knoggin):
        self.knoggin = knoggin
        self.runs: dict[str, _RunRecord] = {}
        self._idempotent_runs: dict[tuple[str, str], str] = {}
        self._closed = False

    async def submit_turn(
        self,
        *,
        session_id: str,
        turn: Turn,
        idempotency_key: str | None = None,
    ) -> RunSnapshot:
        session_id = session_id.strip()
        if not session_id:
            raise ValueError("session_id is required")
        idempotency_key = (idempotency_key or "").strip() or None
        if idempotency_key:
            existing_run_id = self._idempotent_runs.get((session_id, idempotency_key))
            if existing_run_id:
                return self.get_run(existing_run_id)

        stream = await self.knoggin.open_turn_stream(
            session_id=session_id,
            turn=turn,
            idempotency_key=idempotency_key,
        )

        # Opening a stream can await session and document-scope resolution.
        # Recheck afterwards so concurrent retries still map to one UI run.
        if idempotency_key:
            existing_run_id = self._idempotent_runs.get((session_id, idempotency_key))
            if existing_run_id:
                return self.get_run(existing_run_id)

        run = _RunRecord(
            run_id=str(uuid4()),
            session_id=session_id,
            idempotency_key=idempotency_key,
        )
        self.runs[run.run_id] = run
        if idempotency_key:
            self._idempotent_runs[(session_id, idempotency_key)] = run.run_id
        run.task = asyncio.create_task(
            self._run_turn(run, stream),
            name=f"knoggin-ui-run-{run.run_id}",
        )
        self._emit(run, "run_queued", {})
        return self._snapshot(run)

    async def _run_turn(
        self,
        run: _RunRecord,
        stream: AsyncIterator[dict[str, Any]],
    ) -> None:
        run.status = "running"
        self._emit(run, "run_started", {})
        try:
            async for event in stream:
                self._record_engine_event(run, event)

            if run.status == "running":
                run.status = "failed"
                run.error = {"code": "RUN_NO_TERMINAL_EVENT"}
                run.completed_at = datetime.now(timezone.utc)
                self._emit(run, "run_failed", run.error)
        except asyncio.CancelledError:
            run.status = "cancelled"
            run.completed_at = datetime.now(timezone.utc)
            self._emit(run, "run_cancelled", {})
            raise
        except Exception:
            logger.exception("UI API run {} failed", run.run_id)
            run.status = "failed"
            run.error = {"code": "RUN_FAILED"}
            run.completed_at = datetime.now(timezone.utc)
            self._emit(run, "run_failed", run.error)

    def _record_engine_event(self, run: _RunRecord, event: dict[str, Any]) -> None:
        event_name = str(event.get("event", ""))
        data = event.get("data")
        data = dict(data) if isinstance(data, dict) else {}
        if event_name == "response":
            run.status = "completed"
            run.completed_at = datetime.now(timezone.utc)
            run.result = data
            run.sources = source_provenance_from_response(data)
        elif event_name == "clarification":
            run.status = "awaiting_input"
            run.completed_at = datetime.now(timezone.utc)
        elif event_name == "error":
            run.status = "failed"
            run.error = {"code": "RUN_FAILED", "message": data.get("message", "")}
            run.completed_at = datetime.now(timezone.utc)
        self._emit(run, event_name, data)

    def _emit(self, run: _RunRecord, event_name: str, data: dict[str, Any]) -> None:
        run.sequence += 1
        event = RunEvent(
            run_id=run.run_id,
            session_id=run.session_id,
            event=event_name,
            sequence=run.sequence,
            timestamp=datetime.now(timezone.utc),
            data=data,
        )
        run.remember(event)
        for subscriber in tuple(run.subscribers):
            subscriber.put_nowait(event)

    def get_run(self, run_id: str) -> RunSnapshot:
        run = self.runs.get(run_id)
        if run is None:
            raise LookupError("run_not_found")
        return self._snapshot(run)

    async def subscribe_events(self, run_id: str) -> AsyncIterator[RunEvent]:
        run = self.runs.get(run_id)
        if run is None:
            raise LookupError("run_not_found")

        for event in tuple(run.events):
            yield event
        if run.status in _TERMINAL_STATUSES:
            return

        queue: asyncio.Queue[RunEvent] = asyncio.Queue()
        run.subscribers.add(queue)
        try:
            while True:
                event = await queue.get()
                yield event
                if event.event in {
                    "clarification",
                    "response",
                    "error",
                    "run_failed",
                    "run_cancelled",
                }:
                    return
        finally:
            run.subscribers.discard(queue)

    async def cancel(self, run_id: str) -> RunSnapshot:
        run = self.runs.get(run_id)
        if run is None:
            raise LookupError("run_not_found")
        if run.task is not None and not run.task.done():
            run.task.cancel()
            try:
                await run.task
            except asyncio.CancelledError:
                pass
        return self._snapshot(run)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        tasks = [
            run.task
            for run in self.runs.values()
            if run.task is not None and not run.task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    def _snapshot(run: _RunRecord) -> RunSnapshot:
        return RunSnapshot(
            run_id=run.run_id,
            session_id=run.session_id,
            status=run.status,
            created_at=run.created_at,
            completed_at=run.completed_at,
            result=dict(run.result) if run.result is not None else None,
            error=dict(run.error) if run.error is not None else None,
            sources=run.sources,
        )
