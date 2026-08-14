"""Local application facade shared by the Python SDK and FastAPI adapters."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Optional
from uuid import uuid4

from loguru import logger

from application.contracts import (
    DocumentFocus,
    DocumentFocusDocument,
    DocumentFocusFolderUpload,
    DocumentFocusSubtree,
    RunEvent,
    RunSnapshot,
    RunStatus,
    SessionHandle,
    SourceProvenance,
    Turn,
    source_provenance_from_response,
)
from common.schema.document import DocumentFocus as EngineDocumentFocus
from common.schema.document import create_document_focus
from common.schema.primitives import Message
from common.utils.time_utils import get_now_iso
from core.runtime import ApplicationRuntime

_TERMINAL_STATUSES = {"awaiting_input", "completed", "failed", "cancelled"}


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


class _RunManager:
    """Own in-process run tasks without choosing an HTTP representation."""

    def __init__(self, runtime: ApplicationRuntime):
        self.runtime = runtime
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
        content = turn.content.strip()

        idempotency_key = (idempotency_key or "").strip() or None
        if idempotency_key:
            existing_run_id = self._idempotent_runs.get((session_id, idempotency_key))
            if existing_run_id:
                return self.get_run(existing_run_id)

        context = await self.runtime.sessions.get_or_resume_session(session_id)
        if context is None:
            raise LookupError("session_not_found")

        document_focus = await _resolve_document_focus(context, turn.document_focus)

        # A session lookup awaits. Recheck afterwards so concurrent SDK calls
        # with one idempotency key cannot both create a run.
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
            self._run_turn(
                run,
                context,
                content,
                model=turn.model,
                agent_id=turn.agent_id,
                enabled_tools=list(turn.enabled_tools) if turn.enabled_tools else None,
                document_focus=document_focus,
            ),
            name=f"knoggin-run-{run.run_id}",
        )
        self._emit(run, "run_queued", {})
        return self._snapshot(run)

    async def _run_turn(
        self,
        run: _RunRecord,
        context: Any,
        content: str,
        *,
        model: str | None,
        agent_id: str | None,
        enabled_tools: list[str] | None,
        document_focus: EngineDocumentFocus | None,
    ) -> None:
        run.status = "running"
        self._emit(run, "run_started", {})
        try:
            message = Message(
                content=content,
                metadata={"idempotency_key": run.idempotency_key or ""},
            )
            async for event in context.run_agent_stream(
                message,
                model=model,
                agent_id=agent_id,
                enabled_tools=enabled_tools,
                document_focus=document_focus,
                idempotency_key=run.idempotency_key,
            ):
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
            logger.exception("Local SDK run {} failed", run.run_id)
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


async def _resolve_document_focus(
    context: Any,
    focus: DocumentFocus | None,
) -> EngineDocumentFocus | None:
    """Resolve an SDK selection through the session's visible documents."""

    if focus is None:
        return None
    if context.document_service is None:
        raise ValueError("No project document service is available for this request")

    try:
        if isinstance(focus, DocumentFocusDocument):
            target = await context.document_service.resolve_focus_target(
                session_id=context.session_id,
                document_id=focus.document_id,
            )
        elif isinstance(focus, DocumentFocusSubtree):
            target = await context.document_service.resolve_focus_target(
                session_id=context.session_id,
                folder_root_id=focus.folder_root_id,
                path_prefix=focus.path_prefix,
            )
        elif isinstance(focus, DocumentFocusFolderUpload):
            target = await context.document_service.resolve_focus_target(
                session_id=context.session_id,
                folder_root_id=focus.folder_root_id,
            )
        else:
            raise TypeError("document_focus has an unsupported type")
    except FileNotFoundError as exc:
        raise ValueError(
            "The selected document focus is not visible in this session"
        ) from exc

    return create_document_focus(
        mode="request",
        created_at=get_now_iso(),
        **target,
    )


class Knoggin:
    """Canonical application surface for an installed Knoggin."""

    def __init__(self, runtime: ApplicationRuntime):
        self.runtime = runtime
        self._runs = _RunManager(runtime)
        self._closed = False

    @classmethod
    async def start(
        cls,
        *,
        user_name: str,
        num_workers: Optional[int] = None,
    ) -> "Knoggin":
        runtime = await ApplicationRuntime.start(
            user_name=user_name,
            num_workers=num_workers,
        )
        return cls(runtime)

    async def get_engine_health(self) -> dict[str, Any]:
        snapshot = await self.runtime.health_service.get_engine_health()
        return snapshot.model_dump(mode="json")

    async def create_project(
        self,
        *,
        name: str,
        domain_config: dict[str, Any],
        description: str | None = None,
    ) -> dict[str, Any] | None:
        return await self.runtime.projects.create_project(
            name=name,
            domain_config=domain_config,
            description=description,
        )

    async def create_session(
        self,
        *,
        project_id: str,
        model: str | None = None,
        agent_id: str | None = None,
        enabled_tools: list[str] | None = None,
    ) -> SessionHandle:
        session = await self.runtime.sessions.create_session(
            project_id=project_id,
            model=model,
            agent_id=agent_id,
            enabled_tools=enabled_tools,
        )
        return SessionHandle(
            session_id=session.session_id,
            project_id=session.project_id,
            model=session.model,
        )

    async def submit_turn(
        self,
        *,
        session_id: str,
        turn: Turn,
        idempotency_key: str | None = None,
    ) -> RunSnapshot:
        return await self._runs.submit_turn(
            session_id=session_id,
            turn=turn,
            idempotency_key=idempotency_key,
        )

    def get_run(self, run_id: str) -> RunSnapshot:
        return self._runs.get_run(run_id)

    async def subscribe_events(self, run_id: str) -> AsyncIterator[RunEvent]:
        async for event in self._runs.subscribe_events(run_id):
            yield event

    async def cancel_run(self, run_id: str) -> RunSnapshot:
        return await self._runs.cancel(run_id)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._runs.close()
        await self.runtime.shutdown()
