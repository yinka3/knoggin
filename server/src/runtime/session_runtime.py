from __future__ import annotations

import asyncio
import hashlib
import json
from collections import deque
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional
from uuid import uuid4

from loguru import logger

from common.conf.manager import ConfigManager
from common.exceptions import (
    IdempotencyConflictError,
    RequestInProgressError,
    RequestInterruptedError,
    SessionBusyError,
)
from common.response_annotations import with_annotations
from common.schema.agent.research import ResearchMode
from common.schema.agent.settings import PromptMemoryBudget
from common.schema.agent.stream import AgentExecutionEvent
from common.schema.artifacts import ArtifactDraft
from common.schema.document import DocumentFocus
from common.schema.message_origins import BACKGROUND_REPORT_ORIGINS
from common.schema.primitives import Message
from common.schema.settings import RootConfig
from common.schema.source.references import SourceReferenceCandidate
from common.utils.events import emit
from common.utils.time_utils import get_now
from core.agent.run import AgentIdentity
from core.knowledge.documents import DocumentService
from core.session.chat_compaction import ChatCompaction
from runtime.project_runtime import ProjectRuntime
from runtime.resources import RuntimeResources

_REQUEST_FINGERPRINT_METADATA_KEY = "request_fingerprint"


class FollowUpQueueFull(ValueError):
    pass


@dataclass(frozen=True)
class SessionRunSettings:
    config: RootConfig
    model: str | None
    agent_id: str | None
    enabled_tools: tuple[str, ...] | None
    document_focus: DocumentFocus | None
    readable_project_ids: tuple[str, ...]
    resolved_identity: AgentIdentity | None = None
    resolved_model: str | None = None
    prompt_memory_budget: PromptMemoryBudget | None = None
    # Extra page reads for a run given a list of links to read.
    extra_web_reads: int = 0


class _AdmittedAgentStream:
    """Own one accepted exchange, including before the first iteration."""

    def __init__(self, session, accepted, **kwargs):
        self.session = session
        self.accepted = accepted
        self.run_id = kwargs["run_id"]
        self.stream = session._run_admitted_agent_stream(accepted, **kwargs)
        self.started = False
        self.closed = False
        self.executing_task = None
        self.close_lock = asyncio.Lock()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.closed:
            raise StopAsyncIteration
        self.started = True
        self.executing_task = asyncio.current_task()
        try:
            event = await anext(self.stream)
        except BaseException:
            self.executing_task = None
            await self.aclose()
            raise
        finally:
            self.executing_task = None
        if event["event"] in {"response", "clarification", "error"}:
            await anext(self.stream, None)
            self.closed = True
        return event

    async def aclose(self):
        async with self.close_lock:
            if self.closed:
                return
            task = self.executing_task
            if task is not None and task is not asyncio.current_task():
                task.cancel()
                # The iterator's cancellation handler owns its own cleanup.
                # Release this lock before waiting for that handler.
            else:
                if self.started:
                    await self.stream.aclose()
                else:
                    try:
                        await self.session._close_user_exchange(
                            self.accepted.id,
                            outcome="cancelled",
                            terminal_error={"code": "run_cancelled", "retryable": False},
                        )
                    finally:
                        await self.session._release_agent_run(None)
                self.closed = True
                return
        try:
            await task
        except asyncio.CancelledError:
            pass


class SessionRuntime:
    """
    SessionRuntime represents one loaded, in-memory user session.

    It serves as the root orchestration point for a session, binding together user
    state and dynamic configuration. Semantic processing is owned solely by the
    project runtime's durable semantic-window job.

    Initialization and wiring logic is owned by SessionManager to decouple
    the construction of these services from the state container itself.
    """

    def __init__(
        self,
        user_name: str,
        resources: RuntimeResources,
        *,
        session_id: str,
        project_id: str,
        project: ProjectRuntime,
        model: Optional[str],
        agent_id: Optional[str],
        enabled_tools: Optional[List[str]],
        document_focus: Optional[DocumentFocus] = None,
        health_service: Any | None = None,
        agent_orchestrator: Any | None = None,
        config_manager: ConfigManager | None = None,
        research_jobs: Any | None = None,
    ):
        self.resources = resources
        self.research_jobs = research_jobs
        self.health_service = health_service
        self.agent_orchestrator = agent_orchestrator
        self._config_manager = config_manager
        self.user_name: str = user_name
        self.model = model
        self.agent_id = agent_id
        self.enabled_tools = list(enabled_tools) if enabled_tools is not None else None
        self.document_focus = document_focus
        self.document_service: Optional[DocumentService] = getattr(project, "document_service", None)

        self.session_id = session_id
        self.project_id = project_id
        self.project = project

        self._agent_run_lock = asyncio.Lock()
        self._shutdown_lock = asyncio.Lock()
        self._agent_run_reserved = False
        self._active_agent_task: Optional[asyncio.Task] = None
        self._active_agent_stream: _AdmittedAgentStream | None = None
        self._active_idempotency_key: str | None = None
        self._active_request_fingerprint: str | None = None
        self._active_run_id: str | None = None
        self._active_user_message_id: int | None = None
        self._last_run_status: dict[str, Any] | None = None
        self._active_run_state: str | None = None
        self._active_steer: dict[str, str] | None = None
        self._follow_up_queue: deque[str] = deque()
        self._follow_ups: dict[str, dict[str, Any]] = {}
        self._follow_up_task: asyncio.Task | None = None
        self._follow_up_starting = False
        self._run_idle = asyncio.Event()
        self._run_idle.set()
        self._agent_runs_closed = False
        self._closed = False
        self._shutdown_event_emitted = False
        # Conversation share of the last admitted run's prompt; compaction
        # summarizes what falls outside it.
        self._conversation_tokens: int | None = None
        self._compaction_task: asyncio.Task | None = None

    @property
    def current_config(self) -> RootConfig:
        return (self._config_manager or ConfigManager.get()).config

    @property
    def knowledge_store(self):
        return self.resources.knowledge_store

    @property
    def llm(self):
        return self.resources.llm_service

    async def cancel_active_agent_run(self, *, run_id: str | None = None) -> bool:
        """Cancel only this session's active agent execution, if any."""

        async with self._agent_run_lock:
            if run_id is not None and run_id != self._active_run_id:
                return False
            task = self._active_agent_task
            stream = self._active_agent_stream
        if stream is not None and not stream.closed:
            await stream.aclose()
            return True
        if task is None or task.done() or task is asyncio.current_task():
            return False

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return True

    async def get_run_status(self, run_id: str) -> dict[str, Any] | None:
        async with self._agent_run_lock:
            if run_id == self._active_run_id:
                return self._run_status_unlocked()
            if self._last_run_status and run_id == self._last_run_status["run_id"]:
                return deepcopy(self._last_run_status)
            return None

    async def list_run_statuses(self) -> dict[str, Any]:
        """Expose the live run ID when the client missed the first stream event."""

        async with self._agent_run_lock:
            return {
                "active": self._run_status_unlocked() if self._active_run_id else None,
                "recent": deepcopy(self._last_run_status),
            }

    def _run_status_unlocked(self) -> dict[str, Any]:
        return {
            "run_id": self._active_run_id,
            "state": self._active_run_state,
            "steer": deepcopy(self._active_steer),
        }

    async def queue_follow_up(self, run_id: str, query: str, *, annotations: list[dict] | None = None) -> dict[str, Any] | None:
        """Accept one later user turn for this live session's active run."""

        query = query.strip()
        if not query or len(query) > 100_000:
            raise ValueError("Follow-up query must contain 1 to 100000 characters")
        query = with_annotations(query, annotations)
        async with self._agent_run_lock:
            if self._agent_runs_closed or self._closed:
                return None
            if run_id != self._active_run_id or self._active_run_state not in {
                "accepted", "running",
            }:
                return None
            if len(self._follow_up_queue) >= 5:
                raise FollowUpQueueFull("Follow-up queue is full")
            if len(self._follow_ups) >= 100:
                for old_id, old in list(self._follow_ups.items()):
                    if old["state"] in {"completed", "clarification", "failed", "cancelled"}:
                        del self._follow_ups[old_id]
                        break
            queue_id = str(uuid4())
            item = {
                "queue_id": queue_id, "after_run_id": run_id,
                "run_id": str(uuid4()), "query": query,
                "metadata": {"response_annotations": annotations or []},
                "state": "queued", "assistant_message_id": None,
                "error_code": None,
            }
            self._follow_ups[queue_id] = item
            self._follow_up_queue.append(queue_id)
            if self._follow_up_task is None or self._follow_up_task.done():
                self._follow_up_task = asyncio.create_task(self._drain_follow_ups())
            return self._public_follow_up(item)

    async def queue_report(
        self, content: str, *, origin: str, metadata: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Queue a background report (a research job, a folder import) as a
        turn in this chat.

        It runs once the chat is idle. Unlike a follow-up it needs no active
        run and is never refused for a full queue: the background work already
        bounds how many arrive. The turn is marked so it is not memory.
        """

        if origin not in BACKGROUND_REPORT_ORIGINS:
            raise ValueError(f"Unknown report origin: {origin}")

        async with self._agent_run_lock:
            if self._agent_runs_closed or self._closed:
                return None
            queue_id = str(uuid4())
            item = {
                "queue_id": queue_id, "after_run_id": None,
                "run_id": str(uuid4()), "query": content,
                "metadata": {"origin": origin, **metadata},
                "state": "queued", "assistant_message_id": None,
                "error_code": None,
            }
            self._follow_ups[queue_id] = item
            self._follow_up_queue.append(queue_id)
            if self._follow_up_task is None or self._follow_up_task.done():
                self._follow_up_task = asyncio.create_task(self._drain_follow_ups())
            return self._public_follow_up(item)

    @staticmethod
    def _public_follow_up(item: dict[str, Any]) -> dict[str, Any]:
        return {key: item[key] for key in (
            "queue_id", "after_run_id", "run_id", "state", "assistant_message_id",
            "error_code",
        )}

    async def list_follow_ups(self) -> list[dict[str, Any]]:
        async with self._agent_run_lock:
            return [self._public_follow_up(item) for item in self._follow_ups.values()]

    async def get_follow_up(self, queue_id: str) -> dict[str, Any] | None:
        async with self._agent_run_lock:
            item = self._follow_ups.get(queue_id)
            return self._public_follow_up(item) if item is not None else None

    async def cancel_follow_up(self, queue_id: str) -> dict[str, Any] | None:
        async with self._agent_run_lock:
            item = self._follow_ups.get(queue_id)
            if item is None:
                return None
            if item["state"] == "queued":
                self._follow_up_queue.remove(queue_id)
                item["state"] = "cancelled"
                item.pop("query", None)
            return self._public_follow_up(item)

    async def _drain_follow_ups(self) -> None:
        while True:
            await self._run_idle.wait()
            async with self._agent_run_lock:
                if self._agent_runs_closed or not self._follow_up_queue:
                    self._follow_up_task = None
                    return
                if self._agent_run_reserved:
                    continue
                queue_id = self._follow_up_queue.popleft()
                item = self._follow_ups[queue_id]
                item["state"] = "starting"
                self._follow_up_starting = True
                self._run_idle.clear()
            stream = None
            try:
                stream = await self.open_agent_run_stream(
                    Message(content=item["query"], metadata=item.get("metadata", {})),
                    run_id=item["run_id"],
                    _from_follow_up=True,
                )
                async with self._agent_run_lock:
                    self._follow_up_starting = False
                    item["state"] = "running"
                async for event in stream:
                    if event["event"] == "response":
                        item["assistant_message_id"] = event["data"].get("assistant_message_id")
                        item["state"] = "completed"
                    elif event["event"] == "clarification":
                        item["assistant_message_id"] = event["data"].get("assistant_message_id")
                        item["state"] = "clarification"
                    elif event["event"] == "error":
                        item["state"] = "failed"
                        code = event["data"].get("code")
                        item["error_code"] = (
                            code if code in {"llm_budget_exhausted", "workspace_conflict"}
                            else "run_failed"
                        )
                if item["state"] == "running":
                    item["state"] = "failed"
                    item["error_code"] = "run_failed"
            except asyncio.CancelledError:
                item["state"] = "cancelled"
                raise
            except Exception:
                logger.exception("Queued follow-up failed for session {}", self.session_id)
                item["state"] = "failed"
                item["error_code"] = "run_failed"
            finally:
                item.pop("query", None)
                if stream is not None:
                    await stream.aclose()
                async with self._agent_run_lock:
                    self._follow_up_starting = False
                    if not self._agent_run_reserved:
                        self._run_idle.set()

    async def submit_steer(self, run_id: str, instruction: str, *, annotations: list[dict] | None = None) -> dict[str, str] | None:
        if not instruction or not instruction.strip() or len(instruction) > 4000:
            raise ValueError("Steer instruction must contain 1 to 4000 characters")
        async with self._agent_run_lock:
            if run_id != self._active_run_id or self._active_run_state != "running":
                return None
            if self._active_steer is not None:
                raise SessionBusyError()
            steer = {
                "steer_id": str(uuid4()),
                "instruction": instruction.strip(),
                "response_annotations": annotations or [],
                "state": "pending",
            }
            if self._active_user_message_id is None or not await self.knowledge_store.record_run_steer(
                user_name=self.user_name, project_id=self.project_id,
                session_id=self.session_id, message_id=self._active_user_message_id,
                steer=steer,
            ):
                return None
            self._active_steer = steer
            return {"steer_id": steer["steer_id"], "state": "pending"}

    async def _take_steer(self) -> dict[str, str] | None:
        async with self._agent_run_lock:
            steer = self._active_steer
            if steer is None or steer["state"] != "pending":
                return None
            if not await self.knowledge_store.record_run_steer(
                user_name=self.user_name, project_id=self.project_id,
                session_id=self.session_id, message_id=self._active_user_message_id,
                steer={**steer, "state": "applied"}, expected_state="pending",
            ):
                raise RuntimeError("Steer could not be recorded before delivery")
            steer["state"] = "applied"
            return {
                "steer_id": steer["steer_id"],
                "instruction": with_annotations(
                    steer["instruction"], steer.get("response_annotations")
                ),
            }

    async def open_agent_run_stream(
        self,
        message: Message,
        *,
        orchestrator: Any = None,
        user_timezone: Optional[str] = None,
        model: Optional[str] = None,
        agent_id: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
        document_focus: Optional[DocumentFocus] = None,
        pasted_text_spans: Optional[List[Dict]] = None,
        idempotency_key: Optional[str] = None,
        research_mode: ResearchMode = "normal",
        selected_notebook: dict | None = None,
        run_id: str | None = None,
        extra_web_reads: int = 0,
        _from_follow_up: bool = False,
    ) -> AsyncGenerator[AgentExecutionEvent, None]:
        """Admit and persist a run before returning its event stream.

        Adapters can await this preflight operation and report a conflict
        before starting an HTTP/SSE response. A rejected overlapping run does
        not allocate or persist a user message.
        """

        supplied_idempotency_key = self._normalize_idempotency_key(idempotency_key)
        embedded_idempotency_key = self._normalize_idempotency_key(
            message.metadata.get("idempotency_key")
        )
        if (
            supplied_idempotency_key is not None
            and embedded_idempotency_key is not None
            and supplied_idempotency_key != embedded_idempotency_key
        ):
            raise IdempotencyConflictError()
        normalized_idempotency_key = (
            supplied_idempotency_key or embedded_idempotency_key
        )
        request_fingerprint = (
            self._request_fingerprint(
                message=message,
                user_timezone=user_timezone,
                model=model,
                agent_id=agent_id,
                enabled_tools=enabled_tools,
                document_focus=document_focus,
                pasted_text_spans=pasted_text_spans,
                research_mode=research_mode,
                selected_notebook=selected_notebook,
            )
            if normalized_idempotency_key is not None
            else None
        )
        if normalized_idempotency_key is not None:
            message.metadata = {
                **message.metadata,
                "idempotency_key": normalized_idempotency_key,
                _REQUEST_FINGERPRINT_METADATA_KEY: request_fingerprint,
            }
        if selected_notebook is not None:
            message.metadata = {
                **message.metadata,
                "selected_notebook": {
                    "notebook_id": selected_notebook["notebook_id"],
                    "revision": selected_notebook["revision"],
                },
            }

        async with self._agent_run_lock:
            if self._closed or self._agent_runs_closed:
                raise RuntimeError("Session is shutting down")
            self._require_message_ingestion_ready()
            if self._agent_run_reserved:
                if (
                    normalized_idempotency_key is not None
                    and normalized_idempotency_key == self._active_idempotency_key
                ):
                    if request_fingerprint != self._active_request_fingerprint:
                        raise IdempotencyConflictError()
                    raise RequestInProgressError()
                raise SessionBusyError()
            if (self._follow_up_queue or self._follow_up_starting) and not _from_follow_up:
                raise SessionBusyError()
            run_settings = SessionRunSettings(
                config=deepcopy(self.current_config),
                model=self.model,
                agent_id=self.agent_id,
                enabled_tools=tuple(self.enabled_tools)
                if self.enabled_tools is not None
                else None,
                document_focus=deepcopy(self.document_focus),
                readable_project_ids=tuple(self.project.readable_project_ids),
                extra_web_reads=extra_web_reads,
            )
            enabled_tools = list(enabled_tools) if enabled_tools is not None else None
            document_focus = deepcopy(document_focus)
            pasted_text_spans = deepcopy(pasted_text_spans)
            selected_notebook = deepcopy(selected_notebook)
            self._agent_run_reserved = True
            self._run_idle.clear()
            self._active_run_id = run_id or str(uuid4())
            self._active_run_state = "admitting"
            self._active_steer = None
            self._active_user_message_id = None
            self._active_idempotency_key = normalized_idempotency_key
            self._active_request_fingerprint = request_fingerprint
            self._active_agent_task = asyncio.current_task()

        selected_orchestrator = orchestrator or self.agent_orchestrator
        if selected_orchestrator is not None and hasattr(selected_orchestrator, "resolve_run_policy"):
            try:
                identity, selected_model, budget = await selected_orchestrator.resolve_run_policy(
                    config=run_settings.config,
                    requested_agent_id=agent_id,
                    session_agent_id=run_settings.agent_id,
                    requested_model=model,
                    session_model=run_settings.model,
                    fallback_model=self.resources.llm_service.agent_model,
                    llm=self.resources.llm_service,
                )
                run_settings = replace(
                    run_settings,
                    resolved_identity=identity,
                    resolved_model=selected_model,
                    prompt_memory_budget=budget,
                )
                self._conversation_tokens = budget.conversation_tokens
            except BaseException:
                await self._release_agent_run(None)
                raise

        acceptance_task = asyncio.create_task(
            self._accept_user_message(
                message,
                request_fingerprint=request_fingerprint,
                edit_window_seconds=run_settings.config.developer_settings.ingestion.message_edit_window_seconds,
            )
        )
        try:
            accepted, created = await asyncio.shield(acceptance_task)
        except asyncio.CancelledError:
            try:
                accepted, created = await acceptance_task
                if created:
                    await self._close_user_exchange(
                        accepted.id,
                        outcome="cancelled",
                        terminal_error={"code": "run_cancelled", "retryable": False},
                    )
            finally:
                await self._release_agent_run(None)
            raise
        except Exception:
            await self._release_agent_run(None)
            raise

        if not created:
            try:
                exchange = await self.knowledge_store.get_user_agent_exchange(
                    accepted.id,
                    user_name=self.user_name,
                    project_id=self.project_id,
                    session_id=self.session_id,
                )
                if exchange is None:
                    raise RuntimeError("Accepted user message could not be reloaded")
                if exchange.exchange_state == "open":
                    # We cannot safely infer whether an earlier owner is still
                    # running or was interrupted. Either way, replaying the
                    # request could repeat side effects, so keep the accepted
                    # submission intact and require an explicit new request.
                    raise RequestInterruptedError()
                return self._replay_terminal_exchange(exchange)
            finally:
                await self._release_agent_run(None)

        stream = _AdmittedAgentStream(
            self,
            accepted,
            orchestrator=orchestrator,
            user_timezone=user_timezone,
            model=model,
            agent_id=agent_id,
            enabled_tools=enabled_tools,
            document_focus=document_focus,
            pasted_text_spans=pasted_text_spans,
            research_mode=research_mode,
            selected_notebook=selected_notebook,
            run_settings=run_settings,
            run_id=self._active_run_id,
        )
        self._active_agent_stream = stream
        self._active_agent_task = None
        self._active_run_state = "accepted"
        self._active_user_message_id = accepted.id
        return stream

    def _require_message_ingestion_ready(self) -> None:
        if (
            not getattr(self.project, "scheduler", None)
            or getattr(self.project, "project_semantic_processor", None) is None
        ):
            raise RuntimeError("Session is not fully initialized for message ingestion")

    async def _run_admitted_agent_stream(
        self,
        accepted: Message,
        *,
        orchestrator: Any,
        user_timezone: Optional[str],
        model: Optional[str],
        agent_id: Optional[str],
        enabled_tools: Optional[List[str]],
        document_focus: Optional[DocumentFocus],
        pasted_text_spans: Optional[List[Dict]],
        research_mode: ResearchMode,
        selected_notebook: dict | None,
        run_settings: SessionRunSettings,
        run_id: str,
    ) -> AsyncGenerator[AgentExecutionEvent, None]:
        """Execute one already-persisted, exclusively admitted run."""

        exchange_outcome = "failed"
        terminal_error: dict[str, object] = {
            "code": "run_failed",
            "retryable": True,
        }
        # Source candidates are authorized while the Agent run is admitted.
        # Keep that scope through finalization retries instead of consulting a
        # mutable runtime list after the response has been produced.
        captured_readable_project_ids = list(run_settings.readable_project_ids)
        task = asyncio.current_task()
        if task is None:
            await self._release_agent_run(None)
            raise RuntimeError("Agent stream must run in an asyncio task")

        async with self._agent_run_lock:
            shutting_down = self._agent_runs_closed
            self._active_agent_task = task
            self._active_run_state = "running"

        try:
            if shutting_down:
                raise asyncio.CancelledError
            prompt_memory = run_settings.config.developer_settings.prompt_memory
            # Fetch against the largest admitted budget; the chosen run budget
            # makes the final selection at prompt assembly.
            history_budget_tokens = (
                run_settings.prompt_memory_budget.input_tokens
                if run_settings.prompt_memory_budget is not None
                else max([
                    prompt_memory.default.input_tokens,
                    *(row.budget.input_tokens for row in prompt_memory.overrides),
                ])
            )
            history = await self._recent_history_within(
                history_budget_tokens, up_to_msg_id=accepted.id - 1,
            )
            history_summary = await self._chat_summary()
            orchestrator = orchestrator or self.agent_orchestrator
            if orchestrator is None:
                raise RuntimeError("Session has no application-owned AgentOrchestrator")

            response_seen = False
            async for event in orchestrator.run_stream(
                user_query=accepted.content.strip(),
                context=self,
                user_timezone=user_timezone,
                model=model,
                agent_id=agent_id,
                enabled_tools=enabled_tools,
                request_document_focus=document_focus,
                conversation_history=history,
                history_summary=history_summary,
                user_message_id=accepted.id,
                pasted_text_spans=pasted_text_spans,
                research_mode=research_mode,
                selected_notebook=selected_notebook,
                run_settings=run_settings,
                run_id=run_id,
                take_steer=self._take_steer,
            ):
                if event["event"] == "response":
                    if response_seen:
                        raise RuntimeError(
                            "Agent stream emitted multiple final responses"
                        )
                    response_seen = True
                    response = event["data"]
                    resolved_agent_id = response.get("resolved_agent_id")
                    commit = await self.add_assistant_turn(
                        content=response["content"],
                        timestamp=get_now(),
                        metadata=self._assistant_response_metadata(response),
                        user_msg_id=accepted.id,
                        source_candidates=self._response_source_candidates(response),
                        artifact=self._response_artifact(response),
                        readable_project_ids=captured_readable_project_ids,
                    )
                    exchange_outcome = "assistant_final"
                    await self._record_durable_agent_turn(
                        orchestrator,
                        agent_id=(
                            resolved_agent_id
                            if isinstance(resolved_agent_id, str)
                            and resolved_agent_id.strip()
                            else agent_id or run_settings.agent_id
                        ),
                    )
                    response = dict(response)
                    response.pop("resolved_agent_id", None)
                    response["assistant_message_id"] = commit["message_id"]
                    response["source_ref_ids"] = commit["source_ref_ids"]
                    event = {"event": "response", "data": response}
                    exchange_outcome = "assistant_final"
                elif event["event"] == "clarification":
                    clarification = dict(event["data"])
                    commit = await self.add_assistant_turn(
                        content=str(clarification["question"]),
                        timestamp=get_now(),
                        metadata=self._assistant_clarification_metadata(clarification),
                        user_msg_id=accepted.id,
                        source_candidates=self._response_source_candidates(
                            clarification
                        ),
                        readable_project_ids=captured_readable_project_ids,
                        outcome="clarification",
                    )
                    clarification["assistant_message_id"] = commit["message_id"]
                    clarification["source_ref_ids"] = commit["source_ref_ids"]
                    event = {"event": "clarification", "data": clarification}
                    exchange_outcome = "clarification"
                elif event["event"] == "error":
                    exchange_outcome = "failed"
                    terminal_error = self._terminal_error_record(event.get("data"))
                yield event
                if event["event"] in {"response", "clarification", "error"}:
                    return

            if not response_seen and exchange_outcome == "failed":
                logger.error(
                    "Agent stream ended without a final response for session {}",
                    self.session_id,
                )
        except asyncio.CancelledError:
            if exchange_outcome not in {"assistant_final", "clarification"}:
                exchange_outcome = "cancelled"
                terminal_error = {"code": "run_cancelled", "retryable": False}
            raise
        except GeneratorExit:
            if exchange_outcome not in {"assistant_final", "clarification"}:
                exchange_outcome = "cancelled"
                terminal_error = {"code": "run_cancelled", "retryable": False}
            raise
        except Exception:
            if exchange_outcome not in {"assistant_final", "clarification"}:
                exchange_outcome = "failed"
            logger.exception(
                "Canonical agent turn failed for session {}", self.session_id
            )
            yield {
                "event": "error",
                "data": {
                    "message": "The response could not be completed or saved. Please try again."
                },
            }
        finally:
            async with self._agent_run_lock:
                self._active_run_state = exchange_outcome
            try:
                if exchange_outcome not in {"assistant_final", "clarification"}:
                    await self._close_user_exchange(
                        accepted.id,
                        outcome=exchange_outcome,
                        terminal_error=terminal_error,
                    )
            except Exception:
                # Never replace the original stream failure or cancellation with
                # cleanup noise.  The durable close operation is idempotent and
                # each future attempt can safely retry the same outcome.
                logger.exception(
                    "Failed to close {} exchange for session {}",
                    exchange_outcome,
                    self.session_id,
                )
            finally:
                await self._release_agent_run(task)

    async def _release_agent_run(self, task: Optional[asyncio.Task]) -> None:
        async with self._agent_run_lock:
            if task is not None and self._active_agent_task is not task:
                return
            if task is None or self._active_agent_task is task:
                self._active_agent_task = None
            if self._active_run_id is not None:
                if self._active_steer is not None and self._active_steer["state"] == "pending":
                    self._active_steer["state"] = "undelivered"
                    try:
                        await self.knowledge_store.record_run_steer(
                            user_name=self.user_name, project_id=self.project_id,
                            session_id=self.session_id, message_id=self._active_user_message_id,
                            steer=self._active_steer, expected_state="pending",
                        )
                    except Exception:
                        logger.exception("Failed to record undelivered steer for {}", self.session_id)
                if self._active_run_state in {"admitting", "accepted", "running"}:
                    self._active_run_state = "cancelled"
                self._last_run_status = self._run_status_unlocked()
            self._agent_run_reserved = False
            self._run_idle.set()
            self._active_run_id = None
            self._active_user_message_id = None
            self._active_run_state = None
            self._active_steer = None
            self._active_idempotency_key = None
            self._active_request_fingerprint = None
            self._active_agent_stream = None

    @staticmethod
    def _assistant_response_metadata(response: Dict[str, Any]) -> dict:
        """Persist server-owned response metadata, not client presentation state."""

        metadata = {"usage": response["usage"]}
        if response.get("research_mode"):
            metadata["research_mode"] = response["research_mode"]
        if response.get("fallback"):
            metadata["fallback"] = True
        return metadata

    async def _record_durable_agent_turn(
        self,
        orchestrator: Any,
        *,
        agent_id: str | None,
    ) -> None:
        """Record the clock only after the answer's durable transaction commits."""

        mark_turn_completed = getattr(orchestrator, "mark_turn_completed", None)
        if not callable(mark_turn_completed):
            return
        try:
            await mark_turn_completed(agent_id)
        except Exception:
            # The committed answer remains valid when this secondary agent
            # statistic cannot be updated. A later durable answer will repair
            # the clock naturally.
            logger.exception("Failed to record durable agent turn completion")

    @staticmethod
    def _assistant_clarification_metadata(clarification: Dict[str, Any]) -> dict:
        """Persist server-owned metadata for one terminal clarification."""

        metadata = {"usage": clarification.get("usage", {})}
        if clarification.get("fallback"):
            metadata["fallback"] = True
        return metadata

    @staticmethod
    def _response_source_candidates(
        response: Dict[str, Any],
    ) -> List[SourceReferenceCandidate]:
        """Recover strict source candidates from the validated agent event."""

        raw_candidates = response.get("sources_consulted", [])
        if raw_candidates is None:
            return []
        if not isinstance(raw_candidates, list):
            raise ValueError("Agent response sources_consulted must be a list")
        return [
            SourceReferenceCandidate.model_validate(candidate)
            for candidate in raw_candidates
        ]

    @staticmethod
    def _response_artifact(response: Dict[str, Any]) -> ArtifactDraft | None:
        raw_artifact = response.get("artifact")
        if raw_artifact is None:
            return None
        return ArtifactDraft.model_validate(raw_artifact)

    async def _accept_user_message(
        self,
        msg: Message,
        *,
        request_fingerprint: str | None = None,
        edit_window_seconds: int | None = None,
    ) -> tuple[Message, bool]:
        """Persist one user message and report whether it was newly created."""

        msg.timestamp = self._normalize_timestamp(msg.timestamp)

        idempotency_key = self._normalize_idempotency_key(
            msg.metadata.get("idempotency_key")
        )
        if idempotency_key is not None:
            if request_fingerprint is None:
                raise ValueError(
                    "Request idempotency acceptance requires a request fingerprint"
                )
            msg.metadata = {
                **msg.metadata,
                "idempotency_key": idempotency_key,
                _REQUEST_FINGERPRINT_METADATA_KEY: request_fingerprint,
            }
        # PostgreSQL owns this stable acceptance identity.  An application
        # request key wins; internal callers retain deterministic content/time
        # acceptance without a separate cache protocol.
        timestamp_ns = int(msg.timestamp.timestamp() * 1e9)
        fallback_key = hashlib.sha256(
            (f"{self.session_id}:{msg.content.strip()}:{timestamp_ns}").encode()
        ).hexdigest()
        acceptance_key = (
            f"request:{idempotency_key}"
            if idempotency_key
            else f"content:{fallback_key}"
        )
        msg.id = await self.knowledge_store.allocate_message_id()

        persistence_kwargs = {"acceptance_key": acceptance_key}
        if edit_window_seconds is not None:
            persistence_kwargs["edit_window_seconds"] = edit_window_seconds
        if request_fingerprint is not None:
            persistence_kwargs["request_fingerprint"] = request_fingerprint
        acceptance = await self._persist_user_turn(msg, **persistence_kwargs)
        msg.id = acceptance.message_id
        if acceptance.created:
            # An editable message is not semantic evidence until its exchange
            # closes; the closure path owns the project-wide wake edge.
            return msg, True
        return msg, False

    @staticmethod
    def _normalize_timestamp(timestamp: datetime) -> datetime:
        if timestamp.tzinfo is None:
            return timestamp.replace(tzinfo=timezone.utc)
        return timestamp.astimezone(timezone.utc)

    @staticmethod
    def _normalize_idempotency_key(value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("idempotency_key must be a string")
        value = value.strip()
        if not value:
            raise ValueError("idempotency_key must not be blank")
        if len(value) > 200:
            raise ValueError("idempotency_key must not exceed 200 characters")
        return value

    @staticmethod
    def _request_fingerprint(
        *,
        message: Message,
        user_timezone: str | None,
        model: str | None,
        agent_id: str | None,
        enabled_tools: list[str] | None,
        document_focus: DocumentFocus | None,
        pasted_text_spans: list[dict] | None,
        research_mode: ResearchMode,
        selected_notebook: dict | None = None,
    ) -> str:
        """Hash the caller-controlled inputs for one logical submission."""

        focus = (
            document_focus.model_dump(mode="json")
            if hasattr(document_focus, "model_dump")
            else document_focus
        )
        if isinstance(focus, dict):
            # created_at and relative_path are produced while the adapter
            # resolves the caller's stable document_id.
            focus = {
                key: value
                for key, value in focus.items()
                if key != "created_at"
                and not (
                    focus.get("target_type") == "document" and key == "relative_path"
                )
            }
        payload = {
            "query": message.content.strip(),
            "user_timezone": user_timezone,
            "model": model,
            "agent_id": agent_id,
            "enabled_tools": (
                sorted(set(enabled_tools)) if enabled_tools is not None else None
            ),
            "document_focus": focus,
            "pasted_text_spans": pasted_text_spans,
            "research_mode": research_mode,
            "selected_notebook": (
                {"notebook_id": selected_notebook["notebook_id"],
                 "revision": selected_notebook["revision"]}
                if selected_notebook is not None else None
            ),
        }
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return hashlib.sha256(encoded.encode()).hexdigest()

    @staticmethod
    def _terminal_error_record(data: object) -> dict[str, object]:
        """Keep only public, server-owned failure semantics for durable replay."""

        if isinstance(data, dict):
            code = data.get("code")
            retryability = {
                "llm_budget_exhausted": False,
                "workspace_conflict": False,
                "run_cancelled": False,
                "run_failed": True,
            }
            if code in retryability:
                return {"code": code, "retryable": retryability[code]}
        return {"code": "run_failed", "retryable": True}

    async def _persist_user_turn(
        self,
        msg: Message,
        *,
        acceptance_key: str,
        request_fingerprint: str | None = None,
        edit_window_seconds: int | None = None,
    ):
        """Durably create an editable canonical user message and revision one."""
        payload = {
            "id": msg.id,
            "content": msg.content.strip(),
            "role": "user",
            "user_name": self.user_name,
            "session_id": self.session_id,
            "project_id": self.project_id,
            "timestamp": msg.timestamp.timestamp() * 1000,
            "metadata": msg.metadata,
            "user_msg_id": msg.id,
            "acceptance_key": acceptance_key,
        }
        if request_fingerprint is not None:
            payload["request_fingerprint"] = request_fingerprint
        return await self.knowledge_store.create_editable_user_message(
            payload,
            edit_window_seconds=(
                edit_window_seconds
                if edit_window_seconds is not None
                else self.current_config.developer_settings.ingestion.message_edit_window_seconds
            ),
        )

    async def _replay_terminal_exchange(
        self, exchange
    ) -> AsyncGenerator[AgentExecutionEvent, None]:
        """Return the durable result for a duplicate submission without rerunning it."""

        outcome = exchange.exchange_outcome
        metadata = exchange.assistant_metadata
        if outcome == "assistant_final":
            if (
                exchange.assistant_message_id is None
                or exchange.assistant_content is None
                or not isinstance(metadata.get("usage"), dict)
            ):
                raise RuntimeError(
                    "Completed request is missing its canonical response"
                )
            response = {
                "content": exchange.assistant_content,
                "usage": metadata["usage"],
                "assistant_message_id": exchange.assistant_message_id,
                "source_ref_ids": list(exchange.source_ref_ids),
            }
            for field in ("research_mode", "fallback"):
                if field in metadata:
                    response[field] = metadata[field]
            yield {"event": "response", "data": response}
            return
        if outcome == "clarification":
            if (
                exchange.assistant_message_id is None
                or exchange.assistant_content is None
                or not isinstance(metadata.get("usage"), dict)
            ):
                raise RuntimeError("Completed request is missing its clarification")
            clarification = {
                "question": exchange.assistant_content,
                "usage": metadata["usage"],
                "assistant_message_id": exchange.assistant_message_id,
                "source_ref_ids": list(exchange.source_ref_ids),
            }
            if metadata.get("fallback"):
                clarification["fallback"] = True
            yield {"event": "clarification", "data": clarification}
            return
        if outcome == "cancelled":
            message = (
                "The original request was cancelled. Submit a new request to retry."
            )
        elif outcome == "failed":
            message = "The original request failed. Submit a new request to retry."
        elif outcome == "user_only":
            message = "This request was completed without an agent response."
        else:
            raise RuntimeError("Accepted request has an invalid terminal outcome")
        terminal_error = self._terminal_error_record(
            exchange.terminal_error
            or {"code": "run_cancelled" if outcome == "cancelled" else "run_failed"}
        )
        yield {"event": "error", "data": {"message": message, **terminal_error}}

    async def add_assistant_turn(
        self,
        content: str,
        timestamp: datetime,
        metadata: Optional[dict] = None,
        user_msg_id: Optional[int] = None,
        source_candidates: Optional[List[SourceReferenceCandidate]] = None,
        artifact: ArtifactDraft | None = None,
        readable_project_ids: Optional[List[str]] = None,
        outcome: str = "assistant_final",
    ) -> dict[str, Any]:
        """Add assistant turn to conversation log."""
        if metadata is None:
            metadata = {}

        timestamp = self._normalize_timestamp(timestamp)
        message_id = await self.knowledge_store.allocate_message_id()
        if user_msg_id is None:
            raise ValueError("Assistant turns must be linked to a user exchange")
        (
            persisted_message_id,
            source_ref_ids,
        ) = await self._persist_assistant_message_log(
            message_id,
            content,
            timestamp,
            metadata=metadata,
            user_msg_id=user_msg_id,
            source_candidates=source_candidates,
            artifact=artifact,
            readable_project_ids=readable_project_ids,
            outcome=outcome,
        )
        self._signal_exchange_closed()
        self._schedule_chat_compaction()
        return {
            "message_id": persisted_message_id,
            "source_ref_ids": source_ref_ids,
        }

    async def _persist_assistant_message_log(
        self,
        message_id: int,
        content: str,
        timestamp: datetime,
        metadata: Optional[dict] = None,
        user_msg_id: Optional[int] = None,
        source_candidates: Optional[List[SourceReferenceCandidate]] = None,
        artifact: ArtifactDraft | None = None,
        readable_project_ids: Optional[List[str]] = None,
        outcome: str = "assistant_final",
    ) -> tuple[int, list[str]]:
        """Atomically persist an assistant response and close its user exchange."""
        max_retries = 3
        captured_readable_project_ids = list(
            readable_project_ids
            if readable_project_ids is not None
            else self.project.readable_project_ids
        )

        for attempt in range(max_retries):
            try:
                agent_msg_batch = [
                    {
                        "id": message_id,
                        "content": content,
                        "role": "assistant",
                        "user_name": self.user_name,
                        "session_id": self.session_id,
                        "project_id": self.project_id,
                        "timestamp": timestamp.timestamp() * 1000,
                        "metadata": metadata or {},
                        "user_msg_id": user_msg_id,
                        "lifecycle_state": "sealed",
                        "sealed_at_ms": int(timestamp.timestamp() * 1000),
                    }
                ]

                finalization = {
                    "readable_project_ids": captured_readable_project_ids,
                    "artifact": artifact,
                }
                if outcome != "assistant_final":
                    finalization["outcome"] = outcome
                (
                    persisted_id,
                    source_ref_ids,
                    _created,
                ) = await self.knowledge_store.finalize_assistant_exchange(
                    agent_msg_batch[0],
                    source_candidates or [],
                    **finalization,
                )
                return persisted_id, source_ref_ids

            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        "Assistant message log failed "
                        f"(attempt {attempt + 1}/{max_retries}) for message "
                        f"{message_id}: {e}"
                    )
                    await asyncio.sleep(0.5 * (attempt + 1))
                else:
                    logger.error(
                        "Failed to persist assistant message log for message "
                        f"{message_id} after {max_retries} attempts: {e}"
                    )
                    raise

    async def close_user_only_turn(self, user_message_id: int) -> None:
        """Explicitly close a canonical user-only exchange for non-agent callers."""

        await self._close_user_exchange(user_message_id, outcome="user_only")

    async def _close_user_exchange(
        self,
        user_message_id: int,
        *,
        outcome: str,
        terminal_error: dict[str, object] | None = None,
    ) -> None:
        """Close one terminal non-assistant path before waking its project owner."""

        if not isinstance(user_message_id, int) or user_message_id <= 0:
            raise ValueError("user_message_id must be positive")
        closed_at_ms = int(get_now().timestamp() * 1000)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await self.knowledge_store.close_user_exchange(
                    user_name=self.user_name,
                    project_id=self.project_id,
                    session_id=self.session_id,
                    user_message_id=user_message_id,
                    outcome=outcome,
                    closed_at_ms=closed_at_ms,
                    terminal_error=terminal_error,
                )
                self._signal_exchange_closed()
                return
            except Exception:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(0.5 * (attempt + 1))

    def _signal_exchange_closed(self) -> None:
        """Wake the sole project semantic owner; durable storage is authoritative."""

        signal_semantic_work = getattr(self.project, "signal_semantic_work", None)
        if callable(signal_semantic_work):
            try:
                signal_semantic_work()
            except Exception:
                # The committed terminal result remains canonical; the project's
                # periodic durable scan can recover this missed wake.
                logger.exception(
                    "Failed to signal semantic work for {}", self.session_id
                )

    _HISTORY_PAGE = 200
    _HISTORY_MAX_TURNS = 10_000

    def _chat_compaction(self) -> ChatCompaction | None:
        postgres = getattr(self.resources, "postgres", None)
        if postgres is None:
            return None
        return ChatCompaction(
            postgres, self.llm, user_name=self.user_name, session_id=self.session_id,
            count_tokens=getattr(self.llm, "count_tokens", None),
        )

    async def _chat_summary(self) -> str:
        """The summary of turns that no longer fit the prompt, if any."""

        compaction = self._chat_compaction()
        if compaction is None:
            return ""
        try:
            return (await compaction.load())[0]
        except Exception:
            logger.exception("Chat summary unavailable for {}", self.session_id)
            return ""

    def _schedule_chat_compaction(self) -> None:
        """Start one background summary pass; never fails the finished turn."""

        try:
            settings = self.current_config.developer_settings.chat_compaction
        except Exception:
            logger.exception("Chat compaction settings unavailable for {}", self.session_id)
            return
        if (
            not settings.enabled or self._closed or self._conversation_tokens is None
            or (self._compaction_task is not None and not self._compaction_task.done())
        ):
            return
        self._compaction_task = asyncio.create_task(
            self._compact_chat(self._conversation_tokens, settings)
        )

    async def _compact_chat(self, conversation_tokens: int, settings) -> None:
        compaction = self._chat_compaction()
        if compaction is None:
            return
        try:
            history = await self._recent_history_within(
                conversation_tokens + settings.max_input_tokens, up_to_msg_id=None,
            )
            await compaction.extend(
                history, conversation_tokens=conversation_tokens, settings=settings
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            # The next finished turn tries again; the prompt still has the
            # recent turns and message search finds the rest.
            logger.exception("Chat compaction failed for {}", self.session_id)

    async def _recent_history_within(
        self, budget_tokens: int, *, up_to_msg_id: int
    ) -> List[Dict]:
        """Fetch newest-first pages until they could fill the prompt budget.

        Prompt assembly makes the exact cut. Text at or under four characters
        per token (English, code, CJK) cannot fill the budget with fewer
        characters than this, so older pages could never be used.
        """

        collected: List[Dict] = []
        characters = 0
        cursor = up_to_msg_id
        while len(collected) < self._HISTORY_MAX_TURNS:
            page = await self.get_conversation_context(self._HISTORY_PAGE, up_to_msg_id=cursor)
            if not page:
                break
            collected = page + collected
            characters += sum(len(str(turn.get("content") or "")) for turn in page)
            if len(page) < self._HISTORY_PAGE or characters >= budget_tokens * 4:
                break
            cursor = int(page[0]["message_id"]) - 1
        return collected

    async def get_conversation_context(
        self, num_turns: int, up_to_msg_id: Optional[int] = None
    ) -> List[Dict]:
        """Returns list of conversation turns in chronological order."""
        turns = await self.knowledge_store.get_session_history(
            user_name=self.user_name, session_id=self.session_id,
            limit=num_turns, up_to_msg_id=up_to_msg_id,
        )

        results = []
        for turn in turns:
            content = turn["content"]
            steer = (turn.get("metadata") or {}).get("run_steer")
            if (turn["role"] == "user" and isinstance(steer, dict)
                    and steer.get("state") == "applied"
                    and isinstance(steer.get("instruction"), str)):
                instruction = with_annotations(
                    steer["instruction"], steer.get("response_annotations")
                )
                content = f"{content}\n\nUser instruction during this run: {instruction}"
            role_label = "USER" if turn["role"] == "user" else "AGENT"
            ts = datetime.fromtimestamp(turn["timestamp"] / 1000, tz=timezone.utc)
            date_str = ts.strftime("%Y-%m-%d %H:%M")
            results.append(
                {
                    **turn,
                    "content": content,
                    "timestamp": ts.isoformat(),
                    "message": content,
                    "role_label": role_label,
                    "relative": f"[{date_str}]",
                }
            )

        return results

    async def shutdown(self) -> None:
        """Stop all session-owned work without skipping later cleanup phases."""

        async with self._shutdown_lock:
            if self._closed:
                return

            failures: list[Exception] = []
            async with self._agent_run_lock:
                self._agent_runs_closed = True
                for queue_id in self._follow_up_queue:
                    self._follow_ups[queue_id]["state"] = "cancelled"
                    self._follow_ups[queue_id].pop("query", None)
                self._follow_up_queue.clear()
                follow_up_task = self._follow_up_task

            try:
                await self.cancel_active_agent_run()
            except Exception as exc:
                logger.exception(
                    "Failed to cancel agent run for session {}", self.session_id
                )
                failures.append(exc)

            compaction_task = self._compaction_task
            if compaction_task is not None and not compaction_task.done():
                compaction_task.cancel()
                try:
                    await compaction_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.exception("Chat compaction stopped with an error for {}", self.session_id)

            if follow_up_task is not None and not follow_up_task.done():
                follow_up_task.cancel()
                try:
                    await follow_up_task
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    logger.exception("Failed to stop follow-up queue for session {}", self.session_id)
                    failures.append(exc)

            if not self._shutdown_event_emitted:
                try:
                    await emit(self.session_id, "system", "session_shutdown", {})
                    self._shutdown_event_emitted = True
                except Exception as exc:
                    logger.exception(
                        "Failed to emit shutdown event for session {}", self.session_id
                    )
                    failures.append(exc)

            if failures:
                raise RuntimeError(
                    f"SessionRuntime shutdown failed for {self.session_id}"
                ) from failures[0]
            self._closed = True
