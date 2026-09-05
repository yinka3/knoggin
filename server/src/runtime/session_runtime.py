from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional

from loguru import logger

from common.conf.manager import ConfigManager
from common.exceptions import SessionBusyError
from common.schema.agent.research import ResearchMode
from common.schema.agent.stream import AgentExecutionEvent
from common.schema.artifacts import ArtifactDraft
from common.schema.document import DocumentFocus
from common.schema.primitives import Message
from common.schema.settings import RootConfig
from common.schema.source.references import SourceReferenceCandidate
from common.utils.core_utils import (
    fetch_conversation_turns,
)
from common.utils.events import emit
from common.utils.time_utils import get_now, parse_iso_time_or_now
from core.knowledge.documents import DocumentService
from runtime.project_runtime import ProjectRuntime
from runtime.resources import RuntimeResources


class SessionRuntime:
    """
    SessionRuntime represents one loaded, in-memory user session.

    It serves as the root orchestration point for a session, binding together user
    state and dynamic configuration. Semantic processing is owned solely by the
    project runtime's durable semantic-window job.

    Initialization and wiring logic is encapsulated in SessionRuntimeFactory to decouple
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
    ):
        self.resources = resources
        self.health_service = health_service
        self.agent_orchestrator = agent_orchestrator
        self.user_name: str = user_name
        self.model = model
        self.agent_id = agent_id
        self.enabled_tools = list(enabled_tools) if enabled_tools is not None else None
        self.document_focus = document_focus
        self.document_service: Optional[DocumentService] = None

        self.session_id = session_id
        self.project_id = project_id
        self.project = project

        self.config_unsubscribers: List = []
        self._agent_run_lock = asyncio.Lock()
        self._shutdown_lock = asyncio.Lock()
        self._agent_run_reserved = False
        self._active_agent_task: Optional[asyncio.Task] = None
        self._agent_runs_closed = False
        self._closed = False

    @property
    def current_config(self) -> RootConfig:
        return ConfigManager.get().config

    @property
    def knowledge_store(self):
        return self.resources.knowledge_store

    @property
    def llm(self):
        return self.resources.llm_service

    async def cancel_active_agent_run(self) -> bool:
        """Cancel only this session's active agent execution, if any."""

        async with self._agent_run_lock:
            task = self._active_agent_task
        if task is None or task.done() or task is asyncio.current_task():
            return False

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return True

    async def run_agent_stream(
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
    ) -> AsyncGenerator[AgentExecutionEvent, None]:
        """Run one admitted canonical user-message-to-answer workflow."""

        stream = await self.open_agent_run_stream(
            message,
            orchestrator=orchestrator,
            user_timezone=user_timezone,
            model=model,
            agent_id=agent_id,
            enabled_tools=enabled_tools,
            document_focus=document_focus,
            pasted_text_spans=pasted_text_spans,
            idempotency_key=idempotency_key,
            research_mode=research_mode,
        )
        async for event in stream:
            yield event

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
    ) -> AsyncGenerator[AgentExecutionEvent, None]:
        """Admit and persist a run before returning its event stream.

        Adapters can await this preflight operation and report a conflict
        before starting an HTTP/SSE response. A rejected overlapping run does
        not allocate or persist a user message.
        """

        async with self._agent_run_lock:
            if self._closed or self._agent_runs_closed:
                raise RuntimeError("Session is shutting down")
            self._require_message_ingestion_ready()
            if self._agent_run_reserved:
                raise SessionBusyError()
            self._agent_run_reserved = True

        try:
            if idempotency_key:
                message.metadata["idempotency_key"] = idempotency_key
            accepted, _created = await self._accept_user_message(message)
        except Exception:
            await self._release_agent_run(None)
            raise

        return self._run_admitted_agent_stream(
            accepted,
            orchestrator=orchestrator,
            user_timezone=user_timezone,
            model=model,
            agent_id=agent_id,
            enabled_tools=enabled_tools,
            document_focus=document_focus,
            pasted_text_spans=pasted_text_spans,
            research_mode=research_mode,
        )

    def _require_message_ingestion_ready(self) -> None:
        if (
            not getattr(self.project, "scheduler", None)
            or getattr(self.project, "project_semantic_job", None) is None
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
    ) -> AsyncGenerator[AgentExecutionEvent, None]:
        """Execute one already-persisted, exclusively admitted run."""

        exchange_outcome = "failed"
        task = asyncio.current_task()
        if task is None:
            await self._release_agent_run(None)
            raise RuntimeError("Agent stream must run in an asyncio task")

        async with self._agent_run_lock:
            if self._agent_runs_closed:
                self._agent_run_reserved = False
                raise RuntimeError("Session is shutting down")
            self._active_agent_task = task

        try:
            history = await self.get_conversation_context(
                self.current_config.developer_settings.limits.conversation_context_turns,
                up_to_msg_id=accepted.id - 1,
            )
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
                user_message_id=accepted.id,
                pasted_text_spans=pasted_text_spans,
                research_mode=research_mode,
            ):
                if event["event"] == "response":
                    if response_seen:
                        raise RuntimeError(
                            "Agent stream emitted multiple final responses"
                        )
                    response_seen = True
                    response = event["data"]
                    commit = await self.add_assistant_turn(
                        content=response["content"],
                        timestamp=get_now(),
                        metadata=self._assistant_response_metadata(response),
                        user_msg_id=accepted.id,
                        source_candidates=self._response_source_candidates(response),
                        artifact=self._response_artifact(response),
                    )
                    response = dict(response)
                    response["assistant_message_id"] = commit["message_id"]
                    response["source_ref_ids"] = commit["source_ref_ids"]
                    event = {"event": "response", "data": response}
                    exchange_outcome = "assistant_final"
                elif event["event"] == "clarification":
                    exchange_outcome = "clarification"
                elif event["event"] == "error":
                    exchange_outcome = "failed"
                yield event

            if not response_seen and exchange_outcome == "failed":
                logger.error(
                    "Agent stream ended without a final response for session {}",
                    self.session_id,
                )
        except asyncio.CancelledError:
            exchange_outcome = "cancelled"
            raise
        except Exception:
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
            try:
                if exchange_outcome != "assistant_final":
                    await self._close_user_exchange(
                        accepted.id,
                        outcome=exchange_outcome,
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
            if task is None or self._active_agent_task is task:
                self._active_agent_task = None
            self._agent_run_reserved = False

    @staticmethod
    def _assistant_response_metadata(response: Dict[str, Any]) -> dict:
        """Persist server-owned response metadata, not client presentation state."""

        metadata = {"usage": response["usage"]}
        if response.get("research_mode"):
            metadata["research_mode"] = response["research_mode"]
        if response.get("fallback"):
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

    async def _accept_user_message(self, msg: Message) -> tuple[Message, bool]:
        """Persist one user message and report whether it was newly created."""

        msg.timestamp = self._normalize_timestamp(msg.timestamp)

        idempotency_key = str(msg.metadata.get("idempotency_key", "")).strip()
        # PostgreSQL owns this stable acceptance identity.  An application
        # request key wins; internal callers retain deterministic content/time
        # acceptance without a separate cache protocol.
        timestamp_ns = int(msg.timestamp.timestamp() * 1e9)
        fallback_key = hashlib.sha256(
            (
                f"{self.session_id}:{msg.content.strip()}:{timestamp_ns}"
            ).encode()
        ).hexdigest()
        acceptance_key = (
            f"request:{idempotency_key}" if idempotency_key else f"content:{fallback_key}"
        )
        msg.id = await self.knowledge_store.allocate_message_id()

        acceptance = await self._persist_user_turn(msg, acceptance_key=acceptance_key)
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

    async def _persist_user_turn(self, msg: Message, *, acceptance_key: str):
        """Durably create an editable canonical user message and revision one."""
        return await self.knowledge_store.create_editable_user_message(
            {
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
            },
            edit_window_seconds=(
                self.current_config.developer_settings.ingestion.message_edit_window_seconds
            ),
        )

    async def add_assistant_turn(
        self,
        content: str,
        timestamp: datetime,
        metadata: Optional[dict] = None,
        user_msg_id: Optional[int] = None,
        source_candidates: Optional[List[SourceReferenceCandidate]] = None,
        artifact: ArtifactDraft | None = None,
    ) -> dict[str, Any]:
        """Add assistant turn to conversation log."""
        if metadata is None:
            metadata = {}

        timestamp = self._normalize_timestamp(timestamp)
        message_id = await self.knowledge_store.allocate_message_id()
        if user_msg_id is None:
            raise ValueError("Assistant turns must be linked to a user exchange")
        persisted_message_id, source_ref_ids = await self._persist_assistant_message_log(
            message_id,
            content,
            timestamp,
            metadata=metadata,
            user_msg_id=user_msg_id,
            source_candidates=source_candidates,
            artifact=artifact,
        )
        self._signal_exchange_closed()
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
    ) -> tuple[int, list[str]]:
        """Atomically persist an assistant response and close its user exchange."""
        max_retries = 3

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

                if self.project is None:
                    raise RuntimeError("Session project runtime is unavailable")
                persisted_id, source_ref_ids, _created = (
                    await self.knowledge_store.finalize_assistant_exchange(
                        agent_msg_batch[0],
                        source_candidates or [],
                        readable_project_ids=self.project.readable_project_ids,
                        artifact=artifact,
                    )
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

    async def _close_user_exchange(self, user_message_id: int, *, outcome: str) -> None:
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
            signal_semantic_work()

    async def get_conversation_context(
        self, num_turns: int, up_to_msg_id: Optional[int] = None
    ) -> List[Dict]:
        """Returns list of conversation turns in chronological order."""
        turns = await fetch_conversation_turns(
            self.resources.postgres,
            self.user_name,
            self.session_id,
            num_turns,
            up_to_msg_id,
        )

        results = []
        for turn in turns:
            role_label = "USER" if turn["role"] == "user" else "AGENT"
            ts = parse_iso_time_or_now(turn["timestamp"])
            date_str = ts.strftime("%Y-%m-%d %H:%M")
            results.append(
                {
                    **turn,
                    "message": turn["content"],
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

            try:
                await self.cancel_active_agent_run()
            except Exception as exc:
                logger.exception("Failed to cancel agent run for session {}", self.session_id)
                failures.append(exc)

            unsubscribers, self.config_unsubscribers = self.config_unsubscribers, []
            for unsubscribe in unsubscribers:
                try:
                    unsubscribe()
                except Exception as exc:
                    logger.exception(
                        "Session configuration cleanup failed for {}", self.session_id
                    )
                    failures.append(exc)

            self._closed = True
            try:
                await emit(self.session_id, "system", "session_shutdown", {})
            except Exception as exc:
                logger.exception(
                    "Failed to emit shutdown event for session {}", self.session_id
                )
                failures.append(exc)

            if failures:
                raise RuntimeError(
                    f"SessionRuntime shutdown failed for {self.session_id}"
                ) from failures[0]
