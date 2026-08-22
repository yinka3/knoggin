from __future__ import annotations

import asyncio
import hashlib
import json
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional

from loguru import logger

from common.conf.manager import ConfigManager
from common.schema.agent.stream import AgentExecutionEvent
from common.schema.document import DocumentFocus
from common.schema.primitives import Message
from common.schema.settings import RootConfig
from common.schema.source.references import SourceReference, SourceReferenceCandidate
from common.utils.core_utils import (
    fetch_conversation_turns,
)
from common.utils.events import emit
from common.utils.time_utils import get_now, parse_iso_time_or_now
from core.ingestion.batch import IngestionBatch
from core.ingestion.graph_commit import write_ingestion_batch_to_graph
from core.ingestion.pipeline import IngestionPipeline
from core.ingestion.worker import IngestionWorker
from core.knowledge.documents import DocumentService
from infrastructure.redis_client import (
    SESSION_RUNTIME_TTL_SECONDS,
    SHORT_LIVED_DEDUP_TTL_SECONDS,
    RedisKeys,
)
from runtime.project_runtime import ProjectRuntime
from runtime.resources import RuntimeResources

SESSION_KEY_TTL = SESSION_RUNTIME_TTL_SECONDS
MAX_LOCAL_DURABLE_MESSAGE_CLAIMS = 1024


class SessionRuntime:
    """
    SessionRuntime represents one loaded, in-memory user session.

    It serves as the root orchestration point for a session, binding together user
    state, background ingestion workers, and dynamic configuration. It deliberately
    holds references to the ingestion pipeline (`IngestionPipeline`, `IngestionWorker`)
    so it can gracefully orchestrate the shutdown of all asynchronous session tasks.

    Initialization and wiring logic is encapsulated in SessionRuntimeFactory to decouple
    the construction of these services from the state container itself.
    """

    def __init__(
        self,
        user_name: str,
        resources: RuntimeResources,
        health_service: Any | None = None,
        agent_orchestrator: Any | None = None,
    ):
        self.resources = resources
        self.health_service = health_service
        self.agent_orchestrator = agent_orchestrator
        self.user_name: str = user_name
        self.model: Optional[str] = None
        self.agent_id: Optional[str] = None
        self.enabled_tools: Optional[List[str]] = None
        self.document_focus: Optional[DocumentFocus] = None
        self.document_service: Optional[DocumentService] = None

        self.session_id: Optional[str] = None
        self.project_id: Optional[str] = None
        self.project: Optional[ProjectRuntime] = None

        self.batch_processor: Optional[IngestionPipeline] = None
        self.consumer: Optional[IngestionWorker] = None
        self.config_unsubscribers: List = []
        self._message_add_lock = asyncio.Lock()
        # Conversation turns are accepted independently, but only the oldest
        # accepted turn may advance the session's agent state at a time.
        self._agent_submission_lock = asyncio.Lock()
        self._agent_queue_condition = asyncio.Condition()
        self._agent_run_queue: list[tuple[object, int]] = []
        self._active_agent_task: Optional[asyncio.Task] = None
        self._agent_run_state = "idle"
        self._agent_queue_paused = False
        self._agent_runs_closed = False
        self._durable_message_claims: OrderedDict[str, int] = OrderedDict()
        self._shutdown_lock = asyncio.Lock()
        self._closed = False

    @property
    def current_config(self) -> RootConfig:
        return ConfigManager.get().config

    @property
    def redis_client(self):
        return self.resources.redis

    @property
    def knowledge_store(self):
        return self.resources.knowledge_store

    @property
    def llm(self):
        return self.resources.llm_service

    async def add(self, msg: Message) -> Message:
        if self._closed or self._agent_runs_closed:
            raise RuntimeError("Session is shutting down")
        if not self.project or not self.project.scheduler or not self.consumer:
            raise RuntimeError("Session is not fully initialized for message ingestion")

        async with self._message_add_lock:
            return await self._add_user_message(msg)

    def agent_run_snapshot(self) -> dict[str, object]:
        """Return the bounded, session-owned state of agent execution."""

        return {
            "state": self._agent_run_state,
            "active": self._active_agent_task is not None
            and not self._active_agent_task.done(),
            "queued_message_ids": [
                message_id for _, message_id in self._agent_run_queue
            ],
            "queue_paused": self._agent_queue_paused,
        }

    async def resume_agent_queue(self) -> bool:
        """Permit the next already-accepted turn after an interrupted run.

        The caller represents the user or a higher-level interaction policy.
        The session itself never advances queued work automatically after a
        failed, cancelled, or clarification-only run.
        """

        async with self._agent_queue_condition:
            if (
                self._agent_runs_closed
                or self._active_agent_task is not None
                or not self._agent_queue_paused
                or not self._agent_run_queue
            ):
                return False
            self._agent_queue_paused = False
            self._agent_run_state = "idle"
            self._agent_queue_condition.notify_all()
            return True

    async def cancel_active_agent_run(self) -> bool:
        """Cancel only this session's active agent execution, if any."""

        async with self._agent_queue_condition:
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
    ) -> AsyncGenerator[AgentExecutionEvent, None]:
        """Run the canonical server-owned message-to-answer workflow.

        The user message becomes durable before it can wait behind an active
        run. Streamed tokens remain transient; a final ``response`` is exposed
        only after the assistant message and its consulted sources commit.
        """

        ticket = object()
        queued = False
        running = False
        outcome = "failed"
        task: Optional[asyncio.Task] = None

        try:
            # Serialize acceptance enough to preserve FIFO queue order without
            # blocking an unrelated session or a currently running agent.
            async with self._agent_submission_lock:
                if self._agent_runs_closed:
                    raise RuntimeError("Session is shutting down")
                if idempotency_key:
                    message.metadata["idempotency_key"] = idempotency_key
                accepted = await self.add(message)
                async with self._agent_queue_condition:
                    self._agent_run_queue.append((ticket, accepted.id))
                    queued = True
                    self._agent_queue_condition.notify_all()

            task = asyncio.current_task()
            if task is None:
                raise RuntimeError("Agent stream must run in an asyncio task")

            async with self._agent_queue_condition:
                while True:
                    if self._agent_runs_closed:
                        raise RuntimeError("Session is shutting down")
                    is_head = (
                        bool(self._agent_run_queue)
                        and self._agent_run_queue[0][0] is ticket
                    )
                    if (
                        is_head
                        and self._active_agent_task is None
                        and not self._agent_queue_paused
                    ):
                        self._active_agent_task = task
                        self._agent_run_state = "running"
                        running = True
                        break
                    await self._agent_queue_condition.wait()

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
                    )
                    response = dict(response)
                    response["assistant_message_id"] = commit["message_id"]
                    response["source_ref_ids"] = commit["source_ref_ids"]
                    event = {"event": "response", "data": response}
                    outcome = "completed"
                elif event["event"] == "clarification":
                    outcome = "awaiting_input"
                elif event["event"] == "error":
                    outcome = "failed"
                yield event

            if not response_seen and outcome == "failed":
                logger.error(
                    "Agent stream ended without a final response for session {}",
                    self.session_id,
                )
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        except Exception:
            outcome = "failed"
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
            async with self._agent_queue_condition:
                if queued:
                    self._agent_run_queue = [
                        entry
                        for entry in self._agent_run_queue
                        if entry[0] is not ticket
                    ]
                if running and self._active_agent_task is task:
                    self._active_agent_task = None
                if running:
                    self._agent_run_state = outcome
                    if outcome != "completed":
                        self._agent_queue_paused = True
                self._agent_queue_condition.notify_all()

    @staticmethod
    def _assistant_response_metadata(response: Dict[str, Any]) -> dict:
        """Persist server-owned response metadata, not client presentation state."""

        metadata = {"usage": response["usage"]}
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

    async def _add_user_message(self, msg: Message) -> Message:
        """Durably accept and idempotently enqueue one user message."""

        msg.timestamp = self._normalize_timestamp(msg.timestamp)

        idempotency_key = str(msg.metadata.get("idempotency_key", "")).strip()
        # Prefer the application request key for retries. Keep the existing
        # content/timestamp fallback for internal callers that do not provide
        # an application idempotency key.
        timestamp_ns = int(msg.timestamp.timestamp() * 1e9)
        content_hash = hashlib.sha256(
            (
                f"{self.session_id}:request:{idempotency_key}"
                if idempotency_key
                else f"{self.session_id}:{msg.content.strip()}:{timestamp_ns}"
            ).encode()
        ).hexdigest()[:12]

        dedup_key = RedisKeys.message_dedup(
            self.user_name,
            self.session_id,
            content_hash,
        )

        durable = False
        while True:
            existing_id = await self.redis_client.get(dedup_key)
            if existing_id:
                status, message_id = self._parse_message_dedup(existing_id)
                msg.id = message_id
                if status == "accepted":
                    return msg

                resolution = await self._wait_for_pending_message_claim(
                    dedup_key,
                    msg,
                )
                if resolution == "accepted":
                    return msg
                if resolution == "durable":
                    durable = True
                    break
                continue

            new_id = await self.knowledge_store.allocate_message_id()
            if await self.redis_client.set(
                dedup_key,
                f"pending:{new_id}",
                ex=SHORT_LIVED_DEDUP_TTL_SECONDS,
                nx=True,
            ):
                msg.id = new_id
                break

        try:
            if not durable:
                await self._persist_user_turn(msg)
                self._remember_durable_message(dedup_key, msg.id)
                durable = True
            await self._record_conversation_message(
                message_id=msg.id,
                role="user",
                content=msg.content.strip(),
                timestamp=msg.timestamp,
                user_msg_id=msg.id,
                metadata=msg.metadata,
            )
            await self._signal_user_message()
            await self.redis_client.set(
                dedup_key,
                f"accepted:{msg.id}",
                ex=SHORT_LIVED_DEDUP_TTL_SECONDS,
            )
        except Exception:
            if not durable:
                try:
                    await self.redis_client.delete(dedup_key)
                except Exception as cleanup_exc:
                    logger.warning(
                        "Failed to release message dedup claim after add failure: "
                        f"{cleanup_exc}"
                    )
            raise

        self.consumer.signal()
        await self.refresh_session_ttls()

        return msg

    async def _wait_for_pending_message_claim(
        self,
        dedup_key: str,
        msg: Message,
    ) -> str:
        """Wait for another runtime to finish a shared acceptance claim.

        A pending claim can mean either an active writer or a crashed writer
        that already committed PostgreSQL.  Let the active writer publish its
        accepted state first; after a bounded wait, a durable row can safely be
        completed by this runtime without allocating or inserting a second ID.
        """

        durable_id: Optional[int] = None
        for attempt in range(50):
            local_durable_id = self._durable_message_claims.get(dedup_key)
            if local_durable_id is not None:
                msg.id = local_durable_id
                return "durable"

            current = await self.redis_client.get(dedup_key)
            if current is None:
                return "retry"

            status, message_id = self._parse_message_dedup(current)
            msg.id = message_id
            if status == "accepted":
                return "accepted"

            if attempt % 5 == 0:
                durable_id = await self._find_durable_user_message(msg)
                if durable_id is not None and attempt >= 20:
                    msg.id = durable_id
                    return "durable"
            await asyncio.sleep(0.01)

        if durable_id is not None:
            msg.id = durable_id
            return "durable"
        raise RuntimeError("Message dedup claim remained pending")

    async def _find_durable_user_message(self, msg: Message) -> Optional[int]:
        """Find a committed row matching the deterministic acceptance identity."""

        if not self.session_id or not self.project_id:
            return None
        row = await self.resources.postgres.fetch_one(
            """
            SELECT message_id
            FROM public.messages
            WHERE user_name = %s
              AND session_id = %s
              AND project_id = %s
              AND role = 'user'
              AND content = %s
              AND timestamp_ms = %s
            ORDER BY message_id
            LIMIT 1
            """,
            (
                self.user_name,
                self.session_id,
                self.project_id,
                msg.content.strip(),
                int(msg.timestamp.timestamp() * 1000),
            ),
        )
        if not row or row.get("message_id") is None:
            return None
        return int(row["message_id"])

    def _remember_durable_message(self, dedup_key: str, message_id: int) -> None:
        """Keep only a bounded local retry hint; PostgreSQL remains canonical."""

        self._durable_message_claims[dedup_key] = message_id
        self._durable_message_claims.move_to_end(dedup_key)
        while len(self._durable_message_claims) > MAX_LOCAL_DURABLE_MESSAGE_CLAIMS:
            self._durable_message_claims.popitem(last=False)

    @staticmethod
    def _parse_message_dedup(value: str) -> tuple[str, int]:
        """Parse current status values and legacy numeric dedup claims."""
        text = str(value)
        if ":" not in text:
            return "accepted", int(text)
        status, raw_message_id = text.split(":", 1)
        if status not in {"pending", "accepted"}:
            raise ValueError(f"Unknown message dedup status: {status}")
        return status, int(raw_message_id)

    @staticmethod
    def _normalize_timestamp(timestamp: datetime) -> datetime:
        if timestamp.tzinfo is None:
            return timestamp.replace(tzinfo=timezone.utc)
        return timestamp.astimezone(timezone.utc)

    async def _record_conversation_message(
        self,
        message_id: int,
        role: str,
        content: str,
        timestamp: datetime,
        user_msg_id: Optional[int] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        """Atomically cache one canonical message and its conversation position."""
        normalized_timestamp = self._normalize_timestamp(timestamp)
        timestamp_iso = normalized_timestamp.isoformat()
        message_key = str(message_id)

        payload = {
            "message_id": message_id,
            "role": role,
            "role_label": "Assistant" if role == "assistant" else "User",
            "content": content,
            "timestamp": timestamp_iso,
            "metadata": metadata,
            "user_msg_id": user_msg_id,
        }
        content_payload = {
            "id": message_id,
            "message": content,
            "content": content,
            "timestamp": timestamp_iso,
            "role": role,
        }
        max_history = (
            self.current_config.developer_settings.limits.conversation_context_turns
            or 100
        )

        pipe = self.redis_client.pipeline()
        pipe.hset(
            RedisKeys.conversation(self.user_name, self.session_id),
            message_key,
            json.dumps(payload),
        )
        pipe.zadd(
            RedisKeys.recent_conversation(self.user_name, self.session_id),
            {message_key: normalized_timestamp.timestamp()},
        )
        pipe.hset(
            RedisKeys.message_content(self.user_name, self.session_id),
            f"msg_{message_id}",
            json.dumps(content_payload),
        )
        pipe.zremrangebyrank(
            RedisKeys.recent_conversation(self.user_name, self.session_id),
            0,
            -(max_history + 1),
        )
        await pipe.execute()

    async def _persist_user_turn(self, msg: Message):
        """Durably create an editable canonical user message and revision one."""
        await self.knowledge_store.create_editable_user_message(
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
            },
            edit_window_seconds=(
                self.current_config.developer_settings.ingestion.message_edit_window_seconds
            ),
        )

    async def _signal_user_message(self):
        """Refresh operational counters; Postgres owns the ingestion queue."""
        await self.redis_client.incr(
            RedisKeys.heartbeat_counter(self.user_name, self.session_id)
        )
        if not self.project_id:
            raise RuntimeError("Session cannot enqueue messages without project_id")
        await self.redis_client.incr(
            RedisKeys.project_heartbeat_counter(self.user_name, self.project_id)
        )

    async def add_assistant_turn(
        self,
        content: str,
        timestamp: datetime,
        metadata: Optional[dict] = None,
        user_msg_id: Optional[int] = None,
        source_candidates: Optional[List[SourceReferenceCandidate]] = None,
    ) -> dict[str, Any]:
        """Add assistant turn to conversation log."""
        if metadata is None:
            metadata = {}

        timestamp = self._normalize_timestamp(timestamp)
        message_id = await self.knowledge_store.allocate_message_id()
        await self._record_conversation_message(
            message_id=message_id,
            role="assistant",
            content=content,
            timestamp=timestamp,
            metadata=metadata,
            user_msg_id=user_msg_id,
        )

        try:
            source_references = await self._persist_assistant_message_log(
                message_id,
                content,
                timestamp,
                metadata=metadata,
                user_msg_id=user_msg_id,
                source_candidates=source_candidates,
            )
        except Exception:
            try:
                await self._delete_conversation_message(message_id)
            except Exception as cleanup_exc:
                logger.error(
                    "Failed to remove assistant message after persistence "
                    f"failure for message {message_id}: {cleanup_exc}"
                )
            raise
        await self.refresh_session_ttls()
        return {
            "message_id": message_id,
            "source_ref_ids": [
                reference.source_ref_id
                for reference in source_references
                if getattr(reference, "source_ref_id", None)
            ],
        }

    async def _delete_conversation_message(self, message_id: int) -> None:
        """Remove all staged Postgres and Redis state for one canonical message."""
        # Delete from Postgres
        query = (
            "DELETE FROM public.messages WHERE user_name = %(user_name)s "
            "AND session_id = %(session_id)s AND message_id = %(message_id)s"
        )
        await self.resources.postgres.execute(
            query,
            {
                "user_name": self.user_name,
                "session_id": self.session_id,
                "message_id": message_id,
            },
        )

        message_key = str(message_id)
        pipe = self.redis_client.pipeline()
        pipe.hdel(
            RedisKeys.conversation(self.user_name, self.session_id),
            message_key,
        )
        pipe.zrem(
            RedisKeys.recent_conversation(self.user_name, self.session_id),
            message_key,
        )
        pipe.hdel(
            RedisKeys.message_content(self.user_name, self.session_id),
            f"msg_{message_id}",
        )
        await pipe.execute()

    async def _persist_assistant_message_log(
        self,
        message_id: int,
        content: str,
        timestamp: datetime,
        metadata: Optional[dict] = None,
        user_msg_id: Optional[int] = None,
        source_candidates: Optional[List[SourceReferenceCandidate]] = None,
    ) -> List[SourceReference]:
        """Write an assistant message log, raising after bounded retries."""
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
                        "ingestion_state": "excluded",
                        "episode_eligible": False,
                    }
                ]

                if source_candidates:
                    if self.project is None:
                        raise RuntimeError("Session project runtime is unavailable")
                    return await self.knowledge_store.save_assistant_message_with_source_refs(
                        agent_msg_batch[0],
                        source_candidates,
                        readable_project_ids=self.project.readable_project_ids,
                    )
                else:
                    await self.knowledge_store.save_message_logs(agent_msg_batch)
                    return []

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

    async def _write_to_graph_callback(
        self, batch: IngestionBatch
    ):
        return await write_ingestion_batch_to_graph(
            batch,
            knowledge_store=self.knowledge_store,
            entities=self.project.entities,
        )

    async def refresh_session_ttls(self):
        """Refresh TTLs on all fixed session-scoped Redis keys."""
        pipe = self.redis_client.pipeline()
        for key in RedisKeys.session_keys(self.user_name, self.session_id):
            pipe.expire(key, SESSION_KEY_TTL)
        await pipe.execute()

    async def shutdown(self) -> None:
        """Stop all session-owned work without skipping later cleanup phases."""

        async with self._shutdown_lock:
            if self._closed:
                return

            failures: list[Exception] = []
            async with self._agent_queue_condition:
                self._agent_runs_closed = True
                self._agent_queue_paused = True
                self._agent_run_queue.clear()
                self._agent_queue_condition.notify_all()

            try:
                await self.cancel_active_agent_run()
            except Exception as exc:
                logger.exception("Failed to cancel agent run for session {}", self.session_id)
                failures.append(exc)

            if self.consumer is not None:
                try:
                    await self.consumer.stop()
                except Exception as exc:
                    logger.exception("Failed to stop worker for session {}", self.session_id)
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

            self._durable_message_claims.clear()
            self._closed = True
            try:
                await emit(self.session_id, "system", "session_shutdown", {})
            except Exception as exc:
                logger.exception("Failed to emit shutdown event for session {}", self.session_id)
                failures.append(exc)

            if failures:
                raise RuntimeError(
                    f"SessionRuntime shutdown failed for {self.session_id}"
                ) from failures[0]
