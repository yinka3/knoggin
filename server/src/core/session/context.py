from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional

from loguru import logger

from common.conf.manager import ConfigManager
from common.schema.contracts import RelationshipObservation
from common.schema.primitives import Message
from common.schema.settings import RootConfig
from common.schema.source_reference import SourceReferenceCandidate
from common.utils.core_utils import (
    fetch_conversation_turns,
)
from common.utils.events import EventEmitter, emit
from common.utils.tasks import BackgroundTaskGroup
from common.utils.time_utils import parse_iso_time_or_now
from core.ingestion.batch import IngestionBatch
from core.ingestion.services.batch_consumer import IngestionWorker
from core.ingestion.services.pipeline_service import IngestionPipeline
from core.knowledge.db.write_graph_db import (
    write_batch_callback,
    write_ingestion_batch_to_graph,
)
from core.knowledge.documents import DocumentService
from core.project.state import ProjectState
from infrastructure.redis_client import (
    SESSION_RUNTIME_TTL_SECONDS,
    SHORT_LIVED_DEDUP_TTL_SECONDS,
    RedisKeys,
)
from infrastructure.resources import ResourceManager

SESSION_KEY_TTL = SESSION_RUNTIME_TTL_SECONDS


class Session:
    """
    Session represents the state and lifecycle container for an active user session.

    It serves as the root orchestration point for a session, binding together user
    state, background ingestion workers, and dynamic configuration. It deliberately
    holds references to the ingestion pipeline (`IngestionPipeline`, `IngestionWorker`)
    so it can gracefully orchestrate the shutdown of all asynchronous session tasks.

    Initialization and wiring logic is encapsulated in SessionFactory to decouple
    the construction of these services from the state container itself.
    """

    def __init__(self, user_name: str, topics: List[str], resources: ResourceManager):
        self.resources = resources
        self.user_name: str = user_name
        self.active_topics: List[str] = topics
        self.model: Optional[str] = None
        self.document_service: Optional[DocumentService] = None

        self.session_id: Optional[str] = None
        self.project_id: Optional[str] = None
        self.project: Optional[ProjectState] = None

        self._max_conversation_history: int = 10000

        self.batch_processor: Optional[IngestionPipeline] = None
        self.consumer: Optional[IngestionWorker] = None
        self.task_group = BackgroundTaskGroup("SessionTasks")
        self.config_unsubscribers: List = []
        self._message_add_lock = asyncio.Lock()

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

    @property
    def embedding_service(self):
        return self.resources.embedding

    @property
    def executor(self):
        return self.resources.executor

    @classmethod
    async def create(
        cls,
        user_name: str,
        resources: ResourceManager,
        topics_config: dict = None,
        session_id: str = None,
        model: str = None,
        project_state: ProjectState = None,
    ) -> "Session":
        """Assembles and launches a new session context."""
        from core.session.boot import SessionFactory

        assembler = SessionFactory(user_name, resources)
        ctx = await assembler.bootstrap(project_state, session_id, model)

        return ctx

    async def add(self, msg: Message) -> Message:
        if not self.project or not self.project.scheduler or not self.consumer:
            raise RuntimeError("Session is not fully initialized for message ingestion")

        async with self._message_add_lock:
            return await self._add_user_message(msg)

    async def _add_user_message(self, msg: Message) -> Message:
        """Durably accept and idempotently enqueue one user message."""

        msg.timestamp = self._normalize_timestamp(msg.timestamp)

        # Deterministic ID: same content + session + timestamp_ns = same ID
        timestamp_ns = int(msg.timestamp.timestamp() * 1e9)
        content_hash = hashlib.sha256(
            f"{self.session_id}:{msg.content.strip()}:{timestamp_ns}".encode()
        ).hexdigest()[:12]

        dedup_key = RedisKeys.message_dedup(
            self.user_name,
            self.session_id,
            content_hash,
        )

        existing_id = await self.redis_client.get(dedup_key)
        if existing_id:
            status, message_id = self._parse_message_dedup(existing_id)
            msg.id = message_id
            if status == "accepted":
                return msg
        else:
            new_id = await self.knowledge_store.allocate_message_id()
            was_set = await self.redis_client.set(
                dedup_key,
                f"pending:{new_id}",
                ex=SHORT_LIVED_DEDUP_TTL_SECONDS,
                nx=True,
            )

            if was_set:
                msg.id = new_id
            else:
                existing_id = await self.redis_client.get(dedup_key)
                if not existing_id:
                    raise RuntimeError("Message dedup claim disappeared during add")
                status, message_id = self._parse_message_dedup(existing_id)
                msg.id = message_id
                if status == "accepted":
                    return msg

        durable = False
        try:
            await self._persist_user_turn(msg)
            durable = True
            await self._record_conversation_message(
                message_id=msg.id,
                role="user",
                content=msg.content.strip(),
                timestamp=msg.timestamp,
                user_msg_id=msg.id,
            )
            await self._enqueue_user_message(msg)
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

        await self.project.record_session_activity()
        self.consumer.signal()
        await self.refresh_session_ttls()

        return msg

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
        """Write the canonical user message before acknowledging or enqueueing."""
        await self.knowledge_store.save_message_logs(
            [
                {
                    "id": msg.id,
                    "content": msg.content.strip(),
                    "role": "user",
                    "user_name": self.user_name,
                    "session_id": self.session_id,
                    "project_id": self.project_id,
                    "timestamp": msg.timestamp.timestamp() * 1000,
                    "metadata": {},
                    "user_msg_id": msg.id,
                }
            ]
        )

    async def _enqueue_user_message(self, msg: Message):
        await self.redis_client.incr(
            RedisKeys.heartbeat_counter(self.user_name, self.session_id)
        )
        if not self.project_id:
            raise RuntimeError("Session cannot enqueue messages without project_id")
        await self.redis_client.incr(
            RedisKeys.project_heartbeat_counter(self.user_name, self.project_id)
        )

        buffer_key = RedisKeys.buffer(self.user_name, self.session_id)
        payload = json.dumps(
            {
                "id": msg.id,
                "message": msg.content.strip(),
                "timestamp": msg.timestamp.isoformat(),
                "role": "user",
            }
        )
        await self.redis_client.lrem(buffer_key, 0, payload)
        await self.redis_client.rpush(buffer_key, payload)

    async def add_assistant_turn(
        self,
        content: str,
        timestamp: datetime,
        metadata: Optional[dict] = None,
        user_msg_id: Optional[int] = None,
        source_candidates: Optional[List[SourceReferenceCandidate]] = None,
    ):
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
            await self._persist_assistant_message_log(
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
    ):
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
                    }
                ]

                if source_candidates:
                    await self.knowledge_store.save_assistant_message_with_source_refs(
                        agent_msg_batch[0],
                        source_candidates,
                    )
                else:
                    await self.knowledge_store.save_message_logs(agent_msg_batch)
                return

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

    async def _write_to_graph(
        self,
        entity_ids: list[int],
        new_entity_ids: set[int],
        alias_updated_ids: set[int],
        relationship_observations: list[RelationshipObservation],
        alias_updates=None,
    ):
        """Delegate to shared graph write logic."""
        batch = IngestionBatch.open(
            user_name=self.user_name,
            project_id=self.project_id,
            session_id=self.session_id,
            messages=[],
            session_text="",
        )
        batch.validate_input()
        batch.mark_extracted()
        batch.set_resolution(
            entity_ids=entity_ids,
            new_entity_ids=new_entity_ids,
            alias_updated_ids=alias_updated_ids,
            entity_message_map={},
            alias_updates=alias_updates or {},
            candidate_suggestions=[],
        )
        batch.set_relationship_observations(relationship_observations)
        batch.complete()
        await write_ingestion_batch_to_graph(
            batch,
            knowledge_store=self.knowledge_store,
            entities=self.project.entities,
            redis_client=self.redis_client,
        )

    async def _write_to_graph_callback(
        self, batch: IngestionBatch
    ) -> tuple[bool, str | None]:
        return await write_batch_callback(
            batch,
            knowledge_store=self.knowledge_store,
            entities=self.project.entities,
            session_id=self.session_id,
            project_id=self.project_id,
            user_name=self.user_name,
            redis_client=self.redis_client,
        )

    async def refresh_session_ttls(self):
        """Refresh TTLs on all fixed session-scoped Redis keys."""
        pipe = self.redis_client.pipeline()
        for key in RedisKeys.session_keys(self.user_name, self.session_id):
            pipe.expire(key, SESSION_KEY_TTL)
        await pipe.execute()

    async def shutdown(self):
        for unsubscribe in self.config_unsubscribers:
            unsubscribe()
        self.config_unsubscribers.clear()

        if self.consumer:
            await self.consumer.stop()

        await self.task_group.shutdown(timeout=10.0)
        await emit(self.session_id, "system", "session_shutdown", {})
        await EventEmitter.get().cleanup_scope(self.session_id)
