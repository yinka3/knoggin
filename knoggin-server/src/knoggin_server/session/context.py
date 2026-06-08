from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from loguru import logger

from common.conf.manager import ConfigManager
from common.schema.contracts import (
    BatchResult,
    EntityProfilesResult,
    MessageConnections,
    MessageUserConnections,
)
from common.schema.primitives import FactRecord, Message
from common.schema.settings import RootConfig
from common.utils.core_utils import (
    fetch_conversation_turns,
)
from common.utils.events import DebugEventEmitter, emit
from common.utils.tasks import BackgroundTaskGroup
from common.utils.time_utils import get_now, parse_iso_time_or_now
from infrastructure.redis_client import AsyncRedisClient, RedisKeys
from infrastructure.resources import ResourceManager
from knoggin_server.agent.prompts import get_lightweight_extraction_prompt
from knoggin_server.ingestion.services.batch_consumer import BatchConsumer
from knoggin_server.ingestion.services.pipeline_service import BatchProcessor
from knoggin_server.knowledge.db.write_graph_db import (
    write_batch_callback,
    write_batch_to_graph,
)
from knoggin_server.knowledge.services.file_rag import FileRAGService
from knoggin_server.project.state import ProjectState

SESSION_KEY_TTL = 72 * 3600


class Context:
    """
    Context represents the state and lifecycle container for an active user session.

    It serves as the root orchestration point for a session, binding together user
    state, background ingestion workers, and dynamic configuration. It deliberately
    holds references to the ingestion pipeline (`BatchProcessor`, `BatchConsumer`)
    so it can gracefully orchestrate the shutdown of all asynchronous session tasks.

    Initialization and wiring logic is encapsulated in SessionAssembler to decouple
    the construction of these services from the state container itself.
    """

    def __init__(self, user_name: str, topics: List[str], resources: ResourceManager):
        self.resources = resources
        self.user_name: str = user_name
        self.active_topics: List[str] = topics
        self.model: Optional[str] = None
        self.file_rag: Optional[FileRAGService] = None

        self.session_id: Optional[str] = None
        self.project_id: Optional[str] = None
        self.project: Optional[ProjectState] = None

        self._max_conversation_history: int = 10000

        self.batch_processor: Optional[BatchProcessor] = None
        self.consumer: Optional[BatchConsumer] = None
        self.task_group = BackgroundTaskGroup("ContextTasks")
        self.config_unsubscribers: List = []

    @property
    def current_config(self) -> RootConfig:
        return ConfigManager.get().config

    @property
    def redis_client(self):
        return self.resources.redis

    @property
    def graph_client(self):
        return self.resources.graph_client

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
    ) -> "Context":
        """Assembles and launches a new session context."""
        from knoggin_server.session.boot import SessionAssembler

        assembler = SessionAssembler(user_name, resources)
        ctx = await assembler.bootstrap(project_state, session_id, model)

        return ctx

    async def get_next_msg_id(self) -> int:
        return await self.redis_client.incr(RedisKeys.global_next_msg_id())

    async def get_next_ent_id(self) -> int:
        return await self.redis_client.incr(RedisKeys.global_next_ent_id())

    async def get_next_turn_id(self) -> int:
        return await self.redis_client.incr(
            RedisKeys.global_next_turn_id(self.user_name, self.session_id)
        )

    async def add(self, msg: Message) -> Message:
        if not self.project or not self.project.scheduler or not self.consumer:
            raise RuntimeError("Context is not fully initialized for message ingestion")

        # Deterministic ID: same content + session + timestamp_ns = same ID
        timestamp_ns = int(msg.timestamp.timestamp() * 1e9)
        content_hash = hashlib.sha256(
            f"{self.session_id}:{msg.content.strip()}:{timestamp_ns}".encode()
        ).hexdigest()[:12]

        dedup_key = f"msg_dedup:{self.session_id}:{content_hash}"

        existing_id = await self.redis_client.get(dedup_key)
        if existing_id:
            msg.id = int(existing_id)
            return msg

        new_id = await self.get_next_msg_id()
        was_set = await self.redis_client.set(dedup_key, str(new_id), ex=300, nx=True)

        if not was_set:
            existing_id = await self.redis_client.get(dedup_key)
            msg.id = int(existing_id) if existing_id else new_id
            return msg

        msg.id = new_id

        await self._persist_user_turn(msg)
        await self._enqueue_user_message(msg)
        await self.project.record_session_activity()
        self.consumer.signal()
        await self.refresh_session_ttls()
        return msg

    async def _add_to_conversation_log(
        self,
        role: str,
        content: str,
        timestamp: datetime,
        user_msg_id: Optional[int] = None,
        metadata: Optional[dict] = None,
    ) -> int:
        """Saves a conversation turn to Redis via the hardened Smart Client."""
        turn_id = await self.get_next_turn_id()

        payload = {
            "role": role,
            "role_label": "Assistant" if role == "assistant" else "User",
            "content": content,
            "timestamp": timestamp.isoformat(),
            "metadata": metadata,
            "user_msg_id": user_msg_id,
        }

        # Use the Smart Client to handle storage and history pruning
        await AsyncRedisClient.log_conversation_turn(
            user_name=self.user_name,
            session_id=self.session_id,
            turn_id=turn_id,
            payload=payload,
            max_history=self.current_config.developer_settings.limits.conversation_context_turns
            or 100,
        )

        return turn_id

    async def _persist_user_turn(self, msg: Message):
        """Maps a message to a turn and stores its content via the Smart Client."""
        turn_id = await self._add_to_conversation_log(
            role="user",
            content=msg.content.strip(),
            timestamp=msg.timestamp,
            user_msg_id=msg.id,
        )

        # Map the message ID to this turn and store content
        await AsyncRedisClient.update_message_mapping(
            user_name=self.user_name,
            session_id=self.session_id,
            msg_id=msg.id,
            turn_id=turn_id,
            content=msg.content.strip(),
            timestamp=msg.timestamp.isoformat(),
            role="user",
        )

    async def _enqueue_user_message(self, msg: Message):
        await self.redis_client.incr(
            RedisKeys.heartbeat_counter(self.user_name, self.session_id)
        )
        if not self.project_id:
            raise RuntimeError("Context cannot enqueue messages without project_id")
        await self.redis_client.incr(
            RedisKeys.project_heartbeat_counter(self.user_name, self.project_id)
        )

        buffer_key = RedisKeys.buffer(self.user_name, self.session_id)
        await self.redis_client.rpush(
            buffer_key,
            json.dumps(
                {
                    "id": msg.id,
                    "message": msg.content.strip(),
                    "timestamp": msg.timestamp.isoformat(),
                    "role": "user",
                }
            ),
        )

    async def add_assistant_turn(
        self,
        content: str,
        timestamp: datetime,
        metadata: Optional[dict] = None,
        user_msg_id: Optional[int] = None,
    ):
        """Add assistant turn to conversation log."""
        if metadata is None:
            metadata = {}

        turn_id = await self._add_to_conversation_log(
            role="assistant",
            content=content,
            timestamp=timestamp,
            metadata=metadata,
            user_msg_id=user_msg_id,
        )

        self.task_group.create_task(
            self._persist_assistant_message_log(turn_id, content, timestamp),
            name=f"persist_assistant_message_log_{turn_id}",
        )

    async def _maybe_extract_llm(self, content: str, user_msg_id: int) -> bool:
        """
        Classify assistant response and extract facts if worthy via structured Pydantic models.
        Only attaches facts to entities that resolve. Unresolved subjects are skipped.

        Returns: True if facts were found, False otherwise.
        """
        if not content or len(content.strip()) < 50:
            return False

        system_prompt = "You are a knowledge extractor. Be precise and concise."
        user_prompt = get_lightweight_extraction_prompt(content)

        try:
            result: EntityProfilesResult = await self.llm.call_llm(
                system=system_prompt,
                user=user_prompt,
                response_model=EntityProfilesResult,
                temperature=0.0,
            )

            if not result or not result.profiles:
                return False

            # Pass 1: Batch-encode all subject names for resolution
            subject_names = []
            valid_profiles = []
            for profile in result.profiles:
                subject = profile.canonical_name.strip()
                if not subject:
                    continue
                subject_names.append(subject)
                valid_profiles.append(profile)

            if not subject_names:
                return False

            subject_embeddings = await self.embedding_service.encode(subject_names)

            # Pass 2: Resolve subjects to entities, collect fact text
            fact_work: List[Tuple[int, str]] = []  # (target_id, fact_content)
            config = self.current_config

            for i, profile in enumerate(valid_profiles):
                subject = subject_names[i]
                subject_emb = subject_embeddings[i]

                candidates = await self.project.entities.get_candidate_ids(
                    subject, precomputed_embedding=subject_emb
                )

                target_id = None
                threshold = (
                    config.developer_settings.entity_resolution.resolution_threshold
                )

                if candidates:
                    top_id, top_score = candidates[0]
                    if top_score >= threshold:
                        target_id = top_id

                if target_id is None:
                    logger.debug(
                        f"Skipping fact extraction: Subject '{subject}' "
                        f"did not resolve to a known entity."
                    )
                    continue

                for fact_update in profile.facts:
                    fact_content = fact_update.content.strip()
                    if fact_content:
                        fact_work.append((target_id, fact_content))

            if not fact_work:
                return False

            # Pass 3: Batch-encode all fact contents
            fact_contents = [content for _, content in fact_work]
            fact_embeddings = await self.embedding_service.encode(fact_contents)

            # Pass 4: Build Fact objects and write
            facts_by_entity: Dict[int, List[FactRecord]] = {}

            for i, (target_id, fact_content) in enumerate(fact_work):
                new_fact = FactRecord(
                    id=f"fact_{uuid.uuid4().hex[:16]}",
                    source_entity_id=target_id,
                    content=fact_content,
                    valid_at=get_now(),
                    source_msg_id=user_msg_id,
                    confidence=0.9,
                    embedding=fact_embeddings[i],
                    source="llm",
                )
                if target_id not in facts_by_entity:
                    facts_by_entity[target_id] = []
                facts_by_entity[target_id].append(new_fact)

            total_count = 0
            for eid, facts_to_write in facts_by_entity.items():
                try:
                    c = await self.graph_client.create_facts_batch(
                        eid,
                        facts_to_write,
                        user_name=self.user_name,
                        session_id=self.session_id,
                        project_id=self.project_id,
                    )
                    total_count += int(c)
                except Exception as e:
                    logger.error(
                        f"Failed to persist assistant facts for entity {eid}: {e}"
                    )

            if total_count > 0:
                logger.info(
                    f"Extracted {total_count} facts from assistant response (source='llm')"
                )

            return total_count > 0

        except Exception as e:
            logger.warning(f"Error in assistant fact extraction: {e}")
            return False

    async def _persist_assistant_message_log(
        self, turn_id: int, content: str, timestamp: datetime
    ):
        """Background task: write assistant message log with retry."""
        max_retries = 3

        for attempt in range(max_retries):
            try:
                graph_id = turn_id + 1_000_000_000

                agent_msg_batch = [
                    {
                        "id": graph_id,
                        "content": content,
                        "role": "assistant",
                        "user_name": self.user_name,
                        "session_id": self.session_id,
                        "project_id": self.project_id,
                        "timestamp": timestamp.timestamp() * 1000,
                    }
                ]

                await self.graph_client.save_message_logs(agent_msg_batch)
                return

            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Assistant message log failed (attempt {attempt + 1}/{max_retries}) for turn {turn_id}: {e}"
                    )
                    await asyncio.sleep(0.5 * (attempt + 1))
                else:
                    logger.error(
                        f"Failed to persist assistant message log for turn {turn_id} after {max_retries} attempts: {e}"
                    )

    async def get_conversation_context(
        self, num_turns: int, up_to_msg_id: Optional[int] = None
    ) -> List[Dict]:
        """Returns list of conversation turns in chronological order."""
        turns = await fetch_conversation_turns(
            self.redis_client, self.user_name, self.session_id, num_turns, up_to_msg_id
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
        relationship_observations: list[MessageConnections],
        user_relationship_observations: list[MessageUserConnections] = None,
        alias_updates=None,
    ):
        """Delegate to shared graph write logic."""
        batch = BatchResult(
            entity_ids=entity_ids,
            new_entity_ids=new_entity_ids,
            alias_updated_ids=alias_updated_ids,
            relationship_observations=relationship_observations,
            user_relationship_observations=user_relationship_observations or [],
            alias_updates=alias_updates or {},
        )
        batch.set_scope(self.user_name, self.session_id, self.project_id)
        await write_batch_to_graph(
            batch,
            graph_client=self.graph_client,
            entities=self.project.entities,
            session_id=self.session_id,
            project_id=self.project_id,
            user_name=self.user_name,
            redis_client=self.redis_client,
        )

    async def _write_to_graph_callback(
        self, result: BatchResult
    ) -> tuple[bool, str | None]:
        return await write_batch_callback(
            result,
            graph_client=self.graph_client,
            entities=self.project.entities,
            session_id=self.session_id,
            project_id=self.project_id,
            user_name=self.user_name,
            redis_client=self.redis_client,
        )



    async def refresh_session_ttls(self):
        """Refresh TTLs on all session-scoped Redis keys via the Smart Client."""
        await AsyncRedisClient.refresh_session_ttls(
            self.user_name, self.session_id, SESSION_KEY_TTL
        )

    async def shutdown(self):
        for unsubscribe in self.config_unsubscribers:
            unsubscribe()
        self.config_unsubscribers.clear()

        if self.consumer:
            await self.consumer.stop()

        await self.task_group.shutdown(timeout=10.0)
        await emit(self.session_id, "system", "session_shutdown", {})
        await DebugEventEmitter.get().cleanup_scope(self.session_id)
