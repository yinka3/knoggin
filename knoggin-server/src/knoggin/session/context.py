from common.schema.settings import JobSettings
from __future__ import annotations
import asyncio
import hashlib
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import redis.asyncio as aioredis
from loguru import logger

from common.conf.base import deep_merge, get_config
from common.conf.topics_config import TopicConfig
from common.schema.settings import RootConfig
from common.schema.dtypes import BatchResult, EntityProfilesResult, FactRecord, Message
from common.utils.core_utils import (
    fetch_conversation_turns,
    handle_background_task_result,
    safe_update,
)
from common.utils.events import DebugEventEmitter, emit
from infrastructure.memgraph_client import MemgraphClient
from infrastructure.jobs.base import BaseJob, JobContext
from infrastructure.jobs.scheduler import Scheduler
from infrastructure.llm_client import LLMService
from infrastructure.redis_client import AsyncRedisClient, RedisKeys
from infrastructure.resources import ResourceManager
from knoggin.agent.prompts import get_lightweight_extraction_prompt
from knoggin.ingestion.services.batch_consumer import BatchConsumer
from knoggin.ingestion.services.pipeline_service import BatchProcessor
from knoggin.ingestion.services.processor import TextProcessor
from knoggin.knowledge.db.write_graph_db import (
    write_batch_callback,
    write_batch_to_graph,
)
from knoggin.knowledge.services.embedding_service import EmbeddingService
from knoggin.knowledge.services.entity_service import EntityManager
from knoggin.knowledge.services.file_rag import FileRAGService

SESSION_KEY_TTL = 72 * 3600


class Context:
    """
    Context represents the state of an active user session.
    Initialization and wiring logic is encapsulated in SessionAssembler.
    """

    def __init__(self, user_name: str, topics: List[str], redis_client):
        self.user_name: str = user_name
        self.active_topics: List[str] = topics
        self.resources: Optional[ResourceManager] = None
        self.scheduler: Optional[Scheduler] = None
        self.redis_client: aioredis.Redis = redis_client
        self.model: Optional[str] = None
        self.llm: Optional[LLMService] = None
        self.file_rag: Optional[FileRAGService] = None
        self.mcp_manager: Optional[Any] = None

        self.memgraph: Optional[MemgraphClient] = None
        self.processor: Optional[TextProcessor] = None
        self.embedding_service: Optional[EmbeddingService] = None
        self.entities: Optional[EntityManager] = None
        self.session_id: Optional[str] = None
        self.topic_config: Optional[TopicConfig] = None
        self._max_conversation_history: int = 10000

        self.executor: Optional[ThreadPoolExecutor] = None
        self.batch_processor: Optional[BatchProcessor] = None
        self.consumer: Optional[BatchConsumer] = None
        self.profile_job: Optional[BaseJob] = None
        self.merge_job: Optional[BaseJob] = None
        self._background_tasks: set[asyncio.Task] = set()
        self.current_config: RootConfig = get_config()

    @classmethod
    async def create(
        cls,
        user_name: str,
        resources: ResourceManager,
        topics_config: dict = None,
        session_id: str = None,
        model: str = None,
    ) -> "Context":
        """Assembles and launches a new session context."""
        from knoggin.session.boot import SessionAssembler

        assembler = SessionAssembler(user_name, resources)
        ctx = await assembler.bootstrap(topics_config, session_id, model)

        await ctx._verify_user_entity(user_name)

        return ctx

    async def get_next_msg_id(self) -> int:
        return await self.redis_client.incr(RedisKeys.global_next_msg_id())

    async def get_next_ent_id(self) -> int:
        return await self.redis_client.incr(RedisKeys.global_next_ent_id())

    async def get_next_turn_id(self) -> int:
        return await self.redis_client.incr(
            RedisKeys.global_next_turn_id(self.user_name, self.session_id)
        )

    async def update_topics_config(self, new_config: dict):
        self.topic_config.update(new_config)
        await self.topic_config.save(self.redis_client, self.user_name, self.session_id)
        self.entities.hierarchy_config = self.topic_config.hierarchy
        self.processor.refresh_topic_mappings()
        await emit(
            self.session_id,
            "system",
            "topics_updated",
            {"topics": list(new_config.keys())},
        )

    async def _verify_user_entity(self, user_name: str):
        user_id = await self.entities.get_id(user_name)
        if user_id is None:
            logger.critical(
                f"User entity not found for '{user_name}' in entities. Onboarding may not have completed."
            )
            return

        entity = await self.memgraph.get_entity_by_id(user_id)

        if not entity or entity.get("canonical_name") != user_name:
            logger.critical(
                f"User entity lookup mismatch for '{user_name}' (id={user_id}). "
                f"Onboarding may not have completed."
            )
            return

        profile = self.entities.entity_profiles.get(user_id)
        if profile and profile["canonical_name"] == user_name:
            logger.info(f"User entity verified: {user_name} (id={user_id})")
            await emit(
                self.session_id,
                "system",
                "user_entity_verified",
                {"user_name": user_name, "entity_id": user_id},
            )
            return

        logger.warning(
            "User entity exists in graph but missing from entities, backfilling"
        )
        all_aliases = entity.get("aliases") or [user_name]
        await self.entities.register_entity(
            user_id, user_name, all_aliases, "person", "Identity"
        )
        await emit(
            self.session_id, "system", "user_entity_recovered", {"user_name": user_name}
        )

    async def _run_session_jobs(self):
        await emit(self.session_id, "job", "session_jobs_started", {})
        ctx = JobContext(
            user_name=self.user_name, session_id=self.session_id, idle_seconds=0
        )

        # 1. Profile Refinement (Primary focus for consistent views)
        if await self.profile_job.should_run(ctx):
            await self.profile_job.execute(ctx)

        # 2. Merger (Uses refined profiles)
        # Targeted Flush: If we have pending merges, only force refinement for
        # entities that are actively in the merge queue and also dirty.
        is_merge_pending = getattr(
            self.merge_job, "enabled", True
        ) and await self.merge_job.should_run(ctx)

        if is_merge_pending:
            merge_key = RedisKeys.merge_queue(self.user_name, self.session_id)
            dirty_key = RedisKeys.dirty_entities(self.user_name, self.session_id)

            # Fetch both sets to find intersection
            merge_ids = await self.redis_client.smembers(merge_key)
            dirty_ids = await self.redis_client.smembers(dirty_key)

            if merge_ids and dirty_ids:
                intersection = merge_ids.intersection(dirty_ids)
                if intersection:
                    target_ids = [int(eid) for eid in intersection]
                    logger.info(
                        f"Targeted profile flush for {len(target_ids)} entities involved in pending merges"
                    )
                    await self.profile_job.execute(ctx, target_ids=target_ids)

            await self.merge_job.execute(ctx)

        await emit(self.session_id, "job", "session_jobs_complete", {})

    async def add(self, msg: Message) -> Message:
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

        await self.add_to_redis(msg)

        await self.redis_client.incr(
            RedisKeys.heartbeat_counter(self.user_name, self.session_id)
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

        await self.scheduler.record_activity()
        self.consumer.signal()
        await self.refresh_session_ttls()
        return msg

    async def add_to_conversation_log(
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

    async def add_to_redis(self, msg: Message):
        """Maps a message to a turn and stores its content via the Smart Client."""
        # 1. First log the conversation turn (User role)
        turn_id = await self.add_to_conversation_log(
            role="user",
            content=msg.content.strip(),
            timestamp=msg.timestamp,
            user_msg_id=msg.id,
        )

        # 2. Map the message ID to this turn and store content
        await AsyncRedisClient.update_message_mapping(
            user_name=self.user_name,
            session_id=self.session_id,
            msg_id=msg.id,
            turn_id=turn_id,
            content=msg.content.strip(),
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

        turn_id = await self.add_to_conversation_log(
            role="assistant",
            content=content,
            timestamp=timestamp,
            metadata=metadata,
            user_msg_id=user_msg_id,
        )

        task = asyncio.create_task(
            self._persist_assistant_embedding(turn_id, content, timestamp)
        )
        self._background_tasks.add(task)
        task.add_done_callback(
            lambda t: (
                self._background_tasks.discard(t),
                handle_background_task_result(t),
            )
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

        config = get_config()
        try:
            result: EntityProfilesResult = await self.llm.call_llm(
                system=system_prompt,
                user=user_prompt,
                response_model=EntityProfilesResult,
                temperature=0.0,
            )

            if not result or not result.profiles:
                return False

            # ── Pass 1: Batch-encode all subject names for resolution ──
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

            # ── Pass 2: Resolve subjects to entities, collect fact text ──
            fact_work: List[Tuple[int, str]] = []  # (target_id, fact_content)

            for i, profile in enumerate(valid_profiles):
                subject = subject_names[i]
                subject_emb = subject_embeddings[i]

                candidates = await self.entities.get_candidate_ids(
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

            # ── Pass 3: Batch-encode all fact contents ──
            fact_contents = [content for _, content in fact_work]
            fact_embeddings = await self.embedding_service.encode(fact_contents)

            # ── Pass 4: Build Fact objects and write ──
            facts_by_entity: Dict[int, List[FactRecord]] = {}

            for i, (target_id, fact_content) in enumerate(fact_work):
                new_fact = FactRecord(
                    id=f"fact_{uuid.uuid4().hex[:16]}",
                    source_entity_id=target_id,
                    content=fact_content,
                    valid_at=datetime.now(timezone.utc),
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
                    c = await self.memgraph.create_facts_batch(eid, facts_to_write)
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

    async def _persist_assistant_embedding(
        self, turn_id: int, content: str, timestamp: datetime
    ):
        """Background task: compute embedding and write to graph with retry."""
        max_retries = 3

        for attempt in range(max_retries):
            try:
                embedding_list = await self.entities.compute_batch_embeddings([content])
                embedding_vector = embedding_list[0]

                graph_id = turn_id + 1_000_000_000

                agent_msg_batch = [
                    {
                        "id": graph_id,
                        "content": content,
                        "role": "assistant",
                        "timestamp": timestamp.timestamp() * 1000,
                        "embedding": embedding_vector,
                    }
                ]

                await self.memgraph.save_message_logs(agent_msg_batch)
                return

            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Assistant embedding failed (attempt {attempt + 1}/{max_retries}) for turn {turn_id}: {e}"
                    )
                    await asyncio.sleep(0.5 * (attempt + 1))
                else:
                    logger.error(
                        f"Failed to persist assistant embedding for turn {turn_id} after {max_retries} attempts: {e}"
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
            ts = datetime.fromisoformat(turn["timestamp"])
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
        extraction_result,
        alias_updates=None,
    ):
        """Delegate to shared graph write logic."""
        batch = BatchResult(
            entity_ids=entity_ids,
            new_entity_ids=new_entity_ids,
            alias_updated_ids=alias_updated_ids,
            extraction_result=extraction_result,
            alias_updates=alias_updates or {},
        )
        await write_batch_to_graph(
            batch,
            memgraph=self.memgraph,
            entities=self.entities,
            session_id=self.session_id,
            user_name=self.user_name,
            redis_client=self.redis_client,
        )

    async def _write_to_graph_callback(
        self, result: BatchResult
    ) -> tuple[bool, str | None]:
        return await write_batch_callback(
            result,
            memgraph=self.memgraph,
            entities=self.entities,
            session_id=self.session_id,
            user_name=self.user_name,
            redis_client=self.redis_client,
        )

    async def update_runtime_settings(self, new_config_dict: dict):
        """
        Hot-reload runtime settings from the new configuration dictionary.
        Supports partial updates (patches) by merging with the current config
        to ensure active state isn't reset to defaults.
        """
        # 1. Start with CURRENT state to avoid resetting defaults on partial patches
        current_data = self.current_config.model_dump()
        updated_data = deep_merge(current_data, new_config_dict)

        try:
            new_config = RootConfig(**updated_data)
        except Exception as e:
            logger.error(f"Failed to validate new config for hot-reload: {e}")
            return

        logger.info("Applying hot-reload of runtime settings...")
        dev_settings = new_config.developer_settings
        old_dev = self.current_config.developer_settings

        # 2. Dispatch updates using safe_update to prevent crashes from signature drift
        if new_config.default_topics != self.current_config.default_topics:
            await self.update_topics_config(updated_data.get("default_topics", {}))

        if dev_settings.ingestion != old_dev.ingestion and self.consumer:
            safe_update(self.consumer.update_settings, dev_settings.ingestion)

        if dev_settings.jobs != old_dev.jobs:
            self._update_job_settings(dev_settings.jobs, old_dev.jobs)

        if dev_settings.entity_resolution != old_dev.entity_resolution and self.entities:
            safe_update(self.entities.update_settings, dev_settings.entity_resolution)

        if dev_settings.nlp_pipeline != old_dev.nlp_pipeline and self.processor:
            safe_update(self.processor.update_settings, dev_settings.nlp_pipeline)

        self.current_config = new_config

        await emit(
            self.session_id,
            "system",
            "config_updated",
            {"keys": list(new_config_dict.keys())},
        )
        logger.info("Runtime settings update complete.")

    def _update_job_settings(self, new_jobs: JobSettings, old_jobs: JobSettings):
        """Update job-specific settings if they changed."""
        from common.schema.settings import JobSettings

        if new_jobs.profile != old_jobs.profile and self.profile_job:
            safe_update(self.profile_job.update_settings, new_jobs.profile)

        if new_jobs.merger != old_jobs.merger and self.merge_job:
            safe_update(self.merge_job.update_settings, new_jobs.merger)

        if self.scheduler:
            if new_jobs.cleaner != old_jobs.cleaner:
                cleaner = self.scheduler._jobs.get("entity_cleanup")
                if cleaner:
                    cleaner.enabled = new_jobs.cleaner.enabled
                    safe_update(cleaner.update_settings, new_jobs.cleaner)

            if new_jobs.dlq != old_jobs.dlq:
                dlq = self.scheduler._jobs.get("dlq_auto_replay")
                if dlq:
                    safe_update(dlq.update_settings, new_jobs.dlq)

            if new_jobs.archival != old_jobs.archival:
                archiver = self.scheduler._jobs.get("fact_archival")
                if archiver:
                    archiver.enabled = new_jobs.archival.enabled
                    safe_update(archiver.update_settings, new_jobs.archival)

            if new_jobs.topic_config != old_jobs.topic_config:
                tconfig = self.scheduler._jobs.get("topic_config")
                if tconfig:
                    tconfig.enabled = new_jobs.topic_config.enabled

    async def refresh_session_ttls(self):
        """Refresh TTLs on all session-scoped Redis keys via the Smart Client."""
        await AsyncRedisClient.refresh_session_ttls(
            self.user_name, self.session_id, SESSION_KEY_TTL
        )

    async def shutdown(self):
        if self.consumer:
            await self.consumer.stop()
        if self.scheduler:
            await self.scheduler.stop()
        if self.resources:
            self.resources.active_entities = None

        if self._background_tasks:
            logger.info(f"Awaiting {len(self._background_tasks)} background tasks...")
            done, pending = await asyncio.wait(self._background_tasks, timeout=10.0)
            if pending:
                logger.warning(
                    f"Cancelling {len(pending)} background tasks that did not complete in time"
                )
                for task in pending:
                    task.cancel()
        await emit(self.session_id, "system", "session_shutdown", {})
        await DebugEventEmitter.get().cleanup_scope(self.session_id)
