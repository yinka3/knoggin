import asyncio
import hashlib
import uuid

from datetime import datetime, timezone
from common.config.base import get_config
from core.utils import fetch_conversation_turns
import redis.asyncio as aioredis
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
import json
from jobs.base import BaseJob, JobContext
from jobs.scheduler import Scheduler
from core.utils import handle_background_task_result

from core.batch_consumer import BatchConsumer
from common.rag.embedding import EmbeddingService
from core.batch_processor import BatchProcessor
from common.services.write_graph_db import write_batch_callback, write_batch_to_graph
from common.services.llm_service import LLMService
from typing import Dict, List, Optional, Tuple
from common.config.topics_config import TopicConfig
from core.nlp import NLPPipeline
from core.entity_resolver import EntityResolver
from db.store import MemGraphStore
from common.rag.file_rag import FileRAGService
from common.schema.dtypes import MessageData, Fact, BatchResult, EntityProfilesResult
from core.prompts import get_lightweight_extraction_prompt

from common.utils.events import DebugEventEmitter, emit
from common.infra.redis import RedisKeys
from common.infra.resources import ResourceManager

SESSION_KEY_TTL = 72 * 3600

class Context:
    """
    Context represents the state of an active user session.
    Initialization and wiring logic is encapsulated in SessionAssembler.
    """

    def __init__(self, user_name: str, topics: List[str], redis_client):
        self.user_name: str = user_name
        self.active_topics: List[str] = topics
        self.resources: ResourceManager = None
        self.scheduler: Scheduler = None
        self.redis_client: aioredis.Redis = redis_client
        self.model: Optional[str] = None
        self.llm: LLMService = None
        self.file_rag: FileRAGService = None
        self.mcp_manager = None
        
        self.store: MemGraphStore = None
        self.nlp_pipe: NLPPipeline = None
        self.embedding_service: EmbeddingService = None
        self.ent_resolver: EntityResolver = None
        self.session_id: str = None
        self.topic_config: TopicConfig = None
        self._max_conversation_history: int = 10000

        self.executor: ThreadPoolExecutor = None
        self.batch_processor: BatchProcessor = None
        self.consumer: BatchConsumer = None
        self.profile_job: BaseJob = None
        self.merge_job: BaseJob = None
        self._background_tasks: set = set()


    @classmethod
    async def create(
        cls,
        user_name: str,
        resources: ResourceManager,
        topics_config: dict = None,
        session_id: str = None,
        model: str = None
    ) -> "Context":
        """Assembles and launches a new session context."""
        from core.boot import SessionAssembler
        assembler = SessionAssembler(user_name, resources)
        ctx = await assembler.bootstrap(topics_config, session_id, model)
            
        await ctx._verify_user_entity(user_name)
        
        return ctx
        

    async def get_next_msg_id(self) -> int:
        return await self.redis_client.incr(RedisKeys.global_next_msg_id())

    async def get_next_ent_id(self) -> int:
        return await self.redis_client.incr(RedisKeys.global_next_ent_id())
    
    async def get_next_turn_id(self) -> int:
        return await self.redis_client.incr(RedisKeys.global_next_turn_id(self.user_name, self.session_id))
    
    async def update_topics_config(self, new_config: dict):
        self.topic_config.update(new_config)
        await self.topic_config.save(self.redis_client, self.user_name, self.session_id)
        self.ent_resolver.hierarchy_config = self.topic_config.hierarchy
        self.nlp_pipe.refresh_topic_mappings()
        await emit(self.session_id, "system", "topics_updated", {
            "topics": list(new_config.keys())
        })
    
    async def _verify_user_entity(self, user_name: str):
        user_id = await self.ent_resolver.get_id(user_name)
        if user_id is None:
            logger.critical(f"User entity not found for '{user_name}' in resolver. Onboarding may not have completed.")
            return
            
        entity = await self.store.get_entity_by_id(user_id)
        
        if not entity or entity.get("canonical_name") != user_name:
            logger.critical(
                f"User entity lookup mismatch for '{user_name}' (id={user_id}). "
                f"Onboarding may not have completed."
            )
            return
        
        profile = self.ent_resolver.entity_profiles.get(user_id)
        if profile and profile["canonical_name"] == user_name:
            logger.info(f"User entity verified: {user_name} (id={user_id})")
            await emit(self.session_id, "system", "user_entity_verified", {
                "user_name": user_name,
                "entity_id": user_id
            })
            return
        
        logger.warning(f"User entity exists in graph but missing from resolver, backfilling")
        all_aliases = entity.get("aliases") or [user_name]
        await self.ent_resolver.register_entity(
            user_id, user_name, all_aliases, "person", "Identity"
        )
        await emit(self.session_id, "system", "user_entity_recovered", {
            "user_name": user_name
        })
    
    async def _run_session_jobs(self):
        await emit(self.session_id, "job", "session_jobs_started", {})
        ctx = JobContext(
            user_name=self.user_name,
            session_id=self.session_id,
            idle_seconds=0
        )

        # 1. Profile Refinement (Primary focus for consistent views)
        if await self.profile_job.should_run(ctx):
            await self.profile_job.execute(ctx)
        
        # 2. Merger (Uses refined profiles)
        # Targeted Flush: If we have pending merges, only force refinement for 
        # entities that are actively in the merge queue and also dirty.
        is_merge_pending = getattr(self.merge_job, "enabled", True) and await self.merge_job.should_run(ctx)
        
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
                    logger.info(f"Targeted profile flush for {len(target_ids)} entities involved in pending merges")
                    await self.profile_job.execute(ctx, target_ids=target_ids)
            
            await self.merge_job.execute(ctx)
        
        await emit(self.session_id, "job", "session_jobs_complete", {})
            
    
    async def add(self, msg: MessageData) -> MessageData:
        # Deterministic ID: same content + session + timestamp = same ID
        content_hash = hashlib.sha256(
            f"{self.session_id}:{msg.message.strip()}:{msg.timestamp.isoformat()}".encode()
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
        await self.redis_client.rpush(buffer_key, json.dumps({
            "id": msg.id,
            "message": msg.message.strip(),
            "timestamp": msg.timestamp.isoformat(),
            "role": "user"
        }))

        await self.scheduler.record_activity()
        self.consumer.signal()
        await self.refresh_session_ttls()
        return msg

    async def add_to_conversation_log(self, role: str, content: str, timestamp: datetime, user_msg_id: int = None, metadata: dict = None):
        turn_id = await self.get_next_turn_id()
        turn_key = f"turn_{turn_id}"
        
        payload = {
            "role": role,
            "content": content,
            "timestamp": timestamp.isoformat()
        }
        if user_msg_id is not None:
            payload["user_msg_id"] = user_msg_id
        if metadata:
            payload["metadata"] = metadata
        
        conv_key = RedisKeys.conversation(self.user_name, self.session_id)
        sorted_key = RedisKeys.recent_conversation(self.user_name, self.session_id)
        
        await self.redis_client.hset(conv_key, turn_key, json.dumps(payload))
        await self.redis_client.zadd(sorted_key, {turn_key: timestamp.timestamp()})
        
        limit = self._max_conversation_history
        
        count = await self.redis_client.zcard(sorted_key)
        if count > limit:
            old_turns = await self.redis_client.zrange(sorted_key, 0, -(limit + 1))
            if old_turns:
                old_turn_data = await self.redis_client.hmget(conv_key, *old_turns)
                msg_keys = []
                for data_str in old_turn_data:
                    if data_str:
                        turn_payload = json.loads(data_str)
                        if "user_msg_id" in turn_payload:
                            msg_keys.append(f"msg_{turn_payload['user_msg_id']}")

                pipe = self.redis_client.pipeline()
                pipe.zremrangebyrank(sorted_key, 0, -(limit + 1))
                pipe.hdel(conv_key, *old_turns)
                if msg_keys:
                    pipe.hdel(RedisKeys.message_content(self.user_name, self.session_id), *msg_keys)
                    pipe.hdel(RedisKeys.msg_to_turn_lookup(self.user_name, self.session_id), *msg_keys)
                await pipe.execute()
                
        return turn_id
    
    async def add_to_redis(self, msg: MessageData):
        msg_key = f"msg_{msg.id}"
        
        await self.redis_client.hset(
            RedisKeys.message_content(self.user_name, self.session_id), 
            msg_key, json.dumps({
            'message': msg.message.strip(),
            'timestamp': msg.timestamp.isoformat()
        }))

        turn_id = await self.add_to_conversation_log(
            role="user",
            content=msg.message.strip(),
            timestamp=msg.timestamp,
            user_msg_id=msg.id
        )

        await self.redis_client.hset(
            RedisKeys.msg_to_turn_lookup(self.user_name, self.session_id), 
            msg_key, 
            f"turn_{turn_id}"
        )
    
    async def add_assistant_turn(self, content: str, timestamp: datetime, metadata: dict = None, user_msg_id: int = None):
        """Add assistant turn to conversation log."""
        if metadata is None:
            metadata = {}

        turn_id = await self.add_to_conversation_log(
            role="assistant",
            content=content,
            timestamp=timestamp,
            metadata=metadata,
            user_msg_id=user_msg_id
        )
        
        task = asyncio.create_task(
            self._persist_assistant_embedding(turn_id, content, timestamp)
        )
        self._background_tasks.add(task)
        task.add_done_callback(
            lambda t: (self._background_tasks.discard(t), handle_background_task_result(t))
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
                temperature=0.0
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
                
                candidates = await self.ent_resolver.get_candidate_ids(
                    subject, precomputed_embedding=subject_emb
                )
                
                target_id = None
                config = get_config()
                threshold = config.developer_settings.entity_resolution.resolution_threshold

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
            facts_by_entity: Dict[int, List[Fact]] = {}
            
            for i, (target_id, fact_content) in enumerate(fact_work):
                new_fact = Fact(
                    id=f"fact_{uuid.uuid4().hex[:16]}",
                    source_entity_id=target_id,
                    content=fact_content,
                    valid_at=datetime.now(timezone.utc),
                    source_msg_id=user_msg_id,
                    confidence=0.9,
                    embedding=fact_embeddings[i],
                    source="llm"
                )
                if target_id not in facts_by_entity:
                    facts_by_entity[target_id] = []
                facts_by_entity[target_id].append(new_fact)

            total_count = 0
            for eid, facts_to_write in facts_by_entity.items():
                try:
                    c = await self.store.create_facts_batch(eid, facts_to_write)
                    total_count += int(c)
                except Exception as e:
                    logger.error(f"Failed to persist assistant facts for entity {eid}: {e}")
            
            if total_count > 0:
                logger.info(
                    f"Extracted {total_count} facts from assistant response (source='llm')"
                )

            return total_count > 0

        except Exception as e:
            logger.warning(f"Error in assistant fact extraction: {e}")
            return False

    async def _persist_assistant_embedding(self, turn_id: int, content: str, timestamp: datetime):
        """Background task: compute embedding and write to graph with retry."""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                
                embedding_list = await self.ent_resolver.compute_batch_embeddings([content])
                embedding_vector = embedding_list[0]

                graph_id = turn_id + 1_000_000_000
                
                agent_msg_batch = [{
                    "id": graph_id,
                    "content": content,
                    "role": "assistant",
                    "timestamp": timestamp.timestamp() * 1000,
                    "embedding": embedding_vector
                }]
                
                await self.store.save_message_logs(agent_msg_batch)
                return
                
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Assistant embedding failed (attempt {attempt + 1}/{max_retries}) for turn {turn_id}: {e}")
                    await asyncio.sleep(0.5 * (attempt + 1))
                else:
                    logger.error(f"Failed to persist assistant embedding for turn {turn_id} after {max_retries} attempts: {e}")

    
    async def get_conversation_context(self, num_turns: int, up_to_msg_id: int = None) -> List[Dict]:
        """Returns list of conversation turns in chronological order."""
        turns = await fetch_conversation_turns(
            self.redis_client, self.user_name, self.session_id,
            num_turns, up_to_msg_id
        )

        results = []
        for turn in turns:
            role_label = "USER" if turn["role"] == "user" else "AGENT"
            ts = datetime.fromisoformat(turn["timestamp"])
            date_str = ts.strftime("%Y-%m-%d %H:%M")
            results.append({
                **turn,
                "message": turn["content"],
                "role_label": role_label,
                "relative": f"[{date_str}]",
            })

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
            store=self.store,
            resolver=self.ent_resolver,
            session_id=self.session_id,
            user_name=self.user_name,
            redis_client=self.redis_client,
        )
    

    async def _write_to_graph_callback(self, result: BatchResult) -> tuple[bool, str | None]:
        return await write_batch_callback(
            result,
            store=self.store,
            resolver=self.ent_resolver,
            session_id=self.session_id,
            user_name=self.user_name,
            redis_client=self.redis_client,
        )


    async def update_runtime_settings(self, new_config: dict):
        """
        Hot-reload runtime settings from the new configuration dictionary.
        This propagates configuration changes to the active session components.
        """
        logger.info("Applying hot-reload of runtime settings...")
        
        dev_settings = new_config.get("developer_settings", {})

        if "default_topics" in new_config:
            await self.update_topics_config(new_config["default_topics"])
            logger.info(f"Topics updated: {list(new_config['default_topics'].keys())}")

        ingest_cfg = dev_settings.get("ingestion", {})
        if ingest_cfg and self.consumer:
            b_size = ingest_cfg.get("batch_size")
            
            if b_size:
                current_chk = ingest_cfg.get("checkpoint_interval") or (b_size * 4)
                current_win = ingest_cfg.get("session_window") or (b_size * 3)
                
                self.consumer.update_ingestion_settings(
                    batch_size=b_size,
                    batch_timeout=ingest_cfg.get("batch_timeout"),
                    checkpoint_interval=current_chk,
                    session_window=current_win
                )
            
        jobs_cfg = dev_settings.get("jobs", {})
        
        if "profile" in jobs_cfg and self.profile_job:
            self.profile_job.update_settings(
                msg_window=jobs_cfg["profile"].get("msg_window"),
                volume_threshold=jobs_cfg["profile"].get("volume_threshold"),
                idle_threshold=jobs_cfg["profile"].get("idle_threshold"),
                profile_batch_size=jobs_cfg["profile"].get("profile_batch_size"),
                contradiction_sim_low=jobs_cfg["profile"].get("contradiction_sim_low"),
                contradiction_sim_high=jobs_cfg["profile"].get("contradiction_sim_high"),
                contradiction_batch_size=jobs_cfg["profile"].get("contradiction_batch_size")
            )

        if "merger" in jobs_cfg and self.merge_job:
            self.merge_job.update_settings(
                auto_threshold=jobs_cfg["merger"].get("auto_threshold"),
                hitl_threshold=jobs_cfg["merger"].get("hitl_threshold"),
                cosine_threshold=jobs_cfg["merger"].get("cosine_threshold")
            )
        
        if self.scheduler:
            if "cleaner" in jobs_cfg:
                cleaner = self.scheduler._jobs.get("entity_cleanup")
                if cleaner:
                    cleaner.enabled = jobs_cfg["cleaner"].get("enabled", True)
                    cleaner.update_settings(
                        interval_hours=jobs_cfg["cleaner"].get("interval_hours"),
                        orphan_age_hours=jobs_cfg["cleaner"].get("orphan_age_hours"),
                        stale_junk_days=jobs_cfg["cleaner"].get("stale_junk_days")
                    )

            if "dlq" in jobs_cfg:
                dlq = self.scheduler._jobs.get("dlq_auto_replay")
                if dlq:
                    dlq.update_settings(
                        interval=jobs_cfg["dlq"].get("interval_seconds"),
                        batch_size=jobs_cfg["dlq"].get("batch_size"),
                        max_attempts=jobs_cfg["dlq"].get("max_attempts")
                    )
            
            if "archival" in jobs_cfg:
                archiver = self.scheduler._jobs.get("fact_archival")
                if archiver:
                    archiver.enabled = jobs_cfg["archival"].get("enabled", True)
                    archiver.update_settings(
                        retention_days=jobs_cfg["archival"].get("retention_days"),
                        fallback_interval_hours=jobs_cfg["archival"].get("fallback_interval_hours")
                    )
            
            if "topic_config" in jobs_cfg:
                tconfig = self.scheduler._jobs.get("topic_config")
                if tconfig:
                    tconfig.enabled = jobs_cfg["topic_config"].get("enabled", True)
                    # We might need to add `update_settings` to `topic_config` job later if it supports hot reload 
                    # For now just setting the feature flag is enough
        
        er_cfg = dev_settings.get("entity_resolution", {})
        if er_cfg and self.ent_resolver:
            self.ent_resolver.update_settings(
                fuzzy_substring_threshold=er_cfg.get("fuzzy_substring_threshold"),
                fuzzy_non_substring_threshold=er_cfg.get("fuzzy_non_substring_threshold"),
                generic_token_freq=er_cfg.get("generic_token_freq"),
                candidate_fuzzy_threshold=er_cfg.get("candidate_fuzzy_threshold"),
                candidate_vector_threshold=er_cfg.get("candidate_vector_threshold")
            )
        
        nlp_cfg = dev_settings.get("nlp_pipeline", {})
        if nlp_cfg and self.nlp_pipe:
            self.nlp_pipe.update_settings(
                gliner_threshold=nlp_cfg.get("gliner_threshold"),
                vp01_min_confidence=nlp_cfg.get("vp01_min_confidence"),
                llm_ner=nlp_cfg.get("llm_ner"),
            )
            
        await emit(self.session_id, "system", "config_updated", {
            "keys": list(new_config.keys())
        })

        logger.info("Runtime settings update complete.")
    
    async def refresh_session_ttls(self):
        """Refresh TTLs on all session-scoped Redis keys. Call on activity."""
        ttl = SESSION_KEY_TTL
        u, s = self.user_name, self.session_id

        keys = [
            RedisKeys.buffer(u, s),
            RedisKeys.checkpoint(u, s),
            RedisKeys.message_content(u, s),
            RedisKeys.dirty_entities(u, s),
            RedisKeys.profile_complete(u, s),
            RedisKeys.merge_queue(u, s),
            RedisKeys.dlq(u, s),
            RedisKeys.dlq_parked(u, s),
            RedisKeys.last_processed(u, s),
            RedisKeys.conversation(u, s),
            RedisKeys.recent_conversation(u, s),
            RedisKeys.msg_to_turn_lookup(u, s),
            RedisKeys.last_activity(u, s),
            RedisKeys.merge_proposals(u, s),
            RedisKeys.merge_intents_index(u, s),
            RedisKeys.user_profile_ran(u, s),
            RedisKeys.heartbeat_counter(u, s),
            RedisKeys.global_next_turn_id(u, s),
        ]

        pipe = self.redis_client.pipeline()
        for key in keys:
            pipe.expire(key, ttl)
        await pipe.execute()

        
    async def shutdown(self):
        if self.consumer:
            await self.consumer.stop()
        if self.scheduler:
            await self.scheduler.stop()
        if self.resources:
            self.resources.active_resolver = None
        
        if self._background_tasks:
            logger.info(f"Awaiting {len(self._background_tasks)} background tasks...")
            done, pending = await asyncio.wait(self._background_tasks, timeout=10.0)
            if pending:
                logger.warning(f"Cancelling {len(pending)} background tasks that did not complete in time")
                for task in pending:
                    task.cancel()
        await emit(self.session_id, "system", "session_shutdown", {})
        await DebugEventEmitter.get().cleanup_session(self.session_id)
        