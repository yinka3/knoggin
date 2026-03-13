import asyncio
import re
import uuid

from datetime import datetime, timezone
import os
import redis.asyncio as aioredis
from concurrent.futures import ThreadPoolExecutor
from jobs.topics import TopicConfigJob
from loguru import logger
import json
from jobs.archive import FactArchivalJob
from jobs.base import BaseJob, JobContext
from jobs.dlq import DLQReplayJob
from jobs.profile import ProfileRefinementJob
from jobs.scheduler import Scheduler
from jobs.merger import MergeDetectionJob
from jobs.cleaner import EntityCleanupJob
from core.utils import handle_background_task_result, extract_xml_content

from core.batch_consumer import BatchConsumer
from common.rag.embedding import EmbeddingService
from core.batch_processor import BatchProcessor
from common.services.write_graph_db import write_batch_callback, write_batch_to_graph
from common.services.llm_service import LLMService
from typing import Dict, List, Optional
from functools import partial
from common.config.topics_config import TopicConfig
from core.nlp import NLPPipeline
from core.entity_resolver import EntityResolver
from db.store import MemGraphStore
from common.rag.file_rag import FileRAGService
from common.schema.dtypes import MessageData, Fact, BatchResult

from common.utils.events import DebugEventEmitter, emit
from common.infra.redis import RedisKeys
from common.infra.resources import ResourceManager

class Context:

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
        
        from common.config.base import load_config, get_default_config
        
        full_config = load_config() or get_default_config()
        dev_settings = full_config.get("developer_settings", {})
        
        if topics_config is None:
            topics_config = full_config.get("default_topics")
        
        instance = cls(user_name, list(topics_config.keys()), resources.redis)
    
        instance.session_id = session_id or str(uuid.uuid4())
        instance.store = resources.store
        instance.executor = resources.executor
        instance.llm = resources.llm_service
        instance.model = model
        instance.redis_client = resources.redis
        instance.embedding_service = resources.embedding
        instance.mcp_manager = resources.mcp_manager
        
        await resources.redis.hset(
            RedisKeys.session_config(user_name),
            instance.session_id,
            json.dumps(topics_config)
        )
        instance.topic_config = await TopicConfig.load(resources.redis, user_name, instance.session_id)
        await instance.topic_config.save(resources.redis, user_name, instance.session_id)
        
        loop = asyncio.get_running_loop()
        max_id = await loop.run_in_executor(None, instance.store.get_max_entity_id)
        current_redis = await resources.redis.get(RedisKeys.global_next_ent_id())
        if not current_redis or int(current_redis) < max_id:
            await resources.redis.set(RedisKeys.global_next_ent_id(), max_id)
        
        er_cfg = dev_settings.get("entity_resolution", {})
        
        instance.ent_resolver = EntityResolver(
            session_id=instance.session_id,
            store=instance.store,
            embedding_service=instance.embedding_service,
            hierarchy_config=instance.topic_config.hierarchy,
            fuzzy_substring_threshold=er_cfg.get("fuzzy_substring_threshold", 75),
            fuzzy_non_substring_threshold=er_cfg.get("fuzzy_non_substring_threshold", 91),
            generic_token_freq=er_cfg.get("generic_token_freq", 10),
            candidate_fuzzy_threshold=er_cfg.get("candidate_fuzzy_threshold", 85),
            candidate_vector_threshold=er_cfg.get("candidate_vector_threshold", 0.85)
        )
        resources.active_resolver = instance.ent_resolver
        
        await instance._verify_user_entity(user_name)
        nlp_cfg = dev_settings.get("nlp_pipeline", {})
        instance.nlp_pipe = await loop.run_in_executor(
            instance.executor,
            partial(
                NLPPipeline,
                llm=instance.llm,
                topic_config=instance.topic_config,
                get_known_aliases=instance.ent_resolver.get_known_aliases,
                get_profiles=instance.ent_resolver.get_profiles,
                gliner=resources.gliner,
                spacy=resources.spacy,
                gliner_threshold=nlp_cfg.get("gliner_threshold", 0.85),
                vp01_min_confidence=nlp_cfg.get("vp01_min_confidence", 0.8)
            )
        )
        
        instance.batch_processor = BatchProcessor(
            session_id=instance.session_id,
            redis_client=resources.redis,
            llm=instance.llm,
            ent_resolver=instance.ent_resolver,
            nlp_pipe=instance.nlp_pipe,
            store=instance.store,
            cpu_executor=instance.executor,
            user_name=user_name,
            topic_config=instance.topic_config,
            get_next_ent_id=instance.get_next_ent_id,
            resolution_threshold=er_cfg.get("resolution_threshold", 0.85)
        )
        
        ingest_cfg = dev_settings.get("ingestion", {})
        batch_size = ingest_cfg.get("batch_size", 8)
        
        checkpoint_interval = ingest_cfg.get("checkpoint_interval") or (batch_size * 4)
        session_window = ingest_cfg.get("session_window") or (batch_size * 3)
        batch_timeout = ingest_cfg.get("batch_timeout", 300.0)
        


        
        instance.consumer = BatchConsumer(
            user_name=user_name,
            session_id=instance.session_id,
            store=instance.store,
            redis=resources.redis,
            processor=instance.batch_processor,
            get_session_context=instance.get_conversation_context,
            run_session_jobs=instance._run_session_jobs,
            write_to_graph=instance._write_to_graph_callback,
            batch_size=batch_size,
            batch_timeout=batch_timeout,
            checkpoint_interval=checkpoint_interval,
            session_window=session_window
        )
        instance.consumer.start()

        upload_dir = os.path.join(os.getenv("CONFIG_DIR", "./config"), "uploads")
        instance.file_rag = FileRAGService(
            session_id=instance.session_id,
            chroma_client=resources.chroma,
            embedding_service=resources.embedding,
            upload_dir=upload_dir,
        )
        
        jobs_cfg = dev_settings.get("jobs", {})
        nlp_cfg = dev_settings.get("nlp_pipeline", {})
        
        prof_cfg = jobs_cfg.get("profile", {})
        instance.profile_job = ProfileRefinementJob(
            llm=instance.llm,
            resolver=instance.ent_resolver,
            store=instance.store,
            executor=instance.executor,
            embedding_service=instance.embedding_service,
            msg_window=prof_cfg.get("msg_window", 30),
            volume_threshold=prof_cfg.get("volume_threshold", 15),
            idle_threshold=prof_cfg.get("idle_threshold", 90),
            profile_batch_size=prof_cfg.get("profile_batch_size", 8),
            contradiction_sim_low=prof_cfg.get("contradiction_sim_low", 0.70),
            contradiction_sim_high=prof_cfg.get("contradiction_sim_high", 0.95),
            contradiction_batch_size=prof_cfg.get("contradiction_batch_size", 4),
            profile_prompt=nlp_cfg.get("profile_prompt"),
            contradiction_prompt=nlp_cfg.get("contradiction_prompt")
        )
        
        merge_cfg = jobs_cfg.get("merger", {})
        instance.merge_job = MergeDetectionJob(
            user_name=user_name,
            ent_resolver=instance.ent_resolver, 
            store=instance.store,
            llm_client=instance.llm,
            topic_config=instance.topic_config,
            executor=instance.executor,
            auto_threshold=merge_cfg.get("auto_threshold", 0.93),
            hitl_threshold=merge_cfg.get("hitl_threshold", 0.65),
            cosine_threshold=merge_cfg.get("cosine_threshold", 0.65),
            merge_prompt=nlp_cfg.get("merge_prompt")
        )
        
        instance.scheduler = Scheduler(user_name, instance.session_id, resources.redis)
        
        dlq_cfg = jobs_cfg.get("dlq", {})
        instance.scheduler.register(DLQReplayJob(
            ent_resolver=instance.ent_resolver,
            processor=instance.batch_processor,
            write_to_graph=instance._write_to_graph_callback,
            interval=dlq_cfg.get("interval_seconds", 60),
            batch_size=dlq_cfg.get("batch_size", 50),
            max_attempts=dlq_cfg.get("max_attempts", 2)
        ))
        
        clean_cfg = jobs_cfg.get("cleaner", {})
        instance.scheduler.register(EntityCleanupJob(
            user_name=user_name, 
            store=instance.store, 
            ent_resolver=instance.ent_resolver,
            interval_hours=clean_cfg.get("interval_hours", 24),
            orphan_age_hours=clean_cfg.get("orphan_age_hours", 24),
            stale_junk_days=clean_cfg.get("stale_junk_days", 30)
        ))

        arch_cfg = jobs_cfg.get("archival", {})
        instance.scheduler.register(FactArchivalJob(
            user_name=user_name, 
            store=instance.store,
            retention_days=arch_cfg.get("retention_days", 14),
            fallback_interval_hours=arch_cfg.get("fallback_interval_hours", 24)
        ))

        topic_cfg = jobs_cfg.get("topic_config", {})
        instance.scheduler.register(TopicConfigJob(
            llm=instance.llm,
            topic_config=instance.topic_config,
            update_callback=instance.update_topics_config,
            interval_msgs=topic_cfg.get("interval_msgs", 40),
            conversation_window=topic_cfg.get("conversation_window", 50)
        ))

        limits_cfg = dev_settings.get("limits", {})
        instance._max_conversation_history = limits_cfg.get("max_conversation_history", 10000)

        await instance.scheduler.start()
        await emit(instance.session_id, "system", "session_created", {
            "user_name": user_name,
            "topics": list(topics_config.keys()),
            "model": model
        })
        
        logger.info(f"Session started: {instance.session_id}")
        return instance
        

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
        loop = asyncio.get_running_loop()
        
        user_id = self.ent_resolver.get_id(user_name)
        if user_id is None:
            logger.critical(f"User entity not found for '{user_name}' in resolver. Onboarding may not have completed.")
            return
            
        entity = await loop.run_in_executor(
            self.executor, self.store.get_entity_by_id, user_id
        )
        
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
            redis=self.redis_client,
            idle_seconds=0,
            session_id=self.session_id
        )

        if await self.profile_job.should_run(ctx):
            await self.profile_job.execute(ctx)
        
        merge_key = RedisKeys.merge_queue(self.user_name, self.session_id)
        merge_count = await self.redis_client.scard(merge_key)
        profile_flag = await self.redis_client.get(RedisKeys.profile_complete(self.user_name, self.session_id))
        logger.info(f"Merge check: queue_size={merge_count}, profile_complete={profile_flag}")

        if getattr(self.merge_job, "enabled", True) and await self.merge_job.should_run(ctx):
            await self.merge_job.execute(ctx)
        
        await emit(self.session_id, "job", "session_jobs_complete", {})
            
    
    async def add(self, msg: MessageData) -> MessageData:
        msg.id = await self.get_next_msg_id()
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
            num_to_remove = count - limit
            old_turns = await self.redis_client.zrange(sorted_key, 0, num_to_remove - 1)
            
            if old_turns:
                old_turn_data = await self.redis_client.hmget(conv_key, *old_turns)
                msgs_to_delete = []
                for data_str in old_turn_data:
                    if data_str:
                        data = json.loads(data_str)
                        if "user_msg_id" in data:
                            msgs_to_delete.append(f"msg_{data['user_msg_id']}")
                
                pipe = self.redis_client.pipeline()
                pipe.zremrangebyrank(sorted_key, 0, num_to_remove - 1)
                pipe.hdel(conv_key, *old_turns)
                
                if msgs_to_delete:
                    pipe.hdel(RedisKeys.msg_to_turn_lookup(self.user_name, self.session_id), *msgs_to_delete)
                    pipe.hdel(RedisKeys.message_content(self.user_name, self.session_id), *msgs_to_delete)
                    
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



    async def _maybe_extract_assistant(self, content: str, user_msg_id: int) -> bool:
        """
        Classify assistant response and extract facts if worthy.
        If subject entity exists -> link to entity.
        If subject entity missing -> link to User (id=1) as fallback.
        
        Returns: True if facts were found, False otherwise.
        """
        if not content or len(content.strip()) < 50:
            return False

        # 1. Expand classification prompt to also extract
        prompt = (
            f"Review this assistant response in a conversation about various topics:\n\n"
            f"---\n{content}\n---\n\n"
            f"Does this response contain specific facts, definitions, or clear statements worth remembering long-term?\n"
            f"If YES, extract them as concise claims in a <facts> XML block, where each fact has a 'subject' attribute "
            f"(the specific entity name the fact is about).\n"
            f"Format:\n<facts>\n"
            f'  <fact subject="Entity Name">Content of the fact...</fact>\n'
            f"</facts>\n\n"
            f"If NO (it's just chit-chat, advice, or general commentary), respond with only NO."
        )

        try:
            # 2. Call LLM (single pass)
            response = await self.llm.call_llm(
                user=prompt,
                system="You are a knowledge extractor. Be precise and concise.",
                temperature=0.0
            )
            
            if not response or response.strip().upper().startswith("NO"):
                return False
            
            # 3. Parse facts
            xml_content = extract_xml_content(response, "facts")
            if not xml_content:
                return False

            # Regex to capture subject="..." and content
            # Matches: <fact subject="Subject">Content</fact>
            fact_pattern = re.compile(r'<fact\s+subject="([^"]+)">([^<]+)</fact>', re.IGNORECASE | re.DOTALL)
            matches = fact_pattern.findall(xml_content)
            
            if not matches:
                return False



            facts_by_entity: Dict[int, List[Fact]] = {}
            loop = asyncio.get_running_loop()
            
            # 4. Process each fact
            for subject, fact_content in matches:
                subject = subject.strip()
                fact_content = fact_content.strip()
                if not subject or not fact_content:
                    continue
                
                # Compute subject embedding (async wrap)
                emb_list = await self.embedding_service.encode([subject])
                subject_emb = emb_list[0]
                
                # Check candidates
                candidates = await self.ent_resolver.get_candidate_ids(subject, precomputed_embedding=subject_emb)
                
                # Default to User if no candidate found
                user_id = self.ent_resolver.get_id(self.user_name)
                if candidates:
                    target_id = candidates[0] # Best match
                    clean_content = fact_content
                elif user_id is not None:
                    target_id = user_id
                    clean_content = f"[{subject}] {fact_content}" # Add context since attached to user
                else:
                    logger.warning(f"Skipping fact extraction: Subject '{subject}' unresolved and user entity not found.")
                    continue
                
                # Create Fact object
                fact_id = f"fact_{uuid.uuid4().hex[:16]}"
                
                # Compute content embedding (async wrap)
                content_emb_list = await self.embedding_service.encode([clean_content])
                content_emb = content_emb_list[0]

                new_fact = Fact(
                    id=fact_id,
                    source_entity_id=target_id,
                    content=clean_content,
                    valid_at=datetime.now(timezone.utc),
                    source_msg_id=user_msg_id, 
                    confidence=0.9,
                    embedding=content_emb,
                    source="llm"
                )
                
                if target_id not in facts_by_entity:
                    facts_by_entity[target_id] = []
                facts_by_entity[target_id].append(new_fact)

            # 5. Write to graph
            if facts_by_entity:
                count = 0
                for eid, facts in facts_by_entity.items():
                    try:
                        c = self.store.create_facts_batch(eid, facts)
                        count += c
                    except Exception as e:
                        logger.error(f"Failed to persist assistant facts for entity {eid}: {e}")
                
                if count > 0:
                    logger.info(f"Extracted {count} facts from assistant response (source='llm')")

            return True

        except Exception as e:
            logger.warning(f"Error in assistant fact extraction: {e}")
            return False

    async def _persist_assistant_embedding(self, turn_id: int, content: str, timestamp: datetime):
        """Background task: compute embedding and write to graph with retry."""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                loop = asyncio.get_running_loop()
                
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
                
                await loop.run_in_executor(
                    self.executor,
                    partial(self.store.save_message_logs, agent_msg_batch)
                )
                return
                
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Assistant embedding failed (attempt {attempt + 1}/{max_retries}) for turn {turn_id}: {e}")
                    await asyncio.sleep(0.5 * (attempt + 1))
                else:
                    logger.error(f"Failed to persist assistant embedding for turn {turn_id} after {max_retries} attempts: {e}")

    
    async def get_conversation_context(self, num_turns: int, up_to_msg_id: int = None) -> List[Dict]:
        """Returns list of conversation turns in chronological order."""
        sorted_key = RedisKeys.recent_conversation(self.user_name, self.session_id)
        conv_key = RedisKeys.conversation(self.user_name, self.session_id)
        
        if up_to_msg_id:
            turn_key = await self.redis_client.hget(
                RedisKeys.msg_to_turn_lookup(self.user_name, self.session_id), 
                f"msg_{up_to_msg_id}"
            )
            if turn_key:
                turn_score = await self.redis_client.zscore(sorted_key, turn_key)
                turn_ids = await self.redis_client.zrange(
                    sorted_key,
                    f"({turn_score}",
                    "-inf",
                    desc=True,
                    byscore=True,
                    offset=0,
                    num=num_turns
                )
                turn_ids = list(reversed(turn_ids))
            else:
                turn_ids = await self.redis_client.zrange(sorted_key, 0, num_turns - 1, desc=True)
                turn_ids = list(turn_ids)
                turn_ids.reverse()
        else:
            turn_ids = await self.redis_client.zrange(sorted_key, 0, num_turns - 1, desc=True)
            turn_ids = list(turn_ids)
            turn_ids.reverse()
        
        if not turn_ids:
            return []
        
        logger.debug(f"Fetching conversation context for turns: {turn_ids}")
        
        turn_data = await self.redis_client.hmget(conv_key, *turn_ids)
        
        results = []
        
        for turn_id, data in zip(turn_ids, turn_data):
            if data:
                parsed = json.loads(data)
                role_label = "USER" if parsed["role"] == "user" else "AGENT"
                ts = datetime.fromisoformat(parsed['timestamp'])
                date_str = ts.strftime("%Y-%m-%d %H:%M")
                results.append({
                    "turn_id": turn_id,
                    "role": parsed["role"],
                    "role_label": role_label,
                    "content": parsed["content"],
                    "timestamp": parsed["timestamp"],
                    "relative": f"[{date_str}]",
                    "user_msg_id": parsed.get("user_msg_id"),
                    "metadata": parsed.get("metadata")
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
            executor=self.executor,
            resolver=self.ent_resolver,
            session_id=self.session_id,
            user_name=self.user_name,
            redis_client=self.redis_client,
        )
    

    async def _write_to_graph_callback(self, result: BatchResult) -> tuple[bool, str | None]:
        return await write_batch_callback(
            result,
            store=self.store,
            executor=self.executor,
            resolver=self.ent_resolver,
            session_id=self.session_id,
            user_name=self.user_name,
            redis_client=self.redis_client,
        )


    async def update_runtime_settings(self, new_config: dict):
        """
        Hot-reload runtime settings from the new configuration dictionary.
        This propagates changes to the Consumer, Jobs, and LLM Service without a restart.
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
            self.merge_job.enabled = jobs_cfg["merger"].get("enabled", True)
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
    
        
    async def shutdown(self):
        await self.consumer.stop()
        await self.scheduler.stop()
        if self.resources:
            self.resources.active_resolver = None
        
        if self._background_tasks:
            logger.info(f"Awaiting {len(self._background_tasks)} background tasks...")
            await asyncio.wait(self._background_tasks, timeout=10.0)
        await emit(self.session_id, "system", "session_shutdown", {})
        await DebugEventEmitter.get().cleanup_session(self.session_id)
        