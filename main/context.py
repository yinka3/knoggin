import asyncio
from datetime import timezone
from dotenv import load_dotenv
import redis.asyncio as redis
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
import json
from jobs.base import BaseJob, JobContext
from jobs.dlq import DLQReplayJob
from jobs.mood import MoodCheckpointJob
from jobs.profile import ProfileRefinementJob
from jobs.scheduler import Scheduler
from jobs.merger import MergeDetectionJob
from jobs.cleaner import EntityCleanupJob
from config import get_config_value
from main.processor import BatchProcessor
from main.service import LLMService
from redisclient import AsyncRedisClient
from typing import List, Set
from functools import partial
from schema.dtypes import *
from main.nlp_pipe import NLPPipeline
from main.entity_resolve import EntityResolver
from db.memgraph import MemGraphStore
from main.prompts import *
from log.llm_trace import get_trace_logger
from utils import format_relative_time


load_dotenv()

BATCH_SIZE = 10
PROFILE_INTERVAL = 30
SESSION_WINDOW = 60
BATCH_TIMEOUT_SECONDS = 30

class Context:

    def __init__(self, user_name: str, topics: List[str], redis_client):
        self.user_name: str = user_name
        self.active_topics: List[str] = topics
        self.scheduler: Scheduler = None
        self.redis_client: redis.Redis = redis_client
        self.llm: LLMService = None
        
        self.store: 'MemGraphStore' = None
        self.nlp_pipe: 'NLPPipeline' = None
        self.ent_resolver: 'EntityResolver' = None

        self.executor: ThreadPoolExecutor = None
        self._background_tasks: Set[asyncio.Task] = set()
        self._batch_timer_task: asyncio.Task = None
        self._batch_processing_lock = asyncio.Lock()
        self.batch_processor: BatchProcessor = None
        self._batch_in_progress = False
        self.profile_job: BaseJob = None
        self.merge_job: BaseJob = None
        self.cleanup_job: BaseJob = None
        self.trace_logger = get_trace_logger()

    @classmethod
    async def create(
        cls,
        user_name: str,
        store: MemGraphStore,
        cpu_executor: ThreadPoolExecutor,
        topics: List[str] = ["General"]
    ) -> "Context":
        redis_conn = AsyncRedisClient().get_client()
        
        instance = cls(user_name, topics, redis_conn)
        instance.llm = LLMService(trace_logger=get_trace_logger())
        
        instance.store = store
        instance.executor = cpu_executor
        
        loop = asyncio.get_running_loop()

        max_id = await loop.run_in_executor(None, instance.store.get_max_entity_id)
    
        current_redis = await redis_conn.get("global:next_ent_id")
        if not current_redis or int(current_redis) < max_id:
            await redis_conn.set("global:next_ent_id", max_id)
            logger.info(f"Startup Sync: Reset global:next_ent_id to {max_id} from Memgraph")
            
        instance.nlp_pipe = await loop.run_in_executor(
            instance.executor, 
            partial(NLPPipeline, llm=instance.llm)
        )
        
        instance.ent_resolver = EntityResolver(store=instance.store)

        raw_msgs = await redis_conn.hgetall(f"message_content:{user_name}")
        messages = {k: json.loads(v) for k, v in raw_msgs.items()}
        instance.ent_resolver.hydrate_messages(messages)

        raw_turns = await redis_conn.hgetall(f"conversation:{user_name}")
        stella_turns = {}
        for turn_key, data in raw_turns.items():
            parsed = json.loads(data)
            if parsed["role"] == "assistant":
                stella_turns[turn_key] = {"content": parsed["content"]}
        instance.ent_resolver.hydrate_messages(stella_turns)

        await instance._get_or_create_user_entity(user_name)

        instance.batch_processor = BatchProcessor(
            redis_client=redis_conn,
            llm=instance.llm,
            ent_resolver=instance.ent_resolver,
            nlp_pipe=instance.nlp_pipe,
            store=instance.store,
            cpu_executor=instance.executor,
            user_name=user_name,
            active_topics=topics,
            get_next_ent_id=instance.get_next_ent_id)

        instance.profile_job = ProfileRefinementJob(
            llm=instance.llm,
            resolver=instance.ent_resolver,
            store=instance.store,
            executor=instance.executor)
        
        instance.cleanup_job = EntityCleanupJob(
            user_name=user_name,
            store=instance.store,
            ent_resolver=instance.ent_resolver)

        instance.merge_job = MergeDetectionJob(
            user_name, instance.ent_resolver, instance.store, instance.llm)

        # Scheduler only gets DLQ
        instance.scheduler = Scheduler(user_name)
        instance.scheduler.register(DLQReplayJob())
        # instance.scheduler.register(MoodCheckpointJob(user_name, instance.store))
        await instance.scheduler.start()
        
        return instance

    @staticmethod
    def _log_task_exception(task):
        if task.cancelled():
            return
        if exc := task.exception():
            logger.error(f"Background task failed: {exc}")
    
    def _fire_and_forget(self, coroutine):
        task = asyncio.create_task(coroutine)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        task.add_done_callback(self._log_task_exception)
    
    async def _flush_batch_timeout(self):
        try:
            await asyncio.sleep(BATCH_TIMEOUT_SECONDS)
            buffer_len = await self.redis_client.llen(f"buffer:{self.user_name}")
            if buffer_len > 0:
                logger.info("Batch timeout reached")
                self._fire_and_forget(self.process_batch())
        except asyncio.CancelledError:
            pass
        finally:
            self._batch_timer_task = None

    async def get_next_msg_id(self) -> int:
        return await self.redis_client.incr("global:next_msg_id")

    async def get_next_ent_id(self) -> int:
        return await self.redis_client.incr("global:next_ent_id")
    
    async def get_next_turn_id(self) -> int:
        return await self.redis_client.incr(f"global:next_turn_id:{self.user_name}")

    async def add_to_conversation_log(self, role: str, content: str, timestamp: datetime, user_msg_id: int = None):
        """Add a turn to the unified conversation log."""
        turn_id = await self.get_next_turn_id()
        turn_key = f"turn_{turn_id}"
        
        payload = {
            "role": role,
            "content": content,
            "timestamp": timestamp.isoformat()
        }
        if user_msg_id is not None:
            payload["user_msg_id"] = user_msg_id
        
        conv_key = f"conversation:{self.user_name}"
        sorted_key = f"recent_conversation:{self.user_name}"
        
        pipe = self.redis_client.pipeline()
        pipe.hset(conv_key, turn_key, json.dumps(payload))
        pipe.zadd(sorted_key, {turn_key: timestamp.timestamp()})
        pipe.zremrangebyrank(sorted_key, 0, -(SESSION_WINDOW * 2 + 1)) # keep more since it's both sides
        await pipe.execute()
        
        return turn_id
    
    async def add_to_redis(self, msg: MessageData):
        msg_key = f"msg_{msg.id}"
        
        await self.redis_client.hset(f"message_content:{self.user_name}", msg_key, json.dumps({
            'message': msg.message.strip(),
            'timestamp': msg.timestamp.isoformat()
        }))

        await self.add_to_conversation_log(
            role="user",
            content=msg.message.strip(),
            timestamp=msg.timestamp,
            user_msg_id=msg.id
        )
        self.ent_resolver.add_message(msg_key, msg.message.strip())
    

    async def _get_or_create_user_entity(self, user_name: str):
        loop = asyncio.get_running_loop()

        entity_id = await loop.run_in_executor(
            self.executor,
            self.ent_resolver.get_id,
            user_name
        )

        if entity_id:
            logger.info(f"User {user_name} recognized.")
            return entity_id
        
        logger.info(f"Creating new USER entity for {user_name}")
        new_id = await self.get_next_ent_id()
        
        summary = get_config_value("user_summary") or f"The primary user named {user_name}"

        embedding = await loop.run_in_executor(
            self.executor,
            partial(self.ent_resolver.register_entity, new_id, user_name, [user_name], "person", "Personal")
        )
        
        self.ent_resolver.entity_profiles[new_id]["summary"] = summary

        user_entity = {
            "id": new_id,
            "canonical_name": user_name,
            "type": "person",
            "confidence": 1.0,
            "summary": summary,
            "topic": "Personal",
            "embedding": embedding,
            "aliases": [user_name]
        }

        await loop.run_in_executor(
            self.executor,
            partial(self.store.write_batch, [user_entity], [], False)
        )
        
        logger.info(f"User entity {user_name} (ID: {new_id}) written to graph")
        return new_id

    async def _run_session_jobs(self):
        async with self._batch_processing_lock:
            ctx = JobContext(
                user_name=self.user_name,
                redis=self.redis_client,
                idle_seconds=0
            )
            
            if await self.profile_job.should_run(ctx):
                await self.profile_job.execute(ctx)
            
            if await self.cleanup_job.should_run(ctx):
                await self.cleanup_job.execute(ctx)
            
            if await self.merge_job.should_run(ctx):
                await self.merge_job.execute(ctx)

    async def add(self, msg: MessageData):
        msg.id = await self.get_next_msg_id()
        await self.add_to_redis(msg)

        buffer_key = f"buffer:{self.user_name}"
        await self.redis_client.rpush(buffer_key, json.dumps({
            "id": msg.id,
            "message": msg.message.strip(),
            "timestamp": msg.timestamp.isoformat()
        }))

        buffer_len = await self.redis_client.llen(buffer_key)
        await self.scheduler.record_activity()
        
        if buffer_len >= BATCH_SIZE and not self._batch_in_progress:
            if self._batch_timer_task:
                self._batch_timer_task.cancel()
                self._batch_timer_task = None
            self._fire_and_forget(self.process_batch())
        elif buffer_len == 1:
            self._batch_timer_task = asyncio.create_task(self._flush_batch_timeout())
        
        checkpoint_key = f"checkpoint_count:{self.user_name}"
        count = await self.redis_client.incr(checkpoint_key)

        if count >= SESSION_WINDOW // 2:
            await self.redis_client.set(checkpoint_key, 0)
            self._fire_and_forget(self._run_session_jobs())
    
    async def get_conversation_context(self, num_turns: int) -> List[Dict]:
        """Returns list of conversation turns in chronological order."""
        sorted_key = f"recent_conversation:{self.user_name}"
        conv_key = f"conversation:{self.user_name}"
        
        turn_ids = await self.redis_client.zrevrange(sorted_key, 0, num_turns - 1)
        
        if not turn_ids:
            return []
        
        turn_ids.reverse()

        turn_data = await self.redis_client.hmget(conv_key, *turn_ids)
        
        results = []
        now = datetime.now(timezone.utc)
        
        for turn_id, data in zip(turn_ids, turn_data):
            if data:
                parsed = json.loads(data)
                role_label = "User" if parsed["role"] == "user" else "STELLA"
                ts = datetime.fromisoformat(parsed['timestamp'])
                relative = format_relative_time(now, ts)
                results.append({
                    "turn_id": turn_id,
                    "role": parsed["role"],
                    "role_label": role_label,
                    "content": parsed["content"],
                    "timestamp": parsed["timestamp"],
                    "relative": relative,
                    "user_msg_id": parsed.get("user_msg_id")
                })
        
        return results


    async def process_batch(self):
        while await self.redis_client.exists("system:maintenance_lock"):
            logger.warning("Maintenance Lock Active: Pausing Batch Processing...")
            await asyncio.sleep(2)
        
        if self._batch_in_progress:
            return

        self._batch_in_progress = True
        try:  
            async with self._batch_processing_lock:
                logger.info("Starting batch processing...")
                
                buffer_key = f"buffer:{self.user_name}"

                while True:
                    if await self.redis_client.exists("system:maintenance_lock"):
                        logger.info("Maintenance lock detected during loop. Pausing...")
                        await asyncio.sleep(2)
                        continue

                    q_len = await self.redis_client.llen(buffer_key)
                    if q_len == 0:
                        logger.debug("Buffer empty, consumer loop exiting.")
                        break

                    messages = await self.batch_processor.get_buffered_messages(buffer_key, BATCH_SIZE)
                    
                    if not messages:
                        return
                    
                    conversation = await self.get_conversation_context(SESSION_WINDOW * 2)
                    session_text = "\n".join([
                        f"[{turn['role_label']}]: {turn['content']}" 
                        for turn in conversation
                    ])
                    
                    result = await self.batch_processor.run(messages, session_text)
                    
                    if not result.success:
                        await self.batch_processor.move_to_dead_letter(messages, result.error)
                    else:
                        if result.emotions:
                            await self.redis_client.rpush(f"emotions:{self.user_name}", *result.emotions)
                        
                        if result.extraction_result:
                            await self._write_to_graph(
                                result.entity_ids,
                                result.new_entity_ids,
                                result.alias_updated_ids,
                                result.extraction_result
                            )
                    
                    await self.redis_client.ltrim(buffer_key, len(messages), -1)
                    await asyncio.sleep(0.1)
                    
                if self._batch_timer_task:
                    self._batch_timer_task.cancel()
                    self._batch_timer_task = None
                    
        finally:
            self._batch_in_progress = False

    
    async def _write_to_graph(
        self,
        entity_ids: list[int],
        new_entity_ids: set[int],
        alias_updated_ids: set[int],
        extraction_result: ConnectionExtractionResponse
    ):

   
        entity_lookup = {}
        for ent_id in entity_ids:
            profile = self.ent_resolver.entity_profiles.get(ent_id)
            if profile:
                canonical = profile["canonical_name"]
                entry = {
                    "id": ent_id,
                    "canonical_name": canonical,
                    "type": profile.get("type"),
                    "topic": profile.get("topic", "General")
                }
                entity_lookup[canonical.lower()] = entry
                for mention in self.ent_resolver.get_mentions_for_id(ent_id):
                    entity_lookup[mention.lower()] = entry

        entities = []
        for ent_id in new_entity_ids:
            profile = self.ent_resolver.entity_profiles.get(ent_id)
            if profile:
                entities.append({
                    "id": ent_id,
                    "canonical_name": profile["canonical_name"],
                    "type": profile.get("type", ""),
                    "confidence": 1.0,
                    "summary": "",
                    "topic": profile.get("topic", "General"),
                    "embedding": self.ent_resolver.get_embedding_for_id(ent_id),
                    "aliases": self.ent_resolver.get_mentions_for_id(ent_id)
                })
        
        for ent_id in alias_updated_ids:
            if ent_id in new_entity_ids:
                continue
            profile = self.ent_resolver.entity_profiles.get(ent_id)
            if profile:
                entities.append({
                    "id": ent_id,
                    "canonical_name": profile["canonical_name"],
                    "type": profile.get("type", ""),
                    "confidence": 1.0,
                    "summary": "",
                    "topic": profile.get("topic", "General"),
                    "embedding": [],
                    "aliases": self.ent_resolver.get_mentions_for_id(ent_id)
                })

        relationships = []
        for msg_result in extraction_result.message_results:
            msg_id = msg_result.message_id
            
            for pair in msg_result.entity_pairs:
                ent_a = entity_lookup.get(pair.entity_a.lower())
                ent_b = entity_lookup.get(pair.entity_b.lower())
                
                if ent_a and ent_b:
                    relationships.append({
                        "entity_a": ent_a["canonical_name"],
                        "entity_b": ent_b["canonical_name"],
                        "message_id": f"msg_{msg_id}",
                        "confidence": pair.confidence
                    })
                else:
                    logger.warning(f"Skipping pair: {pair.entity_a} - {pair.entity_b}")

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self.executor,
            partial(self.store.write_batch, entities, relationships, True)
        )
        
        if new_entity_ids:
            dirty_key = f"dirty_entities:{self.user_name}"
            await self.redis_client.sadd(dirty_key, *[str(eid) for eid in new_entity_ids])
            await self.redis_client.delete(f"profile_complete:{self.user_name}")
        
        logger.info(f"Wrote {len(entities)} entities, {len(relationships)} relationships to graph")
    

    async def _flush_batch_shutdown(self):
        logger.info("Initiating graceful shutdown...")
        
        if self._batch_timer_task:
            self._batch_timer_task.cancel()
            self._batch_timer_task = None
        
        buffer_key = f"buffer:{self.user_name}"
        if await self.redis_client.llen(buffer_key) > 0:
            await self.process_batch()
        
        logger.info("Running Last job sequence before shutdown")
        await self._run_session_jobs()

        await asyncio.sleep(SESSION_WINDOW // 2)
        if self._background_tasks:
            logger.info(f"Waiting for {len(self._background_tasks)} background tasks...")
            await asyncio.wait(self._background_tasks, timeout=90)
        
        logger.info("Shutdown complete")

        
    async def shutdown(self):

        await self._flush_batch_shutdown()

        if self._background_tasks:
            logger.info(f"Waiting for {len(self._background_tasks)} background tasks...")
            await asyncio.wait(self._background_tasks, timeout=60)
        
        await self.scheduler.stop()

        if self.executor:
            self.executor.shutdown(wait=True)
        if self.redis_client:
            await self.redis_client.aclose()