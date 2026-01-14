import asyncio
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
from main.consumer import BatchConsumer
from main.processor import BatchProcessor, BatchResult
from main.service import LLMService
from main.redisclient import AsyncRedisClient
from typing import List
from functools import partial
from schema.dtypes import *
from main.nlp_pipe import NLPPipeline
from main.entity_resolve import EntityResolver
from db.memgraph import MemGraphStore
from main.prompts import *
from log.llm_trace import get_trace_logger


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
        self.batch_processor: BatchProcessor = None
        self.consumer: BatchConsumer = None
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
        topics_config: dict = None
    ) -> "Context":
        
        if topics_config is None:
            topics_config = {"General": {"labels": [], "hierarchy": {}}}

        redis_conn = AsyncRedisClient().get_client()
        
        instance = cls(user_name, topics_config, redis_conn)
        instance.llm = LLMService(trace_logger=get_trace_logger())
        
        instance.store = store
        instance.executor = cpu_executor
        
        loop = asyncio.get_running_loop()

        max_id = await loop.run_in_executor(None, instance.store.get_max_entity_id)
    
        current_redis = await redis_conn.get("global:next_ent_id")
        if not current_redis or int(current_redis) < max_id:
            await redis_conn.set("global:next_ent_id", max_id)
            logger.info(f"Startup Sync: Reset global:next_ent_id to {max_id} from Memgraph")
        
        hierarchy_config = {
            topic: config.get("hierarchy", {})
            for topic, config in topics_config.items()
        }
            
        instance.nlp_pipe = await loop.run_in_executor(
            instance.executor, 
            partial(NLPPipeline, llm=instance.llm, topics_config=topics_config)
        )
        
        instance.ent_resolver = EntityResolver(store=instance.store, hierarchy_config=hierarchy_config)

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
            topics_config=topics_config,
            get_next_ent_id=instance.get_next_ent_id
        )

        instance.consumer = BatchConsumer(
            user_name=user_name,
            processor=instance.batch_processor,
            get_session_context=instance.get_conversation_context,
            run_session_jobs=instance._run_session_jobs,
            write_to_graph=instance._write_to_graph_callback,
        )
        instance.consumer.start()

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
        await pipe.execute()
        
        return turn_id
    
    async def add_to_redis(self, msg: MessageData):
        msg_key = f"msg_{msg.id}"
        
        await self.redis_client.hset(f"message_content:{self.user_name}", msg_key, json.dumps({
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
            f"lookup:msg_to_turn:{self.user_name}", 
            msg_key, 
            f"turn_{turn_id}"
        )

        self.ent_resolver.add_message(msg_key, msg.message.strip())
    

    async def _get_or_create_user_entity(self, user_name: str):
        loop = asyncio.get_running_loop()

        entity_id = self.ent_resolver.get_id(user_name)

        if entity_id:
            logger.info(f"User {user_name} recognized.")
            return entity_id
        
        logger.info(f"Creating new USER entity for {user_name}")
        new_id = await self.get_next_ent_id()
        
        initial_fact = get_config_value("user_summary") or f"The primary user named {user_name}"

        embedding = await loop.run_in_executor(
            self.executor,
            partial(self.ent_resolver.register_entity, new_id, user_name, [user_name], "person", "Personal")
        )

        self.ent_resolver.entity_profiles[new_id]["facts"] = [initial_fact]

        user_entity = {
            "id": new_id,
            "canonical_name": user_name,
            "type": "person",
            "confidence": 1.0,
            "facts": [initial_fact], 
            "topic": "Identity",
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

        await self.scheduler.record_activity()
        self.consumer.signal()
    
    async def add_assistant_turn(self, content: str, timestamp: datetime):
        turn_id = await self.add_to_conversation_log(
            role="assistant",
            content=content,
            timestamp=timestamp
        )
        self.ent_resolver.add_message(f"turn_{turn_id}", content)
    
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
        
        for turn_id, data in zip(turn_ids, turn_data):
            if data:
                parsed = json.loads(data)
                role_label = "User" if parsed["role"] == "user" else "STELLA"
                ts = datetime.fromisoformat(parsed['timestamp'])
                date_str = ts.strftime("%Y-%m-%d %H:%M")
                results.append({
                    "turn_id": turn_id,
                    "role": parsed["role"],
                    "role_label": role_label,
                    "content": parsed["content"],
                    "timestamp": parsed["timestamp"],
                    "relative": f"[{date_str}]",
                    "user_msg_id": parsed.get("user_msg_id")
                })
        
        return results
    
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
                    "topic": profile.get("topic", "General"),
                    "embedding": self.ent_resolver.get_embedding_for_id(ent_id),
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
        
        if entity_ids:
            dirty_key = f"dirty_entities:{self.user_name}"
            await self.redis_client.sadd(dirty_key, *[str(eid) for eid in entity_ids])
            await self.redis_client.delete(f"profile_complete:{self.user_name}")
        
        logger.info(f"Wrote {len(entities)} entities, {len(relationships)} relationships to graph")
    
    async def _write_to_graph_callback(self, result: BatchResult):
        if result.extraction_result:
            await self._write_to_graph(
                result.entity_ids,
                result.new_entity_ids,
                result.alias_updated_ids,
                result.extraction_result
            )
    
        
    async def shutdown(self):
        await self.consumer.stop()
        await self.scheduler.stop()

        if self.executor:
            self.executor.shutdown(wait=True)
        if self.redis_client:
            await self.redis_client.aclose()