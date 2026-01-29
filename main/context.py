import asyncio
from datetime import datetime, timezone
import redis.asyncio as redis
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
import json
from jobs.archive import FactArchivalJob
from jobs.base import BaseJob, JobContext
from jobs.dlq import DLQReplayJob
from jobs.mood import MoodCheckpointJob
from jobs.profile import ProfileRefinementJob
from jobs.scheduler import Scheduler
from jobs.merger import MergeDetectionJob
from jobs.cleaner import EntityCleanupJob
from main.utils import handle_background_task_result
from shared.config import get_config_value
from main.consumer import BatchConsumer
from main.embedding import EmbeddingService
from main.processor import BatchProcessor, BatchResult
from main.service import LLMService
from typing import Dict, List, Optional
from functools import partial
from main.topics_config import TopicConfig
from main.nlp_pipe import NLPPipeline
from main.entity_resolve import EntityResolver
from db.store import MemGraphStore
import uuid

from schema.dtypes import Fact, MessageConnections, MessageData
from shared.redisclient import RedisKeys
from shared.resource import ResourceManager

class Context:

    def __init__(self, user_name: str, topics: List[str], redis_client):
        self.user_name: str = user_name
        self.active_topics: List[str] = topics
        self.scheduler: Scheduler = None
        self.redis_client: redis.Redis = redis_client
        self.model: Optional[str] = None
        self.llm: LLMService = None
        
        self.store: MemGraphStore = None
        self.nlp_pipe: NLPPipeline = None
        self.embedding_service: EmbeddingService = None
        self.ent_resolver: EntityResolver = None
        self.session_id: str = None
        self.topic_config: TopicConfig = None

        self.executor: ThreadPoolExecutor = None
        self.batch_processor: BatchProcessor = None
        self.consumer: BatchConsumer = None
        self.profile_job: BaseJob = None
        self.merge_job: BaseJob = None

    @classmethod
    async def create(
        cls,
        user_name: str,
        resources: ResourceManager,
        topics_config: dict = None,
        session_id: str = None,
        model: str = None
    ) -> "Context":
        

        if topics_config is None:
            topics_config = {"General": {"labels": [], "hierarchy": {}}}
        

        instance = cls(user_name, list(topics_config.keys()), resources.redis)
    
        instance.session_id = session_id
        instance.store = resources.store
        instance.executor = resources.executor
        instance.llm = resources.llm_service
        instance.model = model
        instance.redis_client = resources.redis
        instance.embedding_service = resources.embedding
        
        await resources.redis.hset(
            RedisKeys.session_config(user_name),
            instance.session_id,
            json.dumps(topics_config)
        )
        instance.topic_config = await TopicConfig.load(resources.redis, user_name, instance.session_id)
        await instance.topic_config.save(resources.redis, user_name, instance.session_id)
        
        loop = asyncio.get_running_loop()
        max_id = await loop.run_in_executor(None, instance.store.get_max_entity_id)
        current_redis = await resources.redis.get("global:next_ent_id")
        if not current_redis or int(current_redis) < max_id:
            await resources.redis.set("global:next_ent_id", max_id)
        
        instance.ent_resolver = EntityResolver(
            session_id=instance.session_id,
            store=instance.store,
            embedding_service=instance.embedding_service,
            hierarchy_config=instance.topic_config.hierarchy
        )
        
        await instance._get_or_create_user_entity(user_name)
        
        instance.nlp_pipe = await loop.run_in_executor(
            instance.executor,
            partial(
                NLPPipeline,
                llm=instance.llm,
                topic_config=instance.topic_config,
                get_known_aliases=lambda: instance.ent_resolver._name_to_id,
                get_profiles=lambda: instance.ent_resolver.entity_profiles,
                gliner=resources.gliner,
                spacy=resources.spacy,
                emotion_classifier=resources.emotion_classifier
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
            get_next_ent_id=instance.get_next_ent_id
        )
        
        instance.consumer = BatchConsumer(
            user_name=user_name,
            session_id=instance.session_id,
            store=instance.store,
            redis=resources.redis,
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
            executor=instance.executor,
            embedding_service=instance.embedding_service
        )
        instance.merge_job = MergeDetectionJob(
            user_name=user_name,
            ent_resolver=instance.ent_resolver, 
            store=instance.store,
            llm_client=instance.llm,
            topic_config=instance.topic_config,
            executor=instance.executor
        )
        
        instance.scheduler = Scheduler(user_name, instance.session_id, resources.redis)
        instance.scheduler.register(DLQReplayJob())
        instance.scheduler.register(EntityCleanupJob(user_name, instance.store, instance.ent_resolver))
        # instance.scheduler.register(FactArchivalJob(user_name, instance.store))
        # instance.scheduler.register(MoodCheckpointJob(user_name, instance.store))
        await instance.scheduler.start()
        
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
    
    async def _get_or_create_user_entity(self, user_name: str):
        loop = asyncio.get_running_loop()
        entity_id = self.ent_resolver.get_id(user_name)

        if entity_id and entity_id == 1:
            logger.info(f"User {user_name} recognized.")
            return entity_id
        
        logger.info(f"Creating new USER entity for {user_name}")
        
        new_id = await self.get_next_ent_id()
        
        initial_fact_content = get_config_value("user_summary") or f"The primary user named {user_name}"

        embedding = await loop.run_in_executor(
            self.executor,
            partial(self.ent_resolver.register_entity, new_id, user_name, [user_name], "person", "Identity")
        )

        # Create initial fact as Fact node
        fact_embedding = self.embedding_service.encode_single(initial_fact_content)
        
        facts: List[Fact] = []

        facts.append(Fact(
            id=str(uuid.uuid4()),
            content=initial_fact_content,
            valid_at=datetime.now(timezone.utc),
            embedding=fact_embedding,
            source_entity_id=new_id
        ))
        
        await loop.run_in_executor(
            self.executor,
            partial(self.store.create_facts_batch, new_id, facts)
        )

        user_entity = {
            "id": new_id,
            "canonical_name": user_name,
            "type": "person",
            "confidence": 1.0,
            "topic": "Identity",
            "embedding": embedding,
            "aliases": [user_name]
        }

        await loop.run_in_executor(
            self.executor,
            partial(self.store.write_batch, [user_entity], [])
        )
        
        logger.info(f"User entity {user_name} (ID: {new_id}) written to graph")
        return new_id
    
    async def _run_session_jobs(self):
        ctx = JobContext(
            user_name=self.user_name,
            redis=self.redis_client,
            idle_seconds=0,
            session_id=self.session_id
        )

        if await self.profile_job.should_run(ctx):
            await self.profile_job.execute(ctx)
        
        logger.info("Profile refinement done, waiting then moving on to merge detection...")
        await asyncio.sleep(0.5)

        if await self.merge_job.should_run(ctx):
            await self.merge_job.execute(ctx)
            
    
    async def add(self, msg: MessageData) -> None:
        msg.id = await self.get_next_msg_id()
        await self.add_to_redis(msg)

        buffer_key = RedisKeys.buffer(self.user_name, self.session_id)
        await self.redis_client.rpush(buffer_key, json.dumps({
            "id": msg.id,
            "message": msg.message.strip(),
            "timestamp": msg.timestamp.isoformat()
        }))

        await self.scheduler.record_activity()
        self.consumer.signal()

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
        
        conv_key = RedisKeys.conversation(self.user_name, self.session_id)
        sorted_key = RedisKeys.recent_conversation(self.user_name, self.session_id)
        
        await self.redis_client.hset(conv_key, turn_key, json.dumps(payload))
        await self.redis_client.zadd(sorted_key, {turn_key: timestamp.timestamp()})
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
    
    async def add_assistant_turn(self, content: str, timestamp: datetime):
        """Add assistant turn to conversation log."""
        turn_id = await self.add_to_conversation_log(
            role="assistant",
            content=content,
            timestamp=timestamp
        )
        
        task = asyncio.create_task(
            self._persist_assistant_embedding(turn_id, content, timestamp)
        )
        task.add_done_callback(handle_background_task_result)


    async def _persist_assistant_embedding(self, turn_id: int, content: str, timestamp: datetime):
        """Background task: compute embedding and write to graph."""
        try:
            loop = asyncio.get_running_loop()
            
            embedding_list = await loop.run_in_executor(
                self.executor,
                partial(self.ent_resolver.compute_batch_embeddings, [content])
            )
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
            
        except Exception as e:
            logger.error(f"Failed to persist assistant embedding for turn {turn_id}: {e}")

    
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
                turn_ids = await self.redis_client.zrevrangebyscore(
                    sorted_key,
                    f"({turn_score}",
                    "-inf",
                    start=0,
                    num=num_turns
                )
                turn_ids = list(reversed(turn_ids))
            else:
                turn_ids = await self.redis_client.zrevrange(sorted_key, 0, num_turns - 1)
                turn_ids = list(turn_ids)
                turn_ids.reverse()
        else:
            turn_ids = await self.redis_client.zrevrange(sorted_key, 0, num_turns - 1)
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
                role_label = "User" if parsed["role"] == "user" else "AGENT"
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
        extraction_result: List[MessageConnections]
    ):

        existing_candidates = list(set(entity_ids) - new_entity_ids)
        
        loop = asyncio.get_running_loop()
        
        valid_existing_ids = set()
        if existing_candidates:
            valid_existing_ids = await loop.run_in_executor(
                self.executor,
                self.store.validate_existing_ids,
                existing_candidates
            )
            

            missing = set(existing_candidates) - valid_existing_ids
            if missing:
                logger.critical(f"SPLIT BRAIN DETECTED: Resolver thinks IDs {missing} exist, but Graph does not. Dropping writes for these IDs to prevent Zombie Resurrection.")
                self.ent_resolver.remove_entities(list(missing))

        safe_ids = valid_existing_ids.union(new_entity_ids)

        entity_lookup = {}
        entities_to_write = []

        for ent_id in new_entity_ids:
            profile = self.ent_resolver.entity_profiles.get(ent_id)
            if profile:
                entities_to_write.append({
                    "id": ent_id,
                    "canonical_name": profile["canonical_name"],
                    "type": profile.get("type", ""),
                    "confidence": 1.0,
                    "topic": profile.get("topic", "General"),
                    "embedding": self.ent_resolver.get_embedding_for_id(ent_id),
                    "aliases": self.ent_resolver.get_mentions_for_id(ent_id),
                    "session_id": profile.get("session_id")
                })

        for ent_id in alias_updated_ids:
            if ent_id in new_entity_ids: continue
            
            if ent_id not in safe_ids:
                logger.warning(f"Skipping alias update for Zombie ID {ent_id}")
                continue
                
            profile = self.ent_resolver.entity_profiles.get(ent_id)
            if profile:
                entities_to_write.append({
                    "id": ent_id,
                    "canonical_name": profile["canonical_name"],
                    "type": profile.get("type", ""),
                    "confidence": 1.0,
                    "topic": profile.get("topic", "General"),
                    "embedding": self.ent_resolver.get_embedding_for_id(ent_id),
                    "aliases": self.ent_resolver.get_mentions_for_id(ent_id),
                    "session_id": profile.get("session_id")
                })


        for ent_id in safe_ids:
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

        relationships = []
        for msg_result in extraction_result:
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
                    logger.warning(f"Skipping pair: {pair.entity_a} - {pair.entity_b} (Entity missing or Zombie)")

        if entities_to_write or relationships:
            await loop.run_in_executor(
                self.executor,
                partial(self.store.write_batch, entities_to_write, relationships)
            )
        
        if safe_ids:
            dirty_key = RedisKeys.dirty_entities(self.user_name, self.session_id)
            await self.redis_client.sadd(dirty_key, *[str(eid) for eid in safe_ids])
            await self.redis_client.delete(RedisKeys.profile_complete(self.user_name, self.session_id))
        
        logger.info(f"Wrote {len(entities_to_write)} entities, {len(relationships)} relationships (Filtered {len(existing_candidates) - len(valid_existing_ids)} Zombies)")
    
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