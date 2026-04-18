import asyncio
import json
import os
import uuid
from loguru import logger
from typing import Optional
from functools import partial

from common.infra.resources import ResourceManager
from common.infra.redis import RedisKeys
from common.config.topics_config import TopicConfig
from common.config.base import load_config, get_default_config, get_config
from core.entity_resolver import EntityResolver
from core.nlp import NLPPipeline
from core.batch_processor import BatchProcessor
from core.batch_consumer import BatchConsumer
from common.rag.file_rag import FileRAGService
from core.context import Context
from jobs.scheduler import Scheduler
from jobs.profile import ProfileRefinementJob
from jobs.merger import MergeDetectionJob
from jobs.dlq import DLQReplayJob
from jobs.cleaner import EntityCleanupJob
from jobs.archive import FactArchivalJob
from jobs.topics import TopicConfigJob


class SessionAssembler:
    """
    Wires together the infrastructure, services, and background jobs for a session.
    Decouples construction from the Context state container.
    """

    def __init__(self, user_name: str, resources: ResourceManager):
        self.user_name = user_name
        self.resources = resources
        self.config = get_config()
        self.dev_settings = self.config.developer_settings

    async def bootstrap(self, topics_config: Optional[dict] = None, session_id: Optional[str] = None, model: Optional[str] = None) -> Context:
        """Perform the multi-phase boot sequence: assemble + launch."""
        ctx = await self.assemble(topics_config, session_id, model)
        await self.launch(ctx)
        return ctx

    async def assemble(self, topics_config: Optional[dict] = None, session_id: Optional[str] = None, model: Optional[str] = None) -> Context:
        """
        Wires together services and infrastructure into a Context.
        Does NOT start background loops.
        """
        session_id = session_id or str(uuid.uuid4())
        
        topic_config = await self._init_topic_config(session_id, topics_config)
        await self._sync_entity_counters()
        
        resolver = self._init_entity_resolver(session_id, topic_config)
        self.resources.active_resolver = resolver
        
        pipeline = await self._init_nlp_pipeline(resolver, topic_config)
        processor = self._init_batch_processor(session_id, resolver, pipeline, topic_config)
        
        consumer = self._init_batch_consumer(session_id, processor)
        file_rag = self._init_file_rag(session_id)
        
        scheduler = Scheduler(self.user_name, session_id, self.resources.redis)
        profile_job = self._init_profile_job(resolver)
        merge_job = self._init_merge_job(resolver, topic_config)
        
        self._register_background_jobs(scheduler, resolver, processor, topic_config)
        
        ctx = Context(self.user_name, list(topic_config.raw.keys()), self.resources.redis)
        ctx.session_id = session_id
        ctx.topic_config = topic_config
        ctx.ent_resolver = resolver
        ctx.nlp_pipe = pipeline
        ctx.batch_processor = processor
        ctx.consumer = consumer
        ctx.file_rag = file_rag
        ctx.scheduler = scheduler
        ctx.profile_job = profile_job
        ctx.merge_job = merge_job
        ctx.model = model
        
        ctx.store = self.resources.store
        ctx.llm = self.resources.llm_service
        ctx.executor = self.resources.executor
        ctx.embedding_service = self.resources.embedding
        ctx.mcp_manager = self.resources.mcp_manager
        
        if ctx.consumer:
            ctx.consumer.get_session_context = ctx.get_conversation_context
            ctx.consumer.run_session_jobs = ctx._run_session_jobs
            ctx.consumer.write_to_graph = ctx._write_to_graph_callback

        if ctx.batch_processor:
            ctx.batch_processor.get_next_ent_id = ctx.get_next_ent_id

        dlq_job = ctx.scheduler._jobs.get("dlq_auto_replay") if ctx.scheduler else None
        if dlq_job:
            dlq_job.write_to_graph = ctx._write_to_graph_callback

        topic_job = ctx.scheduler._jobs.get("topic_config") if ctx.scheduler else None
        if topic_job:
            topic_job.update_callback = ctx.update_topics_config
            
        return ctx
    

    async def launch(self, ctx: Context):
        """Starts background tasks for the context."""
        if ctx.scheduler:
            await ctx.scheduler.start()
        if ctx.consumer:
            ctx.consumer.start()
        
        logger.info(f"System launched successfully for session {ctx.session_id}")

    async def _init_topic_config(self, session_id: str, topics_config: Optional[dict]) -> TopicConfig:
        if topics_config is None:
            topics_config = self.config.default_topics
            
        await self.resources.redis.hset(
            RedisKeys.session_config(self.user_name),
            session_id,
            json.dumps(topics_config)
        )
        t_config = await TopicConfig.load(self.resources.redis, self.user_name, session_id)
        await t_config.save(self.resources.redis, self.user_name, session_id)
        return t_config

    async def _sync_entity_counters(self):
        max_id = (await self.resources.store.get_max_entity_id()) or 0
        current_redis = await self.resources.redis.get(RedisKeys.global_next_ent_id())
        # Set to max_id so next INCR returns max_id + 1 (first unused ID)
        if not current_redis or int(current_redis) < max_id:
            await self.resources.redis.set(RedisKeys.global_next_ent_id(), max_id)

    def _init_entity_resolver(self, session_id: str, topic_config: TopicConfig) -> EntityResolver:
        er_cfg = self.dev_settings.entity_resolution
        return EntityResolver(
            session_id=session_id,
            store=self.resources.store,
            embedding_service=self.resources.embedding,
            hierarchy_config=topic_config.hierarchy,
            fuzzy_substring_threshold=er_cfg.fuzzy_substring_threshold,
            fuzzy_non_substring_threshold=er_cfg.fuzzy_non_substring_threshold,
            generic_token_freq=er_cfg.generic_token_freq,
            candidate_fuzzy_threshold=er_cfg.candidate_fuzzy_threshold,
            candidate_vector_threshold=er_cfg.candidate_vector_threshold
        )

    async def _init_nlp_pipeline(self, resolver: EntityResolver, topic_config: TopicConfig) -> NLPPipeline:
        nlp_cfg = self.dev_settings.nlp_pipeline
        return await asyncio.get_running_loop().run_in_executor(
            self.resources.executor,
            partial(
                NLPPipeline,
                llm=self.resources.llm_service,
                topic_config=topic_config,
                get_known_aliases=resolver.get_known_aliases,
                get_profile=resolver.get_profile,
                gliner=self.resources.gliner,
                spacy=self.resources.spacy,
                gliner_threshold=nlp_cfg.gliner_threshold,
                vp01_min_confidence=nlp_cfg.vp01_min_confidence
            )
        )

    def _init_batch_processor(self, session_id: str, resolver: EntityResolver, pipeline: NLPPipeline, topic_config: TopicConfig) -> BatchProcessor:
        er_cfg = self.dev_settings.entity_resolution
        return BatchProcessor(
            session_id=session_id,
            redis_client=self.resources.redis,
            llm=self.resources.llm_service,
            ent_resolver=resolver,
            nlp_pipe=pipeline,
            store=self.resources.store,
            cpu_executor=self.resources.executor,
            user_name=self.user_name,
            topic_config=topic_config,
            # This is tricky: it needs a callback to the context's get_next_ent_id
            # We'll need to wrap it or satisfy it after Context creation
            get_next_ent_id=None, 
            resolution_threshold=er_cfg.resolution_threshold
        )

    def _init_batch_consumer(self, session_id: str, processor: BatchProcessor) -> BatchConsumer:
        ingest_cfg = self.dev_settings.ingestion
        batch_size = ingest_cfg.batch_size
        batch_timeout = ingest_cfg.batch_timeout
        checkpoint_interval = batch_size * 4
        session_window = batch_size * 3
        
        return BatchConsumer(
            user_name=self.user_name,
            session_id=session_id,
            store=self.resources.store,
            redis=self.resources.redis,
            processor=processor,
            get_session_context=None, # Injected later
            run_session_jobs=None,     # Injected later
            write_to_graph=None,       # Injected later
            batch_size=batch_size,
            batch_timeout=batch_timeout,
            checkpoint_interval=checkpoint_interval,
            session_window=session_window
        )

    def _init_file_rag(self, session_id: str) -> FileRAGService:
        upload_dir = os.path.join(os.getenv("CONFIG_DIR", "./config"), "uploads")
        return FileRAGService(
            session_id=session_id,
            chroma_client=self.resources.chroma,
            embedding_service=self.resources.embedding,
            upload_dir=upload_dir,
        )

    def _init_profile_job(self, resolver: EntityResolver) -> ProfileRefinementJob:
        jobs_cfg = self.dev_settings.jobs
        nlp_cfg = self.dev_settings.nlp_pipeline
        prof_cfg = jobs_cfg.profile
        
        return ProfileRefinementJob(
            llm=self.resources.llm_service,
            resolver=resolver,
            store=self.resources.store,
            executor=self.resources.executor,
            embedding_service=self.resources.embedding,
            redis_client=self.resources.redis,
            msg_window=prof_cfg.msg_window,
            volume_threshold=prof_cfg.volume_threshold,
            idle_threshold=prof_cfg.idle_threshold,
            profile_batch_size=prof_cfg.profile_batch_size,
            contradiction_sim_low=prof_cfg.contradiction_sim_low,
            contradiction_sim_high=prof_cfg.contradiction_sim_high,
            contradiction_batch_size=prof_cfg.contradiction_batch_size,
            profile_prompt=nlp_cfg.profile_prompt,
            contradiction_prompt=nlp_cfg.contradiction_prompt
        )

    def _init_merge_job(self, resolver: EntityResolver, topic_config: TopicConfig) -> MergeDetectionJob:
        jobs_cfg = self.dev_settings.jobs
        nlp_cfg = self.dev_settings.nlp_pipeline
        merge_cfg = jobs_cfg.merger
        
        return MergeDetectionJob(
            user_name=self.user_name,
            ent_resolver=resolver, 
            store=self.resources.store,
            llm_client=self.resources.llm_service,
            topic_config=topic_config,
            executor=self.resources.executor,
            redis_client=self.resources.redis,
            auto_threshold=merge_cfg.auto_threshold,
            hitl_threshold=merge_cfg.hitl_threshold,
            cosine_threshold=merge_cfg.cosine_threshold,
            merge_prompt=nlp_cfg.merge_prompt
        )

    def _register_background_jobs(self, scheduler: Scheduler, resolver: EntityResolver, processor: BatchProcessor, topic_config: TopicConfig):
        jobs_cfg = self.dev_settings.jobs
        
        dlq_cfg = jobs_cfg.dlq
        scheduler.register(DLQReplayJob(
            ent_resolver=resolver,
            processor=processor,
            write_to_graph=None,
            redis_client=self.resources.redis,
            interval=dlq_cfg.interval_seconds,
            batch_size=dlq_cfg.batch_size,
            max_attempts=dlq_cfg.max_attempts
        ))
        
        clean_cfg = jobs_cfg.cleaner
        scheduler.register(EntityCleanupJob(
            user_name=self.user_name, 
            store=self.resources.store, 
            ent_resolver=resolver,
            redis_client=self.resources.redis,
            interval_hours=clean_cfg.interval_hours,
            orphan_age_hours=clean_cfg.orphan_age_hours,
            stale_junk_days=clean_cfg.stale_junk_days
        ))

        arch_cfg = jobs_cfg.archival
        scheduler.register(FactArchivalJob(
            user_name=self.user_name, 
            store=self.resources.store,
            redis_client=self.resources.redis,
            retention_days=arch_cfg.retention_days,
            fallback_interval_hours=arch_cfg.fallback_interval_hours
        ))

        topic_cfg = jobs_cfg.topic_config
        scheduler.register(TopicConfigJob(
            llm=self.resources.llm_service,
            topic_config=topic_config,
            update_callback=None,
            redis_client=self.resources.redis,
            interval_msgs=topic_cfg.interval_msgs,
            conversation_window=topic_cfg.conversation_window
        ))
