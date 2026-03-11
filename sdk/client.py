"""SDK client — boots infrastructure and ML models from a KnogginConfig."""

import asyncio
from functools import partial
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Callable

import torch
import redis.asyncio as aioredis
from loguru import logger

from sdk.config import KnogginConfig
from sdk.events import resolve_handler, EventEmitter
from db.store import MemGraphStore
from shared.rag.embedding import EmbeddingService
from shared.services.llm import LLMService
from sdk.session import KnogginSession
from main.processor import BatchProcessor
from main.nlp_pipe import NLPPipeline
from main.entity_resolve import EntityResolver
from agent.tools import Tools
from shared.services.memory import MemoryManager
from shared.rag.processor import FileRAGService
from shared.config.topics_config import TopicConfig
from shared.infra.redis import RedisKeys
from main.consumer import BatchConsumer
from shared.services.graph import write_batch_callback

class KnogginClient:
    """SDK client holding all Knoggin resources.

    Call `await KnogginClient.boot(cfg)` to create, `await client.close()` to tear down.
    """

    def __init__(self):
        # Infrastructure
        self.store: Optional[MemGraphStore] = None
        self.redis: Optional[aioredis.Redis] = None
        self.executor: Optional[ThreadPoolExecutor] = None

        # ML models
        self.embedding: Optional[EmbeddingService] = None
        self.gliner = None
        self.spacy = None

        # Services
        self.llm: Optional[LLMService] = None
        self.chroma = None
        self.mcp_manager = None

        # Config
        self.config: Optional[KnogginConfig] = None
        self.device: Optional[torch.device] = None

        # Events
        self.events = EventEmitter()
        self.on = self.events.on
        self.on_any = self.events.on_any

        self._sessions: dict[str, KnogginSession] = {}

    @classmethod
    async def boot(
        cls,
        config: KnogginConfig,
        on_event: Optional[Callable] = None,
    ) -> "KnogginClient":
        """Boot a KnogginClient from config.

        Raises ConnectionError if Redis/Memgraph is unreachable,
        ValueError if required config (e.g. API key) is missing.
        """
        client = cls()
        client.config = config
        
        fallback = on_event or resolve_handler(config.events)
        client.events = EventEmitter(fallback_handler=fallback)
        client.on = client.events.on
        client.on_any = client.events.on_any
        
        profile = config.profile

        try:
            # ── Device ──────────────────────────────────────
            client.device = cls._resolve_device(config.models.device)
            logger.info(f"Device: {client.device}")

            # ── Thread pool ─────────────────────────────────
            client.executor = ThreadPoolExecutor(max_workers=config.models.workers)
            logger.info(f"Thread pool: {config.models.workers} workers")

            # ── Infrastructure ──────────────────────────────
            client.redis = aioredis.from_url(
                f"redis://{config.infra.redis_host}:{config.infra.redis_port}",
                decode_responses=True,
            )
            await client.redis.ping()
            logger.info("Redis connected")

            client.store = MemGraphStore(
                uri=f"bolt://{config.infra.memgraph_host}:{config.infra.memgraph_port}"
            )

            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, client.store.get_max_entity_id)
                logger.info("Memgraph connected")
            except Exception as e:
                raise ConnectionError(
                    f"Cannot connect to Memgraph at "
                    f"{config.infra.memgraph_host}:{config.infra.memgraph_port} — {e}"
                ) from e
            
            logger.info("Memgraph connected")

            # ── LLM ────────────────────────────────────────
            api_key = config.llm.api_key
            if not api_key:
                raise ValueError(
                    f"No API key found. Set {config.llm.api_key_env} in your .env file."
                )

            client.llm = LLMService(
                api_key=api_key,
                base_url=config.llm.base_url,
                agent_model=config.llm.agent_model,
                extraction_model=config.llm.extraction_model,
                merge_model=config.llm.merge_model,
                redis_client=client.redis,
            )
            logger.info("LLM service ready")

            # ── Embeddings (all profiles) ───────────────────
            client.embedding = EmbeddingService(
                embedding_model=config.models.embedding_model,
                device=client.device,
            )
            logger.info(f"Embeddings loaded: {config.models.embedding_model}")

            # ── Extraction models ───────────────────────────
            if profile in ("full", "extraction"):
                loop = asyncio.get_running_loop()
                max_id = await loop.run_in_executor(
                    None, client.store.get_max_entity_id
                )
                current_redis = await client.redis.get(
                    "global:next_ent_id"
                )
                if not current_redis or int(current_redis) < max_id:
                    await client.redis.set("global:next_ent_id", max_id)
                    logger.info(f"Entity ID counter synced to {max_id}")

                await cls._load_extraction_models(client)

            # ── ChromaDB ────────────────────────────────────
            if profile in ("full", "agent"):
                import chromadb
                client.chroma = chromadb.PersistentClient(path=config.infra.chroma_path)
                logger.info(f"ChromaDB initialized at {config.infra.chroma_path}")

            # ── MCP Servers ─────────────────────────────────
            if config.mcp.servers:
                from shared.mcp.client import MCPClientManager
                client.mcp_manager = await MCPClientManager.create({"servers": config.mcp.servers})
                logger.info(f"MCP Manager initialized with {len(config.mcp.servers)} servers")

            logger.info(f"KnogginClient ready (profile={profile})")
            return client

        except Exception as e:
            logger.error(f"Boot failed: {e}")
            await client.close()
            raise

    @classmethod
    async def from_env(cls) -> "KnogginClient":
        """Auto-load config and boot the client. Useful for simple initialization."""
        from cli.config import load_toml
        return await cls.boot(load_toml())

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """Release all resources. Safe to call multiple times."""

        for session_id, session in list(self._sessions.items()):
            await self._close_session(session_id)

        if self.executor:
            try:
                self.executor.shutdown(wait=False)
            except Exception as e:
                logger.debug(f"Shutdown: executor cleanup — {e}")
            self.executor = None

        if self.redis:
            try:
                await self.redis.aclose()
            except Exception as e:
                logger.debug(f"Shutdown: Redis cleanup — {e}")
            self.redis = None

        if self.store:
            try:
                self.store.close()
            except Exception as e:
                logger.debug(f"Shutdown: Memgraph cleanup — {e}")
            self.store = None

        if self.embedding:
            try:
                self.embedding.cleanup()
            except Exception as e:
                logger.debug(f"Shutdown: embedding cleanup — {e}")
            self.embedding = None

        if self.llm:
            try:
                await self.llm.close()
            except Exception as e:
                logger.debug(f"Shutdown: LLM cleanup — {e}")
            self.llm = None

        if self.mcp_manager:
            try:
                await self.mcp_manager.shutdown()
            except Exception as e:
                logger.debug(f"Shutdown: MCP cleanup — {e}")
            self.mcp_manager = None

        self.chroma = None
        self.gliner = None
        self.spacy = None

        logger.info("KnogginClient shutdown complete")

    def emit(self, source: str, event: str, data: dict):
        """Fire event callback if registered."""
        self.events.emit(source, event, data)

    @staticmethod
    def generate_session_id() -> str:
        """Generate a unique session ID."""
        return str(uuid.uuid4())
    
    async def _create_session(
        self,
        user_name: str,
        topics: dict,
        session_id: str = None,
        agent_id: str = None,
        agent_name: str = "Knoggin",
        user_timezone: str = None,
        upload_dir: str = None,
        start_jobs: bool = False,
    ) -> KnogginSession:
        """Build all session-scoped components from client resources.

        This mirrors what Context.create() does on the server, but
        returns a plain dataclass instead of starting a consumer loop.

        Args:
            user_name: The user this session belongs to.
            topics: Raw topic config dict (passed to TopicConfig).
            session_id: Optional explicit ID. Generated if omitted.
            agent_id: Agent identity for working memory scoping.
            agent_name: Display name for agent prompt.
            user_timezone: IANA timezone string for date formatting.
            upload_dir: Directory for file uploads. Defaults to ./config/uploads.
            start_jobs: If True, automatically boots the background job scheduler.
        """
        session_id = session_id or str(uuid.uuid4())
        agent_id = agent_id or f"sdk_{uuid.uuid4().hex[:8]}"
        profile = self.config.profile
        loop = asyncio.get_running_loop()

        # ── Topic config ────────────────────────────────────
        topic_config = TopicConfig(topics)
        await self.redis.hset(
            RedisKeys.session_config(user_name),
            session_id,
            __import__("json").dumps(topics),
        )

        # ── Entity resolver ─────────────────────────────────
        resolver = EntityResolver(
            session_id=session_id,
            store=self.store,
            embedding_service=self.embedding,
            hierarchy_config=topic_config.hierarchy,
        )

        # Verify user entity exists in graph
        user_id = resolver.get_id(user_name)
        if user_id is not None:
            entity = await loop.run_in_executor(
                self.executor, self.store.get_entity_by_id, user_id,
            )
            if entity and entity.get("canonical_name") == user_name:
                profile_data = resolver.entity_profiles.get(user_id)
                if not profile_data:
                    all_aliases = entity.get("aliases") or [user_name]
                    await loop.run_in_executor(
                        self.executor,
                        partial(
                            resolver.register_entity,
                            user_id, user_name, all_aliases, "person", "Identity",
                        ),
                    )
                logger.info(f"User entity verified: {user_name} (id={user_id})")

        # ── NLP pipeline & Consumer (extraction profiles) ───
        nlp = None
        processor = None
        consumer = None

        if profile in ("full", "extraction"):
            nlp = await loop.run_in_executor(
                self.executor,
                partial(
                    NLPPipeline,
                    llm=self.llm,
                    topic_config=topic_config,
                    get_known_aliases=resolver.get_known_aliases,
                    get_profiles=resolver.get_profiles,
                    gliner=self.gliner,
                    spacy=self.spacy,
                    llm_ner=self.config.models.llm_ner,
                ),
            )

            async def _get_next_ent_id():
                return int(await self.redis.incr(RedisKeys.global_next_ent_id()))

            processor = BatchProcessor(
                session_id=session_id,
                redis_client=self.redis,
                llm=self.llm,
                ent_resolver=resolver,
                nlp_pipe=nlp,
                store=self.store,
                cpu_executor=self.executor,
                user_name=user_name,
                topic_config=topic_config,
                get_next_ent_id=_get_next_ent_id,
            )

            async def _get_session_context(window: int, msg_id: int):
                return await self.get_conversation_context(user_name, session_id, window, msg_id)

            async def _run_session_jobs():
                await self._run_job(session_id, "profile")
                await self._run_job(session_id, "merger")

            async def _write_to_graph(batch_result):
                return await write_batch_callback(
                    batch=batch_result,
                    store=self.store,
                    executor=self.executor,
                    resolver=resolver,
                    session_id=session_id,
                    user_name=user_name,
                    redis_client=self.redis
                )

            consumer = BatchConsumer(
                user_name=user_name,
                session_id=session_id,
                store=self.store,
                redis=self.redis,
                processor=processor,
                get_session_context=_get_session_context,
                run_session_jobs=_run_session_jobs,
                write_to_graph=_write_to_graph,
                batch_size=8,
                batch_timeout=360.0,
                checkpoint_interval=24,
                session_window=18
            )
            consumer.start()

        # ── Memory manager ──────────────────────────────────
        memory = MemoryManager(
            redis=self.redis,
            user_name=user_name,
            session_id=session_id,
            agent_id=agent_id,
            topic_config=topic_config,
            on_event=self.emit,
        )

        # ── File RAG (agent profiles only) ──────────────────
        file_rag = None
        if profile in ("full", "agent") and self.chroma:
            upload_path = upload_dir or os.path.join(
                os.getenv("CONFIG_DIR", "./config"), "uploads",
            )
            file_rag = FileRAGService(
                session_id=session_id,
                chroma_client=self.chroma,
                embedding_service=self.embedding,
                upload_dir=upload_path,
            )

        # ── Tools ───────────────────────────────────────────
        tools = Tools(
            user_name=user_name,
            store=self.store,
            ent_resolver=resolver,
            redis_client=self.redis,
            session_id=session_id,
            topic_config=topic_config,
            file_rag=file_rag,
            mcp_manager=self.mcp_manager,
            memory=memory,
        )

        # ── Assemble session ────────────────────────────────
        session = KnogginSession(
            session_id=session_id,
            user_name=user_name,
            topic_config=topic_config,
            resolver=resolver,
            nlp=nlp,
            processor=processor,
            tools=tools,
            memory=memory,
            file_rag=file_rag,
            consumer=consumer,
            _client=self,
        )

        self._sessions[session_id] = session
        self.emit("session", "created", {
            "session_id": session_id,
            "user_name": user_name,
            "profile": profile,
            "topics": list(topics.keys()),
        })
        logger.info(f"SDK session created: {session_id} (profile={profile})")

        if start_jobs:
            await self._start_jobs(user_name, session_id)

        return session

    async def _close_session(self, session_id: str):
        """Clean up a session's resources."""
        session = self._sessions.pop(session_id, None)
        if not session:
            logger.warning(f"Session {session_id} not found")
            return

        if session.scheduler:
            try:
                await session.scheduler.stop()
            except Exception as e:
                logger.debug(f"Session cleanup: scheduler — {e}")

        if getattr(session, "consumer", None):
            try:
                await session.consumer.stop()
            except Exception as e:
                logger.debug(f"Session cleanup: consumer — {e}")

        if session.file_rag:
            try:
                session.file_rag.cleanup_session()
            except Exception as e:
                logger.debug(f"Session cleanup: file_rag — {e}")

        # Clear session config from Redis
        try:
            await self.redis.hdel(
                RedisKeys.session_config(session.user_name), session_id,
            )
        except Exception as e:
            logger.debug(f"Session cleanup: Redis config — {e}")

        self.emit("session", "closed", {"session_id": session_id})
        logger.info(f"SDK session closed: {session_id}")

    # ── Scheduler ───────────────────────────────────────────

    async def _start_jobs(
        self,
        user_name: str,
        session_id: str,
        resolver=None,
        topic_config=None,
        extractor=None,
        job_config: Optional[Dict] = None,
        custom_jobs: Optional[List] = None,
    ) -> None:
        """Start the background job scheduler.

        Registers standard jobs (DLQ, Cleaner, Archival), LLM-dependent
        jobs (Profile, Merge, TopicConfig) if configured, and any custom jobs.
        """
        from jobs.factory import build_scheduler

        session = self._sessions.get(session_id)
        if not session:
            logger.error(f"Cannot start scheduler: session {session_id} not found")
            return

        if session.scheduler:
            logger.warning(f"Scheduler already running for session {session_id}, stopping first")
            await self._stop_jobs(session_id)

        cfg = job_config or {}
        
        resolver = resolver or session.resolver
        topic_config = topic_config or session.topic_config
        processor = extractor.session.processor if extractor else session.processor
        write_to_graph_callback = extractor.write_to_graph_callback if extractor else None

        session.scheduler = build_scheduler(
            user_name=user_name,
            session_id=session_id,
            redis_client=self.redis,
            jobs_cfg=cfg,
            store=self.store,
            llm=self.llm,
            executor=self.executor,
            embedding_service=self.embedding,
            resolver=resolver,
            topic_config=topic_config,
            processor=processor,
            write_to_graph_callback=write_to_graph_callback,
            custom_jobs=custom_jobs,
        )

        await session.scheduler.start()
        self.emit("scheduler", "started", {
            "session_id": session_id,
            "jobs": list(session.scheduler._jobs.keys()),
        })
        logger.info(f"SDK scheduler started for {session_id} with jobs: {list(session.scheduler._jobs.keys())}")

    async def _stop_jobs(self, session_id: str) -> None:
        """Stop the background job scheduler."""
        session = self._sessions.get(session_id)
        if session and session.scheduler:
            await session.scheduler.stop()
            session.scheduler = None
            self.emit("scheduler", "stopped", {"session_id": session_id})
            logger.info(f"SDK scheduler stopped for {session_id}")

    async def _run_job(self, session_id: str, job_name: str) -> dict:
        """Trigger a specific job out of band."""
        session = self._sessions.get(session_id)
        if not session or not session.scheduler:
            return {"error": f"Scheduler not running for session {session_id}. Call _start_jobs() first."}

        job = session.scheduler._jobs.get(job_name)
        if not job:
            available = list(session.scheduler._jobs.keys())
            return {"error": f"Job '{job_name}' not found. Available: {available}"}

        from jobs.base import JobContext
        ctx = await session.scheduler._build_context()
        await session.scheduler._execute_job(job, ctx)
        return {"triggered": job_name, "success": True}

    # ── Ingestion Helpers ───────────────────────────────────

    async def add_to_conversation_log(
        self, user_name: str, session_id: str, role: str, content: str, timestamp, user_msg_id: Optional[int] = None
    ) -> int:
        import json
        turn_id = await self.redis.incr(RedisKeys.global_next_turn_id(user_name, session_id))
        turn_key = f"turn_{turn_id}"
        
        payload = {
            "role": role,
            "content": content,
            "timestamp": timestamp.isoformat()
        }
        if user_msg_id is not None:
            payload["user_msg_id"] = user_msg_id
            
        conv_key = RedisKeys.conversation(user_name, session_id)
        sorted_key = RedisKeys.recent_conversation(user_name, session_id)
        
        await self.redis.hset(conv_key, turn_key, json.dumps(payload))
        await self.redis.zadd(sorted_key, {turn_key: timestamp.timestamp()})
        return turn_id

    async def get_conversation_context(
        self, user_name: str, session_id: str, limit: int = 15, before_msg_id: Optional[int] = None
    ) -> List[Dict]:
        import json
        turn_id_str = None
        if before_msg_id is not None:
            turn_id_str = await self.redis.hget(
                RedisKeys.msg_to_turn_lookup(user_name, session_id),
                f"msg_{before_msg_id}"
            )
            
        sorted_key = RedisKeys.recent_conversation(user_name, session_id)
        conv_key = RedisKeys.conversation(user_name, session_id)
        
        end_idx = -1
        if turn_id_str:
            rank = await self.redis.zrank(sorted_key, turn_id_str)
            if rank is not None:
                end_idx = max(0, rank - 1)
        
        if end_idx >= 0:
            start_idx = max(0, end_idx - limit + 1)
            turn_keys = await self.redis.zrange(sorted_key, start_idx, end_idx)
        else:
            turn_keys = await self.redis.zrevrange(sorted_key, 0, limit - 1)
            turn_keys.reverse()
            
        if not turn_keys:
            return []
            
        turn_data = await self.redis.hmget(conv_key, *turn_keys)
        history = []
        for i, data_str in enumerate(turn_data):
            if data_str:
                try:
                    data = json.loads(data_str)
                except json.JSONDecodeError:
                    logger.warning(f"Failed to decode conversation turn {turn_keys[i]}, skipping")
                    continue
                    
                key = turn_keys[i]
                history.append({
                    "id": key,
                    "role": data["role"],
                    "role_label": data["role"].capitalize(),
                    "content": data["content"],
                    "timestamp": data.get("timestamp", "")
                })
        return history

    # ── Private ─────────────────────────────────────────────

    @staticmethod
    def _resolve_device(device_str: str) -> torch.device:
        if device_str == "auto":
            if torch.cuda.is_available():
                return torch.device("cuda")
            if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
                return torch.device("mps")
            return torch.device("cpu")
        return torch.device(device_str)

    @classmethod
    async def _load_extraction_models(cls, client: "KnogginClient"):
        """Load GLiNER and spaCy on executor to avoid blocking the event loop."""
        loop = asyncio.get_running_loop()

        def _load():
            import spacy
            from gliner import GLiNER

            exclude = ["ner", "lemmatizer", "attribute_ruler"]
            nlp = spacy.load("en_core_web_md", exclude=exclude)
            nlp.add_pipe("doc_cleaner")

            model = GLiNER.from_pretrained("urchade/gliner_large-v2.1")
            model.to(client.device)

            return nlp, model

        client.spacy, client.gliner = await loop.run_in_executor(client.executor, _load)
        logger.info("Extraction models loaded (spaCy + GLiNER)")