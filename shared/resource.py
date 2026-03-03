
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
from gliner import GLiNER
from loguru import logger
import redis.asyncio as aioredis
import spacy
import torch
from db.community_store import CommunityStore
from db.store import MemGraphStore
from log.llm_trace import get_trace_logger
from shared.config import get_config_value
from shared.embedding import EmbeddingService
from shared.mcp_client import MCPClientManager
from shared.redisclient import AsyncRedisClient
from shared.service import LLMService
import chromadb

class ResourceManager:
    _instance = None
    _init_lock: asyncio.Lock = None
    
    def __init__(self):
        self.store: MemGraphStore = None
        self.embedding: EmbeddingService = None
        self.redis: aioredis.Redis = None
        self.llm_service: LLMService = None
        self.executor: ThreadPoolExecutor = None
        self.gliner: GLiNER = None
        self.spacy: spacy.Language = None
        self.chroma: chromadb.ClientAPI = None
        self.mcp_manager: MCPClientManager = None
        self.active_resolver = None
        self.community_store: CommunityStore = None

    @classmethod
    async def initialize(cls) -> "ResourceManager":
        if cls._init_lock is None:
            cls._init_lock = asyncio.Lock()
        
        async with cls._init_lock:
            if cls._instance is not None:
                return cls._instance
        
            instance = cls()

            try:
                use_gpu = os.getenv("KNOGGIN_GPU", "false").lower() == "true"
                if use_gpu and torch.cuda.is_available():
                    device = torch.device("cuda")
                    logger.info("GPU enabled — CUDA (NVIDIA/AMD ROCm)")
                elif use_gpu and hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
                    device = torch.device("mps")
                    logger.info("GPU enabled — MPS (Apple Silicon)")
                else:
                    device = torch.device("cpu")
                    if use_gpu:
                        logger.warning("KNOGGIN_GPU=true but no compatible GPU found, falling back to CPU")
                
                num_workers = int(os.getenv("KNOGGIN_WORKERS", "4"))
                instance.executor = ThreadPoolExecutor(max_workers=num_workers)
                logger.info(f"Thread pool: {num_workers} workers")
                instance.store = MemGraphStore()
                instance.redis = await AsyncRedisClient.get_instance()
                
                llm_config = get_config_value("llm", {})
                instance.llm_service = LLMService(
                    api_key=llm_config.get("api_key"),
                    agent_model=llm_config.get("agent_model", "google/gemini-3-flash-preview"),
                    trace_logger=get_trace_logger(),
                    redis_client=instance.redis
                )
                
                instance.embedding = EmbeddingService(device=device)

                chroma_path = os.path.join(os.getenv("CONFIG_DIR", "./config"), "chroma_db")
                instance.chroma = chromadb.PersistentClient(path=chroma_path)
                logger.info(f"ChromaDB initialized at {chroma_path}")

                exclude = ["ner", "lemmatizer", "attribute_ruler"]
                nlp = spacy.load("en_core_web_md", exclude=exclude)
                nlp.add_pipe("doc_cleaner")
                logger.info("Loaded en_core_web_md (CPU)")
                instance.spacy = nlp

                model = GLiNER.from_pretrained("urchade/gliner_large-v2.1")
                model.to(device)
                logger.info("Loaded GLiNER large-v2.1")
                instance.gliner = model

                mcp_config = get_config_value("mcp") or {"servers": {}}
                instance.mcp_manager = await MCPClientManager.create(mcp_config)
                logger.info("MCP manager initialized")
                instance.community_store = CommunityStore(instance.store.driver)

                cls._instance = instance
                logger.info("ResourceManager initialization complete")
                return instance
                
            except Exception as e:
                logger.error(f"ResourceManager initialization failed: {e}")
                await instance._cleanup_partial()
                raise

    async def _cleanup_partial(self):
        """Clean up partially initialized resources."""
        if self.executor:
            self.executor.shutdown(wait=False)
            self.executor = None
        
        try:
            await AsyncRedisClient.close_redis()
        except Exception:
            pass
        self.redis = None
        
        if self.store:
            try:
                self.store.close()
            except Exception:
                pass
            self.store = None
        
        if self.embedding:
            try:
                self.embedding.cleanup()
            except Exception:
                pass
            self.embedding = None
        
        if self.mcp_manager:
            try:
                await self.mcp_manager.shutdown()
            except Exception:
                pass
            self.mcp_manager = None
        
        self.chroma = None
        self.gliner = None
        self.spacy = None
        
        logger.info("Cleaned up partial ResourceManager initialization")
    
    async def shutdown(self):
        """Release all managed resources."""
        if self.executor:
            self.executor.shutdown(wait=True)
        
        await AsyncRedisClient.close_redis()
        
        if self.store:
            self.store.close()
        
        if self.embedding:
            self.embedding.cleanup()
        
        if self.mcp_manager:
            await self.mcp_manager.shutdown()
        
        self.chroma = None
        if self.llm_service:
            await self.llm_service.close()
        
        logger.info("ResourceManager shutdown complete")
        
        self.__class__._instance = None

    