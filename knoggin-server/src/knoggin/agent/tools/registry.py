import httpx
import redis.asyncio as aioredis
from core.entity_manager import EntityManager

from common.conf.topics_config import TopicConfig
from infrastructure.database.memgraph_client import MemgraphClient
from knoggin.agent.tools.graph import GraphToolsMixin
from knoggin.agent.tools.memory import MemoryToolsMixin
from knoggin.agent.tools.search import SearchToolsMixin
from knoggin.knowledge.services.file_rag import FileRAGService

TOOL_DISPATCH = {
    "search_messages": ("search_messages", ["query", "limit"]),
    "search_entity": ("search_entity", ["query", "limit"]),
    "get_connections": ("get_connections", ["entity_name"]),
    "get_recent_activity": ("get_recent_activity", ["entity_name", "hours"]),
    "fact_check": ("fact_check", ["entity_name", "query"]),
    "find_path": ("find_path", ["entity_a", "entity_b"]),
    "get_hierarchy": ("get_hierarchy", ["entity_name", "direction"]),
    "save_memory": ("save_memory", ["content", "topic"]),
    "forget_memory": ("forget_memory", ["memory_id"]),
    "search_files": ("search_files", ["query", "file_name", "limit"]),
    "web_search": ("web_search", ["query", "limit", "freshness"]),
    "news_search": ("news_search", ["query", "limit", "freshness"]),
    "request_clarification": None,  # handled specially
    "save_insight": ("save_insight", ["content"]),
    "spawn_specialist": (
        "spawn_specialist",
        ["name", "persona", "initial_rules", "initial_preferences", "initial_icks"],
    ),
}


class Tools(SearchToolsMixin, GraphToolsMixin, MemoryToolsMixin):
    def __init__(
        self,
        user_name: str,
        memgraph: MemgraphClient,
        entities: EntityManager,
        redis_client: aioredis.Redis,
        session_id: str,
        topic_config: TopicConfig = None,
        search_config: dict = None,
        file_rag: FileRAGService = None,
        mcp_manager=None,
        memory=None,
    ):
        self.session_id = session_id
        self.memgraph = memgraph
        self.entities = entities
        self.user_name = user_name
        self.redis = redis_client
        self.embedding_service = entities.embedding_service
        self.topic_config = topic_config
        self.file_rag = file_rag
        self.active_topics = topic_config.active_topics if topic_config else None
        self.search_cfg = search_config or {}
        self.mcp_manager = mcp_manager
        self.memory = memory

        self._http_client = httpx.AsyncClient(timeout=10.0)
