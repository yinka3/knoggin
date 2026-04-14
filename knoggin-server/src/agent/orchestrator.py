import json
from typing import AsyncGenerator, Dict, List, Optional
from common.infra.resources import ResourceManager
from loguru import logger
import redis.asyncio as aioredis

from common.config.topics_config import TopicConfig
from common.services.memory_manager import MemoryManager
from common.config.base import get_config
from common.schema.dtypes import AgentConfig
from agent.internals import AgentContext, AgentRunConfig, AgentState, RetrievedEvidence
from agent.executor import AgentExecutor
from agent.tools import Tools
from core.entity_resolver import EntityResolver
from common.rag.file_rag import FileRAGService
from common.infra.redis import RedisKeys
import uuid
import os

class Orchestrator:
    """
    Orchestrator manages the high-level flow of an agent run.
    It prepares the environment and delegates the reasoning loop to AgentExecutor.
    """

    def __init__(self, resources: ResourceManager):
        self._resources = resources

    async def run_stream(
        self,
        user_query: str,
        user_name: str,
        session_id: str,
        redis: aioredis.Redis,
        user_timezone: Optional[str] = None,
        model: Optional[str] = None,
        agent_id: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
        simulated_date: Optional[str] = None,
        agent_temperature: float = 0.7,
        agent_instructions: Optional[str] = None,
        agent_rules: Optional[List[str]] = None,
        agent_preferences: Optional[List[str]] = None,
        agent_icks: Optional[List[str]] = None,
        conversation_history: Optional[List[Dict]] = None,
        hot_topics: Optional[List[str]] = None,
        agent_persona_override: Optional[str] = None,
        agent_name_override: Optional[str] = None,
        client_tools: Optional[List[Dict]] = None
    ) -> AsyncGenerator[Dict, None]:
        """
        Main entry point for agent execution.
        """
        tools = None
        try:
            # 1. Configuration & Initialization
            config = get_config()
            limits = config.developer_settings.limits

            run_config = AgentRunConfig(
                max_calls=limits.max_tool_calls,
                max_attempts=limits.max_attempts,
                max_history_turns=limits.agent_history_turns,
                max_accumulated_messages=limits.max_accumulated_messages,
                max_consecutive_errors=limits.max_consecutive_errors,
                tool_limits=tuple(limits.tool_limits.items()),
            )

            # 2. Preparation of Services
            topic_config = await TopicConfig.load(redis, user_name, session_id)
            memory_mgr = MemoryManager(
                redis=redis,
                user_name=user_name,
                session_id=session_id,
                agent_id=agent_id or "default",
                topic_config=topic_config
            )
            
            # Initialize EntityResolver for Tools
            er_cfg = config.developer_settings.entity_resolution
            resolver = EntityResolver(
                session_id=session_id,
                store=self._resources.store,
                embedding_service=self._resources.embedding,
                hierarchy_config=topic_config.hierarchy,
                fuzzy_substring_threshold=er_cfg.fuzzy_substring_threshold,
                fuzzy_non_substring_threshold=er_cfg.fuzzy_non_substring_threshold,
                generic_token_freq=er_cfg.generic_token_freq,
                candidate_fuzzy_threshold=er_cfg.candidate_fuzzy_threshold,
                candidate_vector_threshold=er_cfg.candidate_vector_threshold
            )
            
            # Initialize FileRAG for Tools
            upload_dir = os.path.join(os.getenv("CONFIG_DIR", "./config"), "uploads")
            file_rag = FileRAGService(
                session_id=session_id,
                chroma_client=self._resources.chroma,
                embedding_service=self._resources.embedding,
                upload_dir=upload_dir
            )
            
            search_cfg = config.developer_settings.search.model_dump()
            tools = Tools(
                user_name=user_name,
                store=self._resources.store,
                ent_resolver=resolver,
                redis_client=redis,
                session_id=session_id,
                topic_config=topic_config,
                search_config=search_cfg,
                file_rag=file_rag,
                mcp_manager=self._resources.mcp_manager,
                memory=memory_mgr
            )
            
            effective_hot_topics = hot_topics if hot_topics is not None else topic_config.hot_topics
            
            # 3. Context & State
            agent_cfg = None
            if agent_id:
                agent_data = await redis.hget(RedisKeys.agents(user_name), agent_id)
                if agent_data:
                    try:
                        agent_cfg = AgentConfig.from_dict(json.loads(agent_data))
                    except (json.JSONDecodeError, Exception) as e:
                        logger.warning(f"Failed to parse agent config for '{agent_id}': {e}")
            
            p_name = agent_name_override or (agent_cfg.name if agent_cfg else "Knoggin")
            p_persona = agent_persona_override or (agent_cfg.persona if agent_cfg else "A helpful and thorough personal intelligence assistant.")

            ctx = AgentContext(
                config=run_config,
                state=AgentState(),
                evidence=RetrievedEvidence(),
                user_name=user_name,
                session_id=session_id,
                user_query=user_query,
                run_id=str(uuid.uuid4()),
                hot_topics=effective_hot_topics,
                agent_name=p_name,
                agent_persona=p_persona,
                history=conversation_history or []
            )

            # 4. Execution via AgentExecutor
            executor = AgentExecutor(ctx, self._resources.llm_service, tools, memory_mgr)
            
            async for event in executor.execute(
                user_timezone=user_timezone,
                model=model,
                enabled_tools=enabled_tools or (agent_cfg.enabled_tools if agent_cfg else None),
                simulated_date=simulated_date,
                agent_temperature=agent_temperature,
                agent_instructions=agent_instructions or (agent_cfg.instructions if agent_cfg else None),
                agent_rules=agent_rules,
                agent_preferences=agent_preferences,
                agent_icks=agent_icks,
                client_tools=client_tools
            ):
                yield event

        except Exception as e:
            logger.error(f"Orchestrator error: {e}")
            yield {"event": "error", "data": {"message": str(e)}}
        finally:
            if tools:
                await tools.close()