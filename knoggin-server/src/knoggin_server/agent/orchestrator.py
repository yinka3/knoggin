from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, AsyncGenerator, Dict, List, Optional

if TYPE_CHECKING:
    from knoggin_server.session.context import Context

import redis.asyncio as aioredis
from loguru import logger

from common.conf.manager import ConfigManager
from common.schema.agent_contracts import AgentConfig
from common.utils.json_utils import safe_json_loads
from infrastructure.redis_client import RedisKeys
from knoggin_server.agent.executor import AgentExecutor
from knoggin_server.agent.tools.registry import Tools
from knoggin_server.agent.types import (
    AgentContext,
    AgentRunConfig,
    AgentState,
    RetrievedEvidence,
)
from knoggin_server.knowledge.services.memory_service import MemoryManager


class Orchestrator:
    """
    Orchestrator manages the high-level flow of an agent run.
    It prepares the environment and delegates the reasoning loop to AgentExecutor.
    """

    def __init__(self):
        pass

    async def run_stream(
        self,
        user_query: str,
        user_name: str,
        session_id: str,
        context: Context,
        user_timezone: Optional[str] = None,
        model: Optional[str] = None,
        agent_id: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
        simulated_date: Optional[str] = None,
        agent_temperature: Optional[float] = None,
        agent_instructions: Optional[str] = None,
        agent_rules: Optional[List[str]] = None,
        agent_preferences: Optional[List[str]] = None,
        agent_icks: Optional[List[str]] = None,
        conversation_history: Optional[List[Dict]] = None,
        hot_topics: Optional[List[str]] = None,
        agent_persona_override: Optional[str] = None,
        agent_name_override: Optional[str] = None,
        client_tools: Optional[List[Dict]] = None,
    ) -> AsyncGenerator[Dict, None]:
        """
        Main entry point for agent execution. Uses modular helpers for initialization.
        """
        tools = None
        try:
            # Configuration
            config = ConfigManager.get().config
            limits = config.developer_settings.limits
            run_config = AgentRunConfig(
                max_calls=limits.max_tool_calls,
                max_attempts=limits.max_attempts,
                max_history_turns=limits.agent_history_turns,
                max_accumulated_messages=limits.max_accumulated_messages,
                max_consecutive_errors=limits.max_consecutive_errors,
                tool_limits=tuple(limits.tool_limits.items()),
            )

            # Services (Context-Aware)
            services = await self._bootstrap_services(context, agent_id)
            tools = services["tools"]
            memory_mgr = services["memory"]
            topic_config = services["topic_config"]

            # Identity & Persona
            identity = await self._resolve_agent_identity(
                user_name,
                context.redis_client,
                agent_id,
                agent_name_override,
                agent_persona_override,
            )
            agent_cfg = identity["config"]
            effective_model = model or (agent_cfg.model if agent_cfg else None)
            effective_temperature = (
                agent_temperature
                if agent_temperature is not None
                else (
                    agent_cfg.temperature
                    if agent_cfg and agent_cfg.temperature is not None
                    else 0.7
                )
            )

            # Context & State Assembly
            effective_hot_topics = (
                hot_topics if hot_topics is not None else topic_config.hot_topics
            )
            ctx = AgentContext(
                config=run_config,
                state=AgentState(),
                evidence=RetrievedEvidence(),
                user_name=user_name,
                session_id=session_id,
                user_query=user_query,
                run_id=str(uuid.uuid4()),
                hot_topics=effective_hot_topics,
                agent_name=identity["name"],
                agent_persona=identity["persona"],
                history=conversation_history or [],
            )

            # Execution via AgentExecutor
            executor = AgentExecutor(
                ctx, context.llm, tools, memory_mgr
            )

            async for event in executor.execute(
                user_timezone=user_timezone,
                model=effective_model,
                enabled_tools=enabled_tools
                or (agent_cfg.enabled_tools if agent_cfg else None),
                simulated_date=simulated_date,
                agent_temperature=effective_temperature,
                agent_instructions=agent_instructions
                or (agent_cfg.instructions if agent_cfg else None),
                agent_rules=agent_rules,
                agent_preferences=agent_preferences,
                agent_icks=agent_icks,
                client_tools=client_tools,
            ):
                yield event

        except Exception as e:
            logger.error(f"Orchestrator error: {e}")
            yield {"event": "error", "data": {"message": str(e)}}
        finally:
            if tools:
                await tools.close()

    async def _resolve_agent_identity(
        self,
        user_name: str,
        redis: aioredis.Redis,
        agent_id: Optional[str],
        name_override: Optional[str],
        persona_override: Optional[str],
    ) -> Dict:
        """Fetches agent profile and resolves final name/persona."""
        agent_cfg = None
        if agent_id:
            agent_data = await redis.hget(RedisKeys.agents(user_name), agent_id)
            if agent_data:
                try:
                    parsed_agent_data = safe_json_loads(agent_data)
                    if parsed_agent_data:
                        agent_cfg = AgentConfig.from_dict(parsed_agent_data)
                except Exception as e:
                    logger.warning(
                        f"Failed to parse agent config for '{agent_id}': {e}"
                    )

        return {
            "config": agent_cfg,
            "name": name_override or (agent_cfg.name if agent_cfg else "knoggin_server"),
            "persona": persona_override
            or (
                agent_cfg.persona
                if agent_cfg
                else "A helpful and thorough personal intelligence assistant."
            ),
        }

    async def _bootstrap_services(
        self,
        context: Context,
        agent_id: Optional[str] = None,
    ) -> Dict:
        """
        Retrieves pre-wired service components from the active Context and instantiates MemoryManager.
        """
        config = ConfigManager.get().config
        search_cfg = {
            **config.developer_settings.search.model_dump(),
            **config.search.model_dump(),
        }

        memory_mgr = MemoryManager(
            redis=context.redis_client,
            user_name=context.user_name,
            session_id=context.session_id,
            agent_id=agent_id or "default",
            topic_config=context.project.topic_config,
        )

        tools = Tools(
            user_name=context.user_name,
            entities=context.project.entities,
            session_id=context.session_id,
            topic_config=context.project.topic_config,
            search_config=search_cfg,
            file_rag=context.file_rag,
            memory=memory_mgr,
            graph_client=context.graph_client,
            redis=context.redis_client,
        )

        return {
            "topic_config": context.project.topic_config,
            "memory": memory_mgr,
            "entities": context.project.entities,
            "file_rag": context.file_rag,
            "tools": tools,
        }
