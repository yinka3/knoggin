"""Autonomous AAC opportunity seeding through the normal agent executor."""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import Any, Optional

from loguru import logger

from common.conf.manager import ConfigManager
from common.schema.agent.community_tools import (
    AAC_READ_TOOL_NAMES,
    AAC_SPECIFIC_SCHEMAS,
)
from common.schema.agent.identity import AgentConfig
from core.agent.executor import AgentExecutor
from core.agent.run import AgentIdentity, AgentRun, AgentRunLimits
from core.agent.services.agent_manager import AgentManager
from core.agent.tools.registry import Tools
from core.community.aac_store import AACStore
from core.community.read_context import AACReadContext
from core.community.token_budget import AACTokenBudget
from core.community.tools import AACTools


@dataclass(frozen=True, slots=True)
class SeedDecision:
    """The only durable outcome a Seeder is allowed to produce."""

    action: str
    topic: Optional[str] = None

    @classmethod
    def parse(cls, content: object) -> "SeedDecision":
        if not isinstance(content, str):
            return cls("SKIP")
        text = content.strip()
        if text.upper() == "SKIP":
            return cls("SKIP")
        match = re.fullmatch(r"START\s*\((.+)\)", text, flags=re.IGNORECASE | re.DOTALL)
        if match and match.group(1).strip():
            return cls("START", match.group(1).strip())
        return cls("SKIP")


class AACSeeder:
    """Run one read-capable Seeder opportunity check."""

    _RUN_LIMITS = AgentRunLimits(
        max_calls=8,
        max_attempts=6,
        max_history_turns=2,
        max_accumulated_messages=8,
        max_consecutive_errors=2,
        tool_limits=(
            ("search_entity", 2),
            ("get_connections", 2),
            ("find_path", 2),
            ("search_messages", 2),
            ("episode_check", 2),
            ("read_episode", 2),
            ("read_recent_episodes", 2),
            ("search_documents", 2),
            ("read_document", 2),
            ("list_documents", 2),
            ("search_insights", 2),
        ),
    )

    def __init__(
        self,
        *,
        user_name: str,
        resources: Any,
        read_context: AACReadContext,
        agent_manager: AgentManager,
        store: AACStore,
        config_provider=ConfigManager,
    ) -> None:
        self.user_name = user_name
        self.resources = resources
        self.read_context = read_context
        self.agent_manager = agent_manager
        self.store = store
        self.config_provider = config_provider

    async def decide(self, *, budget: Optional[AACTokenBudget] = None) -> SeedDecision:
        settings = self.config_provider.get().config.developer_settings.community
        budget = budget or AACTokenBudget(settings.token_budget)
        agent = await self._resolve_agent()
        if agent is None:
            return SeedDecision("SKIP")
        if self.resources.llm_service is None:
            return SeedDecision("SKIP")

        run_id = f"aac_seed_{uuid.uuid4().hex}"
        base_tools = Tools(
            user_name=self.user_name,
            entities=self.read_context.entities,
            session_id=f"aac-seed:{run_id}",
            compiled_domain=None,
            search_config={},
            document_service=self.read_context.documents,
            document_focus=None,
            knowledge_retrieval=self.read_context.knowledge_retrieval,
            knowledge_store=self.resources.knowledge_store,
            postgres=self.resources.postgres,
            redis=self.resources.redis,
            agent_id=agent.id,
            workspace_service=None,
        )
        tools = AACTools(
            user_name=self.user_name,
            base_tools=base_tools,
            store=self.store,
            agent_manager=self.agent_manager,
            discussion_id=f"seed:{run_id}",
            agent_id=agent.id,
        )
        read_insight_schema = [
            schema
            for schema in AAC_SPECIFIC_SCHEMAS
            if schema["function"]["name"] == "search_insights"
        ]
        run = AgentRun.open_aac(
            user_name=self.user_name,
            session_id=f"aac-seed:{run_id}",
            user_query=(
                "Inspect the user's accumulated knowledge with read tools. "
                "Decide whether there is something worth an AAC discussion. "
                "Finish by calling submit_answer with exactly SKIP or START(topic)."
            ),
            run_id=run_id,
            agent=AgentIdentity(
                config=agent,
                name=agent.name,
                persona=agent.persona_markdown,
            ),
            limits=self._RUN_LIMITS,
            model=agent.model,
            temperature=agent.temperature,
            brain=agent.brain,
            enabled_tools=list(AAC_READ_TOOL_NAMES),
            additional_tool_schemas=read_insight_schema,
        )
        executor = AgentExecutor(
            run,
            self.resources.llm_service,
            tools,
            on_successful_completion=self.agent_manager.mark_turn_completed,
            aac_budget=budget,
        )
        response: Optional[str] = None
        try:
            async for event in executor.execute():
                if event.get("event") == "response":
                    response = event.get("data", {}).get("content")
        except Exception as exc:
            logger.warning("AAC seeding failed; skipping opportunity: {}", exc)
        finally:
            await tools.close()
        return SeedDecision.parse(response)

    async def _resolve_agent(self) -> Optional[AgentConfig]:
        settings = self.config_provider.get().config.developer_settings.community
        if settings.seeding_agent_id:
            configured = await self.agent_manager.get_agent(settings.seeding_agent_id)
            if configured is not None:
                return configured
        try:
            default_id = await self.agent_manager.get_default_agent_id()
        except Exception:
            return None
        return await self.agent_manager.get_agent(default_id)
