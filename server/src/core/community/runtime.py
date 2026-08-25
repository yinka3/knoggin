"""Application-owned AAC discussion runtime."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

from loguru import logger

from common.conf.manager import ConfigManager
from common.schema.agent.community_tools import (
    AAC_DEFAULT_ENABLED_TOOLS,
    AAC_SPECIFIC_SCHEMAS,
)
from common.schema.agent.identity import AgentConfig
from core.agent.executor import AgentExecutor
from core.agent.run import AgentIdentity, AgentRun, AgentRunLimits
from core.agent.services.agent_manager import AgentManager
from core.agent.tools.registry import Tools
from core.community.aac_store import AACStore
from core.community.read_context import AACReadContext
from core.community.seeding import AACSeeder
from core.community.token_budget import AACTokenBudget
from core.community.tools import AACTools


class AACAdmissionOutcome(str, Enum):
    STARTED = "started"
    SKIPPED = "skipped"


@dataclass(frozen=True, slots=True)
class AACAdmission:
    outcome: AACAdmissionOutcome
    reason: str
    discussion_id: Optional[str] = None


class AACRuntime:
    """Own one user's AAC opportunity loop and active discussion task."""

    _TURN_LIMITS = AgentRunLimits(
        max_calls=12,
        max_attempts=15,
        max_history_turns=7,
        max_accumulated_messages=30,
        max_consecutive_errors=3,
        tool_limits=tuple(
            {
                "search_entity": 4,
                "get_connections": 3,
                "find_path": 3,
                "get_recent_activity": 3,
                "search_messages": 3,
                "episode_check": 4,
                "read_episode": 4,
                "read_recent_episodes": 4,
                "search_documents": 4,
                "read_document": 4,
                "list_documents": 2,
                "search_insights": 3,
                "save_insight": 4,
                "vote_insight": 4,
                "remove_insight_vote": 4,
                "spawn_specialist": 2,
                "consult_specialist": 2,
                "edit_brain": 2,
                "restore_brain_section": 2,
            }.items()
        ),
    )

    @classmethod
    async def create(
        cls,
        *,
        user_name: str,
        resources: Any,
        agent_manager: AgentManager,
        config_provider=ConfigManager,
    ) -> "AACRuntime":
        config = config_provider.get().config
        read_context = await AACReadContext.create(
            user_name=user_name,
            postgres=resources.postgres,
            knowledge_store=resources.knowledge_store,
            embedding_service=resources.embedding,
            redis=resources.redis,
            search_config={
                **config.developer_settings.search.model_dump(),
                **config.search.model_dump(),
            },
        )
        return cls(
            user_name=user_name,
            resources=resources,
            agent_manager=agent_manager,
            read_context=read_context,
            store=AACStore(resources.postgres),
            config_provider=config_provider,
        )

    def __init__(
        self,
        *,
        user_name: str,
        resources: Any,
        agent_manager: AgentManager,
        read_context: AACReadContext,
        store: AACStore,
        config_provider=ConfigManager,
        seeder: Optional[AACSeeder] = None,
    ) -> None:
        self.user_name = user_name
        self.resources = resources
        self.agent_manager = agent_manager
        self.read_context = read_context
        self.store = store
        self.config_provider = config_provider
        self.seeder = seeder or AACSeeder(
            user_name=user_name,
            resources=resources,
            read_context=read_context,
            agent_manager=agent_manager,
            store=store,
            config_provider=config_provider,
        )
        self._ownership_lock = asyncio.Lock()
        self._participants_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._discussion_stop_event = asyncio.Event()
        self._opportunity_task: Optional[asyncio.Task] = None
        self._discussion_task: Optional[asyncio.Task] = None
        self._discussion_id: Optional[str] = None
        self._participants: list[str] = []
        self._stopping = False

    @property
    def active_discussion_id(self) -> Optional[str]:
        return self._discussion_id

    async def start(self) -> None:
        """Recover stale rows and start the local opportunity loop if enabled."""

        self._stopping = False
        self._shutdown_event.clear()
        self._discussion_stop_event.clear()
        await self.store.interrupt_active_discussions(user_name=self.user_name)
        if self._community_settings().enabled and self._opportunity_task is None:
            self._opportunity_task = asyncio.create_task(
                self._opportunity_loop(),
                name=f"aac-opportunities:{self.user_name}",
            )

    async def shutdown(self) -> None:
        """Stop future opportunities and let the current participant turn finish."""

        self._stopping = True
        self._shutdown_event.set()
        self._discussion_stop_event.set()
        opportunity = self._opportunity_task
        self._opportunity_task = None
        if opportunity is not None and not opportunity.done():
            opportunity.cancel()
            await asyncio.gather(opportunity, return_exceptions=True)

        discussion = self._discussion_task
        if discussion is not None and not discussion.done():
            await asyncio.gather(discussion, return_exceptions=True)

    async def trigger_discussion(self) -> AACAdmission:
        """Run one seed check and admit at most one local discussion."""

        async with self._ownership_lock:
            if self._discussion_task is not None and not self._discussion_task.done():
                return AACAdmission(AACAdmissionOutcome.SKIPPED, "already_active")

            participants = await self._enabled_participants()
            if not participants:
                return AACAdmission(AACAdmissionOutcome.SKIPPED, "no_enabled_agents")

            budget = AACTokenBudget(self._community_settings().token_budget)
            decision = await self.seeder.decide(budget=budget)
            if decision.action != "START" or not decision.topic:
                return AACAdmission(AACAdmissionOutcome.SKIPPED, "no_seed")

            discussion_id = str(uuid.uuid4())
            await self.store.create_discussion(
                discussion_id=discussion_id,
                user_name=self.user_name,
                topic=decision.topic,
                token_budget=budget.limit,
            )
            self._discussion_id = discussion_id
            self._participants = participants
            self._discussion_task = asyncio.create_task(
                self._run_discussion(
                    discussion_id=discussion_id,
                    topic=decision.topic,
                    participants=participants,
                    budget=budget,
                ),
                name=f"aac-discussion:{self.user_name}:{discussion_id}",
            )
            self._discussion_task.add_done_callback(self._clear_discussion)
            return AACAdmission(
                AACAdmissionOutcome.STARTED,
                "admitted",
                discussion_id,
            )

    async def request_stop(self) -> bool:
        """Stop an active discussion after its current AgentRun finishes."""

        discussion_id = self._discussion_id
        discussion = self._discussion_task
        if discussion_id is None or discussion is None or discussion.done():
            return False
        self._discussion_stop_event.set()
        await self.store.append_timeline(
            discussion_id=discussion_id,
            user_name=self.user_name,
            kind="system_event",
            content="AAC discussion stop requested by user.",
        )
        return True

    async def list_discussions(self, *, limit: int = 20) -> list[dict[str, Any]]:
        """Expose the current user's durable AAC discussion history."""

        return await self.store.list_discussions(user_name=self.user_name, limit=limit)

    async def list_timeline(
        self,
        discussion_id: str,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Expose one user-owned AAC transcript and its system events."""

        return await self.store.list_timeline(
            discussion_id=discussion_id,
            user_name=self.user_name,
            limit=limit,
        )

    async def list_insights(
        self,
        *,
        query: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Expose shared and private Insights to their owning user."""

        return await self.store.list_user_insights(
            user_name=self.user_name,
            query=query,
            limit=limit,
        )

    async def list_insight_votes(
        self,
        insight_id: str,
    ) -> list[dict[str, Any]]:
        """Expose advisory AAC Insight votes to their owning user."""

        return await self.store.list_insight_votes(
            insight_id=insight_id,
            user_name=self.user_name,
        )

    async def set_participation(
        self,
        agent_id: str,
        enabled: bool,
    ) -> Optional[AgentConfig]:
        """Persist an AAC participation choice and apply it to active work.

        The durable flag remains the source of truth.  Reconciling immediately
        gives a caller deterministic join/leave events; the discussion loop
        also reconciles before every participant run for callers that update
        ``AgentManager`` directly.
        """

        agent = await self.agent_manager.set_aac_enabled(agent_id, enabled)
        if agent is not None and self._discussion_id is not None:
            await self._reconcile_participants()
        return agent

    async def _opportunity_loop(self) -> None:
        while not self._stopping:
            try:
                await self.trigger_discussion()
            except Exception:
                logger.exception("AAC opportunity check failed")
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self._community_settings().interval_minutes * 60,
                )
            except asyncio.TimeoutError:
                continue

    async def _run_discussion(
        self,
        *,
        discussion_id: str,
        topic: str,
        participants: list[str],
        budget: AACTokenBudget,
    ) -> None:
        status = "completed"
        history: list[dict[str, str]] = []
        await self.store.append_timeline(
            discussion_id=discussion_id,
            user_name=self.user_name,
            kind="system_event",
            content=f"AAC discussion started: {topic}",
        )
        turn = 0
        try:
            while budget.allow_call() and not self._discussion_stop_event.is_set():
                current = await self._reconcile_participants()
                if not current:
                    break
                agent_id = current[turn % len(current)]
                turn += 1
                agent = await self.agent_manager.get_agent(agent_id)
                if agent is None or not agent.aac_enabled:
                    continue
                response = await self._agent_turn(
                    discussion_id=discussion_id,
                    topic=topic,
                    agent=agent,
                    history=history,
                    participants=current,
                    budget=budget,
                )
                if not response:
                    break
                history.append({"role": "assistant", "agent_id": agent.id, "content": response})
                history = history[-8:]
                await self.store.append_timeline(
                    discussion_id=discussion_id,
                    user_name=self.user_name,
                    kind="agent_message",
                    agent_id=agent.id,
                    content=response,
                )
        except asyncio.CancelledError:
            status = "interrupted"
            raise
        except Exception:
            status = "failed"
            logger.exception("AAC discussion {} failed", discussion_id)
        finally:
            if self._discussion_stop_event.is_set() and status == "completed":
                status = "stopped"
            try:
                await self.store.finish_discussion(
                    discussion_id=discussion_id,
                    user_name=self.user_name,
                    status=status,
                    tokens_used=budget.used,
                )
            except Exception:
                logger.exception("Failed to finalize AAC discussion {}", discussion_id)

    async def _agent_turn(
        self,
        *,
        discussion_id: str,
        topic: str,
        agent: AgentConfig,
        history: list[dict[str, str]],
        participants: list[str],
        budget: AACTokenBudget,
    ) -> Optional[str]:
        run_id = f"aac_run_{uuid.uuid4().hex}"
        base_tools = Tools(
            user_name=self.user_name,
            entities=self.read_context.entities,
            session_id=f"aac:{discussion_id}",
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
            discussion_id=discussion_id,
            agent_id=agent.id,
            specialist_runner=(
                None
                if not participants
                else self._specialist_runner(
                    discussion_id=discussion_id,
                    topic=topic,
                    budget=budget,
                )
            ),
        )
        enabled = list(agent.enabled_tools or AAC_DEFAULT_ENABLED_TOOLS)
        additional = [
            schema
            for schema in AAC_SPECIFIC_SCHEMAS
            if schema["function"]["name"] in enabled
        ]
        run = AgentRun.open_aac(
            user_name=self.user_name,
            session_id=f"aac:{discussion_id}",
            user_query=(
                f"AAC discussion topic: {topic}\n"
                "Reason with the other participants, use read tools for evidence, "
                "and call submit_answer with your contribution."
            ),
            run_id=run_id,
            agent=AgentIdentity(config=agent, name=agent.name, persona=agent.persona_markdown),
            limits=self._TURN_LIMITS,
            model=agent.model,
            temperature=agent.temperature,
            brain=agent.brain,
            enabled_tools=enabled,
            additional_tool_schemas=additional,
            history=history,
            is_community=True,
            current_participants=participants,
        )
        response: Optional[str] = None
        executor = AgentExecutor(
            run,
            self.resources.llm_service,
            tools,
            on_successful_completion=self.agent_manager.mark_turn_completed,
            aac_budget=budget,
        )
        try:
            async for event in executor.execute():
                if event.get("event") == "response":
                    response = event.get("data", {}).get("content")
        finally:
            await tools.close()
        return response.strip() if isinstance(response, str) and response.strip() else None

    def _specialist_runner(self, *, discussion_id: str, topic: str, budget: AACTokenBudget):
        async def run_specialist(agent: AgentConfig, question: str) -> object:
            return await self._agent_turn(
                discussion_id=discussion_id,
                topic=f"{topic}\nPrivate specialist question: {question}",
                agent=agent,
                history=[],
                participants=[],
                budget=budget,
            ) or ""

        return run_specialist

    async def _enabled_participants(self) -> list[str]:
        agents = [agent for agent in await self.agent_manager.list_agents() if agent.aac_enabled]
        return sorted(agent.id for agent in agents)

    async def _reconcile_participants(self) -> list[str]:
        """Reflect durable AAC choices in an active discussion's next turn."""

        enabled = await self._enabled_participants()
        async with self._participants_lock:
            previous = set(self._participants)
            current = set(enabled)
            joined = sorted(current - previous)
            left = sorted(previous - current)
            self._participants = enabled

        for agent_id in joined:
            await self._append_event(f"Agent {agent_id} joined the discussion.")
        for agent_id in left:
            await self._append_event(f"Agent {agent_id} left the discussion.")
        return list(enabled)

    def _community_settings(self):
        return self.config_provider.get().config.developer_settings.community

    def _clear_discussion(self, task: asyncio.Task) -> None:
        if self._discussion_task is task:
            self._discussion_task = None
            self._discussion_id = None
            self._discussion_stop_event.clear()

    async def _append_event(self, content: str) -> None:
        if self._discussion_id:
            await self.store.append_timeline(
                discussion_id=self._discussion_id,
                user_name=self.user_name,
                kind="system_event",
                content=content,
            )
