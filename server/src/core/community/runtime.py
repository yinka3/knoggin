"""Application-owned AAC discussion runtime."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from enum import Enum
from functools import partial
from typing import Any, Callable, Optional

from loguru import logger

from common.conf.manager import ConfigManager
from common.schema.agent.community_tools import (
    AAC_DEFAULT_ENABLED_TOOLS,
    AAC_READ_TOOL_NAMES,
    AAC_SPECIFIC_SCHEMAS,
)
from common.schema.agent.identity import AgentConfig
from common.utils.lifecycle import settle_owned_task
from core.agent.executor import AgentExecutor
from core.agent.run import AgentIdentity, AgentRun, AgentRunLimits
from core.agent.services.agent_manager import AgentManager
from core.agent.tools.registry import Tools
from core.community.aac_store import AACStore
from core.community.execution import config_owner, execute_aac_run
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
                "search_knowledge_entities": 4,
                "get_entity_relationships": 3,
                "find_relationship_path": 3,
                "get_entity_recent_activity": 3,
                "search_knowledge_messages": 3,
                "search_episodes": 4,
                "read_episode_messages": 4,
                "read_recent_episodes": 4,
                "search_project_documents": 4,
                "read_project_document": 4,
                "list_project_documents": 2,
                "search_community_insights": 3,
                "save_community_insight": 4,
                "vote_community_insight": 4,
                "remove_community_insight_vote": 4,
                "spawn_community_specialist": 2,
                "consult_community_specialist": 2,
                "edit_agent_brain": 2,
                "restore_agent_brain_section": 2,
            }.items()
        ),
    )
    _SPECIALIST_ENABLED_TOOLS = frozenset(
        [
            *AAC_READ_TOOL_NAMES,
            "search_community_insights",
            "edit_agent_brain",
            "restore_agent_brain_section",
        ]
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
        config = config_owner(config_provider).config
        read_context = await AACReadContext.create(
            user_name=user_name,
            postgres=resources.postgres,
            knowledge_store=resources.knowledge_store,
            embedding_service=resources.embedding,
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
        self._participation_write_lock = asyncio.Lock()
        self._pending_participation_events: dict[str, dict] = {}
        self._shutdown_event = asyncio.Event()
        self._discussion_stop_event = asyncio.Event()
        self._opportunity_wake_event = asyncio.Event()
        self._opportunity_task: Optional[asyncio.Task] = None
        self._discussion_task: Optional[asyncio.Task] = None
        self._discussion_id: Optional[str] = None
        self._participants: list[str] = []
        self._discussion_history: Optional[list[dict[str, str]]] = None
        self._config_unsubscribe: Optional[Callable[[], None]] = None
        self._stopping = False
        self._started = False
        self._shutdown_lock = asyncio.Lock()
        self._pending_finalizations: dict[str, dict] = {}
        self._pending_stop_events: dict[str, dict] = {}

    @property
    def active_discussion_id(self) -> Optional[str]:
        return self._discussion_id

    async def start(self) -> None:
        """Recover stale rows and start the local, config-reactive opportunity loop."""
        async with self._ownership_lock:
            if self._started:
                return
            if self._stopping:
                raise RuntimeError("AAC runtime is shutting down")
            await self.store.interrupt_active_discussions(user_name=self.user_name)
            self._subscribe_to_config()
            self._started = True
            self._opportunity_task = asyncio.create_task(
                self._opportunity_loop(),
                name=f"aac-opportunities:{self.user_name}",
            )

    async def shutdown(self) -> None:
        """Stop future opportunities and let the current participant turn finish."""

        # Close admission before scheduling owned cleanup or waiting for its lock.
        self._stopping = True
        self._shutdown_event.set()
        self._discussion_stop_event.set()
        self._opportunity_wake_event.set()
        async with self._shutdown_lock:
            cleanup = asyncio.create_task(self._shutdown_owned())
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                try:
                    await settle_owned_task(cleanup)
                except Exception:
                    logger.exception("AAC cleanup failed while caller was cancelled")
                raise

    async def _shutdown_owned(self) -> None:

        self._stopping = True
        self._shutdown_event.set()
        self._discussion_stop_event.set()
        self._opportunity_wake_event.set()
        async with self._ownership_lock:
            pass  # Wait for a pending durable admission to publish its task.
        failures = []
        unsubscribe = self._config_unsubscribe
        if unsubscribe is not None:
            try:
                unsubscribe()
            except Exception as exc:
                logger.exception("Failed to unsubscribe AAC config listener")
                failures.append(exc)
            else:
                self._config_unsubscribe = None
        opportunity = self._opportunity_task
        if opportunity is not None and not opportunity.done():
            opportunity.cancel()
            await asyncio.gather(opportunity, return_exceptions=True)
        self._opportunity_task = None

        discussion = self._discussion_task
        if discussion is not None and not discussion.done():
            await asyncio.gather(discussion, return_exceptions=True)
        try:
            await self._retry_finalizations()
        except Exception as exc:
            failures.append(exc)
        if failures:
            raise RuntimeError("AAC shutdown cleanup failed") from failures[0]

    async def _retry_finalizations(self) -> None:
        failures = []
        try:
            await self._retry_participation_events()
        except Exception as exc:
            failures.append(exc)
        for discussion_id, event in tuple(self._pending_stop_events.items()):
            try:
                await self.store.append_timeline(**event)
            except Exception as exc:
                failures.append(exc)
            else:
                del self._pending_stop_events[discussion_id]
        for discussion_id, outcome in tuple(self._pending_finalizations.items()):
            try:
                await self.store.finish_discussion(**outcome)
            except Exception as exc:
                failures.append(exc)
            else:
                del self._pending_finalizations[discussion_id]
        if failures:
            raise RuntimeError("AAC discussion finalization failed") from failures[0]

    async def trigger_discussion(self) -> AACAdmission:
        """Run one seed check and admit at most one local discussion."""

        async with self._ownership_lock:
            if self._stopping:
                return AACAdmission(AACAdmissionOutcome.SKIPPED, "shutting_down")
            await self._retry_finalizations()
            if not self._community_settings().enabled:
                return AACAdmission(AACAdmissionOutcome.SKIPPED, "disabled")
            if self._discussion_task is not None and not self._discussion_task.done():
                return AACAdmission(AACAdmissionOutcome.SKIPPED, "already_active")

            await self._refresh_read_context()
            participants = await self._enabled_participants()
            if not participants:
                return AACAdmission(AACAdmissionOutcome.SKIPPED, "no_enabled_agents")

            budget = AACTokenBudget(self._community_settings().token_budget)
            decision = await self.seeder.decide(budget=budget)
            if decision.action != "START" or not decision.topic:
                return AACAdmission(AACAdmissionOutcome.SKIPPED, "no_seed")

            if self._stopping:
                return AACAdmission(AACAdmissionOutcome.SKIPPED, "shutting_down")
            if not self._community_settings().enabled:
                return AACAdmission(AACAdmissionOutcome.SKIPPED, "disabled")
            participants = await self._enabled_participants()
            if not participants:
                return AACAdmission(AACAdmissionOutcome.SKIPPED, "no_enabled_agents")

            discussion_id = str(uuid.uuid4())
            write = asyncio.create_task(self.store.create_discussion(
                discussion_id=discussion_id,
                user_name=self.user_name,
                topic=decision.topic,
                token_budget=budget.limit,
            ))
            cancelled = False
            try:
                await asyncio.shield(write)
            except asyncio.CancelledError:
                await settle_owned_task(write)
                cancelled = True
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
            if cancelled:
                raise asyncio.CancelledError
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
        if not self._discussion_stop_event.is_set():
            self._discussion_stop_event.set()
            content = "AAC discussion stop requested by user."
            self._pending_stop_events[discussion_id] = dict(
                discussion_id=discussion_id, user_name=self.user_name,
                kind="system_event", content=content,
                timeline_id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"aac:{discussion_id}:user-stop")),
            )
            if self._discussion_history is not None:
                self._discussion_history.append({"role": "system", "content": content})
                del self._discussion_history[:-8]
        event = self._pending_stop_events.get(discussion_id)
        if event is not None:
            await self.store.append_timeline(**event)
            self._pending_stop_events.pop(discussion_id, None)
        return True

    async def list_discussions(self, *, limit: int = 20) -> list[dict[str, Any]]:
        """Expose the current user's durable AAC discussion history."""

        return await self.store.list_discussions(user_name=self.user_name, limit=limit)

    async def list_timeline(
        self,
        discussion_id: str,
        *,
        limit: int = 100,
        after_sequence: int = 0,
    ) -> list[dict[str, Any]]:
        """Expose one user-owned AAC transcript and its system events."""

        return await self.store.list_timeline(
            discussion_id=discussion_id,
            user_name=self.user_name,
            limit=limit,
            after_sequence=after_sequence,
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
            await self._reconcile_participants(history=self._discussion_history)
        return agent

    async def _opportunity_loop(self) -> None:
        while not self._stopping:
            self._opportunity_wake_event.clear()
            if self._community_settings().enabled:
                try:
                    await self.trigger_discussion()
                except Exception:
                    logger.exception("AAC opportunity check failed")
            try:
                await asyncio.wait_for(
                    self._opportunity_wake_event.wait(),
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
        end_reason = "completed"
        history: list[dict[str, str]] = []
        self._discussion_history = history
        turn = 0
        try:
            await self._append_timeline_event(
                discussion_id,
                f"AAC discussion started: {topic}",
                history=history,
            )
            while True:
                if self._shutdown_event.is_set():
                    status = "interrupted"
                    end_reason = "shutdown"
                    break
                if self._discussion_stop_event.is_set():
                    status = "stopped"
                    end_reason = "user_stopped"
                    break
                if not budget.allow_call():
                    end_reason = "token_budget"
                    break
                current = await self._reconcile_participants(history=history)
                if not current:
                    end_reason = "no_participants"
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
                    status = "failed"
                    end_reason = "failed"
                    break
                history.append({"role": "assistant", "agent_id": agent.id, "content": response})
                del history[:-8]
                await self.store.append_timeline(
                    discussion_id=discussion_id,
                    user_name=self.user_name,
                    kind="agent_message",
                    agent_id=agent.id,
                    content=response,
                )
        except asyncio.CancelledError:
            status = "interrupted"
            end_reason = "shutdown" if self._shutdown_event.is_set() else "interrupted"
            raise
        except Exception:
            status = "failed"
            end_reason = "failed"
            logger.exception("AAC discussion {} failed", discussion_id)
        finally:
            if end_reason == "user_stopped":
                try:
                    await self._append_timeline_event(
                        discussion_id,
                        "Discussion stopped by user.",
                        history=history,
                    )
                except Exception:
                    logger.exception(
                        "Failed to record AAC user-stop event for {}", discussion_id
                    )
            outcome = dict(discussion_id=discussion_id, user_name=self.user_name,
                           status=status, end_reason=end_reason, tokens_used=budget.used)
            self._pending_finalizations[discussion_id] = outcome
            try:
                await self.store.finish_discussion(**outcome)
            except Exception:
                logger.exception("Failed to finalize AAC discussion {}", discussion_id)
            else:
                self._pending_finalizations.pop(discussion_id, None)
            finally:
                if self._discussion_history is history:
                    self._discussion_history = None

    async def _agent_turn(
        self,
        *,
        discussion_id: str,
        topic: str,
        agent: AgentConfig,
        history: list[dict[str, str]],
        participants: list[str],
        budget: AACTokenBudget,
        is_specialist: bool = False,
    ) -> Optional[str]:
        await self._refresh_read_context()
        run_id = f"aac_run_{uuid.uuid4().hex}"
        base_tools = Tools(
            user_name=self.user_name,
            project_id=self.read_context.knowledge_retrieval.project_id,
            session_id=f"aac:{discussion_id}",
            compiled_domain=None,
            search_config={},
            document_service=self.read_context.documents,
            document_focus=None,
            knowledge_retrieval=self.read_context.knowledge_retrieval,
            knowledge_store=self.resources.knowledge_store,
            postgres=self.resources.postgres,
            agent_id=agent.id,
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
        configured_tools = (
            AAC_DEFAULT_ENABLED_TOOLS if agent.enabled_tools is None else agent.enabled_tools
        )
        enabled = [
            name for name in configured_tools if name in AAC_DEFAULT_ENABLED_TOOLS
        ]
        if is_specialist:
            enabled = [
                name for name in enabled if name in self._SPECIALIST_ENABLED_TOOLS
            ]
        additional = [
            schema
            for schema in AAC_SPECIFIC_SCHEMAS
            if schema["function"]["name"] in enabled
        ]
        open_run = partial(AgentRun.open_aac,
            user_name=self.user_name,
            session_id=f"aac:{discussion_id}",
            user_query=(
                (
                    f"Private specialist task: {topic}\n"
                    "Investigate with read tools and call submit_answer with your "
                    "advice for the parent agent."
                    if is_specialist
                    else f"AAC discussion topic: {topic}\n"
                    "Reason with the other participants, use read tools for evidence, "
                    "and call submit_answer with your contribution."
                )
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
            is_community=not is_specialist,
            current_participants=participants,
        )
        response = await execute_aac_run(
            open_run=open_run, executor_factory=AgentExecutor,
            llm=self.resources.llm_service, tools=tools,
            on_completion=self.agent_manager.mark_turn_completed, budget=budget,
        )
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
                is_specialist=True,
            ) or ""

        return run_specialist

    async def _enabled_participants(self) -> list[str]:
        agents = [agent for agent in await self.agent_manager.list_agents() if agent.aac_enabled]
        return sorted(agent.id for agent in agents)

    async def _reconcile_participants(
        self,
        *,
        history: Optional[list[dict[str, str]]] = None,
    ) -> list[str]:
        """Reflect durable AAC choices in an active discussion's next turn."""

        enabled = await self._enabled_participants()
        async with self._participants_lock:
            previous = set(self._participants)
            current = set(enabled)
            joined = sorted(current - previous)
            left = sorted(previous - current)
            self._participants = enabled

            if self._discussion_id:
                for agents, action in ((joined, "joined"), (left, "left")):
                    for agent_id in agents:
                        content = f"Agent {agent_id} {action} the discussion."
                        event_id = str(uuid.uuid4())
                        self._pending_participation_events[event_id] = dict(
                            discussion_id=self._discussion_id,
                            user_name=self.user_name,
                            kind="system_event",
                            content=content,
                            timeline_id=event_id,
                        )
                        # Membership is effective even if transcript storage fails.
                        if history is not None:
                            history.append({"role": "system", "content": content})
                            del history[:-8]
        await self._retry_participation_events()
        return list(enabled)

    async def _retry_participation_events(self) -> None:
        """Save each transition once, retaining ordered retries after failure."""
        async with self._participation_write_lock:
            for event_id, event in tuple(self._pending_participation_events.items()):
                await self.store.append_timeline(**event)
                del self._pending_participation_events[event_id]

    def _community_settings(self):
        return config_owner(self.config_provider).config.developer_settings.community

    def _subscribe_to_config(self) -> None:
        if self._config_unsubscribe is not None:
            return
        subscribe = getattr(config_owner(self.config_provider), "subscribe", None)
        if callable(subscribe):
            self._config_unsubscribe = subscribe(
                self._on_community_settings_changed,
                "developer_settings.community",
            )

    def _on_community_settings_changed(self, _settings: object) -> None:
        """Wake the local loop so enabled/cadence changes take effect promptly."""

        # Disabling AAC blocks future opportunities; an admitted discussion
        # continues until its budget ends or the user requests a graceful stop.
        self._opportunity_wake_event.set()

    async def _refresh_read_context(self) -> None:
        """Refresh user project visibility before each AAC decision or run."""

        config = config_owner(self.config_provider).config
        context = await AACReadContext.create(
            user_name=self.user_name,
            postgres=self.resources.postgres,
            knowledge_store=self.resources.knowledge_store,
            embedding_service=self.resources.embedding,
            search_config={
                **config.developer_settings.search.model_dump(),
                **config.search.model_dump(),
            },
        )
        self.read_context = context
        if isinstance(self.seeder, AACSeeder):
            self.seeder.read_context = context

    def _clear_discussion(self, task: asyncio.Task) -> None:
        if self._discussion_task is task:
            self._discussion_task = None
            self._discussion_id = None
            self._discussion_stop_event.clear()
            self._discussion_history = None

    async def _append_event(
        self,
        content: str,
        *,
        history: Optional[list[dict[str, str]]] = None,
    ) -> None:
        if self._discussion_id:
            await self._append_timeline_event(
                self._discussion_id,
                content,
                history=history,
            )

    async def _append_timeline_event(
        self,
        discussion_id: str,
        content: str,
        *,
        history: Optional[list[dict[str, str]]] = None,
    ) -> None:
        await self.store.append_timeline(
            discussion_id=discussion_id,
            user_name=self.user_name,
            kind="system_event",
            content=content,
        )
        if history is not None:
            history.append({"role": "system", "content": content})
            del history[:-8]
