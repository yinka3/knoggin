from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, AsyncGenerator, Dict, List, Optional

from loguru import logger

from common.conf.manager import ConfigManager
from common.schema.agent_stream import PublicAgentStreamEvent
from common.schema.document import DocumentFocus
from common.utils.json_utils import safe_json_loads
from core.agent.executor import AgentExecutor
from core.agent.maintenance import build_maintenance_candidates
from core.agent.services.agent_manager import AgentManager
from core.agent.tools.registry import Tools
from core.agent.types import (
    AgentContext,
    AgentRunConfig,
    AgentState,
    RetrievedEvidence,
)

if TYPE_CHECKING:
    from core.session.context import Session


PUBLIC_AGENT_ERROR_MESSAGE = (
    "The agent couldn't complete this request. Please try again."
)


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
        context: Session,
        user_timezone: Optional[str] = None,
        model: Optional[str] = None,
        agent_id: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
        simulated_date: Optional[str] = None,
        agent_temperature: Optional[float] = None,
        agent_brain: Optional[str] = None,
        agent_directives: Optional[str] = None,
        conversation_history: Optional[List[Dict]] = None,
        hot_topics: Optional[List[str]] = None,
        agent_persona_override: Optional[str] = None,
        agent_name_override: Optional[str] = None,
        client_tools: Optional[List[Dict]] = None,
    ) -> AsyncGenerator[PublicAgentStreamEvent, None]:
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
                tool_timeout=limits.tool_timeout,
                max_attempts=limits.max_attempts,
                max_history_turns=limits.agent_history_turns,
                max_accumulated_messages=limits.max_accumulated_messages,
                max_consecutive_errors=limits.max_consecutive_errors,
                tool_limits=tuple(limits.tool_limits.items()),
            )

            # Identity & Persona
            identity = await self._resolve_agent_identity(
                context,
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

            # Services (Session-Aware)
            services = await self._bootstrap_services(
                context,
                agent_cfg.id if agent_cfg else None,
            )
            tools = services["tools"]
            topic_config = services["topic_config"]

            effective_enabled_tools = (
                enabled_tools
                if enabled_tools is not None
                else (agent_cfg.enabled_tools if agent_cfg else None)
            )
            maintenance_candidates = await build_maintenance_candidates(
                redis=context.redis_client,
                user_name=user_name,
                project_id=context.project_id,
                enabled_tools=effective_enabled_tools,
                topic_settings=config.developer_settings.topic_evaluation,
            )

            # Context & State Assembly
            requested_hot_topics = (
                hot_topics if hot_topics is not None else topic_config.hot_topics
            )
            effective_hot_topics = topic_config.validate_hot_topics(
                requested_hot_topics
            )
            hot_topic_context = {}
            if effective_hot_topics:
                try:
                    hot_topic_context = await tools.get_hot_topic_context(
                        effective_hot_topics,
                        slim=True,
                    )
                except Exception as exc:
                    logger.warning(f"Failed to preload hot topic context: {exc}")

            ctx = AgentContext(
                config=run_config,
                state=AgentState(),
                evidence=RetrievedEvidence(),
                user_name=user_name,
                session_id=session_id,
                project_id=context.project_id or "",
                user_query=user_query,
                run_id=str(uuid.uuid4()),
                hot_topics=effective_hot_topics,
                active_topics=topic_config.active_topics,
                hot_topic_context=hot_topic_context,
                agent_name=identity["name"],
                agent_persona=identity["persona"],
                history=conversation_history or [],
                maintenance_candidates=maintenance_candidates,
                use_local_references=getattr(
                    getattr(config.developer_settings, "local_references", None),
                    "enabled",
                    True,
                ),
            )

            # Execution via AgentExecutor
            executor = AgentExecutor(
                ctx, context.llm, tools
            )

            async for event in executor.execute(
                user_timezone=user_timezone,
                model=effective_model,
                enabled_tools=effective_enabled_tools,
                simulated_date=simulated_date,
                agent_temperature=effective_temperature,
                agent_brain=agent_brain
                or (agent_cfg.brain if agent_cfg else None),
                agent_directives=agent_directives,
                client_tools=client_tools,
            ):
                yield event

        except Exception as e:
            logger.exception(f"Orchestrator error: {e}")
            yield {
                "event": "error",
                "data": {"message": PUBLIC_AGENT_ERROR_MESSAGE},
            }
        finally:
            if tools:
                try:
                    await tools.close()
                except Exception:
                    logger.exception("Failed to close agent tools")

    async def _resolve_agent_identity(
        self,
        context: Session,
        agent_id: Optional[str],
        name_override: Optional[str],
        persona_override: Optional[str],
    ) -> Dict:
        """Resolve the durable Postgres agent used for this run."""
        manager = AgentManager(context.resources, context.user_name, {})
        resolved_id = agent_id or await manager.get_default_agent_id()
        agent_cfg = await manager.get_agent(resolved_id)
        if agent_cfg is None:
            raise ValueError(f"Agent identity not found: {resolved_id}")

        return {
            "config": agent_cfg,
            "name": name_override
            or agent_cfg.name,
            "persona": persona_override
            or agent_cfg.persona_markdown
            or "A helpful and thorough personal intelligence assistant.",
        }

    async def _bootstrap_services(
        self,
        context: Session,
        agent_id: Optional[str] = None,
    ) -> Dict:
        """Retrieve context services and instantiate the agent tool suite."""
        config = ConfigManager.get().config
        search_cfg = {
            **config.developer_settings.search.model_dump(),
            **config.search.model_dump(),
        }

        document_focus = await self._load_document_focus(context)

        tools = Tools(
            user_name=context.user_name,
            entities=context.project.entities,
            session_id=context.session_id,
            topic_config=context.project.topic_config,
            search_config=search_cfg,
            document_service=context.document_service,
            document_focus=document_focus,
            knowledge_store=context.knowledge_store,
            postgres=context.resources.postgres,
            redis=context.redis_client,
            agent_id=agent_id,
            episode_settings=config.developer_settings.jobs.episode,
            topic_refresh_callback=(
                context.project.refresh_topic_mappings
                if context.project
                else None
            ),
        )

        return {
            "topic_config": context.project.topic_config,
            "entities": context.project.entities,
            "document_service": context.document_service,
            "tools": tools,
        }

    async def _load_document_focus(
        self,
        context: Session,
    ) -> Optional[dict]:
        """Load and validate Postgres-owned session focus for this run."""
        rows = await context.resources.postgres.fetch_all(
            """
            SELECT document_focus
            FROM public.sessions
            WHERE user_name = %(user_name)s
              AND session_id = %(session_id)s
            """,
            {
                "user_name": context.user_name,
                "session_id": context.session_id,
            },
        )
        if not rows:
            return None
        focus = rows[0].get("document_focus")
        if isinstance(focus, str):
            focus = safe_json_loads(focus, {})
        if focus is None:
            return None
        try:
            persisted = DocumentFocus.model_validate(focus)
            if context.document_service is None:
                return None
            target = await context.document_service.resolve_focus_target(
                session_id=context.session_id,
                document_id=(
                    persisted.document_id
                    if persisted.target_type == "document"
                    else None
                ),
                folder_root_id=(
                    persisted.folder_root_id
                    if persisted.target_type != "document"
                    else None
                ),
                path_prefix=(
                    persisted.path_prefix
                    if persisted.target_type == "subtree"
                    else None
                ),
            )
            return DocumentFocus(
                mode="pinned",
                created_at=persisted.created_at,
                **target,
            ).model_dump(mode="json")
        except (FileNotFoundError, ValueError) as exc:
            logger.warning(
                "Ignoring invalid or inaccessible document focus for session "
                f"{context.session_id}: {exc}"
            )
            return None
