from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, AsyncGenerator, Dict, List, NamedTuple, Optional

from loguru import logger

from common.conf.manager import ConfigManager
from common.schema.agent_contracts import AgentConfig
from common.schema.agent_stream import (
    PublicAgentStreamEvent,
    validate_public_agent_stream_event,
)
from common.schema.document import (
    DocumentFocus,
    create_document_focus,
    dump_document_focus,
    parse_document_focus,
)
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import get_now_iso
from core.agent.document_selection import (
    DocumentSelectionError,
    parse_document_path_command,
)
from core.agent.executor import AgentExecutor
from core.agent.maintenance import build_maintenance_candidates
from core.agent.run import AgentRun, AgentRunLimits
from core.agent.services.agent_manager import AgentManager
from core.agent.source_adapters import build_pasted_text_candidates
from core.agent.tools.registry import Tools

if TYPE_CHECKING:
    from core.session.context import Session


PUBLIC_AGENT_ERROR_MESSAGE = (
    "The agent couldn't complete this request. Please try again."
)


class ResolvedAgentIdentity(NamedTuple):
    """Private identity data passed once into the owning AgentRun factory."""

    config: AgentConfig
    name: str
    persona: str


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
        user_message_id: Optional[int] = None,
        pasted_text_spans: Optional[List[Dict]] = None,
    ) -> AsyncGenerator[PublicAgentStreamEvent, None]:
        """
        Main entry point for agent execution. Uses modular helpers for initialization.
        """
        tools = None
        try:
            incoming_user_query = user_query
            run_id = str(uuid.uuid4())
            document_command = parse_document_path_command(user_query)
            request_document_focus = None
            if document_command is not None:
                request_document_focus = await self._resolve_request_document_focus(
                    context,
                    document_command.relative_path,
                )
                user_query = document_command.remaining_query

            # Configuration
            config = ConfigManager.get().config
            limits = config.developer_settings.limits
            run_limits = AgentRunLimits.from_settings(limits)

            # Identity & Persona
            identity = await self._resolve_agent_identity(
                context,
                agent_id,
                agent_name_override,
                agent_persona_override,
            )
            agent_cfg = identity.config
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
            tools = await self._bootstrap_services(
                context,
                agent_cfg.id if agent_cfg else None,
            )
            if request_document_focus is not None:
                # Request focus is ephemeral.  It overrides a persisted session
                # focus for this run without writing to session state.
                tools.document_focus = request_document_focus.model_dump(mode="json")
            topic_config = context.project.topic_config

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

            # One aggregate owns all mutable state for this execution.
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

            run = AgentRun.open(
                user_name=user_name,
                session_id=session_id,
                project_id=context.project_id or "",
                user_query=user_query,
                run_id=run_id,
                agent_config=identity.config,
                agent_name=identity.name,
                persona=identity.persona,
                limits=run_limits,
                model=effective_model,
                temperature=effective_temperature,
                enabled_tools=effective_enabled_tools,
                hot_topics=effective_hot_topics,
                active_topics=topic_config.active_topics,
                hot_topic_context=hot_topic_context,
                history=conversation_history or [],
                maintenance_candidates=maintenance_candidates,
                document_focus=request_document_focus,
                initial_source_candidates=(
                    build_pasted_text_candidates(
                        project_id=context.project_id or "",
                        session_id=session_id,
                        source_message_id=user_message_id,
                        message_content=incoming_user_query,
                        agent_run_id=run_id,
                        spans=pasted_text_spans,
                    )
                    if user_message_id is not None
                    else []
                ),
            )

            # Execution via AgentExecutor
            executor = AgentExecutor(run, context.llm, tools)

            async for event in executor.execute(
                user_timezone=user_timezone,
                model=effective_model,
                enabled_tools=effective_enabled_tools,
                simulated_date=simulated_date,
                agent_temperature=effective_temperature,
                agent_brain=agent_brain or (agent_cfg.brain if agent_cfg else None),
                agent_directives=agent_directives,
                client_tools=client_tools,
            ):
                yield validate_public_agent_stream_event(event)

        except DocumentSelectionError as exc:
            logger.info("Document selection rejected: {}", exc)
            yield validate_public_agent_stream_event(
                {
                    "event": "error",
                    "data": {"message": str(exc)},
                }
            )
        except Exception as e:
            logger.exception(f"Orchestrator error: {e}")
            yield validate_public_agent_stream_event(
                {
                    "event": "error",
                    "data": {"message": PUBLIC_AGENT_ERROR_MESSAGE},
                }
            )
        finally:
            if tools:
                try:
                    await tools.close()
                except Exception:
                    logger.exception("Failed to close agent tools")

    async def _resolve_request_document_focus(
        self,
        context: Session,
        relative_path: str,
    ) -> DocumentFocus:
        """Resolve one user-entered path through the current visibility scope."""
        if context.document_service is None:
            raise DocumentSelectionError(
                "No project document service is available for this request"
            )
        try:
            document = await context.document_service.get_document_info(
                session_id=context.session_id,
                relative_path=relative_path,
            )
            target = await context.document_service.resolve_focus_target(
                session_id=context.session_id,
                document_id=document["document_id"],
            )
        except FileNotFoundError as exc:
            raise DocumentSelectionError(
                f"Document path '/{relative_path}' is not visible in this project"
            ) from exc
        except ValueError as exc:
            raise DocumentSelectionError(
                f"Document path '/{relative_path}' is ambiguous; select one document"
            ) from exc
        return create_document_focus(
            mode="request",
            created_at=get_now_iso(),
            **target,
        )

    async def _resolve_agent_identity(
        self,
        context: Session,
        agent_id: Optional[str],
        name_override: Optional[str],
        persona_override: Optional[str],
    ) -> ResolvedAgentIdentity:
        """Resolve the durable Postgres agent used for this run."""
        manager = AgentManager(context.resources, context.user_name, {})
        resolved_id = agent_id or await manager.get_default_agent_id()
        agent_cfg = await manager.get_agent(resolved_id)
        if agent_cfg is None:
            raise ValueError(f"Agent identity not found: {resolved_id}")

        return ResolvedAgentIdentity(
            config=agent_cfg,
            name=name_override or agent_cfg.name,
            persona=(
                persona_override
                or agent_cfg.persona_markdown
                or "A helpful and thorough personal intelligence assistant."
            ),
        )

    async def _bootstrap_services(
        self,
        context: Session,
        agent_id: Optional[str] = None,
    ) -> Tools:
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
                context.project.refresh_topic_mappings if context.project else None
            ),
        )

        return tools

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
            persisted = parse_document_focus(focus)
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
            return dump_document_focus(
                create_document_focus(
                    mode="pinned",
                    created_at=persisted.created_at,
                    **target,
                )
            )
        except (FileNotFoundError, ValueError) as exc:
            logger.warning(
                "Ignoring invalid or inaccessible document focus for session "
                f"{context.session_id}: {exc}"
            )
            return None
