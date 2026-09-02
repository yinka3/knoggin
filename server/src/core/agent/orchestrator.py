from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, AsyncGenerator, Dict, List, Optional

from loguru import logger

from common.schema.agent.research import ResearchMode, resolve_research_profile
from common.schema.agent.stream import (
    AgentExecutionEvent,
    validate_agent_execution_event,
)
from common.schema.document import (
    DocumentFocus,
    create_document_focus,
    dump_document_focus,
    parse_document_focus,
)
from common.schema.source.references import SourceReferenceCandidate
from core.agent.executor import AgentExecutor
from core.agent.run import AgentIdentity, AgentRun, AgentRunLimits
from core.agent.services.agent_manager import AgentManager
from core.agent.sources.document_selection import build_document_selection_candidate
from core.agent.sources.pasted_text import build_pasted_text_candidates
from core.agent.tools.registry import Tools

if TYPE_CHECKING:
    from runtime.session_runtime import SessionRuntime


PUBLIC_AGENT_ERROR_MESSAGE = (
    "The agent couldn't complete this request. Please try again."
)


class AgentOrchestrator:
    """
    AgentOrchestrator manages the high-level flow of an agent run.
    It prepares the environment and delegates the reasoning loop to AgentExecutor.
    """

    def __init__(self, agent_manager: AgentManager, *, config_provider):
        self._agent_manager = agent_manager
        self._config_provider = config_provider

    async def run_stream(
        self,
        user_query: str,
        context: SessionRuntime,
        user_timezone: Optional[str] = None,
        model: Optional[str] = None,
        agent_id: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
        conversation_history: Optional[List[Dict]] = None,
        hot_topics: Optional[List[str]] = None,
        user_message_id: Optional[int] = None,
        pasted_text_spans: Optional[List[Dict]] = None,
        request_document_focus: Optional[DocumentFocus] = None,
        research_mode: ResearchMode = "normal",
    ) -> AsyncGenerator[AgentExecutionEvent, None]:
        """
        Main entry point for agent execution. Uses modular helpers for initialization.
        """
        tools = None
        try:
            incoming_user_query = user_query
            run_id = str(uuid.uuid4())
            if request_document_focus is not None:
                request_document_focus = parse_document_focus(request_document_focus)

            # Configuration
            config = self._config_provider.get().config
            limits = config.developer_settings.limits
            research_profile = resolve_research_profile(research_mode)
            run_limits = AgentRunLimits.from_settings(limits).for_research_profile(
                research_profile
            )

            # Identity & Persona
            identity = await self._resolve_agent_identity(
                agent_id if agent_id is not None else context.agent_id,
            )
            agent_cfg = identity.config
            effective_model = (
                model
                if model is not None
                else (context.model if context.model is not None else agent_cfg.model)
            )
            effective_temperature = (
                agent_cfg.temperature
                if agent_cfg and agent_cfg.temperature is not None
                else 0.7
            )
            effective_brain = agent_cfg.brain if agent_cfg else ""

            # Services (Session-Aware)
            effective_document_focus = await self._resolve_document_focus(
                context,
                request_document_focus or context.document_focus,
            )
            document_selection_context = await self._resolve_document_selection_context(
                context,
                effective_document_focus,
            )
            tools = await self._bootstrap_services(
                context,
                agent_cfg.id if agent_cfg else None,
                effective_document_focus,
            )
            compiled_domain = context.project.compiled_domain

            effective_enabled_tools = (
                enabled_tools
                if enabled_tools is not None
                else (
                    context.enabled_tools
                    if context.enabled_tools is not None
                    else agent_cfg.enabled_tools
                )
            )
            # One aggregate owns all mutable state for this execution.
            requested_hot_topics = hot_topics or []
            effective_hot_topics = []
            for topic in requested_hot_topics:
                normalized = compiled_domain.normalize_topic(topic)
                if normalized and normalized not in effective_hot_topics:
                    effective_hot_topics.append(normalized)
            hot_topic_context = {}
            if effective_hot_topics:
                try:
                    hot_topic_context = await tools.get_hot_topic_context(
                        effective_hot_topics,
                    )
                except Exception as exc:
                    logger.warning(f"Failed to preload hot topic context: {exc}")

            run = AgentRun.open(
                user_name=context.user_name,
                project_id=context.project_id or "",
                session_id=context.session_id,
                user_query=user_query,
                run_id=run_id,
                agent=identity,
                limits=run_limits,
                research_profile=research_profile,
                model=effective_model,
                temperature=effective_temperature,
                brain=effective_brain,
                enabled_tools=effective_enabled_tools,
                hot_topics=effective_hot_topics,
                hot_topic_context=hot_topic_context,
                history=conversation_history or [],
                document_focus=effective_document_focus,
                document_selection_context=document_selection_context,
                initial_source_candidates=self._initial_source_candidates(
                    context=context,
                    run_id=run_id,
                    user_message_id=user_message_id,
                    message_content=incoming_user_query,
                    pasted_text_spans=pasted_text_spans,
                    selection_context=document_selection_context,
                ),
            )

            # Execution via AgentExecutor
            executor = AgentExecutor(
                run,
                context.llm,
                tools,
                on_successful_completion=self._agent_manager.mark_turn_completed,
            )

            async for event in executor.execute(user_timezone=user_timezone):
                yield validate_agent_execution_event(event)

        except Exception as e:
            logger.exception(f"Agent orchestration error: {e}")
            yield validate_agent_execution_event(
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

    async def _resolve_agent_identity(
        self,
        agent_id: Optional[str],
    ) -> AgentIdentity:
        """Resolve the durable Postgres agent used for this run."""
        resolved_id = agent_id or await self._agent_manager.get_default_agent_id()
        agent_cfg = await self._agent_manager.get_agent(resolved_id)
        if agent_cfg is None:
            raise ValueError(f"Agent identity not found: {resolved_id}")

        return AgentIdentity(
            config=agent_cfg,
            name=agent_cfg.name,
            persona=(
                agent_cfg.persona_markdown
                or "A helpful and thorough personal intelligence assistant."
            ),
        )

    async def _bootstrap_services(
        self,
        context: SessionRuntime,
        agent_id: Optional[str] = None,
        document_focus: Optional[DocumentFocus] = None,
    ) -> Tools:
        """Retrieve context services and instantiate the agent tool suite."""
        config = self._config_provider.get().config
        search_cfg = {
            **config.developer_settings.search.model_dump(),
            **config.search.model_dump(),
        }

        tools = Tools(
            user_name=context.user_name,
            entities=context.project.entities,
            session_id=context.session_id,
            compiled_domain=context.project.compiled_domain,
            search_config=search_cfg,
            document_service=context.document_service,
            document_focus=(
                dump_document_focus(document_focus)
                if document_focus is not None
                else None
            ),
            knowledge_retrieval=context.project.knowledge_retrieval,
            knowledge_store=context.knowledge_store,
            postgres=context.resources.postgres,
            agent_id=agent_id,
            health_service=getattr(context, "health_service", None),
        )

        return tools

    @staticmethod
    def _initial_source_candidates(
        *,
        context: SessionRuntime,
        run_id: str,
        user_message_id: Optional[int],
        message_content: str,
        pasted_text_spans: Optional[List[Dict]],
        selection_context: Optional[Dict],
    ) -> List[SourceReferenceCandidate]:
        candidates = (
            build_pasted_text_candidates(
                project_id=context.project_id or "",
                session_id=context.session_id,
                source_message_id=user_message_id,
                message_content=message_content,
                agent_run_id=run_id,
                spans=pasted_text_spans,
            )
            if user_message_id is not None
            else []
        )
        if selection_context is not None:
            candidates.append(
                build_document_selection_candidate(
                    project_id=context.project_id or "",
                    agent_run_id=run_id,
                    selection_context=selection_context,
                )
            )
        return candidates

    async def _resolve_document_focus(
        self,
        context: SessionRuntime,
        focus: Optional[DocumentFocus],
    ) -> Optional[DocumentFocus]:
        """Validate session-owned focus without rereading session persistence."""
        if focus is None or context.document_service is None:
            return None
        try:
            persisted = parse_document_focus(focus)
            target = await context.document_service.resolve_focus_target(
                session_id=context.session_id,
                document_id=(
                    persisted.document_id
                    if persisted.target_type == "document"
                    else None
                ),
                path_prefix=(
                    persisted.path_prefix
                    if persisted.target_type == "subtree"
                    else None
                ),
            )
            if persisted.target_type == "document" and persisted.selection is not None:
                target["selection"] = persisted.selection
            return create_document_focus(
                mode=("request" if persisted.mode == "request" else "pinned"),
                behavior=persisted.behavior,
                created_at=persisted.created_at,
                **target,
            )
        except (FileNotFoundError, ValueError) as exc:
            logger.warning(
                "Ignoring invalid or inaccessible document focus for session "
                f"{context.session_id}: {exc}"
            )
            return None

    async def _resolve_document_selection_context(
        self,
        context: SessionRuntime,
        focus: Optional[DocumentFocus],
    ) -> Optional[Dict]:
        """Read the request-selected passage for prompt context.

        The focus model carries only versioned identity and coordinates. The
        excerpt is loaded from the document service for this run and is never
        accepted from the public request.
        """
        if (
            focus is None
            or focus.target_type != "document"
            or focus.selection is None
            or context.document_service is None
        ):
            return None
        result = await context.document_service.resolve_document_selection(
            document_id=focus.document_id,
            selection=focus.selection,
        )
        return {
            "document_id": result["document_id"],
            "project_id": result["project_id"],
            "relative_path": result["relative_path"],
            "document_name": result["document_name"],
            "extension": result["extension"],
            "chunk_index": result.get("chunk_index"),
            "content_hash": result["content_hash"],
            "locator": result["locator"],
            "excerpt": result["content"],
        }
