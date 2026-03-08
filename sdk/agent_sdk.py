"""SDK agent interface — non-streaming agent loop with graph queries and memory."""

import ast
import asyncio
import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo
from loguru import logger

from sdk.session import KnogginSession
from sdk.types import (
    AgentResult,
    MemorySaveResult,
    MemoryForgetResult,
    MemoryListResult,
    WorkingMemoryAddResult,
    WorkingMemoryRemoveResult,
    WorkingMemoryListResult,
    WorkingMemoryClearResult,
    PromptContext,
)
from shared.services.memory import MemoryManager
from agent.tools import Tools
from agent.system_prompt import get_agent_prompt, get_fallback_summary_prompt
from agent.internals import (
    AgentRunConfig,
    AgentState,
    RetrievedEvidence,
    AgentContext,
    build_user_message,
    update_accumulators,
    summarize_result,
    execute_tool,
)
from agent.formatters import (
    format_entity_results,
    format_memory_context,
    format_retrieved_messages,
    format_graph_results,
    format_path_results,
    format_hierarchy_results,
    format_files_context,
)
from agent.streaming import call_agent_streaming, execute_pending_tools, generate_fallback_summary
from main.entity_resolve import EntityResolver
from shared.config.topics_config import TopicConfig
from shared.mcp.bridge import mcp_tools_to_schemas
from shared.services.rag import FileRAGService
from shared.models.schema.dtypes import (
    FinalResponse,
    ClarificationRequest,
    ToolCall,
)
from shared.models.schema.tool_schema import get_filtered_schemas, TOOL_SCHEMAS


class KnogginAgent:
    """Agent that queries the knowledge graph and returns grounded responses."""

    def __init__(
        self,
        session: KnogginSession,
        run_config: AgentRunConfig = None,
        agent_id: str = None,
        persona: str = "",
        agent_name: str = "Knoggin",
        instructions: str = None,
        temperature: float = 0.7,
        enabled_tools: List[str] = None,
        user_timezone: str = None,
    ):
        self.session = session
        self.client = session._client
        self.user_name = session.user_name
        self.session_id = session.session_id
        
        self.agent_id = agent_id or session.memory.agent_id or f"sdk_{uuid.uuid4().hex[:8]}"
        self.persona = persona
        self.agent_name = agent_name
        self.instructions = instructions
        self.temperature = temperature
        self.enabled_tools = enabled_tools
        self.user_timezone = user_timezone

        # Use session's components
        self.topic_config = session.topic_config
        self.memory = session.memory
        self.resolver = session.resolver
        self.file_rag = session.file_rag
        self.tools = session.tools

        # Run config — defaults match server's defaults from developer_settings
        self.run_config = run_config or AgentRunConfig(
            max_calls=6,
            max_attempts=8,
            max_history_turns=7,
            max_accumulated_messages=30,
            max_consecutive_errors=3,
        )

    # ════════════════════════════════════════════════════════
    #  AGENT LOOP
    # ════════════════════════════════════════════════════════

    async def chat(
        self,
        query: str,
        history: List[Dict] = None,
        hot_topics: List[str] = None,
        model: str = None,
    ) -> AgentResult:
        """Run the agent loop. Returns AgentResult with response, tools used, and evidence."""
        history = history or []
        hot_topics = hot_topics or []
        model = model or self.client.config.llm.agent_model

        # Build prompt context (memory + working memory in one call)
        mem_ctx, rules, prefs, icks = await self.memory.load_prompt_strings(hot_topics)

        # Files context (#2 fix)
        files_ctx = ""
        if self.file_rag:
            files = self.file_rag.list_files()
            if files:
                files_ctx = format_files_context(files)

        # Tool schemas — merge MCP tools (#1 fix)
        tool_schemas = (
            get_filtered_schemas(self.enabled_tools)
            if self.enabled_tools is not None
            else list(TOOL_SCHEMAS)
        )
        mcp_mgr = getattr(self.client, 'mcp_manager', None)
        if mcp_mgr:
            mcp_schemas = mcp_tools_to_schemas(mcp_mgr.get_all_tools())
            if mcp_schemas:
                tool_schemas = tool_schemas + mcp_schemas

        prompt_ctx = PromptContext(
            memory_ctx=mem_ctx,
            agent_rules=rules,
            agent_prefs=prefs,
            agent_icks=icks,
            files_ctx=files_ctx,
            tool_schemas=tool_schemas,
            model=model,
        )

        # Agent context
        state = AgentState()
        evidence = RetrievedEvidence()
        valid_hot_topics = self.topic_config.validate_hot_topics(hot_topics)

        # Timezone-aware date string (#7 fix)
        try:
            tz = ZoneInfo(self.user_timezone) if self.user_timezone else ZoneInfo("UTC")
        except Exception:
            tz = ZoneInfo("UTC")
        date_str = datetime.now(tz).strftime("%Y-%m-%d %H:%M %Z")

        ctx = AgentContext(
            config=self.run_config,
            state=state,
            evidence=evidence,
            user_query=query,
            hot_topics=valid_hot_topics,
            active_topics=self.topic_config.active_topics,
            trace_id=str(uuid.uuid4()),
            history=history,
            session_id=self.session_id,
            agent_name=self.agent_name,
            agent_persona=self.persona,
            prompt=prompt_ctx,
        )

        # Preload hot topic context
        if valid_hot_topics:
            ctx.hot_topic_context = await self.tools.get_hot_topic_context(
                valid_hot_topics, slim=False
            )

        self.client.emit("agent", "run_start", {
            "query": query,
            "trace_id": ctx.trace_id,
            "hot_topics": valid_hot_topics,
        })

        last_result = None

        for _ in range(self.run_config.max_attempts):
            ctx.state.attempt_count += 1

            if (
                ctx.state.attempt_count >= self.run_config.max_attempts - 1
                and ctx.evidence.has_any()
            ):
                ctx.state.last_error = (
                    "Final attempt. You MUST respond now using accumulated evidence. "
                    "Do not call any tools."
                )

            response_obj = None
            try:
                async for chunk in call_agent_streaming(
                    llm=self.client.llm,
                    ctx=ctx,
                    user_name=self.user_name,
                    last_result=last_result,
                    date=date_str,
                    model=prompt_ctx.model,
                    tools=prompt_ctx.tool_schemas,
                    memory_context=prompt_ctx.memory_ctx,
                    files_context=prompt_ctx.files_ctx,
                    agent_rules=prompt_ctx.agent_rules,
                    agent_preferences=prompt_ctx.agent_prefs,
                    agent_icks=prompt_ctx.agent_icks,
                    agent_temperature=self.temperature,
                    agent_instructions=self.instructions
                ):
                    if isinstance(chunk, (FinalResponse, ClarificationRequest, ToolCall, list)):
                        response_obj = chunk
            except Exception as e:
                logger.error(f"Agent streaming failed: {e}")
                ctx.state.last_error = f"LLM error: {e}"
                continue

            # Ensure we don't proceed with None
            if not response_obj:
                response_obj = FinalResponse(content="No response from LLM.")

            if isinstance(response_obj, FinalResponse):
                self.client.emit("agent", "run_complete", {
                    "trace_id": ctx.trace_id,
                    "tools_used": ctx.state.tools_used,
                    "attempts": ctx.state.attempt_count,
                })
                return AgentResult(
                    response=response_obj.content,
                    state="complete",
                    tools_used=ctx.state.tools_used,
                    evidence={
                        "profiles": ctx.evidence.profiles,
                        "messages": ctx.evidence.messages,
                        "graph": ctx.evidence.graph,
                        "sources": ctx.evidence.sources,  # #8 fix
                    },
                    usage=response_obj.usage or {},
                )

            if isinstance(response_obj, ClarificationRequest):
                return AgentResult(
                    response=response_obj.question,
                    state="clarification",
                    tools_used=ctx.state.tools_used,
                    usage=response_obj.usage or {},
                )

            tool_calls = [response_obj] if isinstance(response_obj, ToolCall) else response_obj
            last_result = []
            async for _evt in execute_pending_tools(ctx, self.tools, tool_calls, last_result):
                pass # Just consume events to populate last_result

        # Fallback
        summary = await generate_fallback_summary(ctx, self.client.llm, self.user_name, query)
        
        self.client.emit("agent", "run_fallback", {
            "trace_id": ctx.trace_id,
            "tools_used": ctx.state.tools_used,
        })

        return AgentResult(
            response=summary or "I found information but couldn't summarize it.",
            state="fallback",
            tools_used=ctx.state.tools_used,
            evidence={
                "profiles": ctx.evidence.profiles,
                "messages": ctx.evidence.messages,
                "graph": ctx.evidence.graph,
                "sources": ctx.evidence.sources,
            },
        )

    # ════════════════════════════════════════════════════════
    #  SESSION MEMORY (delegates to MemoryManager)
    # ════════════════════════════════════════════════════════

    async def save_memory(self, content: str, topic: str = "General") -> MemorySaveResult:
        return await self.memory.save_memory(content, topic)

    async def forget_memory(self, memory_id: str) -> MemoryForgetResult:
        return await self.memory.forget_memory(memory_id)

    async def get_memory_blocks(self) -> MemoryListResult:
        return await self.memory.get_memory_blocks()

    # ════════════════════════════════════════════════════════
    #  WORKING MEMORY (delegates to MemoryManager)
    # ════════════════════════════════════════════════════════

    async def add_working_memory(self, category: str, content: str) -> WorkingMemoryAddResult:
        return await self.memory.add_working_memory(category, content)

    async def remove_working_memory(self, category: str, memory_id: str) -> WorkingMemoryRemoveResult:
        return await self.memory.remove_working_memory(category, memory_id)

    async def list_working_memory(self, category: str = None) -> WorkingMemoryListResult:
        return await self.memory.list_working_memory(category)

    async def clear_working_memory(self, category: str) -> WorkingMemoryClearResult:
        return await self.memory.clear_working_memory(category)

    # ════════════════════════════════════════════════════════
    #  TOPICS
    # ════════════════════════════════════════════════════════

    def update_topics(self, topics: dict):
        """Replace the topic configuration."""
        self.topic_config = TopicConfig(topics)
        self.memory.topic_config = self.topic_config
        self.tools.topic_config = self.topic_config
        self.tools.active_topics = self.topic_config.active_topics
