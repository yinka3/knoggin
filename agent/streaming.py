from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, List
import uuid

from loguru import logger
import redis

from agent.orchestrator import (
    call_agent,
    execute_tool,
    update_accumulators,
    summarize_result
)
from agent.tools import Tools
from agent.system_prompt import get_benchmark_fallback_prompt
from agent.internals import AgentConfig, AgentState, RetrievedEvidence, AgentContext
from agent.formatters import format_entity_results, format_retrieved_messages, format_graph_results
from main.service import LLMService
from main.topics_config import TopicConfig
from schema.dtypes import ClarificationRequest, FinalResponse, ToolCall


async def run_stream(
    user_query: str,
    user_name: str,
    session_id: str,
    conversation_history: List[Dict],
    hot_topics: List[str],
    topic_config: TopicConfig,
    llm: LLMService,
    store,
    ent_resolver,
    redis_client: redis.Redis,
    persona: str = ""
) -> AsyncGenerator[Dict, None]:
    """Streaming version of orchestrator.run()"""
    
    try:
        config = AgentConfig()
        state = AgentState()
        evidence = RetrievedEvidence()
        valid_hot_topics = topic_config.validate_hot_topics(hot_topics)

        ctx = AgentContext(
            config=config,
            state=state,
            evidence=evidence,
            user_query=user_query,
            hot_topics=valid_hot_topics,
            active_topics=topic_config.active_topics,
            trace_id=str(uuid.uuid4()),
            history=conversation_history
        )

        tools = Tools(user_name, store, ent_resolver, redis_client, session_id, topic_config)

        if hot_topics:
            yield {"event": "status", "data": {"message": "Loading context..."}}
            ctx.hot_topic_context = await tools.get_hot_topic_context(hot_topics, slim=False)

        last_result = None
        current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        while ctx.state.attempt_count < ctx.config.max_attempts:
            ctx.state.attempt_count += 1

            should_force = (
                ctx.state.attempt_count >= ctx.config.max_attempts - 1
                and ctx.evidence.has_any()
            )
            if should_force:
                ctx.state.last_error = "Final attempt. Respond now with accumulated evidence."

            response = await call_agent(llm, ctx, user_name, last_result, persona, current_time)

            if isinstance(response, FinalResponse):
                yield {"event": "response", "data": {"content": response.content}}
                return

            if isinstance(response, ClarificationRequest):
                yield {"event": "clarification", "data": {"question": response.question}}
                return

            # Process tool calls
            tool_calls = [response] if isinstance(response, ToolCall) else response

            if tool_calls and tool_calls[0].thinking:
                yield {"event": "thinking", "data": {"content": tool_calls[0].thinking}}

            all_results = []

            for tc in tool_calls:
                tool_name = tc.name
                args = tc.args

                if ctx.state.is_duplicate(tool_name, args):
                    ctx.state.consecutive_errors += 1
                    if ctx.state.consecutive_errors >= 3:
                        break
                    ctx.state.last_error = f"Already called {tool_name} with these args."
                    continue

                if ctx.state.tool_limit_reached(tool_name, ctx.config):
                    ctx.state.last_error = f"{tool_name} limit reached."
                    continue

                if ctx.state.call_count >= ctx.config.max_calls:
                    ctx.state.last_error = "Call limit reached."
                    break

                ctx.state.consecutive_errors = 0

                # Yield tool start event
                yield {"event": "tool_start", "data": {"tool": tool_name, "args": args}}

                result = await execute_tool(tools, tool_name, args)
                result_summary, result_count = summarize_result(tool_name, result)

                # Yield tool result event
                yield {"event": "tool_result", "data": {
                    "tool": tool_name,
                    "summary": result_summary,
                    "count": result_count
                }}

                ctx.state.record_call(tool_name, args)
                update_accumulators(ctx, tool_name, result)
                all_results.append({"tool": tool_name, "result": result})

            last_result = all_results

        # Max attempts reached — fallback
        if ctx.evidence.has_any():
            evidence_ctx = ""
            if ctx.evidence.profiles:
                evidence_ctx += f"Profiles:\n{format_entity_results(ctx.evidence.profiles)}\n\n"
            if ctx.evidence.messages:
                evidence_ctx += f"Messages:\n{format_retrieved_messages(ctx.evidence.messages)}\n\n"
            if ctx.evidence.graph:
                evidence_ctx += f"Connections:\n{format_graph_results(ctx.evidence.graph)}\n\n"

            summary = await llm.call_llm(
                system=get_benchmark_fallback_prompt(user_name),
                user=f"Query: {user_query}\n\nEvidence:\n{evidence_ctx}"
            )

            yield {"event": "response", "data": {"content": summary or "I found information but couldn't summarize it."}}
        else:
            yield {"event": "clarification", "data": {"question": "I'm having trouble with that. Could you rephrase?"}}

    except Exception as e:
        logger.error(f"Stream orchestrator error: {e}")
        yield {"event": "error", "data": {"message": str(e)}}