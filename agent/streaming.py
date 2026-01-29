from datetime import datetime, timezone
import json
from typing import AsyncGenerator, Dict, List, Optional, Union
import uuid

from loguru import logger
import redis

from agent.orchestrator import (
    execute_tool,
    update_accumulators,
    summarize_result
)
from agent.tools import Tools
from agent.system_prompt import get_agent_prompt, get_benchmark_fallback_prompt
from agent.internals import AgentConfig, AgentState, RetrievedEvidence, AgentContext, build_user_message
from agent.formatters import format_entity_results, format_retrieved_messages, format_graph_results
from main.service import LLMService
from main.topics_config import TopicConfig
from schema.dtypes import ClarificationRequest, FinalResponse, ToolCall
from schema.tool_schema import TOOL_SCHEMAS


async def call_agent_streaming(
    llm: LLMService,
    ctx: AgentContext,
    user_name: str,
    last_result: Optional[Dict] = None,
    persona: str = "",
    date: str = ""
) -> AsyncGenerator[Union[Dict, ToolCall, List[ToolCall], ClarificationRequest, FinalResponse], None]:
    """
    Streaming version of call_agent.
    Yields token dicts for text, then final ToolCall/ClarificationRequest/FinalResponse.
    """
    system_prompt = get_agent_prompt(user_name, date, persona)
    user_message = build_user_message(ctx, last_result)
    
    content = ""
    tool_calls = None
    usage = None
    
    async for chunk in llm.call_llm_with_tools_streaming(
        system=system_prompt,
        user=user_message,
        tools=TOOL_SCHEMAS
    ):
        chunk_type = chunk.get("type")
        
        if chunk_type == "token":
            yield chunk

        elif chunk_type == "thinking":
            yield chunk
            
        elif chunk_type == "tool_calls":
            content = chunk.get("content", "")
            tool_calls = chunk.get("calls", [])
            
        elif chunk_type == "done":
            usage = chunk.get("usage")
            
            # No tool calls - pure text response
            if not tool_calls:
                yield FinalResponse(content=chunk.get("content", ""), usage=usage)
                return
            
            # Process tool calls
            if content:
                logger.info(f"[AGENT THOUGHT]: {content[:200]}")
            
            # Single tool call
            if len(tool_calls) == 1:
                tc = tool_calls[0]
                name = tc["name"]
                args = json.loads(tc["arguments"])
                
                if name == "request_clarification":
                    yield ClarificationRequest(question=args.get("question", ""), usage=usage)
                    return
                
                yield ToolCall(name=name, args=args, thinking=content if content else None)
                yield {"type": "usage", "data": usage}
                return
            
            # Multiple tool calls
            yield [
                ToolCall(name=tc["name"], args=json.loads(tc["arguments"]), thinking=content if content else None)
                for tc in tool_calls
            ]
            yield {"type": "usage", "data": usage}
            return
            
        elif chunk_type == "error":
            yield FinalResponse(content=f"System Error: {chunk.get('message', 'Unknown error')}")
            return


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
        usage_accumulator = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        while ctx.state.attempt_count < ctx.config.max_attempts:
            ctx.state.attempt_count += 1

            should_force = (
                ctx.state.attempt_count >= ctx.config.max_attempts - 1
                and ctx.evidence.has_any()
            )
            if should_force:
                ctx.state.last_error = "Final attempt. Respond now with accumulated evidence."

            # Collect tool calls while streaming tokens
            pending_tool_calls = []
            
            async for chunk in call_agent_streaming(llm, ctx, user_name, last_result, persona, current_time):
                
                # Token - pass through to frontend
                if isinstance(chunk, dict) and chunk.get("type") == "token":
                    yield {"event": "token", "data": {"content": chunk["content"]}}
                
                elif isinstance(chunk, dict) and chunk.get("type") == "thinking":
                    yield {"event": "thinking", "data": {"content": chunk["content"]}}
                
                # Usage stats after tool calls
                elif isinstance(chunk, dict) and chunk.get("type") == "usage":
                    if chunk.get("data"):
                        usage_accumulator["prompt_tokens"] += chunk["data"].get("prompt_tokens", 0)
                        usage_accumulator["completion_tokens"] += chunk["data"].get("completion_tokens", 0)
                        usage_accumulator["total_tokens"] += chunk["data"].get("total_tokens", 0)
                
                # Final response - done
                elif isinstance(chunk, FinalResponse):
                    if chunk.usage:
                        usage_accumulator["prompt_tokens"] += chunk.usage.get("prompt_tokens", 0)
                        usage_accumulator["completion_tokens"] += chunk.usage.get("completion_tokens", 0)
                        usage_accumulator["total_tokens"] += chunk.usage.get("total_tokens", 0)
                    
                    yield {"event": "response", "data": {
                        "content": chunk.content,
                        "usage": usage_accumulator
                    }}
                    return

                # Clarification request - done
                elif isinstance(chunk, ClarificationRequest):
                    if chunk.usage:
                        usage_accumulator["prompt_tokens"] += chunk.usage.get("prompt_tokens", 0)
                        usage_accumulator["completion_tokens"] += chunk.usage.get("completion_tokens", 0)
                        usage_accumulator["total_tokens"] += chunk.usage.get("total_tokens", 0)
                    
                    yield {"event": "clarification", "data": {
                        "question": chunk.question,
                        "usage": usage_accumulator
                    }}
                    return

                # Tool calls - collect for processing
                elif isinstance(chunk, ToolCall):
                    pending_tool_calls = [chunk]
                elif isinstance(chunk, list) and chunk and isinstance(chunk[0], ToolCall):
                    pending_tool_calls = chunk

            # Process tool calls if any
            if not pending_tool_calls:
                continue
                
            # Emit thinking if present
            if pending_tool_calls[0].thinking:
                yield {"event": "thinking", "data": {"content": pending_tool_calls[0].thinking}}

            all_results = []

            for tc in pending_tool_calls:
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

                yield {"event": "tool_start", "data": {"tool": tool_name, "args": args}}

                result = await execute_tool(tools, tool_name, args)
                result_summary, result_count = summarize_result(tool_name, result)

                yield {"event": "tool_result", "data": {
                    "tool": tool_name,
                    "summary": result_summary,
                    "count": result_count
                }}

                ctx.state.record_call(tool_name, args)
                update_accumulators(ctx, tool_name, result)
                all_results.append({"tool": tool_name, "result": result})

            last_result = all_results

        # Max attempts - fallback
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

            yield {"event": "response", "data": {
                "content": summary or "I found information but couldn't summarize it.",
                "usage": usage_accumulator
            }}
        else:
            yield {"event": "clarification", "data": {
                "question": "I'm having trouble with that. Could you rephrase?",
                "usage": usage_accumulator
            }}

    except Exception as e:
        logger.error(f"Stream orchestrator error: {e}")
        yield {"event": "error", "data": {"message": str(e)}}