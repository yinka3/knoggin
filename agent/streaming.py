import asyncio
from datetime import datetime
import json
from typing import AsyncGenerator, Dict, List, Optional, Union
import uuid
from zoneinfo import ZoneInfo
from loguru import logger
import redis.asyncio as aioredis

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
    execute_tool
)
from agent.formatters import (
    format_entity_results,
    format_memory_context,
    format_retrieved_messages, 
    format_graph_results,
    format_path_results,
    format_hierarchy_results
)

from shared.mcp.bridge import mcp_tools_to_schemas
from shared.services.memory import MemoryManager
from shared.services.llm import LLMService
from shared.config.topics_config import TopicConfig
from shared.models.schema.dtypes import AgentResponse, ClarificationRequest, FinalResponse, ToolCall
from shared.models.schema.tool_schema import get_filtered_schemas, TOOL_SCHEMAS
from shared.config.base import get_config_value
from shared.utils.events import emit


async def call_agent_streaming(
    llm: LLMService,
    ctx: AgentContext,
    user_name: str,
    last_result: Optional[Dict] = None,
    date: str = "",
    model: Optional[str] = None,
    tools: Optional[List[Dict]] = None,
    memory_context: str = "",
    files_context: str = "",
    agent_rules: Optional[str] = "",
    agent_preferences: Optional[str] = "",
    agent_icks: Optional[str] = "",
    agent_temperature: float = 0.7,
    agent_instructions: Optional[str] = ""
) -> AsyncGenerator[Union[Dict, AgentResponse], None]:
    """
    Core LLM interaction loop for the streaming agent.
    Yields token dictionaries for real-time text visualization, followed by the final resolved 
    `ToolCall`, `ClarificationRequest`, or `FinalResponse` object parsed from the model's output.
    """
    system_prompt = get_agent_prompt(
        user_name, date, ctx.agent_persona, ctx.agent_name,
        memory_context=memory_context,
        files_context=files_context,
        agent_rules=agent_rules,
        agent_preferences=agent_preferences,
        agent_icks=agent_icks,
        instructions=agent_instructions
    )
    user_message = build_user_message(ctx, last_result)

    content = ""
    tool_calls = None
    usage = None

    await emit(ctx.session_id, "agent", "llm_call", {
        "run_id": ctx.run_id,
        "prompt": user_message,
        "calls_used": ctx.state.call_count,
        "attempt": ctx.state.attempt_count,
        "evidence_state": {
            "profiles": len(ctx.evidence.profiles),
            "messages": len(ctx.evidence.messages),
            "graph": len(ctx.evidence.graph)
        }
    }, verbose_only=True)
        
    async for chunk in llm.call_llm_with_tools_streaming(
        system=system_prompt,
        user=user_message,
        tools=tools if tools is not None else TOOL_SCHEMAS,
        model=model,
        temperature=agent_temperature
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
            
            if not tool_calls:
                yield FinalResponse(
                    content=chunk.get("content", ""), 
                    usage=usage,
                    sources=ctx.evidence.sources if ctx.evidence.sources else None
                )
                return
            
            if content:
                logger.info(f"[AGENT THOUGHT]: {content[:200]}")
            
            def safe_parse(json_str):
                import ast
                if isinstance(json_str, dict):
                    return json_str
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError:
                    try:
                        return json.loads(json_str.replace('\n', '\\n'))
                    except Exception:
                        try:
                            val = ast.literal_eval(json_str)
                            if isinstance(val, dict):
                                return val
                        except Exception:
                            pass
                return {"_parse_error": json_str}

            if len(tool_calls) == 1:
                tc = tool_calls[0]
                name = tc["name"]
                args = safe_parse(tc["arguments"])
                
                if "_parse_error" in args:
                    logger.warning(f"Malformed tool args for {name}: {tc['arguments']}")
                
                if name == "request_clarification":
                    yield ClarificationRequest(question=args.get("question", "I need to clarify something but my response was malformed."), usage=usage)
                    return
                
                yield ToolCall(name=name, args=args, thinking=content if content else None)
                yield {"type": "usage", "data": usage}
                return
            
            parsed_calls = []
            for tc in tool_calls:
                args = safe_parse(tc["arguments"])
                if "_parse_error" in args:
                    logger.warning(f"Malformed tool args for {tc['name']}: {tc['arguments']}")
                parsed_calls.append(ToolCall(name=tc["name"], args=args, thinking=content if content else None))

            if parsed_calls:
                yield parsed_calls
                yield {"type": "usage", "data": usage}
                return
            else:
                yield FinalResponse(content="I had trouble processing that. Could you rephrase?", usage=usage)
                return
            
        elif chunk_type == "error":
            yield FinalResponse(content=f"System Error: {chunk.get('message', 'Unknown error')}")
            return



async def execute_pending_tools(ctx: AgentContext, tools: Tools, pending_tool_calls: List[ToolCall], all_results: List[Dict]) -> AsyncGenerator[Dict, None]:
    """
    Process an array of `ToolCall`s requested by the LLM by executing the corresponding python methods.
    Yields granular event dicts (`tool_start`, `tool_result`) used by the frontend to render progress spinners.
    """
    if pending_tool_calls and pending_tool_calls[0].thinking:
        yield {"event": "thinking", "data": {"content": pending_tool_calls[0].thinking}}

    for tc in pending_tool_calls:
        tool_name = tc.name
        args = tc.args

        if "_parse_error" in args:
            error_msg = f"Failed to parse tool arguments as JSON: {args['_parse_error']}. Please fix the JSON syntax (e.g. escape newlines)."
            ctx.state.last_error = error_msg
            all_results.append({"tool": tool_name, "error": error_msg})
            yield {"event": "tool_start", "data": {"tool": tool_name, "args": {}}}
            yield {"event": "tool_result", "data": {"tool": tool_name, "summary": "JSON Error", "count": 1}}
            continue

        if ctx.state.is_duplicate(tool_name, args):
            ctx.state.consecutive_errors += 1
            if ctx.state.consecutive_errors >= ctx.config.max_consecutive_errors:
                break
            ctx.state.last_error = f"Already called {tool_name} with these args."
            continue

        if ctx.state.call_count >= ctx.config.max_calls:
            ctx.state.last_error = "Call limit reached."
            break

        if ctx.state.tool_limit_reached(tool_name, ctx.config):
            ctx.state.last_error = f"{tool_name} limit reached ({ctx.config.get_tool_limit(tool_name)}). Use a different tool."
            all_results.append({"tool": tool_name, "error": ctx.state.last_error})
            continue

        ctx.state.consecutive_errors = 0

        yield {"event": "tool_start", "data": {"tool": tool_name, "args": args}}

        try:
            result = await asyncio.wait_for(
                execute_tool(tools, tool_name, args),
                timeout=ctx.config.tool_timeout
            )
        except asyncio.TimeoutError:
            logger.warning(f"Tool {tool_name} timed out after 30s")
            result = {"error": f"Tool {tool_name} timed out"}
        result_summary, result_count = summarize_result(tool_name, result)

        await emit(ctx.session_id, "agent", "tool_executed", {
            "tool": tool_name,
            "args": args,
            "result_count": result_count,
            "success": "error" not in result
        })

        yield {"event": "tool_result", "data": {
            "tool": tool_name,
            "summary": result_summary,
            "count": result_count
        }}

        ctx.state.record_call(tool_name, args)
        update_accumulators(ctx, tool_name, result)
        all_results.append({"tool": tool_name, "result": result})


async def generate_fallback_summary(ctx: AgentContext, llm: LLMService, user_name: str, user_query: str) -> Optional[str]:
    """
    Generate a final response summary when the agent exhausts its maximum allowed reasoning attempts.
    Passes all accumulated evidence directly to the fallback prompt.
    """
    evidence_ctx = ""
    if ctx.evidence.profiles:
        evidence_ctx += f"Profiles:\n{format_entity_results(ctx.evidence.profiles)}\n\n"
    if ctx.evidence.messages:
        evidence_ctx += f"Messages:\n{format_retrieved_messages(ctx.evidence.messages)}\n\n"
    if ctx.evidence.graph:
        evidence_ctx += f"Connections:\n{format_graph_results(ctx.evidence.graph)}\n\n"
    if ctx.evidence.paths:
        evidence_ctx += f"Paths:\n{format_path_results(ctx.evidence.paths)}\n\n"
    if ctx.evidence.hierarchy:
        evidence_ctx += f"Hierarchy:\n{format_hierarchy_results(ctx.evidence.hierarchy)}\n\n"

    try:
        return await asyncio.wait_for(
            llm.call_llm(
                system=get_fallback_summary_prompt(user_name, ctx.agent_name),
                user=f"Query: {user_query}\n\nEvidence:\n{evidence_ctx}"
            ),
            timeout=30.0
        )
    except asyncio.TimeoutError:
        logger.warning("Fallback summary timed out")
        return None


async def run_stream(
    user_query: str,
    user_name: str,
    session_id: str,
    agent_id: str,
    agent_name: str,
    agent_persona: str,
    conversation_history: List[Dict],
    hot_topics: List[str],
    topic_config: TopicConfig,
    llm: LLMService,
    store,
    ent_resolver,
    redis_client: aioredis.Redis,
    model: str = None,
    enabled_tools: List[str] = None,
    file_rag = None,
    user_timezone: str = None,
    mcp_manager=None,
    agent_temperature: float = 0.7,
    agent_instructions: Optional[str] = None,
    agent_rules: Optional[str] = None,
    agent_preferences: Optional[str] = None,
    agent_icks: Optional[str] = None,
    simulated_date: Optional[str] = None
) -> AsyncGenerator[Dict, None]:
    """
    Top-level streaming orchestrator loop.
    Initializes the agent's context, pre-fetches active memory and hot topics, constructs the 
    tools manifest, and orchestrates the cyclical reasoning stream until a final response is generated.
    """
    
    try:
        dev_settings = get_config_value("developer_settings", {})
        limits = dev_settings.get("limits", {})

        tool_limits_raw = limits.get("tool_limits", {})

        if tool_limits_raw:
            defaults = dict(AgentRunConfig.tool_limits)
            defaults.update(tool_limits_raw)
            tool_limits_tuple = tuple(defaults.items())
        else:
            tool_limits_tuple = AgentRunConfig.tool_limits

        config = AgentRunConfig(
            max_calls=limits.get("max_tool_calls", 6),
            max_attempts=limits.get("max_attempts", 8),
            max_history_turns=limits.get("agent_history_turns", 7),
            max_accumulated_messages=limits.get("max_accumulated_messages", 30),
            max_consecutive_errors=limits.get("max_consecutive_errors", 3),
            tool_limits=tool_limits_tuple
        )

        run_id = str(uuid.uuid4())
        state = AgentState()
        evidence = RetrievedEvidence()
        valid_hot_topics = topic_config.validate_hot_topics(hot_topics)

        ctx = AgentContext(
            config=config,
            state=state,
            evidence=evidence,
            user_query=user_query,
            session_id=session_id,
            run_id=run_id,
            agent_id=agent_id,
            agent_name=agent_name,
            agent_persona=agent_persona,
            hot_topics=valid_hot_topics,
            active_topics=topic_config.active_topics,
            history=conversation_history
        )

        await emit(ctx.session_id, "agent", "run_started", {
            "query": user_query,
            "run_id": run_id,
            "agent_id": agent_id,
            "hot_topics": valid_hot_topics,
            "history_turns": len(conversation_history)
        })

        search_cfg = dev_settings.get("search", {}).copy()
        search_cfg.update(get_config_value("search", {}))

        memory_mgr = MemoryManager(
            redis=redis_client,
            user_name=user_name,
            session_id=session_id,
            agent_id=agent_id,
            topic_config=topic_config,
            on_event=lambda src, evt, data: asyncio.create_task(
                emit(session_id, src, evt, data)
            ),
        )            

        tools = Tools(
            user_name, store, ent_resolver, redis_client, session_id, 
            topic_config, search_config=search_cfg, file_rag=file_rag,
            mcp_manager=mcp_manager, memory=memory_mgr
        )

        if hot_topics:
            yield {"event": "status", "data": {"message": "Loading context..."}}
            ctx.hot_topic_context = await tools.get_hot_topic_context(hot_topics, slim=False)
        memory_blocks = await tools.get_memory_blocks(valid_hot_topics)
        memory_context = format_memory_context(memory_blocks)
        
        rules_str, prefs_str, icks_str = await memory_mgr._load_working_memory_strings()

        last_result = None
        try:
            tz = ZoneInfo(user_timezone) if user_timezone else ZoneInfo("UTC")
        except Exception:
            tz = ZoneInfo("UTC")
            
        if simulated_date:
            current_time = simulated_date
        else:
            current_time = datetime.now(tz).strftime("%Y-%m-%d %H:%M %Z")
        usage_accumulator = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        while ctx.state.attempt_count < ctx.config.max_attempts:
            ctx.state.attempt_count += 1

            should_force = (
                ctx.state.attempt_count >= ctx.config.max_attempts - 1
                and ctx.evidence.has_any()
            )
            should_abandon = (
                ctx.state.call_count >= ctx.config.max_calls // 2
                and not ctx.evidence.has_any()
                and ctx.state.call_count > 0
            )

            if should_force:
                ctx.state.last_error = "Final attempt. Respond now with accumulated evidence."
            elif should_abandon:
                ctx.state.last_error = "No relevant data found after extensive searching. Respond stating you could not find the requested information. Do not call more tools."
            
            # Collect tool calls while streaming tokens
            pending_tool_calls = []
            active_schemas = get_filtered_schemas(enabled_tools)
            if mcp_manager:
                active_schemas = active_schemas + mcp_tools_to_schemas(mcp_manager.get_all_tools())
            a_rules = agent_rules if agent_rules is not None else rules_str
            a_prefs = agent_preferences if agent_preferences is not None else prefs_str
            a_icks = agent_icks if agent_icks is not None else icks_str
            
            async for chunk in call_agent_streaming(
                llm, ctx, user_name, last_result, current_time, model, active_schemas,
                memory_context=memory_context,
                agent_rules=a_rules or "",
                agent_preferences=a_prefs or "",
                agent_icks=a_icks or "",
                agent_temperature=agent_temperature,
                agent_instructions=agent_instructions or ""
            ):
                
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
                        "usage": usage_accumulator,
                        "sources": ctx.evidence.sources if ctx.evidence.sources else None
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
                ctx.state.last_error = None
                continue
            ctx.state.last_error = None

            all_results = []
            async for event in execute_pending_tools(ctx, tools, pending_tool_calls, all_results):
                yield event

            last_result = all_results
            if ctx.state.consecutive_errors > 0:
                await asyncio.sleep(0.5 * ctx.state.consecutive_errors)

        # Max attempts - fallback
        if ctx.evidence.has_any():
            summary = await generate_fallback_summary(ctx, llm, user_name, user_query)

            yield {"event": "response", "data": {
                "content": summary or "I found information but couldn't summarize it.",
                "usage": usage_accumulator,
                "sources": ctx.evidence.sources if ctx.evidence.sources else None
            }}
        else:
            yield {"event": "clarification", "data": {
                "question": "I'm having trouble with that. Could you rephrase?",
                "usage": usage_accumulator
            }}

    except Exception as e:
        logger.error(f"Stream orchestrator error: {e}")
        yield {"event": "error", "data": {"message": str(e)}}