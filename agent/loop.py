from datetime import datetime, timezone
import json
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING
import uuid

from loguru import logger
import redis

from agent.tools import Tools
from main.service import LLMService
from agent.system_prompt import get_fallback_summary_prompt, get_stella_prompt
from schema.dtypes import (
    ClarificationRequest,
    ClarificationResult,
    CompleteResult,
    FinalResponse,
    QueryTrace,
    RunResult,
    StellaResponse,
    ToolCall,
    TraceEntry,
)
from agent.context import (
    AgentConfig, AgentState, 
    RetrievedEvidence, AgentContext)

from agent.formatters import (
    format_retrieved_messages,
    format_entity_results,
    format_graph_results,
    format_path_results,
    format_hot_topic_context,
)
from schema.tool_schema import TOOL_SCHEMAS
import time

if TYPE_CHECKING:
    from db.memgraph import MemGraphStore
    from main.entity_resolve import EntityResolver


def build_user_message(ctx: AgentContext, last_result: Optional[Dict] = None) -> str:
    msg = ""

    if ctx.history:
        recent = ctx.history[-ctx.config.max_history_turns:]
        msg += "**Recent conversation:**\n"
        for turn in recent:
            role = "User" if turn["role"] == "user" else "STELLA"
            ts = turn.get("timestamp")
            if ts:
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    ts_fmt = dt.strftime("%H:%M")
                    msg += f"[{ts_fmt}] {role}: {turn['content']}\n"
                except:
                    msg += f"{role}: {turn['content']}\n"
            else:
                msg += f"{role}: {turn['content']}\n"
        msg += "\n"

    msg += f"**Query:** {ctx.user_query}\n"
    msg += f"**Calls remaining:** {ctx.config.max_calls - ctx.state.call_count}\n"

    if ctx.state.last_error:
        msg += f"\n**Last action rejected:** {ctx.state.last_error}\n"
        ctx.state.last_error = None

    if last_result:
        msg += "\n**Last tool result(s):**\n"
        results = last_result if isinstance(last_result, list) else [last_result]
        for r in results:
            tool = r.get("tool", "unknown")
            data = r.get("result", {}).get("data")
            
            if tool in ("search_messages", "search_entity", "get_connections", "get_activity", "find_path"):
                count = len(data) if isinstance(data, list) else 0
                if count > 0:
                    msg += f"- `{tool}`: Success. Found {count} items. (See 'Retrieved Context' below)\n"
                else:
                    msg += f"- `{tool}`: No results found.\n"
            elif "error" in r:
                msg += f"- `{tool}`: Error - {r['error']}\n"
            else:
                if not data:
                    msg += f"- `{tool}`: No results found\n"
                else:
                    msg += f"- `{tool}`: {json.dumps(data, indent=2, default=str)}\n"

    if ctx.hot_topic_context:
        msg += f"\n**Hot topic context (pre-fetched):**\n{format_hot_topic_context(ctx.hot_topic_context)}\n"

    if ctx.evidence.profiles:
        msg += f"\n**Accumulated profiles ({len(ctx.evidence.profiles)}):**\n{format_entity_results(ctx.evidence.profiles)}\n"

    if ctx.evidence.graph:
        msg += f"\n**Accumulated graph results ({len(ctx.evidence.graph)}):**\n{format_graph_results(ctx.evidence.graph)}\n"

    if ctx.evidence.paths:
        msg += f"\n**Path results:**\n{format_path_results(ctx.evidence.paths)}\n"

    if ctx.evidence.messages:
        msg += f"\n**Accumulated messages ({len(ctx.evidence.messages)}):**\n{format_retrieved_messages(ctx.evidence.messages)}\n"

    return msg


async def call_the_doctor(
    llm: LLMService,
    ctx: AgentContext,
    user_name: str,
    last_result: Optional[Dict] = None,
    persona: str = "",
    date: str = ""
) -> StellaResponse:
    
    # current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    system_prompt = get_stella_prompt(user_name, date, persona)
    user_message = build_user_message(ctx, last_result)

    response = await llm.call_with_tools(
        system=system_prompt,
        user=user_message,
        tools=TOOL_SCHEMAS
    )

    if not response:
        return FinalResponse(content="System Error: LLM failed to respond.")
    
    content = response.get("content", "") or ""
    
    if not response.get("tool_calls"):
        return FinalResponse(content=content)
    
    if content:
        logger.info(f"[STELLA THOUGHT]: {content}")

    tool_calls = response["tool_calls"]
    if len(tool_calls) == 1:
        tc = tool_calls[0]
        name = tc["name"]
        args = json.loads(tc["arguments"])

        if name == "request_clarification":
            return ClarificationRequest(question=args.get("question", ""))
        return ToolCall(name=name, args=args)

    return [ToolCall(name=tc["name"], args=json.loads(tc["arguments"])) for tc in tool_calls]


async def execute_tool(tools: Tools, name: str, args: Dict) -> Dict:
    dispatch = {
        "search_messages": lambda: tools.search_messages(args.get("query", ""), min(args.get("limit", 8), 8)),
        "search_entity": lambda: tools.search_entity(args.get("query", ""), min(args.get("limit", 5), 5)),
        "get_connections": lambda: tools.get_connections(args.get("entity_name", "")),
        "get_activity": lambda: tools.get_recent_activity(args.get("entity_name", ""), args.get("hours", 24)),
        "find_path": lambda: tools.find_path(args.get("entity_a", ""), args.get("entity_b", "")),
        "get_hierarchy": lambda: tools.get_hierarchy(args.get("entity_name", ""), args.get("direction", "both")),
    }

    logger.info(f"[TOOL CALL] {name}: {json.dumps(args)}")
    if name not in dispatch:
        return {"error": f"Unknown tool: {name}"}

    try:
        result = await dispatch[name]()
        return {"data": result}
    except Exception as e:
        logger.error(f"Tool {name} failed: {e}")
        return {"error": str(e)}


def update_accumulators(ctx: AgentContext, tool_name: str, result: Dict):
    if not result or "error" in result:
        return

    data = result.get("data")
    if not data:
        return

    def _merge_unique(target_list: List, new_items, key_func):
        existing_keys = {key_func(item) for item in target_list}
        for item in new_items:
            k = key_func(item)
            if k not in existing_keys:
                target_list.append(item)
                existing_keys.add(k)

    if tool_name == "search_messages":
        _merge_unique(ctx.evidence.messages, data if isinstance(data, list) else [], lambda x: x['id'])
        if len(ctx.evidence.messages) > ctx.config.max_accumulated_messages:
            ctx.evidence.messages.sort(key=lambda x: x.get('score', 0), reverse=True)
            ctx.evidence.messages = ctx.evidence.messages[:ctx.config.max_accumulated_messages]
    elif tool_name == "search_entity":
        _merge_unique(ctx.evidence.profiles, data if isinstance(data, list) else [], lambda x: x['id'])
    elif tool_name in ("get_connections", "get_activity"):
        ctx.evidence.graph.extend(data if isinstance(data, list) else [])
    elif tool_name == "find_path":
        ctx.evidence.paths.extend(data if isinstance(data, list) else [])


def summarize_result(tool_name: str, result: Dict) -> Tuple[str, int]:
    """Summarize tool result for trace."""
    if "error" in result:
        return f"Error: {result['error']}", 0

    data = result.get("data")
    if data is None:
        return "No results", 0

    if tool_name in ("get_connections", "get_activity", "search_messages", "search_entity"):
        count = len(data) if isinstance(data, list) else 0
        return f"Found {count} results", count

    if tool_name == "find_path":
        if data:
            return f"Path found: {len(data)} hops", len(data)
        return "No path", 0

    return "Completed", 1

def _log_trace_summary(trace: QueryTrace, state: str, query: str = ""):
    """Log trace summary at exit points."""
    if state == "complete":
        logger.info(f"[STELLA] Trace {trace.trace_id} completed: {len(trace.entries)} tool calls")
    elif state == "clarify":
        logger.info(f"[STELLA] Trace {trace.trace_id} ended with clarification: {len(trace.entries)} tool calls")
    elif state == "fallback":
        logger.warning(f"[STELLA] Max attempts reached for query: {query[:50]}...")
        logger.info(f"[STELLA] Trace {trace.trace_id} fallback: {len(trace.entries)} tool calls")
        for entry in trace.entries:
            logger.debug(f"  Step {entry.step}: {entry.tool} -> {entry.result_summary} ({entry.duration_ms:.0f}ms)")

async def _process_tool_calls(
    ctx: AgentContext,
    trace: QueryTrace,
    tools: Tools,
    response: ToolCall | List[ToolCall],
    step: int
) -> List[Dict]:
    """Process tool calls, returns results for next iteration."""
    tool_calls = [response] if isinstance(response, ToolCall) else response
    all_results = []
    step_start = time.perf_counter()

    for tc in tool_calls:
        tool_name = tc.name
        args = tc.args

        # Check duplicate
        if ctx.state.is_duplicate(tool_name, args):
            ctx.state.consecutive_errors += 1
            if ctx.state.consecutive_errors >= 3:
                logger.warning(f"Breaking loop: {ctx.state.consecutive_errors} consecutive errors.")
                ctx.state.last_error = "Too many repeated errors. Stopping to save cost."
                break

            ctx.state.last_error = f"Already called {tool_name} with these args. Use accumulated context or try different parameters."
            trace.entries.append(TraceEntry(
                step=step,
                state="active",
                tool=tool_name,
                args=args,
                resolved_args={},
                result_summary="Rejected: duplicate call",
                result_count=0,
                duration_ms=(time.perf_counter() - step_start) * 1000,
                error=ctx.state.last_error
            ))
            all_results.append({"tool": tool_name, "error": ctx.state.last_error})
            continue

        # Check per-tool limit
        if ctx.state.tool_limit_reached(tool_name, ctx.config):
            ctx.state.last_error = f"{tool_name} limit reached ({ctx.config.get_tool_limit(tool_name)}). Use a different tool or conclude."
            all_results.append({"tool": tool_name, "error": ctx.state.last_error})
            continue

        # Check call limit
        if ctx.state.call_count >= ctx.config.max_calls:
            ctx.state.last_error = "Call limit reached. You must finish with accumulated evidence or request clarification."
            trace.entries.append(TraceEntry(
                step=step,
                state="active",
                tool=tool_name,
                args=args,
                resolved_args={},
                result_summary="Rejected: call limit",
                result_count=0,
                duration_ms=(time.perf_counter() - step_start) * 1000,
                error=ctx.state.last_error
            ))
            all_results.append({"tool": tool_name, "error": ctx.state.last_error})
            continue
        
        ctx.state.consecutive_errors = 0
        
        # Execute
        result = await execute_tool(tools, tool_name, args)
        logger.info(f"[TOOL RESULT] {tool_name}: {json.dumps(result, default=str)[:1000]}")
        result_summary, result_count = summarize_result(tool_name, result)
        
        trace.entries.append(TraceEntry(
            step=step,
            state="active",
            tool=tool_name,
            args=args,
            resolved_args=args,
            result_summary=result_summary,
            result_count=result_count,
            duration_ms=(time.perf_counter() - step_start) * 1000,
            error=result.get("error") if isinstance(result, dict) else None
        ))
        
        ctx.state.record_call(tool_name, args)
        update_accumulators(ctx, tool_name, result)
        all_results.append({"tool": tool_name, "result": result})

    return all_results  

async def run(
    user_query: str,
    user_name: str,
    conversation_history: List[Dict],
    hot_topics: List[str],
    active_topics: List[str],
    llm: LLMService,
    store: 'MemGraphStore',
    ent_resolver: 'EntityResolver',
    redis_client: redis.Redis,
    persona: str = "",
    slim_hot_context: bool = False,
    date: str = ""
) -> RunResult:
    
    # Check for system warnings
    system_warning = ""
    try:
        raw_warning = await redis_client.get("system:active_job_warning")
        if raw_warning:
            system_warning = f"{raw_warning}\n\n---\n\n"
    except Exception as e:
        logger.error(f"Failed to check system warning: {e}")

    trace = QueryTrace(
        trace_id=str(uuid.uuid4()),
        user_query=user_query,
        started_at=datetime.now(timezone.utc)
    )

    config = AgentConfig()
    state = AgentState()
    evidence = RetrievedEvidence()

    ctx = AgentContext(
        config=config,
        state=state,
        evidence=evidence,
        user_query=user_query,
        hot_topics=hot_topics,
        active_topics=active_topics,
        trace_id=trace.trace_id,
        history=conversation_history
    )

    tools = Tools(user_name, store, ent_resolver, redis_client, active_topics)

    if hot_topics:
        ctx.hot_topic_context = await tools.get_hot_topic_context(hot_topics, slim=slim_hot_context)
        logger.info(f"[HOT CONTEXT] {json.dumps(ctx.hot_topic_context, indent=2, default=str)[:1000]}")

    last_result = None
    step = 0

    while ctx.state.attempt_count < ctx.config.max_attempts:
        ctx.state.attempt_count += 1
        should_force_conclusion = (
            ctx.state.attempt_count >= ctx.config.max_attempts - 1
            and ctx.evidence.has_any()
        )
        step += 1

        if should_force_conclusion:
            ctx.state.last_error = "Final attempt. You MUST respond now using accumulated evidence. Do not call any tools."

        response = await call_the_doctor(llm, ctx, user_name, last_result, persona, date)

        if isinstance(response, FinalResponse):
            _log_trace_summary(trace, "complete")
            final_text = system_warning + response.content if system_warning else response.content
            return CompleteResult(
                status="complete",
                response=final_text,
                tools_used=ctx.state.tools_used,
                state="complete",
                messages=ctx.evidence.messages,
                profiles=ctx.evidence.profiles,
                graph=ctx.evidence.graph
            )

        if isinstance(response, ClarificationRequest):
            _log_trace_summary(trace, "clarify")
            final_q = system_warning + response.question if system_warning else response.question
            return ClarificationResult(
                status="clarification_needed",
                question=final_q,
                tools_used=ctx.state.tools_used,
                state="clarify"
            )

        last_result = await _process_tool_calls(ctx, trace, tools, response, step)
    
    _log_trace_summary(trace, "fallback", user_query)
    
    if ctx.evidence.has_any():
        evidence_ctx = ""
        if ctx.evidence.profiles:
            evidence_ctx += f"Profiles:\n{format_entity_results(ctx.evidence.profiles)}\n\n"
        if ctx.evidence.messages:
            evidence_ctx += f"Messages:\n{format_retrieved_messages(ctx.evidence.messages)}\n\n"
        if ctx.evidence.graph:
            evidence_ctx += f"Connections:\n{format_graph_results(ctx.evidence.graph)}\n\n"
        
        summary = await llm.call_reasoning(
            system=get_fallback_summary_prompt(user_name),
            user=f"Query: {user_query}\n\nEvidence:\n{evidence_ctx}"
        )
        
        return CompleteResult(
            status="complete",
            response=system_warning + (summary or "I found some information but couldn't summarize it."),
            tools_used=ctx.state.tools_used,
            state="fallback",
            messages=ctx.evidence.messages,
            profiles=ctx.evidence.profiles,
            graph=ctx.evidence.graph
        )

    return ClarificationResult(
        status="clarification_needed",
        question=system_warning + "I'm having trouble with that. Could you rephrase or be more specific?",
        tools_used=ctx.state.tools_used,
        state="fallback"
    )