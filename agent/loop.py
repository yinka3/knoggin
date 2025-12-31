from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from typing import Dict, List, Optional, Set, Tuple, TYPE_CHECKING
import uuid

from loguru import logger
import redis

from agent.tools import Tools
from main.service import LLMService
from main.system_prompt import get_stella_prompt
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
from schema.tool_schema import TOOL_SCHEMAS
import time

if TYPE_CHECKING:
    from db.memgraph import MemGraphStore
    from main.entity_resolve import EntityResolver


@dataclass
class AgentContext:
    user_query: str = ""
    call_count: int = 0
    max_calls: int = 8
    attempt_count: int = 0
    max_attempts: int = 8
    trace_id: str = ""
    
    history: List[Dict] = field(default_factory=list)
    hot_topics: List[str] = field(default_factory=list)
    active_topics: List[str] = field(default_factory=list)
    
    hot_topic_context: Dict[str, List[Dict]] = field(default_factory=dict)
    retrieved_messages: List[Dict] = field(default_factory=list)
    entity_profiles: List[Dict] = field(default_factory=list)
    graph_results: List[Dict] = field(default_factory=list)
    
    tools_used: List[str] = field(default_factory=list)
    _previous_calls: Set[Tuple[str, str]] = field(default_factory=set)
    _last_error: Optional[str] = None

    def is_duplicate(self, tool_name: str, args: Dict) -> bool:
        call_sig = (tool_name, str(sorted(args.items())))
        return call_sig in self._previous_calls

    def record_call(self, tool_name: str, args: Dict):
        call_sig = (tool_name, str(sorted(args.items())))
        self._previous_calls.add(call_sig)
        self.call_count += 1
        self.tools_used.append(tool_name)

    def has_evidence(self) -> bool:
        return bool(self.entity_profiles or self.retrieved_messages or self.graph_results)


def build_user_message(ctx: AgentContext, last_result: Optional[Dict] = None) -> str:
    msg = ""

    if ctx.history:
        recent = ctx.history[-7:]
        msg += "**Recent conversation:**\n"
        for turn in recent:
            role = "User" if turn["role"] == "user" else "STELLA"
            ts = turn.get("timestamp", "")
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
    msg += f"**Calls remaining:** {ctx.max_calls - ctx.call_count}\n"

    if ctx._last_error:
        msg += f"\n**Last action rejected:** {ctx._last_error}\n"
        ctx._last_error = None

    if last_result:
        msg += "\n**Last tool result(s):**\n"
        results = last_result if isinstance(last_result, list) else [last_result]
        for r in results:
            tool = r.get("tool", "unknown")
            if "error" in r:
                msg += f"- `{tool}`: Error - {r['error']}\n"
            else:
                data = r.get("result", {}).get("data")
                if data is None or data == [] or data == {}:
                    msg += f"- `{tool}`: No results found\n"
                else:
                    msg += f"- `{tool}`: {json.dumps(data, indent=2, default=str)[:500]}\n"

    if ctx.hot_topic_context:
        msg += f"\n**Hot topic context (pre-fetched):**\n```json\n{json.dumps(ctx.hot_topic_context, indent=2, default=str)}\n```\n"

    if ctx.entity_profiles:
        msg += f"\n**Accumulated profiles ({len(ctx.entity_profiles)}):**\n```json\n{json.dumps(ctx.entity_profiles, indent=2, default=str)}\n```\n"

    if ctx.graph_results:
        msg += f"\n**Accumulated graph results ({len(ctx.graph_results)}):**\n```json\n{json.dumps(ctx.graph_results, indent=2, default=str)}\n```\n"

    if ctx.retrieved_messages:
        msg += f"\n**Accumulated messages ({len(ctx.retrieved_messages)}):**\n```json\n{json.dumps(ctx.retrieved_messages, indent=2, default=str)}\n```\n"

    return msg


async def call_the_doctor(
    llm: LLMService,
    ctx: AgentContext,
    user_name: str,
    last_result: Optional[Dict] = None,
    persona: str = ""
) -> StellaResponse:
    
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    system_prompt = get_stella_prompt(user_name, current_time, persona)
    user_message = build_user_message(ctx, last_result)

    response = await llm.call_with_tools(
        system=system_prompt,
        user=user_message,
        tools=TOOL_SCHEMAS
    )

    if not response or not response.get("tool_calls"):
        return FinalResponse(content="I couldn't determine how to help.")

    tool_calls = response["tool_calls"]
    if len(tool_calls) == 1:
        tc = tool_calls[0]
        name = tc["name"]
        args = json.loads(tc["arguments"])

        if name == "finish":
            return FinalResponse(content=args.get("response", ""))
        if name == "request_clarification":
            return ClarificationRequest(question=args.get("question", ""))
        return ToolCall(name=name, args=args)

    return [ToolCall(name=tc["name"], args=json.loads(tc["arguments"])) for tc in tool_calls]


async def execute_tool(tools: Tools, name: str, args: Dict) -> Dict:
    dispatch = {
        "search_messages": lambda: tools.search_messages(args.get("query", ""), args.get("limit", 5)),
        "search_entities": lambda: tools.search_entities(args.get("query", "")),
        "get_profile": lambda: tools.get_profile(args.get("entity_name", "")),
        "get_connections": lambda: tools.get_connections(args.get("entity_name", "")),
        "get_activity": lambda: tools.get_recent_activity(args.get("entity_name", ""), args.get("hours", 24)),
        "find_path": lambda: tools.find_path(args.get("entity_a", ""), args.get("entity_b", ""))
    }

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

    if tool_name == "search_messages":
        ctx.retrieved_messages.extend(data if isinstance(data, list) else [])
    elif tool_name == "search_entities":
        ctx.entity_profiles.extend(data if isinstance(data, list) else [])
    elif tool_name == "get_profile":
        if data:
            ctx.entity_profiles.append(data)
    elif tool_name in ("get_connections", "get_activity", "find_path"):
        ctx.graph_results.extend(data if isinstance(data, list) else [])


def summarize_result(tool_name: str, result: Dict) -> Tuple[str, int]:
    """Summarize tool result for trace."""
    if "error" in result:
        return f"Error: {result['error']}", 0

    data = result.get("data")
    if data is None:
        return "No results", 0

    if tool_name == "get_profile":
        if data:
            return f"Found: {data.get('name', data.get('canonical_name', 'unknown'))}", 1
        return "Not found", 0

    if tool_name in ("get_connections", "get_activity", "search_messages", "search_entities"):
        count = len(data) if isinstance(data, list) else 0
        return f"Found {count} results", count

    if tool_name == "find_path":
        if data:
            return f"Path found: {len(data)} hops", len(data)
        return "No path", 0

    return "Completed", 1


async def run(
    user_query: str,
    user_name: str,
    conversation_history: List[Dict],
    hot_topics: List[str],
    active_topics: List[str],
    llm: LLMService,
    store: 'MemGraphStore',
    ent_resolver: 'EntityResolver',
    redis_client: redis.Redis
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

    ctx = AgentContext(
        user_query=user_query,
        hot_topics=hot_topics,
        active_topics=active_topics,
        trace_id=trace.trace_id,
        history=conversation_history
    )

    tools = Tools(user_name, store, ent_resolver, redis_client, active_topics)

    if hot_topics:
        ctx.hot_topic_context = await tools.get_hot_topic_context(hot_topics)

    last_result = None
    step = 0

    while ctx.attempt_count < ctx.max_attempts:
        ctx.attempt_count += 1
        step += 1
        step_start = time.perf_counter()

        response = await call_the_doctor(llm, ctx, user_name, last_result)

        # Handle final response
        if isinstance(response, FinalResponse):
            logger.info(f"[STELLA] Trace {trace.trace_id} completed: {len(trace.entries)} tool calls")
            for entry in trace.entries:
                logger.debug(f"  Step {entry.step}: {entry.tool} -> {entry.result_summary} ({entry.duration_ms:.0f}ms)")
            
            final_text = system_warning + response.content if system_warning else response.content
            return CompleteResult(
                status="complete",
                response=final_text,
                tools_used=ctx.tools_used,
                state="complete",
                messages=ctx.retrieved_messages,
                profiles=ctx.entity_profiles,
                graph=ctx.graph_results
            )

        # Handle clarification
        if isinstance(response, ClarificationRequest):
            logger.info(f"[STELLA] Trace {trace.trace_id} ended with clarification: {len(trace.entries)} tool calls")
            
            final_q = system_warning + response.question if system_warning else response.question
            return ClarificationResult(
                status="clarification_needed",
                question=final_q,
                tools_used=ctx.tools_used,
                state="clarify"
            )

        # Handle tool calls
        tool_calls = [response] if isinstance(response, ToolCall) else response
        all_results = []

        for tc in tool_calls:
            tool_name = tc.name
            args = tc.args

            # Check duplicate
            if ctx.is_duplicate(tool_name, args):
                ctx._last_error = f"Already called {tool_name} with these args. Use accumulated context or try different parameters."
                trace.entries.append(TraceEntry(
                    step=step,
                    state="active",
                    tool=tool_name,
                    args=args,
                    resolved_args={},
                    result_summary=f"Rejected: duplicate call",
                    result_count=0,
                    duration_ms=(time.perf_counter() - step_start) * 1000,
                    error=ctx._last_error
                ))
                all_results.append({"tool": tool_name, "error": ctx._last_error})
                continue

            # Check call limit
            if ctx.call_count >= ctx.max_calls:
                ctx._last_error = "Call limit reached. You must finish with accumulated evidence or request clarification."
                trace.entries.append(TraceEntry(
                    step=step,
                    state="active",
                    tool=tool_name,
                    args=args,
                    resolved_args={},
                    result_summary=f"Rejected: call limit",
                    result_count=0,
                    duration_ms=(time.perf_counter() - step_start) * 1000,
                    error=ctx._last_error
                ))
                all_results.append({"tool": tool_name, "error": ctx._last_error})
                continue

            # Execute
            result = await execute_tool(tools, tool_name, args)
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
            
            ctx.record_call(tool_name, args)
            update_accumulators(ctx, tool_name, result)
            all_results.append({"tool": tool_name, "result": result})

        last_result = all_results

    # Fallback: max attempts reached
    logger.warning(f"[STELLA] Max attempts reached for query: {user_query[:50]}...")
    logger.info(f"[STELLA] Trace {trace.trace_id} fallback: {len(trace.entries)} tool calls")
    for entry in trace.entries:
        logger.debug(f"  Step {entry.step}: {entry.tool} -> {entry.result_summary} ({entry.duration_ms:.0f}ms)")
    
    if ctx.has_evidence():
        return CompleteResult(
            status="complete",
            response=system_warning + "Here's what I found, though I may not have fully answered your question.",
            tools_used=ctx.tools_used,
            state="fallback",
            messages=ctx.retrieved_messages,
            profiles=ctx.entity_profiles,
            graph=ctx.graph_results
        )

    return ClarificationResult(
        status="clarification_needed",
        question=system_warning + "I'm having trouble with that. Could you rephrase or be more specific?",
        tools_used=ctx.tools_used,
        state="fallback"
    )