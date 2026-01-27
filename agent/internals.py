import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from loguru import logger

from agent.formatters import (
    format_hierarchy_results,
    format_retrieved_messages,
    format_entity_results,
    format_graph_results,
    format_path_results,
    format_hot_topic_context,
)
from schema.dtypes import QueryTrace


@dataclass(frozen=True)
class AgentConfig:
    """Immutable settings for agent run."""
    max_calls: int = 6
    max_attempts: int = 8
    max_history_turns: int = 7
    max_accumulated_messages: int = 30
    tool_limits: Tuple[Tuple[str, int], ...] = (
        ("search_messages", 2),
        ("get_connections", 4),
        ("search_entity", 4),
        ("get_activity", 5),
        ("find_path", 5),
        ("get_hierarchy", 5),
    )
    
    def get_tool_limit(self, tool_name: str, default: int = 6) -> int:
        for name, limit in self.tool_limits:
            if name == tool_name:
                return limit
        return default


@dataclass
class AgentState:
    """Mutable tracking during run."""
    call_count: int = 0
    attempt_count: int = 0
    consecutive_errors: int = 0
    tools_used: List[str] = field(default_factory=list)
    previous_calls: Set[Tuple[str, str]] = field(default_factory=set)
    last_error: Optional[str] = None
    tool_call_counts: Dict[str, int] = field(default_factory=dict)
    
    def is_duplicate(self, tool_name: str, args: Dict) -> bool:
        call_sig = (tool_name, json.dumps(args, sort_keys=True))
        return call_sig in self.previous_calls
    
    def tool_limit_reached(self, tool_name: str, config: AgentConfig) -> bool:
        limit = config.get_tool_limit(tool_name, config.max_calls)
        return self.tool_call_counts.get(tool_name, 0) >= limit
    
    def record_call(self, tool_name: str, args: Dict):
        call_sig = (tool_name, json.dumps(args, sort_keys=True))
        self.previous_calls.add(call_sig)
        self.call_count += 1
        self.tools_used.append(tool_name)
        self.tool_call_counts[tool_name] = self.tool_call_counts.get(tool_name, 0) + 1


@dataclass
class RetrievedEvidence:
    """Accumulated results from tool calls."""
    messages: List[Dict] = field(default_factory=list)
    profiles: List[Dict] = field(default_factory=list)
    graph: List[Dict] = field(default_factory=list)
    paths: List[Dict] = field(default_factory=list)
    hierarchy: List[Dict] = field(default_factory=list)
    
    def has_any(self) -> bool:
        return bool(self.profiles or self.messages or self.graph or self.paths or self.hierarchy)


@dataclass
class AgentContext:
    """Container for agent run."""
    config: AgentConfig
    state: AgentState
    evidence: RetrievedEvidence
    
    user_query: str = ""
    trace_id: str = ""
    history: List[Dict] = field(default_factory=list)
    hot_topics: List[str] = field(default_factory=list)
    active_topics: List[str] = field(default_factory=list)
    hot_topic_context: Dict[str, Dict] = field(default_factory=dict)

def build_user_message(ctx: AgentContext, last_result: Optional[Dict] = None) -> str:
    msg = ""

    if ctx.history:
        recent = ctx.history[-ctx.config.max_history_turns:]
        msg += "**Recent conversation:**\n"
        for turn in recent:
            role = "User" if turn["role"] == "user" else "AGENT"
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
    
    if ctx.evidence.hierarchy:
        msg += f"\n**Hierarchy results ({len(ctx.evidence.hierarchy)}):**\n{format_hierarchy_results(ctx.evidence.hierarchy)}\n"

    return msg

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
    elif tool_name == "get_hierarchy":
        if isinstance(data, dict):
            ctx.evidence.hierarchy.append(data)
        elif isinstance(data, list):
            ctx.evidence.hierarchy.extend(data)


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
        logger.info(f"[AGENT] Trace {trace.trace_id} completed: {len(trace.entries)} tool calls")
    elif state == "clarify":
        logger.info(f"[AGENT] Trace {trace.trace_id} ended with clarification: {len(trace.entries)} tool calls")
    elif state == "fallback":
        logger.warning(f"[AGENT] Max attempts reached for query: {query[:50]}...")
        logger.info(f"[AGENT] Trace {trace.trace_id} fallback: {len(trace.entries)} tool calls")
        for entry in trace.entries:
            logger.debug(f"  Step {entry.step}: {entry.tool} -> {entry.result_summary} ({entry.duration_ms:.0f}ms)")