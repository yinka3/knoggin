from datetime import datetime
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
from agent.tools import Tools
from shared.mcp_client import parse_mcp_tool_name


@dataclass(frozen=True)
class AgentRunConfig:
    """Immutable settings for agent run."""
    max_calls: int = 12
    max_attempts: int = 15
    max_history_turns: int = 7
    max_accumulated_messages: int = 30
    max_consecutive_errors: int = 3
    tool_timeout: float = 30.0
    tool_limits: Tuple[Tuple[str, int], ...] = (
        ("search_messages", 6),
        ("get_connections", 8),
        ("search_entity", 8),
        ("get_recent_activity", 8),
        ("find_path", 8),
        ("get_hierarchy", 8),
        ("web_search", 8),
        ("news_search", 8),
        ("save_memory", 4),
        ("save_insight", 4),
        ("forget_memory", 4),
        ("spawn_specialist", 2),
        ("mcp__*", 3),
    )
    
    def get_tool_limit(self, tool_name: str, default: int = 6) -> int:
        limits_dict = dict(self.tool_limits)
        if tool_name in limits_dict:
            return limits_dict[tool_name]
        
        for key, limit in limits_dict.items():
            if key.endswith("*") and tool_name.startswith(key[:-1]):
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
        call_sig = (tool_name, json.dumps(args, sort_keys=True, default=str))
        return call_sig in self.previous_calls
    
    def tool_limit_reached(self, tool_name: str, config: AgentRunConfig) -> bool:
        limit = config.get_tool_limit(tool_name, config.max_calls)
        return self.tool_call_counts.get(tool_name, 0) >= limit
    
    def record_call(self, tool_name: str, args: Dict):
        call_sig = (tool_name, json.dumps(args, sort_keys=True, default=str))
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
    sources: List[Dict] = field(default_factory=list)
    
    def has_any(self) -> bool:
        return bool(self.profiles or self.messages or self.graph or self.paths or self.hierarchy or self.sources)


@dataclass
class AgentContext:
    """Container for agent run."""
    config: AgentRunConfig
    state: AgentState
    evidence: RetrievedEvidence
    user_query: str = ""
    session_id: str = ""
    run_id: str = ""
    agent_id: str = ""
    agent_name: str = "STELLA"
    agent_persona: str = ""
    history: List[Dict] = field(default_factory=list)
    hot_topics: List[str] = field(default_factory=list)
    active_topics: List[str] = field(default_factory=list)
    hot_topic_context: Dict[str, Dict] = field(default_factory=dict)

def build_user_message(ctx: AgentContext, last_result=None) -> str:
    msg = ""

    if ctx.history:
        recent = ctx.history[-ctx.config.max_history_turns:]
        msg += "**Recent conversation:**\n"
        for turn in recent:
            role = "USER" if turn["role"] == "user" else "AGENT"
            ts = turn.get("timestamp")
            if ts:
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    msg += f"[{dt.strftime('%H:%M')}] {role}: {turn['content']}\n"
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

    # Latest tool results — full detail
    if last_result:
        msg += "\n**Last tool result(s):**\n"
        results = last_result if isinstance(last_result, list) else [last_result]
        for r in results:
            tool = r.get("tool", "unknown")
            data = r.get("result", {}).get("data")

            if tool in ("search_messages", "search_entity", "get_connections", 
                        "get_recent_activity", "find_path"):
                count = len(data) if isinstance(data, list) else 0
                if count > 0:
                    msg += f"- `{tool}`: Found {count} items. (See 'Retrieved Context' below)\n"
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

    if ctx.evidence.has_any():
        msg += "\n**Accumulated context:**\n"
        msg += _format_evidence(ctx.evidence, last_result)

    return msg


def _format_evidence(evidence: RetrievedEvidence, last_result=None) -> str:
    """
    Format evidence with full detail for new results,
    compact summary for previously seen data.
    """
    msg = ""
    
    new_profile_ids = set()
    new_message_ids = set()
    new_graph_keys = set()
    
    if last_result:
        results = last_result if isinstance(last_result, list) else [last_result]
        for r in results:
            tool = r.get("tool")
            data = r.get("result", {}).get("data")
            if not data or not isinstance(data, list):
                continue
            if tool == "search_entity":
                new_profile_ids = {d.get("id") for d in data if d.get("id")}
            elif tool == "search_messages":
                new_message_ids = {d.get("id") for d in data if d.get("id")}
            elif tool in ("get_connections", "get_recent_activity"):
                new_graph_keys = {
                    (d.get("source"), d.get("target")) for d in data
                    if d.get("source") and d.get("target")
                }

    if evidence.profiles:
        new_profiles = [p for p in evidence.profiles if p.get("id") in new_profile_ids]
        old_profiles = [p for p in evidence.profiles if p.get("id") not in new_profile_ids]
        
        if old_profiles:
            names = [p.get("canonical_name", "?") for p in old_profiles]
            msg += f"Previously retrieved entities: {', '.join(names)}\n"
        if new_profiles:
            msg += f"\n**New entity results:**\n{format_entity_results(new_profiles)}\n"

    if evidence.graph:
        new_graph = [g for g in evidence.graph 
                     if (g.get("source"), g.get("target")) in new_graph_keys]
        old_graph = [g for g in evidence.graph 
                     if (g.get("source"), g.get("target")) not in new_graph_keys]
        
        if old_graph:
            msg += f"Previously retrieved connections: {len(old_graph)} edges\n"
        if new_graph:
            msg += f"\n**New connection results:**\n{format_graph_results(new_graph)}\n"

    if evidence.paths:
        msg += f"\n**Path results:**\n{format_path_results(evidence.paths)}\n"

    if evidence.messages:
        new_msgs = [m for m in evidence.messages if m.get("id") in new_message_ids]
        old_msgs = [m for m in evidence.messages if m.get("id") not in new_message_ids]
        
        if old_msgs:
            msg += f"Previously retrieved messages: {len(old_msgs)} results\n"
        if new_msgs:
            msg += f"\n**New message results:**\n{format_retrieved_messages(new_msgs)}\n"

    if evidence.hierarchy:
        msg += f"\n**Hierarchy results:**\n{format_hierarchy_results(evidence.hierarchy)}\n"

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
    elif tool_name in ("get_connections", "get_recent_activity"):
        _merge_unique(
            ctx.evidence.graph, 
            data if isinstance(data, list) else [], 
            lambda x: (x.get("source"), x.get("target"))
        )
    elif tool_name == "find_path":
        ctx.evidence.paths.extend(data if isinstance(data, list) else [])
    elif tool_name == "get_hierarchy":
        if isinstance(data, dict):
            ctx.evidence.hierarchy.append(data)
        elif isinstance(data, list):
            ctx.evidence.hierarchy.extend(data)
    elif tool_name == "search_files":
        if isinstance(data, list) and data and "error" not in data[0]:
            _merge_unique(ctx.evidence.messages, data, lambda x: f"{x.get('file_id')}_{x.get('chunk_index')}")
    elif tool_name == "web_search":
        if isinstance(data, list):
            _merge_unique(ctx.evidence.sources, data, lambda x: x.get('url'))
    elif tool_name == "news_search":
        if isinstance(data, list):
            _merge_unique(ctx.evidence.sources, data, lambda x: x.get('url'))
    elif tool_name.startswith("mcp__"):
        if isinstance(data, str):
            ctx.evidence.messages.append({
                "id": f"mcp_{ctx.state.call_count}",
                "role": "tool",
                "message": data,
                "source": tool_name
            })
        elif isinstance(data, list):
            for item in data:
                ctx.evidence.messages.append(item)
    elif tool_name in ("save_memory", "forget_memory"):
        ctx.evidence.messages.append({
            "id": f"{tool_name}_{ctx.state.call_count}",
            "role": "tool",
            "message": f"{tool_name} completed successfully",
            "source": tool_name
        })


def summarize_result(tool_name: str, result: Dict) -> Tuple[str, int]:
    """Summarize tool result for trace."""
    if "error" in result:
        return f"Error: {result['error']}", 0

    data = result.get("data")
    if data is None:
        return "No results", 0

    if tool_name in ("get_connections", "get_recent_activity", "search_messages", "search_entity"):
        count = len(data) if isinstance(data, list) else 0
        return f"Found {count} results", count

    if tool_name == "find_path":
        if data:
            return f"Path found: {len(data)} hops", len(data)
        return "No path", 0
    
    if tool_name in ("save_memory", "forget_memory"):
        if "error" in result:
            return f"Error: {result['error']}", 0
        return "Memory updated", 1
    
    if tool_name == "search_files":
        count = len(data) if isinstance(data, list) else 0
        if count > 0 and "error" not in (data[0] if data else {}):
            return f"Found {count} relevant chunks", count
        return "No results", 0
    
    if tool_name.startswith("mcp__"):
        if isinstance(data, str):
            preview = data[:100] + "..." if len(data) > 100 else data
            return f"MCP result: {preview}", 1
        return f"MCP result: {len(data)} items" if isinstance(data, list) else "MCP completed", 1

    return "Completed", 1

async def execute_tool(tools: Tools, name: str, args: Dict) -> Dict:
    parsed = parse_mcp_tool_name(name)
    if parsed:
        server_name, tool_name = parsed
        if not tools.mcp_manager:
            return {"error": "MCP not configured"}
        logger.info(f"[MCP TOOL CALL] {server_name}.{tool_name}: {json.dumps(args)}")
        return await tools.mcp_manager.call_tool(server_name, tool_name, args)
    
    
    dispatch = {
        "search_messages": lambda: tools.search_messages(args.get("query", ""), min(args.get("limit", 8), 8)),
        "search_entity": lambda: tools.search_entity(args.get("query", ""), min(args.get("limit", 5), 5)),
        "get_connections": lambda: tools.get_connections(args.get("entity_name", "")),
        "get_recent_activity": lambda: tools.get_recent_activity(args.get("entity_name", ""), args.get("hours", 24)),
        "find_path": lambda: tools.find_path(args.get("entity_a", ""), args.get("entity_b", "")),
        "get_hierarchy": lambda: tools.get_hierarchy(args.get("entity_name", ""), args.get("direction", "both")),
        "save_memory": lambda: tools.save_memory(args.get("content", ""), args.get("topic", "General")),
        "forget_memory": lambda: tools.forget_memory(args.get("memory_id", "")),
        "search_files": lambda: tools.search_files(args.get("query", ""), args.get("file_name"), args.get("limit", 5)),
        "web_search": lambda: tools.web_search(args.get("query", ""), args.get("limit", 5), args.get("freshness")),
        "news_search": lambda: tools.news_search(args.get("query", ""), args.get("limit", 5), args.get("freshness")),
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