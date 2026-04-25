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
    format_fact_results,
)
from agent.tools import Tools, TOOL_DISPATCH
from common.schema.memory import PromptContext
from common.mcp.bridge import parse_mcp_tool_name
from common.errors.agent import ToolExecutionError


@dataclass(frozen=True)
class AgentRunConfig:
    """Immutable settings governing limits and timeouts for an agent run."""
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
        ("fact_check", 6),
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
    """Mutable tracking state maintained during a single agent reasoning loop."""
    call_count: int = 0
    attempt_count: int = 0
    consecutive_errors: int = 0
    tools_used: List[str] = field(default_factory=list)
    previous_calls: Set[Tuple[str, str]] = field(default_factory=set)
    last_error: Optional[str] = None
    tool_call_counts: Dict[str, int] = field(default_factory=dict)
    usage: Dict[str, int] = field(default_factory=lambda: {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})
    
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
    """Accumulated contextual results gathered from tool executions."""
    messages: List[Dict] = field(default_factory=list)
    profiles: List[Dict] = field(default_factory=list)
    graph: List[Dict] = field(default_factory=list)
    paths: List[Dict] = field(default_factory=list)
    hierarchy: List[Dict] = field(default_factory=list)
    facts: List[Dict] = field(default_factory=list)
    sources: List[Dict] = field(default_factory=list)
    summary: Optional[str] = None
    token_count: int = 0
    
    def has_any(self) -> bool:
        return bool(self.profiles or self.messages or self.graph or self.paths or self.hierarchy or self.facts or self.sources or self.summary)


@dataclass
class AgentContext:
    """Core container aggregating configuration, state, and evidence for an agent execution."""
    config: AgentRunConfig
    state: AgentState
    evidence: RetrievedEvidence
    user_name: str = ""
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
    prompt: PromptContext = field(default_factory=PromptContext)
    is_community: bool = False
    current_participants: List[str] = field(default_factory=list)

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
                except Exception:
                    msg += f"{role}: {turn['content']}\n"
            else:
                msg += f"{role}: {turn['content']}\n"
        msg += "\n"

    if ctx.is_community and ctx.current_participants:
        msg += f"**Participants:** {', '.join(ctx.current_participants)}\n\n"

    msg += f"**Query:** {ctx.user_query}\n"
    msg += f"**Calls remaining:** {ctx.config.max_calls - ctx.state.call_count}\n"

    if ctx.state.last_error:
        msg += f"\n**Last action rejected:** {ctx.state.last_error}\n"

    # Latest tool results — full detail
    if last_result:
        msg += "\n**Last tool result(s):**\n"
        results = last_result if isinstance(last_result, list) else [last_result]
        for r in results:
            tool = r.get("tool", "unknown")
            data = r.get("result", {}).get("data")

            if tool in ("search_messages", "search_entity", "get_connections", 
                        "get_recent_activity", "find_path", "fact_check", "get_hierarchy", "search_files", "web_search", "news_search"):
                data_val = data if isinstance(data, list) else []
                count = len(data_val)
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
            elif tool == "search_files":
                new_message_ids = {f"{d.get('file_id', 'file')}_{d.get('chunk_index', 0)}" for d in data}
            elif tool in ("get_connections", "get_recent_activity"):
                new_graph_keys = {
                    (d.get("source"), d.get("target")) for d in data
                    if d.get("source") and d.get("target")
                }
            elif tool == "fact_check":
                # For fact_check, we'll treat all results in the latest call as 'new'
                pass

    if evidence.summary:
        msg += f"**Core Evidence Summary:**\n{evidence.summary}\n\n"

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

    if evidence.facts:
        msg += f"\n**Fact check results:**\n{format_fact_results(evidence.facts)}\n"
    
    if evidence.summary:
        msg += f"\n**Evidence summary (compressed):**\n{evidence.summary}\n"

    return msg

def build_evidence_context(evidence: RetrievedEvidence) -> str:
    """Serialize all evidence to a string for token counting."""
    return _format_evidence(evidence, last_result=None)

def update_accumulators(ctx: AgentContext, tool_name: str, result: Dict):
    """
    Merge the newly retrieved tool results into the agent's accumulated evidence context.
    Prevents duplicate entries and applies ranking or limits where required.
    """
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
            ctx.evidence.messages.sort(key=lambda x: x.get('score') if x.get('score') is not None else 0.5, reverse=True)
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
    elif tool_name == "fact_check":
        if isinstance(data, dict):
            ctx.evidence.facts.append(data)
        elif isinstance(data, list):
            ctx.evidence.facts.extend(data)
    elif tool_name == "search_files":
        if isinstance(data, list) and data and "error" not in data[0]:
            normalized = []
            for chunk in data:
                normalized.append({
                    "id": f"{chunk.get('file_id', 'file')}_{chunk.get('chunk_index', 0)}",
                    "content": chunk.get("content", ""),
                    "message": chunk.get("content", ""),
                    "role": "file",
                    "score": chunk.get("score", 0.5),
                    "source": chunk.get("file_name", "uploaded file")
                })
            _merge_unique(ctx.evidence.messages, normalized, lambda x: x["id"])
    elif tool_name == "web_search":
        if isinstance(data, list):
            _merge_unique(ctx.evidence.sources, data, lambda x: x.get('url'))
    elif tool_name == "news_search":
        if isinstance(data, list):
            _merge_unique(ctx.evidence.sources, data, lambda x: x.get('url'))
    elif tool_name.startswith("mcp__"):
        content = data if isinstance(data, str) else json.dumps(data, default=str) if data else ""
        ctx.evidence.messages.append({
            "id": f"mcp_{ctx.state.call_count}",
            "score": 0.5,
            "context": [{
                "role": "tool",
                "content": content[:2000],
                "timestamp": "",
                "is_hit": True
            }]
        })
    elif tool_name in ("save_memory", "forget_memory"):
        pass 


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
    
    if tool_name == "fact_check":
        if isinstance(data, dict):
            res_type = data.get("resolution", "unknown")
            results = data.get("results", [])
            count = len(results)
            return f"Resolved via {res_type} ({count} matches)", count
        return "No results", 0
    
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
        return (f"MCP result: {len(data)} items" if isinstance(data, list) else "MCP completed"), 1

    return "Completed", 1



async def execute_tool(tools: Tools, name: str, args: Dict) -> Dict:
    parsed = parse_mcp_tool_name(name)
    if parsed:
        server_name, tool_name = parsed
        if not tools.mcp_manager:
            raise ToolExecutionError(name, "MCP not configured")
        logger.info(f"[MCP TOOL CALL] {server_name}.{tool_name}: {json.dumps(args)}")
        try:
            return await tools.mcp_manager.call_tool(server_name, tool_name, args)
        except Exception as e:
            raise ToolExecutionError(name, str(e))

    if name == "request_clarification":
        return {"clarification": args.get("question", "Could you clarify?")}

    dispatch_entry = TOOL_DISPATCH.get(name)
    if dispatch_entry is None:
        raise ToolExecutionError(name, f"Unknown tool: {name}")

    method_name, param_keys = dispatch_entry
    method = getattr(tools, method_name, None)
    if method is None:
        raise ToolExecutionError(name, f"Tool method not found: {method_name}")

    logger.info(f"[TOOL CALL] {name}: {json.dumps(args)}")

    try:
        kwargs = {k: args.get(k) for k in param_keys if k in args}
        result = await method(**kwargs)
        return {"data": result}
    except Exception as e:
        logger.error(f"Tool {name} failed: {e}")
        raise ToolExecutionError(name, str(e))