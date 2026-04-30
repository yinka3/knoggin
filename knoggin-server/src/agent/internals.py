from datetime import datetime
import json
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
from agent.tools.registry import Tools, TOOL_DISPATCH
from common.mcp.bridge import parse_mcp_tool_name
from common.errors.agent import ToolExecutionError
from agent.types import AgentRunConfig, AgentState, RetrievedEvidence, AgentContext

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