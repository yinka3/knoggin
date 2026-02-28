
import asyncio
import copy
from fastapi import APIRouter, Depends, HTTPException, Request
from api.deps import get_app_state
from api.state import AppState
from shared.config import load_config, save_config, get_default_config, deep_merge, redact_config
from shared.schema.settings import ConfigUpdate
from shared.schema.tool_schema import TOOL_SCHEMAS

router = APIRouter()

@router.get("/")
async def get_config():
    config = load_config()
    if not config:
        return get_default_config()
    return redact_config(config)


@router.get("/status")
async def get_config_status():
    config = load_config()
    
    has_api_key = bool(config and config.get("llm", {}).get("api_key"))
    has_user_name = bool(config and config.get("user_name"))
    
    return {
        "configured": has_api_key and has_user_name,
        "has_api_key": has_api_key,
        "has_user_name": has_user_name
    }


@router.get("/tools")
async def get_tools(request: Request):
    """
    Return a comprehensive list of available tools.
    Combines standard Knoggin tools with dynamic MCP tools.
    """
    schema_map = {
        s["function"]["name"]: s["function"]
        for s in TOOL_SCHEMAS
        if s["function"]["name"] != "request_clarification"
    }

    standard_tools = []
    for tool_id in ["search_entity", "save_memory", "forget_memory", "get_connections", 
                    "find_path", "get_hierarchy", "search_messages", "get_recent_activity", "search_files", "web_search", "news_search"]:
        schema = schema_map.get(tool_id, {})
        group_map = {
            "search_entity": "Memory", "save_memory": "Memory", "forget_memory": "Memory",
            "get_connections": "Graph", "find_path": "Graph", "get_hierarchy": "Graph",
            "search_messages": "History", "get_recent_activity": "History",
            "search_files": "RAG",
            "web_search": "Search",
            "news_search": "Search",
        }
        standard_tools.append({
            "id": tool_id,
            "name": schema.get("name", tool_id).replace("_", " ").title(),
            "description": schema.get("description", ""),
            "parameters": schema.get("parameters", {}),
            "source": "knoggin",
            "group": group_map.get(tool_id, "Other"),
        })
    
    mcp_tools = []
    if request.app.state.app_state.resources.mcp_manager:
        raw_mcp = request.app.state.app_state.resources.mcp_manager.get_all_tools()
        for tool in raw_mcp:
            name = tool.get("namespaced")
            if not name: continue
            
            # Parse namespaces: mcp__server__tool_name
            parts = name.split("__", 2)
            if len(parts) == 3:
                server = parts[1]
                display_name = parts[2].replace("_", " ").title()
                group = f"MCP: {server.title()}"
            else:
                server = "external"
                display_name = name
                group = "MCP: External"
            
            mcp_tools.append({
                "id": name,
                "name": display_name,
                "source": "mcp",
                "server": server,
                "group": group,
                "description": tool.get("description", "")
            })
            
    return {
        "tools": standard_tools + mcp_tools
    }

def _strip_redacted_keys(d: dict):
    """Remove redacted API key values (starting with '...') so they don't overwrite real keys."""
    keys_to_remove = []
    for k, v in d.items():
        if isinstance(v, dict):
            _strip_redacted_keys(v)
            if not v:  # Remove empty dicts after stripping
                keys_to_remove.append(k)
        elif isinstance(v, str) and v.startswith("..."):
            keys_to_remove.append(k)
    for k in keys_to_remove:
        del d[k]

_config_lock = asyncio.Lock()
@router.patch("/")
async def update_config(
    body: ConfigUpdate,
    state: AppState = Depends(get_app_state)
):
    async with _config_lock:
        updates = body.model_dump(exclude_none=True)
        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")

        # Strip redacted API keys — never overwrite real keys with "...xxxx"
        _strip_redacted_keys(updates)

        current_config = load_config() or get_default_config()
        merged_config = deep_merge(copy.deepcopy(current_config), updates)

        success = save_config(merged_config)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to save config")
        
        if "user_name" in updates and updates["user_name"]:
            state.user_name = merged_config["user_name"]
        
        llm_cfg = updates.get("llm")
        if llm_cfg:
            state.resources.llm_service.update_settings(
                api_key=llm_cfg.get("api_key"),
                agent_model=llm_cfg.get("agent_model")
            )
        
        active_count = 0
        for _, context in state.active_sessions.items():
            await context.update_runtime_settings(merged_config)
            active_count += 1
    
    return redact_config(merged_config)