from fastapi import APIRouter, HTTPException, Request
from api.state import AppState
from common.config.base import load_config, get_default_config, save_config
from common.schema.settings import MCPServerCreate

router = APIRouter()


@router.get("/presets")
async def get_mcp_presets():
    """Return curated list of MCP server presets."""
    from common.config.base import MCP_SERVER_PRESETS
    return {"presets": MCP_SERVER_PRESETS}


@router.get("/servers")
async def get_mcp_servers(request: Request):
    """Return all configured MCP servers with live connection status."""
    state: AppState = request.app.state.app_state
    mcp = state.resources.mcp_manager
    
    if not mcp:
        return {"servers": []}
    
    status = mcp.get_status()
    config = load_config() or get_default_config()
    server_configs = config.get("mcp", {}).get("servers", {})
    
    servers = []
    for name, live in status.items():
        cfg = server_configs.get(name, {})
        servers.append({
            "name": name,
            "command": cfg.get("command", "uvx"),
            "args": cfg.get("args", []),
            "transport": cfg.get("transport", "stdio"),
            "enabled": live["enabled"],
            "connected": live["connected"],
            "tool_count": live["tool_count"],
            "tools": live["tools"],
            "last_error": live["last_error"],
        })
    
    return {"servers": servers}


@router.post("/servers")
async def add_mcp_server(body: MCPServerCreate, request: Request):
    """Add a new MCP server, save to config, and optionally connect."""
    state: AppState = request.app.state.app_state
    mcp = state.resources.mcp_manager
    
    if not mcp:
        raise HTTPException(status_code=500, detail="MCP manager not initialized")
    
    server_config = {
        "command": body.command,
        "args": body.args,
        "enabled": body.enabled,
        "transport": "stdio",
    }
    if body.env:
        server_config["env"] = body.env
    if body.allowed_tools:
        server_config["allowed_tools"] = body.allowed_tools
    
    result = await mcp.add_server(body.name, server_config, connect=body.enabled)
    
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    
    config = load_config() or get_default_config()
    if "mcp" not in config:
        config["mcp"] = {"servers": {}}
    config["mcp"]["servers"][body.name] = server_config
    save_config(config)
    
    return result


@router.delete("/servers/{name}")
async def remove_mcp_server(name: str, request: Request):
    """Disconnect and remove an MCP server."""
    state: AppState = request.app.state.app_state
    mcp = state.resources.mcp_manager
    
    if not mcp:
        raise HTTPException(status_code=500, detail="MCP manager not initialized")
    
    success = await mcp.remove_server(name)
    if not success:
        raise HTTPException(status_code=404, detail=f"Server '{name}' not found")
    
    config = load_config() or get_default_config()
    mcp_cfg = config.get("mcp", {})
    servers = mcp_cfg.get("servers", {})
    servers.pop(name, None)
    save_config(config)
    
    return {"removed": name}


@router.post("/servers/{name}/toggle")
async def toggle_mcp_server(name: str, request: Request):
    """Enable or disable an MCP server."""
    state: AppState = request.app.state.app_state
    mcp = state.resources.mcp_manager
    
    if not mcp:
        raise HTTPException(status_code=500, detail="MCP manager not initialized")
    
    status = mcp.get_status()
    if name not in status:
        raise HTTPException(status_code=404, detail=f"Server '{name}' not found")
    
    currently_enabled = status[name]["enabled"]
    
    if currently_enabled:
        success = await mcp.disable_server(name)
    else:
        success = await mcp.enable_server(name)
    
    if not success:
        raise HTTPException(status_code=500, detail="Toggle failed")
    
    # Persist the change
    config = load_config() or get_default_config()
    mcp_cfg = config.get("mcp", {}).get("servers", {})
    if name in mcp_cfg:
        mcp_cfg[name]["enabled"] = not currently_enabled
        save_config(config)
    
    new_status = mcp.get_status().get(name, {})
    return {
        "name": name,
        "enabled": not currently_enabled,
        "connected": new_status.get("connected", False),
        "tool_count": new_status.get("tool_count", 0),
        "tools": new_status.get("tools", []),
    }
