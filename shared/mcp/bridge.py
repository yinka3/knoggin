
from typing import List
from loguru import logger


def mcp_tools_to_schemas(tools: List[dict]) -> List[dict]:
    """
    Convert discovered MCP tools into OpenAI function-calling schema format.
    
    Input tools come from MCPClientManager.get_all_tools(), each with:
        - namespaced: "mcp__gmail__search_emails"
        - description: str
        - input_schema: dict (already JSON Schema from MCP)
    
    Output matches TOOL_SCHEMAS format in tool_schema.py.
    """
    schemas = []

    for tool in tools:
        name = tool.get("namespaced")
        if not name:
            logger.warning(f"[MCP Bridge] Skipping tool with no namespaced name: {tool}")
            continue

        description = tool.get("description", "")
        input_schema = tool.get("input_schema")

        if not input_schema or not isinstance(input_schema, dict):
            input_schema = {"type": "object", "properties": {}}

        if "type" not in input_schema:
            input_schema["type"] = "object"
        if "properties" not in input_schema:
            input_schema["properties"] = {}

        parts = name.split("__", 2)
        server_label = parts[1] if len(parts) == 3 else "external"

        schemas.append({
            "type": "function",
            "function": {
                "name": name,
                "description": f"[{server_label}] {description}" if description else f"[{server_label}] MCP tool",
                "parameters": input_schema
            }
        })

    if schemas:
        logger.debug(f"[MCP Bridge] Converted {len(schemas)} MCP tools to function schemas")

    return schemas


def get_mcp_tool_names(tools: List[dict]) -> List[str]:
    """Return list of namespaced MCP tool names. Useful for ToolToggles integration."""
    return [t.get("namespaced") for t in tools if t.get("namespaced")]