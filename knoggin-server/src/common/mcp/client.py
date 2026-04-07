
import asyncio
from contextlib import AsyncExitStack
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


class MCPServerConnection:
    """Represents a single MCP server connection."""

    def __init__(self, name: str, config: dict):
        self.name = name
        self.config = config
        self.session: Optional[ClientSession] = None
        self.exit_stack: Optional[AsyncExitStack] = None
        self.tools: List[dict] = []
        self.connected: bool = False
        self.last_error: Optional[str] = None

    @property
    def enabled(self) -> bool:
        return self.config.get("enabled", False)

    @property
    def auto_extract(self) -> bool:
        return self.config.get("auto_extract", False)

    @property
    def allowed_tools(self) -> Optional[List[str]]:
        return self.config.get("allowed_tools")

    @property
    def transport(self) -> str:
        return self.config.get("transport", "stdio")


class MCPClientManager:
    """
    Manages multiple MCP server connections.
    
    Lifecycle: initialized during ResourceManager.initialize(),
    shutdown during ResourceManager.shutdown(). Process-level,
    shared across all sessions.
    """

    def __init__(self):
        self._servers: Dict[str, MCPServerConnection] = {}
        self._tool_registry: Dict[str, Tuple[str, str]] = {}  # namespaced_name -> (server_name, original_tool_name)
        self._lock = asyncio.Lock()

    @classmethod
    async def create(cls, mcp_config: dict) -> "MCPClientManager":
        instance = cls()
        servers_config = mcp_config.get("servers", {})

        for name, config in servers_config.items():
            conn = MCPServerConnection(name, config)
            async with instance._lock:
                instance._servers[name] = conn

            if conn.enabled:
                await instance._connect_server(conn)

        connected = [n for n, s in instance._servers.items() if s.connected]
        total_tools = len(instance._tool_registry)
        logger.info(f"[MCP] Manager ready: {len(connected)}/{len(instance._servers)} servers connected, {total_tools} tools registered")

        return instance

    async def _connect_server(self, conn: MCPServerConnection) -> bool:
        if conn.transport != "stdio":
            logger.warning(f"[MCP] Unsupported transport '{conn.transport}' for server '{conn.name}', skipping")
            return False

        command = conn.config.get("command", "uvx")
        args = conn.config.get("args", [])
        env = conn.config.get("env")

        server_params = StdioServerParameters(
            command=command,
            args=args,
            env=env if env else None
        )

        try:
            conn.exit_stack = AsyncExitStack()

            async with asyncio.timeout(15):
                stdio_transport = await conn.exit_stack.enter_async_context(
                    stdio_client(server_params)
                )
                read, write = stdio_transport

                conn.session = await conn.exit_stack.enter_async_context(
                    ClientSession(read, write)
                )

                await conn.session.initialize()
                response = await conn.session.list_tools()

            allowed = conn.allowed_tools
            discovered = []

            for tool in response.tools:
                if allowed and tool.name not in allowed:
                    continue

                namespaced = f"mcp__{conn.name}__{tool.name}"
                async with self._lock:
                    self._tool_registry[namespaced] = (conn.name, tool.name)

                discovered.append({
                    "name": tool.name,
                    "namespaced": namespaced,
                    "description": tool.description or "",
                    "input_schema": tool.inputSchema if hasattr(tool, "inputSchema") else {}
                })

            conn.tools = discovered
            conn.connected = True
            conn.last_error = None

            tool_names = [t["name"] for t in discovered]
            logger.info(f"[MCP] Connected to '{conn.name}': {len(discovered)} tools — {tool_names}")
            return True
        except TimeoutError:
            conn.connected = False
            conn.last_error = "Connection timed out after 15s"
            logger.error(f"[MCP] Timed out connecting to '{conn.name}'")
            if conn.exit_stack:
                try:
                    await conn.exit_stack.aclose()
                except Exception:
                    pass
                conn.exit_stack = None
                conn.session = None
            return False

        except Exception as e:
            conn.connected = False
            conn.last_error = str(e)
            logger.error(f"[MCP] Failed to connect to '{conn.name}': {e}")

            if conn.exit_stack:
                try:
                    await conn.exit_stack.aclose()
                except Exception as ex:
                    logger.warning(f"[MCP] Exit stack close failed for '{conn.name}': {ex}")
                conn.exit_stack = None
                conn.session = None

            return False

    async def _reconnect_server(self, conn: MCPServerConnection) -> bool:
        logger.info(f"[MCP] Attempting reconnection to '{conn.name}'...")

        if conn.exit_stack:
            try:
                await conn.exit_stack.aclose()
            except Exception:
                pass
            conn.exit_stack = None
            conn.session = None

        async with self._lock:
            stale_keys = [k for k, (srv, _) in self._tool_registry.items() if srv == conn.name]
            for k in stale_keys:
                del self._tool_registry[k]

        conn.tools = []
        conn.connected = False

        return await self._connect_server(conn)

    async def call_tool(self, server_name: str, tool_name: str, args: dict) -> Dict[str, Any]:
        conn = self._servers.get(server_name)
        if not conn:
            return {"error": f"MCP server '{server_name}' not configured"}

        if not conn.enabled:
            return {"error": f"MCP server '{server_name}' is disabled"}

        if not conn.connected:
            async with self._lock:
                if not conn.connected:
                    success = await self._reconnect_server(conn)
                    if not success:
                        return {"error": f"MCP server '{server_name}' is unavailable: {conn.last_error}"}

        try:
            result = await conn.session.call_tool(tool_name, arguments=args)

            text_parts = []
            for block in result.content:
                if hasattr(block, "text"):
                    text_parts.append(block.text)

            return {"data": "\n".join(text_parts) if text_parts else "No content returned"}

        except (ConnectionError, BrokenPipeError, EOFError, OSError) as e:
            logger.error(f"[MCP] Connection lost to '{server_name}': {e}")
            conn.connected = False
            conn.last_error = str(e)
            return {"error": f"MCP server '{server_name}' connection lost: {e}"}

        except Exception as e:
            logger.error(f"[MCP] Tool call failed — {server_name}.{tool_name}: {e}")
            conn.last_error = str(e)
            return {"error": f"MCP tool call failed: {e}"}

    def get_server_tools(self, server_name: str) -> List[dict]:
        conn = self._servers.get(server_name)
        if not conn or not conn.connected:
            return []
        return conn.tools

    def get_all_tools(self) -> List[dict]:
        tools = []
        for conn in self._servers.values():
            if conn.connected:
                tools.extend(conn.tools)
        return tools

    def get_server_config(self, server_name: str) -> Optional[dict]:
        conn = self._servers.get(server_name)
        return conn.config if conn else None

    def is_server_connected(self, server_name: str) -> bool:
        conn = self._servers.get(server_name)
        return conn.connected if conn else False

    def get_status(self) -> Dict[str, dict]:
        return {
            name: {
                "connected": conn.connected,
                "enabled": conn.enabled,
                "tool_count": len(conn.tools),
                "tools": [t["name"] for t in conn.tools],
                "last_error": conn.last_error
            }
            for name, conn in self._servers.items()
        }

    async def enable_server(self, server_name: str) -> bool:
        conn = self._servers.get(server_name)
        if not conn:
            return False

        conn.config["enabled"] = True
        if not conn.connected:
            return await self._connect_server(conn)
        return True

    async def disable_server(self, server_name: str) -> bool:
        conn = self._servers.get(server_name)
        if not conn:
            return False

        conn.config["enabled"] = False

        if conn.connected:
            async with self._lock:
                stale_keys = [k for k, (srv, _) in self._tool_registry.items() if srv == conn.name]
                for k in stale_keys:
                    del self._tool_registry[k]

            conn.tools = []
            conn.connected = False

            if conn.exit_stack:
                try:
                    await conn.exit_stack.aclose()
                except Exception:
                    pass
                conn.exit_stack = None
                conn.session = None

        logger.info(f"[MCP] Disabled server '{server_name}'")
        return True

    async def add_server(self, name: str, config: dict, connect: bool = True) -> dict:
        """Add a new server at runtime. Returns status dict."""
        if name in self._servers:
            return {"error": f"Server '{name}' already exists"}

        conn = MCPServerConnection(name, config)
        async with self._lock:
            self._servers[name] = conn

        if connect and conn.enabled:
            success = await self._connect_server(conn)
            if not success:
                return {
                    "name": name,
                    "connected": False,
                    "error": conn.last_error
                }

        return {
            "name": name,
            "connected": conn.connected,
            "tool_count": len(conn.tools),
            "tools": [t["name"] for t in conn.tools]
        }

    async def remove_server(self, name: str) -> bool:
        """Disconnect and remove a server."""
        conn = self._servers.get(name)
        if not conn:
            return False

        if conn.exit_stack:
            try:
                await conn.exit_stack.aclose()
            except Exception:
                pass

        async with self._lock:
            stale_keys = [k for k, (srv, _) in self._tool_registry.items() if srv == name]
            for k in stale_keys:
                del self._tool_registry[k]

            del self._servers[name]
        logger.info(f"[MCP] Removed server '{name}'")
        return True

    async def shutdown(self):
        for name, conn in self._servers.items():
            if conn.exit_stack:
                try:
                    await conn.exit_stack.aclose()
                    logger.info(f"[MCP] Disconnected from '{name}'")
                except Exception as e:
                    logger.warning(f"[MCP] Error disconnecting from '{name}': {e}")

        async with self._lock:
            self._servers.clear()
            self._tool_registry.clear()
        logger.info("[MCP] Manager shutdown complete")
