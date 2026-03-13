from typing import List, Dict, Optional, Any
from .agent_sdk import KnogginAsyncAgent

class AgentAsyncManager:
    """Manages agent-related operations for the asynchronous client."""
    
    def __init__(self, client: Any):
        self.client = client
        self.http = client.http

    async def list(self) -> List[Dict[str, Any]]:
        """List all available agents."""
        res = await self.http.get("/v1/agents/")
        res.raise_for_status()
        return res.json().get("agents", [])

    async def get(self, agent_id: str) -> Dict[str, Any]:
        """Get an agent by ID."""
        res = await self.http.get(f"/v1/agents/{agent_id}")
        res.raise_for_status()
        return res.json()

    async def get_by_name(self, name: str) -> Dict[str, Any]:
        """Get an agent by name."""
        res = await self.http.get(f"/v1/agents/by-name/{name}")
        res.raise_for_status()
        return res.json()

    async def create(
        self,
        name: str,
        persona: str,
        instructions: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = 0.7,
        enabled_tools: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Create a new agent."""
        payload = {
            "name": name,
            "persona": persona,
            "instructions": instructions,
            "model": model,
            "temperature": temperature,
            "enabled_tools": enabled_tools
        }
        res = await self.http.post("/v1/agents/", json={k: v for k, v in payload.items() if v is not None})
        res.raise_for_status()
        return res.json()

    async def update(
        self,
        agent_id: str,
        name: Optional[str] = None,
        persona: Optional[str] = None,
        instructions: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        enabled_tools: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Update an existing agent."""
        payload = {
            "name": name,
            "persona": persona,
            "instructions": instructions,
            "model": model,
            "temperature": temperature,
            "enabled_tools": enabled_tools
        }
        res = await self.http.patch(f"/v1/agents/{agent_id}", json={k: v for k, v in payload.items() if v is not None})
        res.raise_for_status()
        return res.json()

    async def delete(self, agent_id: str) -> bool:
        """Delete an agent."""
        res = await self.http.delete(f"/v1/agents/{agent_id}")
        res.raise_for_status()
        return res.json().get("success", False)

    async def set_default(self, agent_id: str) -> bool:
        """Set an agent as the default."""
        res = await self.http.post(f"/v1/agents/{agent_id}/set-default")
        res.raise_for_status()
        return res.json().get("success", False)


class AgentManager:
    """Manages agent-related operations for the synchronous client."""

    def __init__(self, client: Any):
        self.client = client
        self.http = client.http

    def list(self) -> List[Dict[str, Any]]:
        """List all available agents."""
        res = self.http.get("/v1/agents/")
        res.raise_for_status()
        return res.json().get("agents", [])

    def get(self, agent_id: str) -> Dict[str, Any]:
        """Get an agent by ID."""
        res = self.http.get(f"/v1/agents/{agent_id}")
        res.raise_for_status()
        return res.json()

    def get_by_name(self, name: str) -> Dict[str, Any]:
        """Get an agent by name."""
        res = self.http.get(f"/v1/agents/by-name/{name}")
        res.raise_for_status()
        return res.json()

    def create(
        self,
        name: str,
        persona: str,
        instructions: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = 0.7,
        enabled_tools: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Create a new agent."""
        payload = {
            "name": name,
            "persona": persona,
            "instructions": instructions,
            "model": model,
            "temperature": temperature,
            "enabled_tools": enabled_tools
        }
        res = self.http.post("/v1/agents/", json={k: v for k, v in payload.items() if v is not None})
        res.raise_for_status()
        return res.json()

    def update(
        self,
        agent_id: str,
        name: Optional[str] = None,
        persona: Optional[str] = None,
        instructions: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        enabled_tools: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Update an existing agent."""
        payload = {
            "name": name,
            "persona": persona,
            "instructions": instructions,
            "model": model,
            "temperature": temperature,
            "enabled_tools": enabled_tools
        }
        res = self.http.patch(f"/v1/agents/{agent_id}", json={k: v for k, v in payload.items() if v is not None})
        res.raise_for_status()
        return res.json()

    def delete(self, agent_id: str) -> bool:
        """Delete an agent."""
        res = self.http.delete(f"/v1/agents/{agent_id}")
        res.raise_for_status()
        return res.json().get("success", False)

    def set_default(self, agent_id: str) -> bool:
        """Set an agent as the default."""
        res = self.http.post(f"/v1/agents/{agent_id}/set-default")
        res.raise_for_status()
        return res.json().get("success", False)


class SessionAsyncManager:
    """Manages session-level operations for the asynchronous client."""

    def __init__(self, client: Any):
        self.client = client
        self.http = client.http

    async def list(self, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """List sessions with pagination."""
        res = await self.http.get("/v1/sessions/", params={"limit": limit, "offset": offset})
        res.raise_for_status()
        return res.json()


class SessionManager:
    """Manages session-level operations for the synchronous client."""

    def __init__(self, client: Any):
        self.client = client
        self.http = client.http

    def list(self, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """List sessions with pagination."""
        res = self.http.get("/v1/sessions/", params={"limit": limit, "offset": offset})
        res.raise_for_status()
        return res.json()


class TopicAsyncManager:
    """Manages topic-related operations for the asynchronous client."""

    def __init__(self, client: Any, session_id: Optional[str] = None):
        self.client = client
        self.http = client.http
        self.session_id = session_id

    async def generate(self, description: str) -> Dict[str, Any]:
        """Generate topic suggestions from a description using LLM."""
        res = await self.http.post("/v1/topics/generate", json={"description": description})
        res.raise_for_status()
        return res.json()

    async def list(self) -> Dict[str, Any]:
        """List topics for the current session."""
        if not self.session_id:
            raise ValueError("session_id is required for list()")
        res = await self.http.get(f"/v1/topics/{self.session_id}")
        res.raise_for_status()
        return res.json()

    async def create(self, name: str, **kwargs) -> Dict[str, Any]:
        """Create a new topic in the current session."""
        if not self.session_id:
            raise ValueError("session_id is required for create()")
        payload = {"name": name, **kwargs}
        res = await self.http.post(f"/v1/topics/{self.session_id}", json=payload)
        res.raise_for_status()
        return res.json()

    async def update(self, name: str, **kwargs) -> Dict[str, Any]:
        """Update an existing topic."""
        if not self.session_id:
            raise ValueError("session_id is required for update()")
        res = await self.http.patch(f"/v1/topics/{self.session_id}/{name}", json=kwargs)
        res.raise_for_status()
        return res.json()

    async def delete(self, name: str, confirm: bool = False) -> Dict[str, Any]:
        """Delete a topic."""
        if not self.session_id:
            raise ValueError("session_id is required for delete()")
        res = await self.http.delete(f"/v1/topics/{self.session_id}/{name}", params={"confirm": confirm})
        res.raise_for_status()
        return res.json()


class TopicManager:
    """Manages topic-related operations for the synchronous client."""

    def __init__(self, client: Any, session_id: Optional[str] = None):
        self.client = client
        self.http = client.http
        self.session_id = session_id

    def generate(self, description: str) -> Dict[str, Any]:
        """Generate topic suggestions from a description using LLM."""
        res = self.http.post("/v1/topics/generate", json={"description": description})
        res.raise_for_status()
        return res.json()

    def list(self) -> Dict[str, Any]:
        """List topics for the current session."""
        if not self.session_id:
            raise ValueError("session_id is required for list()")
        res = self.http.get(f"/v1/topics/{self.session_id}")
        res.raise_for_status()
        return res.json()

    def create(self, name: str, **kwargs) -> Dict[str, Any]:
        """Create a new topic in the current session."""
        if not self.session_id:
            raise ValueError("session_id is required for create()")
        payload = {"name": name, **kwargs}
        res = self.http.post(f"/v1/topics/{self.session_id}", json=payload)
        res.raise_for_status()
        return res.json()

    def update(self, name: str, **kwargs) -> Dict[str, Any]:
        """Update an existing topic."""
        if not self.session_id:
            raise ValueError("session_id is required for update()")
        res = self.http.patch(f"/v1/topics/{self.session_id}/{name}", json=kwargs)
        res.raise_for_status()
        return res.json()

    def delete(self, name: str, confirm: bool = False) -> Dict[str, Any]:
        """Delete a topic."""
        if not self.session_id:
            raise ValueError("session_id is required for delete()")
        res = self.http.delete(f"/v1/topics/{self.session_id}/{name}", params={"confirm": confirm})
        res.raise_for_status()
        return res.json()


class FileAsyncManager:
    """Manages file-related operations (RAG) for the asynchronous client."""

    def __init__(self, client: Any):
        self.client = client
        self.http = client.http

    async def upload(self, file_path: str, purpose: str = "rag") -> Dict[str, Any]:
        """Upload a file to the server for RAG or other purposes."""
        import os
        filename = os.path.basename(file_path)
        with open(file_path, "rb") as f:
            files = {"file": (filename, f)}
            res = await self.http.post("/v1/files/upload", files=files, data={"purpose": purpose})
            res.raise_for_status()
            return res.json()

    async def list(self) -> List[Dict[str, Any]]:
        """List uploaded files."""
        res = await self.http.get("/v1/files/")
        res.raise_for_status()
        return res.json()

    async def delete(self, file_id: str) -> bool:
        """Delete an uploaded file."""
        res = await self.http.delete(f"/v1/files/{file_id}")
        res.raise_for_status()
        return res.json().get("success", False)


class FileManager:
    """Manages file-related operations (RAG) for the synchronous client."""

    def __init__(self, client: Any):
        self.client = client
        self.http = client.http

    def upload(self, file_path: str, purpose: str = "rag") -> Dict[str, Any]:
        """Upload a file to the server for RAG or other purposes."""
        import os
        filename = os.path.basename(file_path)
        with open(file_path, "rb") as f:
            files = {"file": (filename, f)}
            res = self.http.post("/v1/files/upload", files=files, data={"purpose": purpose})
            res.raise_for_status()
            return res.json()

    def list(self) -> List[Dict[str, Any]]:
        """List uploaded files."""
        res = self.http.get("/v1/files/")
        res.raise_for_status()
        return res.json()

    def delete(self, file_id: str) -> bool:
        """Delete an uploaded file."""
        res = self.http.delete(f"/v1/files/{file_id}")
        res.raise_for_status()
        return res.json().get("success", False)


class MCPAsyncManager:
    """Manages MCP server integrations for the asynchronous client."""

    def __init__(self, client: Any):
        self.client = client
        self.http = client.http

    async def list(self) -> List[Dict[str, Any]]:
        """List configured MCP servers."""
        res = await self.http.get("/v1/mcp/")
        res.raise_for_status()
        return res.json()

    async def add(self, name: str, command: str, args: List[str], env: Optional[Dict[str, str]] = None) -> bool:
        """Add a new MCP server."""
        payload = {"name": name, "command": command, "args": args, "env": env or {}}
        res = await self.http.post("/v1/mcp/", json=payload)
        res.raise_for_status()
        return res.json().get("success", False)

    async def remove(self, name: str) -> bool:
        """Remove an MCP server."""
        res = await self.http.delete(f"/v1/mcp/{name}")
        res.raise_for_status()
        return res.json().get("success", False)

    async def toggle(self, name: str, enabled: bool) -> bool:
        """Enable or disable an MCP server."""
        res = await self.http.post(f"/v1/mcp/{name}/toggle", json={"enabled": enabled})
        res.raise_for_status()
        return res.json().get("success", False)


class MCPManager:
    """Manages MCP server integrations for the synchronous client."""

    def __init__(self, client: Any):
        self.client = client
        self.http = client.http

    def list(self) -> List[Dict[str, Any]]:
        """List configured MCP servers."""
        res = self.http.get("/v1/mcp/")
        res.raise_for_status()
        return res.json()

    def add(self, name: str, command: str, args: List[str], env: Optional[Dict[str, str]] = None) -> bool:
        """Add a new MCP server."""
        payload = {"name": name, "command": command, "args": args, "env": env or {}}
        res = self.http.post("/v1/mcp/", json=payload)
        res.raise_for_status()
        return res.json().get("success", False)

    def remove(self, name: str) -> bool:
        """Remove an MCP server."""
        res = self.http.delete(f"/v1/mcp/{name}")
        res.raise_for_status()
        return res.json().get("success", False)

    def toggle(self, name: str, enabled: bool) -> bool:
        """Enable or disable an MCP server."""
        res = self.http.post(f"/v1/mcp/{name}/toggle", json={"enabled": enabled})
        res.raise_for_status()
        return res.json().get("success", False)
