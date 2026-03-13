"""SDK agent interface — lightweight HTTP client for SSE streaming chat."""

import json
import asyncio
import httpx_sse
from httpx import HTTPStatusError
from typing import Dict, List, Callable, TYPE_CHECKING

from loguru import logger
from knoggin.types import AgentResult
from knoggin.decorators import tool_to_schema

if TYPE_CHECKING:
    from .session import KnogginAsyncSession

class KnogginAsyncAgent:
    """An agent pointing to the Knoggin REST Server."""

    def __init__(self, session: "KnogginAsyncSession"):
        self.session = session
        self.client = session._client
        self.session_id = session.session_id
        
        self.working_memory = {}
        self._tools: List[Callable] = []

        # Event hooks
        self.on = self.client.on
        self.on_any = self.client.on_any

    def use(self, tools: List[Callable]):
        """Register local Python functions (tools)."""
        self._tools.extend(tools)

    async def chat(
        self,
        query: str,
        history: List[Dict] = None,
        hot_topics: List[str] = None,
        model: str = None,
    ) -> AgentResult:
        """Run the agent loop by calling the server `/v1/chat` endpoint."""
        
        # Build tool schemas for server
        client_tools_schema = [tool_to_schema(t) for t in self._tools]

        payload = {
            "message": query,
            "hot_topics": hot_topics or [],
            "model": model,
            "timezone": "UTC",
            "working_memory": self.working_memory,
            "client_tools": client_tools_schema
        }

        self.client.emit("agent", "run_start", {"query": query})

        result_content = ""
        tools_used = []
        state = "complete"
        evidence_dict = {}

        try:
            url = f"/v1/chat/{self.session_id}"
            
            async with httpx_sse.aconnect_sse(
                self.client.http,
                "POST",
                url,
                json=payload
            ) as event_source:
                async for sse in event_source.aiter_sse():
                    
                    if sse.event == "response":
                        try:
                            # Note: The new server chat.py streams a single "response" at the end, 
                            # not token-by-token. 
                            data = json.loads(sse.data)
                            result_content = data.get("content", "")
                        except json.JSONDecodeError:
                            result_content = sse.data
                            
                    elif sse.event == "clarification":
                        data = json.loads(sse.data)
                        result_content = data.get("question", "")
                        state = "clarification"
                        
                    elif sse.event == "tool_start":
                        data = json.loads(sse.data)
                        tools_used.append(data.get("tool"))
                        self.client.emit("agent", "tool_start", data)
                        
                    elif sse.event == "tool_result":
                        self.client.emit("agent", "tool_result", json.loads(sse.data))
                        
                    elif sse.event == "error":
                        data = json.loads(sse.data)
                        state = "error"
                        result_content += f"\\nError: {data.get('message', 'Unknown')}"
                        self.client.emit("agent", "error", data)
                        
                    elif sse.event == "call_client_tool":
                        data = json.loads(sse.data)
                        tool_name = data.get("tool")
                        args = data.get("args", {})
                        
                        logger.info(f"Server requested local tool execution: {tool_name}")
                        tool_func = next((t for t in self._tools if t.__name__ == tool_name), None)
                        if tool_func:
                            try:
                                if asyncio.iscoroutinefunction(tool_func):
                                    await tool_func(**args)
                                else:
                                    tool_func(**args)
                            except Exception as e:
                                logger.error(f"Client tool {tool_name} failed: {e}")
                                
            self.client.emit("agent", "run_complete", {
                "tools_used": tools_used,
            })
                    
            return AgentResult(
                response=result_content,
                state=state,
                tools_used=tools_used,
                evidence=evidence_dict
            )
            
        except HTTPStatusError as e:
            logger.error(f"HTTP Error: {e.response.text}")
            return AgentResult(
                response=f"Server error: {e.response.text}",
                state="error"
            )

    # ════════════════════════════════════════════════════════
    #  SESSION MEMORY (via REST API)
    # ════════════════════════════════════════════════════════

    async def save_memory(self, content: str, topic: str = "General") -> Dict:
        res = await self.client.http.post(
            f"/v1/memory/{self.session_id}",
            json={"content": content, "topic": topic}
        )
        res.raise_for_status()
        return res.json()

    async def forget_memory(self, memory_id: str) -> Dict:
        res = await self.client.http.delete(f"/v1/memory/{self.session_id}/{memory_id}")
        res.raise_for_status()
        return res.json()

    # ════════════════════════════════════════════════════════
    #  CONTEXT HYDRATION (Learn)
    # ════════════════════════════════════════════════════════

    async def learn(self, content: str) -> Dict[str, Any]:
        """Manually trigger extraction of facts from content into the current session."""
        res = await self.client.http.post(
            f"/v1/extract/{self.session_id}",
            json={"content": content, "user_msg_id": -1}
        )
        res.raise_for_status()
        return res.json()

    async def get_working_memory(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get the agent's working memory (rules, preferences, icks)."""
        # Note: We use the agent_id from the session if available, otherwise we'd need it passed in.
        # But KnogginAsyncAgent is instance-bound to a session which knows its agent_id.
        # However, the SDK classes don't currently store agent_id locally from the session creation.
        # I'll need to fetch session info or assume it's available.
        # For now, let's look at how sessions are created.
        res = await self.client.http.get(f"/v1/sessions/{self.session_id}")
        res.raise_for_status()
        agent_id = res.json().get("agent_id")
        
        res = await self.client.http.get(f"/v1/agents/{agent_id}/memory")
        res.raise_for_status()
        return res.json()

    async def add_working_memory(self, category: str, content: str) -> Dict[str, Any]:
        """Add an entry to the agent's working memory."""
        res = await self.client.http.get(f"/v1/sessions/{self.session_id}")
        res.raise_for_status()
        agent_id = res.json().get("agent_id")

        res = await self.client.http.post(
            f"/v1/agents/{agent_id}/memory/{category}",
            json={"content": content}
        )
        res.raise_for_status()
        return res.json()

    async def remove_working_memory(self, category: str, memory_id: str) -> bool:
        """Remove an entry from the agent's working memory."""
        res = await self.client.http.get(f"/v1/sessions/{self.session_id}")
        res.raise_for_status()
        agent_id = res.json().get("agent_id")

        res = await self.client.http.delete(f"/v1/agents/{agent_id}/memory/{category}/{memory_id}")
        res.raise_for_status()
        return res.json().get("success", False)

    # ════════════════════════════════════════════════════════
    #  STATE MANAGEMENT (Export / Import)
    # ════════════════════════════════════════════════════════

    async def export_brain(self) -> Dict:
        """Export the agent's entire graph state."""
        res = await self.client.http.get(f"/v1/sessions/{self.session_id}/export")
        res.raise_for_status()
        return res.json()

    async def import_brain(self, graph_data: Dict) -> Dict:
        """Import a previously exported graph state."""
        res = await self.client.http.post(
            f"/v1/sessions/{self.session_id}/import",
            json=graph_data
        )
        res.raise_for_status()
        return res.json()

    # ════════════════════════════════════════════════════════
    #  WORKING MEMORY (Local Context)
    # ════════════════════════════════════════════════════════
    # Working memory is sent dynamically in each ChatRequest

    def add_working_memory(self, category: str, content: str):
        if category not in self.working_memory:
            self.working_memory[category] = ""
        self.working_memory[category] += "\\n" + content

    def clear_working_memory(self, category: str):
        self.working_memory.pop(category, None)
