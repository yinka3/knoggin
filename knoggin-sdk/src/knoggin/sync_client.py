"""Synchronous wrappers for the Knoggin Light SDK."""

import json
from typing import Dict, List, Callable, Optional, Any
import httpx
import httpx_sse
from loguru import logger

from .types import AgentResult
from .decorators import tool_to_schema
from .managers import (
    AgentManager, 
    SessionManager, 
    TopicManager, 
    FileManager, 
    MCPManager
)

class KnogginExtractor:
    def __init__(self, session: "KnogginSession"):
        self.session = session
        self.client = session._client

    def trigger_extraction(self, content: str, user_msg_id: int) -> Dict:
        res = self.client.http.post(
            f"/v1/extract/{self.session.session_id}",
            json={"content": content, "user_msg_id": user_msg_id}
        )
        res.raise_for_status()
        return res.json()


class KnogginAgent:
    def __init__(self, session: "KnogginSession"):
        self.session = session
        self.client = session._client
        self.session_id = session.session_id
        
        self.working_memory = {}
        self._tools: List[Callable] = []
        self.on = self.client.on
        self.on_any = self.client.on_any

    def use(self, tools: List[Callable]):
        self._tools.extend(tools)

    def learn(self, content: str) -> Dict:
        res = self.client.http.post(
            f"/v1/extract/{self.session_id}",
            json={"content": content, "user_msg_id": -1}
        )
        res.raise_for_status()
        return res.json()

    def get_working_memory(self) -> Dict:
        """Get the agent's working memory (rules, preferences, icks)."""
        res = self.client.http.get(f"/v1/sessions/{self.session_id}")
        res.raise_for_status()
        agent_id = res.json().get("agent_id")
        
        res = self.client.http.get(f"/v1/agents/{agent_id}/memory")
        res.raise_for_status()
        return res.json()

    def add_working_memory(self, category: str, content: str) -> Dict:
        """Add an entry to the agent's working memory."""
        res = self.client.http.get(f"/v1/sessions/{self.session_id}")
        res.raise_for_status()
        agent_id = res.json().get("agent_id")

        res = self.client.http.post(
            f"/v1/agents/{agent_id}/memory/{category}",
            json={"content": content}
        )
        res.raise_for_status()
        return res.json()

    def remove_working_memory(self, category: str, memory_id: str) -> bool:
        """Remove an entry from the agent's working memory."""
        res = self.client.http.get(f"/v1/sessions/{self.session_id}")
        res.raise_for_status()
        agent_id = res.json().get("agent_id")

        res = self.client.http.delete(f"/v1/agents/{agent_id}/memory/{category}/{memory_id}")
        res.raise_for_status()
        return res.json().get("success", False)

    def export_brain(self) -> Dict:
        res = self.client.http.get(f"/v1/sessions/{self.session_id}/export")
        res.raise_for_status()
        return res.json()

    def import_brain(self, graph_data: Dict) -> Dict:
        res = self.client.http.post(
            f"/v1/sessions/{self.session_id}/import",
            json=graph_data
        )
        res.raise_for_status()
        return res.json()

    def chat(
        self,
        query: str,
        history: List[Dict] = None,
        hot_topics: List[str] = None,
        model: str = None,
    ) -> AgentResult:
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
            
            with httpx_sse.connect_sse(
                self.client.http,
                "POST",
                url,
                json=payload
            ) as event_source:
                for sse in event_source.iter_sse():
                    
                    if sse.event == "response":
                        try:
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
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP Error: {e.response.text}")
            return AgentResult(
                response=f"Server error: {e.response.text}",
                state="error"
            )


class KnogginSession:
    def __init__(self, session_id: str, user_name: str, client: "KnogginClient"):
        self.session_id = session_id
        self.user_name = user_name
        self._client = client

    @property
    def agent(self):
        if not hasattr(self, "_agent"):
            self._agent = KnogginAgent(session=self)
        return self._agent

    @property
    def extractor(self):
        if not hasattr(self, "_extractor"):
            self._extractor = KnogginExtractor(session=self)
        return self._extractor

    @property
    def topics(self):
        """Topic management manager for this session."""
        if not hasattr(self, "_topics"):
            from .managers import TopicManager
            self._topics = TopicManager(client=self._client, session_id=self.session_id)
        return self._topics

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._client.dev_mode:
            logger.info(f"[DevMode] Sync Session context exited: {self.session_id}")

    def update(
        self,
        title: Optional[str] = None,
        model: Optional[str] = None,
        agent_id: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None
    ) -> bool:
        """Update session metadata."""
        payload = {
            "title": title,
            "model": model,
            "agent_id": agent_id,
            "enabled_tools": enabled_tools
        }
        res = self._client.http.patch(
            f"/v1/sessions/{self.session_id}",
            json={k: v for k, v in payload.items() if v is not None}
        )
        res.raise_for_status()
        return res.json().get("success", False)

    def delete(self, force: bool = False) -> bool:
        """Delete this session."""
        res = self._client.http.delete(f"/v1/sessions/{self.session_id}", params={"force": force})
        res.raise_for_status()
        return res.json().get("success", False)

    def export(self) -> Dict:
        """Export session conversation history."""
        return self.agent.export_brain()


class KnogginClient:
    def __init__(self, base_url: str = "http://localhost:8000", dev_mode: bool = False):
        self.base_url = base_url.rstrip("/")
        self.dev_mode = dev_mode
        self.http = httpx.Client(base_url=self.base_url, timeout=60.0)
        self.agents = AgentManager(self)
        self.sessions = SessionManager(self)
        self.topics = TopicManager(self)
        self.files = FileManager(self)
        self.mcp = MCPManager(self)
        self._handlers = {}

    @classmethod
    def boot(cls, base_url: str = "http://localhost:8000", dev_mode: bool = False) -> "KnogginClient":
        client = cls(base_url=base_url, dev_mode=dev_mode)
        try:
            res = client.http.get("/v1/sessions/")
            res.raise_for_status()
            if dev_mode:
                logger.info(f"[DevMode] SyncKnogginClient booted, connected to {base_url}")
        except Exception as e:
            logger.error(f"Failed to connect to Knoggin server at {base_url}: {e}")
            raise
        return client

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.http.close()
        if self.dev_mode:
            logger.info(f"[DevMode] SyncKnogginClient closed")

    def session(
        self,
        user_name: str,
        topics: dict = None,
        session_id: str = None,
        agent_id: str = None,
        model: str = None,
        enabled_tools: list = None
    ) -> KnogginSession:
        payload = {
            "topics_config": topics or {},
            "model": model,
            "agent_id": agent_id,
            "enabled_tools": enabled_tools
        }
        res = self.http.post("/v1/sessions/", json={k: v for k, v in payload.items() if v is not None})
        res.raise_for_status()
        data = res.json()
        
        sid = data["session_id"]
        if self.dev_mode:
            logger.info(f"[DevMode] Sync Session created/resumed: {sid}")
            
        return KnogginSession(
            session_id=sid,
            user_name=user_name,
            client=self
        )

    def chat(self, user_name: str, query: str, **kwargs) -> AgentResult:
        """Shorthand to chat with an agent without manually managing sessions."""
        with self.session(user_name, **kwargs) as session:
            return session.agent.chat(query)

    def on(self, event_name: str):
        def decorator(func):
            if event_name not in self._handlers:
                self._handlers[event_name] = []
            self._handlers[event_name].append(func)
            return func
        return decorator

    def on_any(self):
        return self.on("*")

    def emit(self, source: str, event: str, data: Dict):
        if self.dev_mode:
            logger.debug(f"[DevMode] Emit -> {source}:{event} | Data: {data}")
            
        handlers = self._handlers.get(event, []) + self._handlers.get("*", [])
        for handler in handlers:
            try:
                handler(source, event, data)
            except Exception as e:
                logger.error(f"Error in event handler for {event}: {e}")
