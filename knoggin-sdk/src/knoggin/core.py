"""Embedded SDK facade over the Knoggin engine."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone as dt_timezone
from pathlib import Path
from typing import Any, AsyncIterator, ClassVar, Dict, List, Optional

from .types import (
    AgentConfig,
    ChatEvent,
    ChatResult,
    ConversationTurn,
    FileInfo,
    FileSearchResult,
    ProjectInfo,
)


def _project_info_from_dict(data: Dict[str, Any], session_count: int = 0) -> ProjectInfo:
    return ProjectInfo(
        id=data["id"],
        name=data.get("name", data["id"]),
        description=data.get("description"),
        access_mode=data.get("access_mode", "open"),
        allowed_projects=list(data.get("allowed_projects") or []),
        session_count=int(data.get("session_count", session_count) or 0),
        created_at=data.get("created_at"),
        updated_at=data.get("updated_at"),
    )


def _file_info_from_dict(data: Dict[str, Any]) -> FileInfo:
    return FileInfo(
        file_id=data["file_id"],
        original_name=data.get("original_name") or data.get("file_name", data["file_id"]),
        extension=data.get("extension", ""),
        size_bytes=int(data.get("size_bytes", 0) or 0),
        chunk_count=int(data.get("chunk_count", 0) or 0),
        uploaded_at=data.get("uploaded_at"),
    )


def _file_search_result_from_dict(data: Dict[str, Any]) -> FileSearchResult:
    return FileSearchResult(
        content=data.get("content", ""),
        file_name=data.get("file_name", ""),
        file_id=data.get("file_id", ""),
        score=float(data.get("score", 0.0) or 0.0),
        raw_score=data.get("raw_score"),
    )


def _agent_config_from_engine(config: Any) -> AgentConfig:
    created_at = getattr(config, "created_at", None)
    if hasattr(created_at, "isoformat"):
        created_at = created_at.isoformat()

    return AgentConfig(
        id=config.id,
        name=config.name,
        persona=config.persona,
        instructions=config.instructions,
        model=config.model,
        temperature=config.temperature,
        enabled_tools=config.enabled_tools,
        is_default=config.is_default,
        is_spawned=config.is_spawned,
        spawned_by=config.spawned_by,
        created_at=created_at,
    )


class Knoggin:
    """Top-level embedded Knoggin SDK facade."""

    _lock: ClassVar[Optional[asyncio.Lock]] = None
    _resource_workers: ClassVar[Optional[int]] = None
    _open_facades: ClassVar[int] = 0

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    @classmethod
    async def boot(cls, user_name: str, workers: int = 4) -> "Knoggin":
        """Initialize engine resources and return an SDK facade."""
        from infrastructure.resources import ResourceManager

        async with cls._get_lock():
            if cls._resource_workers is not None and cls._resource_workers != workers:
                raise RuntimeError(
                    "Knoggin resources are already initialized with "
                    f"workers={cls._resource_workers}; requested workers={workers}."
                )

            resources = await ResourceManager.initialize(num_workers=workers)
            cls._resource_workers = workers
            facade = cls(user_name=user_name, resources=resources)
            cls._open_facades += 1

        return facade

    def __init__(self, user_name: str, resources: Any):
        from knoggin_server.agent.orchestrator import Orchestrator
        from knoggin_server.agent.services.agent_manager import AgentManager
        from knoggin_server.project.project_manager import ProjectManager
        from knoggin_server.session.session_manager import SessionManager

        self.user_name = user_name
        self.resources = resources
        self._active_sessions: Dict[str, Any] = {}
        self._project_manager = ProjectManager(resources, user_name)
        self._session_manager = SessionManager(
            resources, user_name, self._active_sessions, self._project_manager
        )
        self.agents = AgentDirectory(
            AgentManager(resources, user_name, self._active_sessions)
        )
        self._orchestrator = Orchestrator()
        self._closed = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("Knoggin facade is closed")

    async def close(self) -> None:
        """Close active sessions for this facade, then release shared resources."""
        if self._closed:
            return

        for session_id in list(self._active_sessions):
            await self._session_manager.close_session(session_id)

        async with self.__class__._get_lock():
            self.__class__._open_facades = max(0, self.__class__._open_facades - 1)
            should_shutdown = self.__class__._open_facades == 0
            if should_shutdown:
                await self.resources.shutdown()
                self.__class__._resource_workers = None

        self._closed = True

    async def __aenter__(self) -> "Knoggin":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def create_project(
        self,
        name: str,
        description: Optional[str] = None,
        access_mode: str = "open",
        allowed_projects: Optional[List[str]] = None,
    ) -> "Project":
        self._ensure_open()
        metadata = await self._project_manager.create_project(
            name=name,
            description=description,
            access_mode=access_mode,
            allowed_projects=allowed_projects,
        )
        return Project(self, metadata["id"], _project_info_from_dict(metadata))

    def project(self, project_id: str = "global") -> "Project":
        self._ensure_open()
        return Project(self, project_id)

    async def list_projects(self) -> List[ProjectInfo]:
        self._ensure_open()
        return [
            _project_info_from_dict(p)
            for p in await self._project_manager.list_projects()
        ]

    async def get_project(self, project_id: str) -> Optional[ProjectInfo]:
        self._ensure_open()
        metadata = await self._project_manager.get_project(project_id)
        if metadata is None:
            return None
        return _project_info_from_dict(metadata)

    async def delete_project(self, project_id: str) -> List[str]:
        self._ensure_open()
        orphaned_session_ids = await self._project_manager.delete_project(project_id)

        for session_id in orphaned_session_ids:
            if session_id in self._active_sessions:
                await self._session_manager.close_session(session_id)
            await self._session_manager.delete_session_data(session_id)

        return orphaned_session_ids


class Project:
    """SDK handle for a durable project scope."""

    def __init__(
        self,
        client: Knoggin,
        project_id: str,
        info: Optional[ProjectInfo] = None,
    ):
        self._client = client
        self.project_id = project_id
        self._info = info

    @property
    def id(self) -> str:
        return self.project_id

    async def info(self) -> Optional[ProjectInfo]:
        self._client._ensure_open()
        if self._info is None:
            self._info = await self._client.get_project(self.project_id)
        return self._info

    async def session(
        self,
        model: Optional[str] = None,
        agent_id: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
    ) -> "Session":
        self._client._ensure_open()
        context = await self._client._session_manager.create_session(
            model=model,
            agent_id=agent_id,
            enabled_tools=enabled_tools,
            project_id=self.project_id,
        )
        return Session(self._client, context)

    async def resume_session(self, session_id: str) -> Optional["Session"]:
        self._client._ensure_open()
        context = await self._client._session_manager.get_or_resume_session(session_id)
        if context is None:
            return None
        return Session(self._client, context)

    async def delete(self) -> List[str]:
        self._client._ensure_open()
        return await self._client.delete_project(self.project_id)


class Session:
    """SDK handle for an active engine session."""

    def __init__(self, client: Knoggin, context: Any):
        self._client = client
        self.context = context
        self.session_id = context.session_id
        self.project_id = context.project_id
        self.files = SessionFiles(context)
        self._last_message_id: Optional[int] = None

    @property
    def id(self) -> str:
        return self.session_id

    async def stream(
        self,
        message: str,
        model: Optional[str] = None,
        agent_id: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
        hot_topics: Optional[List[str]] = None,
        timezone: Optional[str] = None,
    ) -> AsyncIterator[ChatEvent]:
        self._client._ensure_open()
        from common.schema.dtypes import Message

        user_message = await self.context.add(Message(content=message))
        self._last_message_id = user_message.id

        history_limit = (
            self.context.current_config.developer_settings.limits.agent_history_turns
        )
        conversation_history = await self.context.get_conversation_context(
            history_limit,
            up_to_msg_id=user_message.id,
        )

        assistant_content = None
        assistant_metadata: Dict[str, Any] = {}

        async for raw_event in self._client._orchestrator.run_stream(
            user_query=message,
            user_name=self._client.user_name,
            session_id=self.session_id,
            context=self.context,
            user_timezone=timezone,
            model=model,
            agent_id=agent_id,
            enabled_tools=enabled_tools,
            conversation_history=conversation_history,
            hot_topics=hot_topics,
        ):
            event = ChatEvent(
                event=raw_event.get("event", "unknown"),
                data=raw_event.get("data") or {},
            )

            if event.event == "response":
                assistant_content = event.data.get("content", "")
                assistant_metadata = {
                    "usage": event.data.get("usage"),
                    "sources": event.data.get("sources"),
                }

            yield event

        if assistant_content:
            await self.context.add_assistant_turn(
                assistant_content,
                datetime.now(dt_timezone.utc),
                metadata=assistant_metadata,
                user_msg_id=user_message.id,
            )

    async def chat(
        self,
        message: str,
        model: Optional[str] = None,
        agent_id: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
        hot_topics: Optional[List[str]] = None,
        timezone: Optional[str] = None,
    ) -> ChatResult:
        events: List[ChatEvent] = []
        response = ""
        state = "complete"
        usage: Dict[str, Any] = {}
        sources = None
        tools_used: List[str] = []

        async for event in self.stream(
            message,
            model=model,
            agent_id=agent_id,
            enabled_tools=enabled_tools,
            hot_topics=hot_topics,
            timezone=timezone,
        ):
            events.append(event)
            if event.event == "response":
                response = event.data.get("content", "")
                usage = event.data.get("usage") or {}
                sources = event.data.get("sources")
            elif event.event == "clarification":
                response = event.data.get("question", "")
                usage = event.data.get("usage") or {}
                state = "clarification"
            elif event.event == "error":
                response = event.data.get("message", "")
                state = "error"
            elif event.event == "tool_start":
                tool_name = event.data.get("tool")
                if tool_name:
                    tools_used.append(tool_name)

        return ChatResult(
            response=response,
            state=state,
            session_id=self.session_id,
            message_id=self._last_message_id,
            usage=usage,
            sources=sources,
            tools_used=tools_used,
            events=events,
        )

    async def history(self, limit: int = 100) -> List[ConversationTurn]:
        self._client._ensure_open()
        turns = await self._client._session_manager.get_session_history_readonly(
            self.session_id,
            limit=limit,
        )
        return [ConversationTurn(**turn) for turn in turns]

    async def close(self) -> bool:
        return await self._client._session_manager.close_session(self.session_id)

    async def delete(self) -> int:
        self._client._ensure_open()
        if self.session_id in self._client._active_sessions:
            await self.close()
        return await self._client._session_manager.delete_session_data(self.session_id)


class SessionFiles:
    """Session-scoped file retrieval facade."""

    def __init__(self, context: Any):
        self._context = context

    @property
    def _file_rag(self):
        file_rag = getattr(self._context, "file_rag", None)
        if file_rag is None:
            raise RuntimeError("File RAG is not initialized for this session")
        return file_rag

    async def add(self, path: str, original_name: Optional[str] = None) -> FileInfo:
        original_name = original_name or Path(path).name
        result = await self._file_rag.ingest_file(path, original_name)
        return _file_info_from_dict(result)

    async def list(self) -> List[FileInfo]:
        files = await asyncio.to_thread(self._file_rag.list_files)
        return [_file_info_from_dict(file_data) for file_data in files]

    async def search(
        self,
        query: str,
        limit: int = 5,
        file_id: Optional[str] = None,
    ) -> List[FileSearchResult]:
        results = await self._file_rag.search(
            query,
            n_results=limit,
            file_filter=file_id,
        )
        return [_file_search_result_from_dict(result) for result in results]

    async def delete(self, file_id: str) -> bool:
        return await self._file_rag.delete_file(file_id)


class AgentDirectory:
    """SDK wrapper around the engine agent manager."""

    def __init__(self, manager: Any):
        self._manager = manager

    async def list(self) -> List[AgentConfig]:
        return [
            _agent_config_from_engine(agent)
            for agent in await self._manager.list_agents()
        ]

    async def get(self, agent_id: str) -> Optional[AgentConfig]:
        agent = await self._manager.get_agent(agent_id)
        if agent is None:
            return None
        return _agent_config_from_engine(agent)

    async def get_by_name(self, name: str) -> Optional[AgentConfig]:
        agent = await self._manager.get_agent_by_name(name)
        if agent is None:
            return None
        return _agent_config_from_engine(agent)

    async def get_default_id(self) -> str:
        return await self._manager.get_default_agent_id()

    async def create(
        self,
        name: str,
        persona: str,
        instructions: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = 0.7,
        enabled_tools: Optional[List[str]] = None,
    ) -> AgentConfig:
        agent = await self._manager.create_agent(
            name=name,
            persona=persona,
            instructions=instructions,
            model=model,
            temperature=temperature,
            enabled_tools=enabled_tools,
        )
        return _agent_config_from_engine(agent)

    async def update(
        self,
        agent_id: str,
        name: Optional[str] = None,
        persona: Optional[str] = None,
        instructions: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        enabled_tools: Optional[List[str]] = None,
    ) -> Optional[AgentConfig]:
        agent = await self._manager.update_agent(
            agent_id=agent_id,
            name=name,
            persona=persona,
            instructions=instructions,
            model=model,
            temperature=temperature,
            enabled_tools=enabled_tools,
        )
        if agent is None:
            return None
        return _agent_config_from_engine(agent)

    async def delete(self, agent_id: str) -> bool:
        return await self._manager.delete_agent(agent_id)

    async def set_default(self, agent_id: str) -> bool:
        return await self._manager.set_default_agent(agent_id)
