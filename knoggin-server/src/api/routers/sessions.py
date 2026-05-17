from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from api.deps import (
    SessionID,
    get_agent_manager,
    get_app_state,
    get_memory_manager,
    get_session_manager,
)
from api.state import AppState
from common.schema.api import (
    CreateSessionResponse,
    GenericSuccess,
    SessionDetail,
    SessionListItem,
    SessionListResponse,
    SessionMemoryResponse,
)
from common.schema.dtypes import Message
from common.schema.tool_schema import ALL_TOOL_NAMES
from knoggin.agent.services.agent_manager import AgentManager
from knoggin.knowledge.services.memory_service import MemoryManager
from knoggin.session.services.session_manager import SessionManager

router = APIRouter()


class CreateSessionRequest(BaseModel):
    topics_config: Optional[dict] = None
    model: Optional[str] = None
    agent_id: Optional[str] = None
    enabled_tools: Optional[List[str]] = None


class UpdateSessionRequest(BaseModel):
    model: Optional[str] = None
    agent_id: Optional[str] = None
    enabled_tools: Optional[List[str]] = None
    title: Optional[str] = None


class ImportSessionRequest(BaseModel):
    messages: List[dict]
    enabled_tools: Optional[List[str]] = None
    title: Optional[str] = None


@router.get("/", response_model=SessionListResponse)
async def list_sessions(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session_manager: SessionManager = Depends(get_session_manager),
    state: AppState = Depends(get_app_state),
):
    sessions = await session_manager.list_sessions()

    sorted_sessions = sorted(
        sessions.items(), key=lambda x: x[1].get("last_active", ""), reverse=True
    )
    paginated = sorted_sessions[offset : offset + limit]

    result = []
    for session_id, metadata in paginated:
        result.append(
            SessionListItem(
                session_id=session_id,
                title=metadata.get("title"),
                created_at=metadata.get("created_at"),
                last_active=metadata.get("last_active"),
                is_active=session_id in state.active_sessions,
            )
        )

    return SessionListResponse(
        sessions=result,
        total=len(sessions),
        limit=limit,
        offset=offset,
    )


@router.post("/", response_model=CreateSessionResponse)
async def create_session(
    body: CreateSessionRequest = CreateSessionRequest(),
    session_manager: SessionManager = Depends(get_session_manager),
    agent_manager: AgentManager = Depends(get_agent_manager),
    state: AppState = Depends(get_app_state),
):
    agent_id = None
    if body.agent_id:
        agent = await agent_manager.get_agent(body.agent_id)
        if not agent:
            raise HTTPException(status_code=400, detail="Agent not found")
        agent_id = body.agent_id
    else:
        agent_id = await agent_manager.get_default_agent_id()

    context = await session_manager.create_session(
        body.topics_config,
        model=body.model,
        agent_id=agent_id,
        enabled_tools=body.enabled_tools,
    )

    await state.resources.redis.delete(f"topic_gen_count:{state.user_name}")

    if body.enabled_tools is not None:
        await session_manager.update_session_metadata(
            context.session_id, {"enabled_tools": body.enabled_tools}
        )

    return CreateSessionResponse(
        session_id=context.session_id,
        created_at=datetime.now(timezone.utc),
        model=body.model,
        agent_id=agent_id,
    )


@router.get("/export/all")
async def export_all_sessions(
    session_manager: SessionManager = Depends(get_session_manager),
    state: AppState = Depends(get_app_state),
):
    """Export all sessions' conversation histories as JSON."""
    sessions = await session_manager.list_sessions()

    all_exports = []
    for session_id, metadata in sessions.items():
        try:
            if session_id in state.active_sessions:
                history = await state.active_sessions[
                    session_id
                ].get_conversation_context(num_turns=1000)
                messages = [
                    {
                        "role": turn["role"],
                        "content": turn["content"],
                        "timestamp": turn["timestamp"],
                    }
                    for turn in history
                ]
            else:
                messages = await session_manager.get_session_history_readonly(
                    session_id
                )

            all_exports.append(
                {
                    "session_id": session_id,
                    "title": metadata.get("title", "Untitled"),
                    "created_at": metadata.get("created_at"),
                    "last_active": metadata.get("last_active"),
                    "agent_id": metadata.get("agent_id"),
                    "messages": messages,
                }
            )
        except Exception:
            continue

    export_data = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "total_sessions": len(all_exports),
        "sessions": all_exports,
    }

    return JSONResponse(
        content=export_data,
        headers={
            "Content-Disposition": 'attachment; filename="knoggin_all_sessions.json"'
        },
    )


@router.get("/{session_id}", response_model=SessionDetail)
async def get_session(
    session_id: SessionID, 
    session_manager: SessionManager = Depends(get_session_manager),
    state: AppState = Depends(get_app_state),
):
    sessions = await session_manager.list_sessions()

    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    metadata = sessions[session_id]

    return SessionDetail(
        session_id=session_id,
        title=metadata.get("title"),
        created_at=metadata.get("created_at"),
        last_active=metadata.get("last_active"),
        topics_config=metadata.get("topics_config"),
        model=metadata.get("model"),
        agent_id=metadata.get("agent_id"),
        enabled_tools=metadata.get("enabled_tools"),
        is_active=session_id in state.active_sessions,
    )


@router.patch("/{session_id}", response_model=SessionDetail)
async def update_session(
    session_id: SessionID,
    body: UpdateSessionRequest,
    session_manager: SessionManager = Depends(get_session_manager),
    agent_manager: AgentManager = Depends(get_agent_manager),
    state: AppState = Depends(get_app_state),
):
    updates = {}

    if body.model is not None:
        updates["model"] = body.model

    if body.agent_id is not None:
        agent = await agent_manager.get_agent(body.agent_id)
        if not agent:
            raise HTTPException(status_code=400, detail="Agent not found")
        updates["agent_id"] = body.agent_id

    if body.enabled_tools is not None:
        invalid = set(body.enabled_tools) - set(ALL_TOOL_NAMES)
        if invalid:
            raise HTTPException(
                status_code=400, detail=f"Invalid tool names: {invalid}"
            )
        updates["enabled_tools"] = body.enabled_tools

    if body.title is not None:
        updates["title"] = body.title

    updates["last_active"] = datetime.now(timezone.utc).isoformat()

    updated_metadata = await session_manager.update_session_metadata(session_id, updates)

    if session_id in state.active_sessions:
        if body.model is not None:
            state.active_sessions[session_id].model = body.model

    return SessionDetail(
        session_id=session_id,
        title=updated_metadata.get("title"),
        created_at=updated_metadata.get("created_at"),
        last_active=updated_metadata.get("last_active"),
        topics_config=updated_metadata.get("topics_config"),
        model=updated_metadata.get("model"),
        agent_id=updated_metadata.get("agent_id"),
        enabled_tools=updated_metadata.get("enabled_tools"),
        is_active=session_id in state.active_sessions,
    )


@router.delete("/{session_id}", response_model=GenericSuccess)
async def delete_session(
    session_id: SessionID,
    force: bool = Query(False),
    session_manager: SessionManager = Depends(get_session_manager),
    state: AppState = Depends(get_app_state),
):
    sessions = await session_manager.list_sessions()

    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    if session_id in state.active_sessions:
        if not force:
            raise HTTPException(
                status_code=409,
                detail="Session is active. Use ?force=true to close and delete.",
            )
        await session_manager.close_session(session_id)

    deleted_count = await session_manager.delete_session_data(session_id)

    return GenericSuccess(
        success=True, 
        message=f"Deleted {deleted_count} session keys."
    )


@router.get("/{session_id}/memory", response_model=SessionMemoryResponse)
async def get_session_memory(memory_mgr: MemoryManager = Depends(get_memory_manager)):
    """Read agent-saved memory notes for a session."""
    result = await memory_mgr.get_memory_blocks()

    # Re-format to match expected API output (dict of lists of MemoryItem)
    blocks = {}
    for topic, entries in result.blocks.items():
        blocks[topic] = [
            {
                "id": e.id,
                "content": e.content,
                "created_at": e.created_at,
                "topic": topic,
            }
            for e in entries
        ]

    return SessionMemoryResponse(memories=blocks, total=result.total)


@router.get("/{session_id}/export")
async def export_session(
    session_id: SessionID, 
    session_manager: SessionManager = Depends(get_session_manager),
    state: AppState = Depends(get_app_state),
):
    """Export a single session's conversation history as JSON."""
    context = await session_manager.get_or_resume_session(session_id)
    if not context:
        raise HTTPException(status_code=404, detail="Session not found")

    sessions = await session_manager.list_sessions()
    metadata = sessions.get(session_id, {})
    history = await context.get_conversation_context(num_turns=1000)

    export_data = {
        "session_id": session_id,
        "title": metadata.get("title", "Untitled"),
        "created_at": metadata.get("created_at"),
        "last_active": metadata.get("last_active"),
        "agent_id": metadata.get("agent_id"),
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "messages": [
            {
                "role": turn["role"],
                "content": turn["content"],
                "timestamp": turn["timestamp"],
            }
            for turn in history
        ],
    }

    return JSONResponse(
        content=export_data,
        headers={
            "Content-Disposition": f'attachment; filename="knoggin_session_{session_id[:8]}.json"'
        },
    )


@router.post("/{session_id}/import", response_model=GenericSuccess)
async def import_session(
    session_id: SessionID,
    body: ImportSessionRequest,
    session_manager: SessionManager = Depends(get_session_manager),
):
    """Import a session's conversation history."""
    context = await session_manager.get_or_resume_session(session_id)
    if not context:
        raise HTTPException(status_code=404, detail="Session not found")

    imported = 0
    for msg in body.messages:
        role = msg.get("role")
        content = msg.get("content")
        timestamp_str = msg.get("timestamp")

        if not role or not content:
            continue

        # Parse timestamp if available
        timestamp = datetime.now(timezone.utc)
        if timestamp_str:
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                pass

        if role == "user":
            m = Message(content=content, timestamp=timestamp)
            await context.add(m)
            imported += 1
        elif role == "assistant":
            await context.add_assistant_turn(content=content, timestamp=timestamp)
            imported += 1

    return GenericSuccess(
        success=True, 
        message=f"Imported {imported} messages."
    )
