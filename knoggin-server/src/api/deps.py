from typing import Annotated
from fastapi import Depends, HTTPException, Path, Request

from api.state import AppState
from common.conf.topics_config import TopicConfig
from knoggin.agent.services.agent_manager import AgentManager
from knoggin.knowledge.services.memory_service import MemoryManager
from knoggin.session.services.session_manager import SessionManager

# --- Validated Parameter Types ---

# UUID-like or hex strings of reasonable length
SessionID = Annotated[str, Path(description="The unique identifier for the session", min_length=8, pattern=r"^[a-zA-Z0-9_\-]+$")]
AgentID = Annotated[str, Path(description="The unique identifier for the agent", min_length=4, pattern=r"^[a-zA-Z0-9_\-]+$")]

# --- Dependencies ---

def get_app_state(request: Request) -> AppState:
    return request.app.state.app_state


def get_session_manager(state: AppState = Depends(get_app_state)) -> SessionManager:
    return state.session_manager


def get_agent_manager(state: AppState = Depends(get_app_state)) -> AgentManager:
    return state.agent_manager


async def get_memory_manager(
    session_id: SessionID, state: AppState = Depends(get_app_state)
) -> MemoryManager:
    """Provides a MemoryManager initialized for a specific session."""
    context = await state.session_manager.get_or_resume_session(session_id)
    if not context:
        raise HTTPException(status_code=404, detail="Session not found")

    sessions = await state.session_manager.list_sessions()
    session_meta = sessions.get(session_id, {})
    agent_id = (
        session_meta.get("agent_id") or await state.agent_manager.get_default_agent_id()
    )

    return MemoryManager(
        redis=state.resources.redis,
        user_name=state.user_name,
        session_id=session_id,
        agent_id=agent_id,
        topic_config=context.topic_config,
    )


async def get_working_memory_manager(
    agent_id: AgentID, state: AppState = Depends(get_app_state)
) -> MemoryManager:
    """Provides a MemoryManager initialized for agent-level working memory."""
    agent = await state.agent_manager.get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    return MemoryManager(
        redis=state.resources.redis,
        user_name=state.user_name,
        session_id="system",
        agent_id=agent_id,
        topic_config=TopicConfig(TopicConfig.DEFAULT_CONFIG),
    )
