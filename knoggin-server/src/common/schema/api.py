from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

# --- Base Models ---


class GenericSuccess(BaseModel):
    success: bool = True
    message: Optional[str] = None


class ErrorDetail(BaseModel):
    message: str
    type: str
    details: Optional[Any] = None


class ErrorResponse(BaseModel):
    success: bool = False
    error: ErrorDetail


# --- Session Models ---


class SessionListItem(BaseModel):
    session_id: str
    title: Optional[str] = None
    created_at: Optional[datetime] = None
    last_active: Optional[datetime] = None
    is_active: bool = False


class SessionListResponse(BaseModel):
    sessions: List[SessionListItem]
    total: int
    limit: int
    offset: int


class SessionDetail(BaseModel):
    session_id: str
    title: Optional[str] = None
    created_at: Optional[datetime] = None
    last_active: Optional[datetime] = None
    topics_config: Optional[Dict[str, Any]] = None
    model: Optional[str] = None
    agent_id: Optional[str] = None
    enabled_tools: Optional[List[str]] = None
    is_active: bool = False


class CreateSessionResponse(BaseModel):
    session_id: str
    created_at: datetime
    model: Optional[str] = None
    agent_id: Optional[str] = None


# --- Agent Models ---


class AgentDetail(BaseModel):
    id: str
    name: str
    persona: str
    instructions: str = ""
    model: Optional[str] = None
    temperature: float = 0.7
    enabled_tools: Optional[List[str]] = None
    is_spawned: bool = False
    is_default: bool = False
    spawned_by: Optional[str] = None
    created_at: Optional[datetime] = None


class AgentListResponse(BaseModel):
    agents: List[AgentDetail]


# --- Memory Models ---


class MemoryItem(BaseModel):
    id: str
    content: str
    topic: Optional[str] = None
    created_at: Optional[datetime] = None


class SessionMemoryResponse(BaseModel):
    memories: Dict[str, List[MemoryItem]]
    total: int


class WorkingMemoryItem(BaseModel):
    """API response model for a single working memory entry.

    Named differently from common.schema.memory.WorkingMemoryEntry (dataclass)
    to avoid collision.
    """

    id: str
    content: str
    created_at: Optional[datetime] = None


class WorkingMemoryResponse(BaseModel):
    rules: List[WorkingMemoryItem] = []
    preferences: List[WorkingMemoryItem] = []
    icks: List[WorkingMemoryItem] = []


# --- Chat & History Models ---


class HistoryMessage(BaseModel):
    role: str
    content: str
    timestamp: datetime
    msg_id: Optional[str] = None
    # Frontend-facing fields (spread from metadata for compatibility)
    tool_calls: Optional[List[Dict[str, Any]]] = None
    usage: Optional[Dict[str, Any]] = None
    sources: Optional[List[Dict[str, Any]]] = None
    total_duration: Optional[int] = None


class HistoryResponse(BaseModel):
    session_id: str
    messages: List[HistoryMessage]
