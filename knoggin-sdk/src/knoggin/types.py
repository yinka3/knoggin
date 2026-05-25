"""SDK-facing dataclasses for Knoggin."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ChatEvent:
    """Single event emitted during an agent run."""

    event: str
    data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ChatResult:
    """Aggregated result from ``Session.chat()``."""

    response: str
    state: str
    session_id: str
    message_id: Optional[int] = None
    usage: Dict[str, Any] = field(default_factory=dict)
    sources: Any = None
    tools_used: List[str] = field(default_factory=list)
    events: List[ChatEvent] = field(default_factory=list)


@dataclass
class ProjectInfo:
    """Project metadata exposed by the SDK."""

    id: str
    name: str
    description: Optional[str] = None
    access_mode: str = "open"
    allowed_projects: List[str] = field(default_factory=list)
    session_count: int = 0
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass
class SessionInfo:
    """Session metadata exposed by the SDK."""

    id: str
    project_id: str
    model: Optional[str] = None
    agent_id: Optional[str] = None
    enabled_tools: Optional[List[str]] = None
    created_at: Optional[str] = None
    last_active: Optional[str] = None


@dataclass
class ConversationTurn:
    """Conversation history turn."""

    role: str
    content: str
    timestamp: str


@dataclass
class FileInfo:
    """Uploaded session file metadata."""

    file_id: str
    original_name: str
    extension: str = ""
    size_bytes: int = 0
    chunk_count: int = 0
    uploaded_at: Optional[str] = None


@dataclass
class FileSearchResult:
    """Search result from session file retrieval."""

    content: str
    file_name: str
    file_id: str
    score: float
    raw_score: Optional[float] = None


@dataclass
class AgentConfig:
    """SDK-facing agent configuration."""

    id: str
    name: str
    persona: str
    instructions: Optional[str] = None
    model: Optional[str] = None
    temperature: float = 0.7
    enabled_tools: Optional[List[str]] = None
    is_default: bool = False
    is_spawned: bool = False
    spawned_by: Optional[str] = None
    created_at: Optional[str] = None
