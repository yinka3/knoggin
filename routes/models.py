from pydantic import BaseModel, Field
from typing import List, Optional, Literal


class ChatMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str
    timestamp: Optional[str] = None

class ChatRequest(BaseModel):
    message: str = Field(..., min_length=5, max_length=500)
    history: List[ChatMessage] = []

class ChatEventData(BaseModel):
    response: Optional[str] = None
    question: Optional[str] = None
    tools_used: List[str] = []
    state: Optional[str] = None
    profiles: List[dict] = []
    messages: List[dict] = []
    done: bool = False
    error: Optional[str] = None

class TopicList(BaseModel):
    active: List[str]
    hot: List[str]
    inactive: List[str]


class TopicUpdate(BaseModel):
    status: Literal["active", "hot", "inactive"]

class EntitySummary(BaseModel):
    name: str
    type: str
    summary_snippet: Optional[str] = None
    topic: Optional[str] = None


class EntityProfile(BaseModel):
    id: int
    canonical_name: str
    type: Optional[str] = None
    aliases: List[str] = []
    summary: Optional[str] = None
    topic: Optional[str] = None
    last_mentioned: Optional[int] = None
    last_updated: Optional[int] = None

class MoodCheckpoint(BaseModel):
    primary: str
    primary_count: int
    secondary: Optional[str] = None
    secondary_count: int = 0
    timestamp: int
    message_count: int


class MoodResponse(BaseModel):
    current: Optional[MoodCheckpoint] = None
    history: List[MoodCheckpoint] = []