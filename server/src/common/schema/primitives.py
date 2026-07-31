"""Domain primitives for entities, relationships, episodes, and messages."""

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

from common.utils.time_utils import get_now


class Entity(BaseModel):
    """Lightweight entity extracted by the LLM."""

    name: str = Field(..., description="The name of the entity as mentioned in text")
    type: str = Field(
        ..., description="Semantic type (e.g., person, organization, location, concept)"
    )
    topic: str = Field(..., description="High-level topic category")
    confidence: float = Field(
        1.0, ge=0.0, le=1.0, description="Extraction confidence score"
    )
    metadata: Dict[str, Any] = Field(default_factory=dict)


class Connection(BaseModel):
    """Lightweight connection extracted by the LLM."""

    entity_a: str = Field(..., description="Name of the first entity")
    entity_b: str = Field(..., description="Name of the second entity")
    relationship: str = Field(..., description="Brief description of the connection")
    confidence: float = Field(1.0, ge=0.0, le=1.0)
    context: Optional[str] = Field(
        None, description="Short snippet proving the connection"
    )
    metadata: Dict[str, Any] = Field(default_factory=dict)


class Message(BaseModel):
    """A single message in the conversation."""

    content: str = Field(..., description="The message text")
    id: int = Field(-1, description="DB-assigned message ID")
    timestamp: datetime = Field(default_factory=get_now)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    episode_eligible: bool = False
    episode_type: Optional[str] = None
