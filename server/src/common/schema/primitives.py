"""Conversation-domain primitives."""

from datetime import datetime
from typing import Any, Dict

from pydantic import BaseModel, Field

from common.utils.time_utils import get_now


class Message(BaseModel):
    """A single message in the conversation."""

    content: str = Field(..., description="The message text")
    id: int = Field(-1, description="DB-assigned message ID")
    timestamp: datetime = Field(default_factory=get_now)
    metadata: Dict[str, Any] = Field(default_factory=dict)
