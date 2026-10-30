"""Conversation-domain primitives."""

from datetime import datetime, timezone
from typing import Any, Dict

from pydantic import BaseModel, Field, field_validator, model_validator

from common.utils.time_utils import get_now


class Message(BaseModel):
    """A single message in the conversation."""

    content: str = Field(..., description="The message text")
    id: int = Field(-1, description="DB-assigned message ID")
    timestamp: datetime = Field(default_factory=get_now)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ImportedExchange(BaseModel):
    """One recorded user turn and its optional reply, at their original times."""

    user_content: str
    user_timestamp: datetime
    assistant_content: str | None = None
    assistant_timestamp: datetime | None = None

    @field_validator("user_timestamp", "assistant_timestamp")
    @classmethod
    def _as_utc(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _reply_follows_turn(self) -> "ImportedExchange":
        if not self.user_content.strip():
            raise ValueError("user_content must not be blank")
        if (self.assistant_content is None) != (self.assistant_timestamp is None):
            raise ValueError(
                "assistant_content and assistant_timestamp must be given together"
            )
        if (
            self.assistant_timestamp is not None
            and self.assistant_timestamp < self.user_timestamp
        ):
            raise ValueError("assistant_timestamp must not precede user_timestamp")
        return self
