"""Domain primitives for entities, relationships, episodes, and messages."""

import math
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

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


class EntityRecord(Entity):
    """DB-stored entity with message source tracking."""

    msg_id: int = Field(
        ..., description="ID of the source message this entity was extracted from"
    )

    @classmethod
    def from_extraction(cls, entity: Entity, msg_id: int, **kwargs) -> "EntityRecord":
        return cls(**entity.model_dump(), msg_id=msg_id, **kwargs)


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


class ConnectionRecord(Connection):
    """DB-bound connection with message source tracking."""

    msg_id: int = Field(..., description="ID of the source message")

    @classmethod
    def from_extraction(
        cls, conn: Connection, msg_id: int, **kwargs
    ) -> "ConnectionRecord":
        return cls(**conn.model_dump(), msg_id=msg_id, **kwargs)


class MessageEpisode(BaseModel):
    """A canonical source message and its contribution to an episode."""

    message_id: int = Field(..., gt=0)
    influence_weight: float = Field(0.0, ge=0.0)
    influence_reason: Optional[str] = None
    message_position: int = Field(..., ge=0)
    attached_at: Optional[datetime] = None


class EntityEpisode(BaseModel):
    """An entity observed in an episode's source messages."""

    entity_id: int = Field(..., gt=0)
    prominence_weight: float = Field(0.0, ge=0.0)
    role: Optional[str] = None
    is_focus_entity: bool = False
    source_message_count: int = Field(0, ge=0)
    first_seen_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None


class RelationshipEpisode(BaseModel):
    """A relationship evidenced by an episode's source messages."""

    relationship_id: str = Field(..., min_length=1)
    prominence_weight: float = Field(0.0, ge=0.0)
    is_central_relationship: bool = False
    source_message_count: int = Field(0, ge=0)


class Episode(BaseModel):
    """A complete episodic memory and its source-linked graph context."""

    episode_id: str = Field(..., min_length=1)
    project_id: str = Field(..., min_length=1)
    session_id: str = Field(..., min_length=1)
    summary: str = Field(..., min_length=1)
    new_developments: List[str] = Field(default_factory=list)
    updates: List[str] = Field(default_factory=list)
    unresolved: List[str] = Field(default_factory=list)
    importance: float = Field(0.0, ge=0.0, le=1.0)
    source_message_count: int = Field(0, ge=0)
    first_message_at: Optional[datetime] = None
    last_message_at: Optional[datetime] = None
    embedding: Optional[List[float]] = Field(default=None, exclude=True)
    messages: List[MessageEpisode] = Field(default_factory=list, min_length=1)
    entities: List[EntityEpisode] = Field(default_factory=list)
    relationships: List[RelationshipEpisode] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=get_now)
    updated_at: datetime = Field(default_factory=get_now)
    generator_metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("embedding")
    @classmethod
    def validate_embedding(
        cls, embedding: Optional[List[float]]
    ) -> Optional[List[float]]:
        if embedding is None:
            return None
        if len(embedding) != 1024:
            raise ValueError(
                "episode embedding must contain exactly 1024 dimensions"
            )
        if not all(math.isfinite(value) for value in embedding):
            raise ValueError("episode embedding must contain only finite values")
        return embedding

    @field_validator("messages")
    @classmethod
    def validate_messages(
        cls, messages: List[MessageEpisode]
    ) -> List[MessageEpisode]:
        message_ids = [message.message_id for message in messages]
        if len(set(message_ids)) != len(message_ids):
            raise ValueError("messages must not contain duplicate message IDs")
        message_positions = [message.message_position for message in messages]
        if len(set(message_positions)) != len(message_positions):
            raise ValueError("messages must not contain duplicate message positions")
        return messages

    @field_validator("entities")
    @classmethod
    def validate_entities(cls, entities: List[EntityEpisode]) -> List[EntityEpisode]:
        entity_ids = [entity.entity_id for entity in entities]
        if len(set(entity_ids)) != len(entity_ids):
            raise ValueError("entities must not contain duplicate entity IDs")
        if sum(entity.is_focus_entity for entity in entities) > 2:
            raise ValueError("episodes may contain at most two focus entities")
        return entities

    @field_validator("relationships")
    @classmethod
    def validate_relationships(
        cls, relationships: List[RelationshipEpisode]
    ) -> List[RelationshipEpisode]:
        relationship_ids = [relationship.relationship_id for relationship in relationships]
        if len(set(relationship_ids)) != len(relationship_ids):
            raise ValueError("relationships must not contain duplicate relationship IDs")
        return relationships


class Message(BaseModel):
    """A single message in the conversation."""

    content: str = Field(..., description="The message text")
    id: int = Field(-1, description="DB-assigned message ID")
    timestamp: datetime = Field(default_factory=get_now)
    metadata: Dict[str, Any] = Field(default_factory=dict)
