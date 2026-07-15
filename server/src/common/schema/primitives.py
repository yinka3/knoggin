"""Domain Primitives — the 4 universal building blocks of the Knoggin knowledge graph.

Every piece of data in the system is one of these:
  Entity     — a discrete concept, person, place, or thing
  Connection — a semantic relationship between two entities
  Fact       — an atomic piece of episodic evidence about an entity
  Message    — a raw user or system input

Each base type is lightweight (designed for LLM extraction output).
Each *Record subclass adds DB-layer fields (IDs, timestamps, embeddings)
and provides a `from_extraction()` promotion method to carry metadata forward.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

from common.utils.time_utils import get_now, parse_iso_time

# ═══════════════════════════════════════════════════════════════════
#  ENTITY — any discrete concept in the knowledge graph
# ═══════════════════════════════════════════════════════════════════


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
        """Promote a lightweight Entity into a DB-ready record."""
        return cls(**entity.model_dump(), msg_id=msg_id, **kwargs)


# ═══════════════════════════════════════════════════════════════════
#  CONNECTION — a semantic relationship edge between two entities
# ═══════════════════════════════════════════════════════════════════


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
        """Promote a lightweight Connection into a DB-ready record."""
        return cls(**conn.model_dump(), msg_id=msg_id, **kwargs)


# ═══════════════════════════════════════════════════════════════════
#  FACT — an atomic piece of episodic evidence about an entity
# ═══════════════════════════════════════════════════════════════════


class Fact(BaseModel):
    """Lightweight fact extracted during profile synthesis."""

    content: str = Field(..., description="The atomic fact content")
    source_entity: Optional[str] = Field(
        None, description="Name of the entity this fact is about"
    )
    source_msg_id: Optional[int] = Field(None, description="ID of the source message")
    supersedes: Optional[str] = Field(
        None, description="Exact text of an existing fact this replaces"
    )
    invalidates: Optional[str] = Field(
        None, description="Exact text of an existing fact this removes"
    )
    metadata: Dict[str, Any] = Field(default_factory=dict)


def _parse_dt(val) -> datetime:
    """Parse a datetime from various formats (ISO string, unix timestamp, or datetime)."""
    if isinstance(val, str):
        result = parse_iso_time(val)
        if result is None:
            raise ValueError(f"Cannot parse datetime from string: {val}")
        return result
    if isinstance(val, (int, float)):
        return datetime.fromtimestamp(val, tz=timezone.utc)
    if isinstance(val, datetime):
        return val
    raise TypeError(f"Cannot parse datetime from {type(val)}: {val}")


class FactRecord(Fact):
    """DB-stored fact"""

    source_user_name: Optional[str] = Field(
        None, description="User scope for the source message"
    )
    source_session_id: Optional[str] = Field(
        None, description="Session scope for the source message"
    )
    id: str = Field(..., description="Unique fact identifier")
    source_entity_id: int = Field(..., description="DB ID of the source entity")
    valid_at: datetime = Field(default_factory=get_now)
    invalid_at: Optional[datetime] = None
    confidence: float = 1.0
    source: str = "user"
    embedding: List[float] = Field(default_factory=list, exclude=True)

    def to_dict(self, exclude: set = None) -> dict:
        """Serialize for API/agent consumption, excluding embedding by default."""
        if exclude is None:
            exclude = {"embedding"}
        data = self.model_dump(exclude=exclude)
        # Ensure datetimes are ISO strings
        for key in ("valid_at", "invalid_at"):
            if key in data and isinstance(data[key], datetime):
                data[key] = data[key].isoformat()
        return data

    @classmethod
    def from_db_record(cls, record: dict) -> "FactRecord":
        """Hydrate from a KnowledgeStore query result."""
        return cls(
            id=record["id"],
            content=record["content"],
            source_entity_id=record["source_entity_id"],
            valid_at=_parse_dt(record["valid_at"]),
            invalid_at=_parse_dt(record["invalid_at"])
            if record.get("invalid_at")
            else None,
            confidence=record.get("confidence", 1.0),
            embedding=record.get("embedding") or [],
            source_msg_id=record.get("source_msg_id"),
            source_user_name=record.get("source_user_name"),
            source_session_id=record.get("source_session_id"),
            source=record.get("source", "user"),
        )


# ═══════════════════════════════════════════════════════════════════
#  MESSAGE — a raw user or system input
# ═══════════════════════════════════════════════════════════════════


# EPISODE - a traceable, mutable summary over a set of source messages


class MessageEpisode(BaseModel):
    """A canonical source message and its contribution to an episode."""

    message_id: int = Field(..., gt=0)
    influence_weight: float = Field(0.0, ge=0.0)
    influence_reason: Optional[str] = None
    message_position: int = Field(..., ge=0)


class EntityEpisode(BaseModel):
    """An entity observed in an episode's source messages."""

    entity_id: int = Field(..., gt=0)
    prominence_weight: float = Field(0.0, ge=0.0)
    role: Optional[str] = None
    is_focus_entity: bool = False
    source_message_count: int = Field(0, ge=0)


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
    messages: List[MessageEpisode] = Field(default_factory=list, min_length=1)
    entities: List[EntityEpisode] = Field(default_factory=list)
    relationships: List[RelationshipEpisode] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=get_now)
    updated_at: datetime = Field(default_factory=get_now)
    generator_metadata: Dict[str, Any] = Field(default_factory=dict)

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
        relationship_ids = [
            relationship.relationship_id for relationship in relationships
        ]
        if len(set(relationship_ids)) != len(relationship_ids):
            raise ValueError(
                "relationships must not contain duplicate relationship IDs"
            )
        return relationships


class Message(BaseModel):
    """A single message in the conversation."""

    content: str = Field(..., description="The message text")
    id: int = Field(-1, description="DB-assigned message ID")
    timestamp: datetime = Field(default_factory=get_now)
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ═══════════════════════════════════════════════════════════════════
#  PROFILE UPDATE — container pairing an entity name with its facts
# ═══════════════════════════════════════════════════════════════════


class ProfileUpdate(BaseModel):
    """Groups extracted facts under their source entity name."""

    canonical_name: str = Field(..., description="The name of the entity")
    facts: List[Fact] = Field(
        default_factory=list, description="List of structured fact updates"
    )
