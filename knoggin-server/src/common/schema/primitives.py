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

from pydantic import BaseModel, Field


# ═══════════════════════════════════════════════════════════════════
#  ENTITY — any discrete concept in the knowledge graph
# ═══════════════════════════════════════════════════════════════════

class Entity(BaseModel):
    """Lightweight entity extracted by the LLM."""
    name: str = Field(..., description="The name of the entity as mentioned in text")
    type: str = Field(..., description="Semantic type (e.g., person, organization, location, concept)")
    topic: str = Field(..., description="High-level topic category")
    confidence: float = Field(1.0, ge=0.0, le=1.0, description="Extraction confidence score")
    metadata: Dict[str, Any] = Field(default_factory=dict)


class EntityRecord(Entity):
    """DB-stored entity with message source tracking."""
    msg_id: int = Field(..., description="ID of the source message this entity was extracted from")

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
    context: Optional[str] = Field(None, description="Short snippet proving the connection")
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ConnectionRecord(Connection):
    """DB-bound connection with message source tracking."""
    msg_id: int = Field(..., description="ID of the source message")

    @classmethod
    def from_extraction(cls, conn: Connection, msg_id: int, **kwargs) -> "ConnectionRecord":
        """Promote a lightweight Connection into a DB-ready record."""
        return cls(**conn.model_dump(), msg_id=msg_id, **kwargs)


# ═══════════════════════════════════════════════════════════════════
#  FACT — an atomic piece of episodic evidence about an entity
# ═══════════════════════════════════════════════════════════════════

class Fact(BaseModel):
    """Lightweight fact extracted during profile synthesis."""
    content: str = Field(..., description="The atomic fact content")
    source_entity: Optional[str] = Field(None, description="Name of the entity this fact is about")
    source_msg_id: Optional[int] = Field(None, description="ID of the source message")
    supersedes: Optional[str] = Field(None, description="Exact text of an existing fact this replaces")
    invalidates: Optional[str] = Field(None, description="Exact text of an existing fact this removes")
    metadata: Dict[str, Any] = Field(default_factory=dict)


def _parse_dt(val) -> datetime:
    """Parse a datetime from various formats (ISO string, unix timestamp, or datetime)."""
    if isinstance(val, str):
        return datetime.fromisoformat(val)
    if isinstance(val, (int, float)):
        return datetime.fromtimestamp(val, tz=timezone.utc)
    if isinstance(val, datetime):
        return val
    raise TypeError(f"Cannot parse datetime from {type(val)}: {val}")


class FactRecord(Fact):
    """DB-stored fact"""
    id: str = Field(..., description="Unique fact identifier")
    source_entity_id: int = Field(..., description="DB ID of the source entity")
    valid_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
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
        """Hydrate from a Memgraph query result."""
        return cls(
            id=record["id"],
            content=record["content"],
            source_entity_id=record["source_entity_id"],
            valid_at=_parse_dt(record["valid_at"]),
            invalid_at=_parse_dt(record["invalid_at"]) if record.get("invalid_at") else None,
            confidence=record.get("confidence", 1.0),
            embedding=record.get("embedding") or [],
            source_msg_id=record.get("source_msg_id"),
            source=record.get("source", "user"),
        )


# ═══════════════════════════════════════════════════════════════════
#  MESSAGE — a raw user or system input
# ═══════════════════════════════════════════════════════════════════

class Message(BaseModel):
    """A single message in the conversation."""
    content: str = Field(..., description="The message text")
    id: int = Field(-1, description="DB-assigned message ID")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ═══════════════════════════════════════════════════════════════════
#  PROFILE UPDATE — container pairing an entity name with its facts
# ═══════════════════════════════════════════════════════════════════

class ProfileUpdate(BaseModel):
    """Groups extracted facts under their source entity name."""
    canonical_name: str = Field(..., description="The name of the entity")
    facts: List[Fact] = Field(default_factory=list, description="List of structured fact updates")
