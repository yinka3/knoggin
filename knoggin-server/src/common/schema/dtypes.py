"""Data types for the Knoggin pipeline.

Domain Primitives (Entity, Connection, Fact, Message, ProfileUpdate) live in
`common.schema.primitives`. This module re-exports them and holds pipeline-specific
processing types (BatchResult, DLQEntry, etc.) and LLM judgment models.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import time
from typing import Dict, List, Literal, Optional, Set, Union
from pydantic import BaseModel, Field

# ── Re-export Domain Primitives ──────────────────────────────────
from common.schema.primitives import (
    Entity,
    EntityRecord,
    Connection,
    ConnectionRecord,
    Fact,
    FactRecord,
    Message,
    ProfileUpdate,
    _parse_dt,
)

# ═══════════════════════════════════════════════════════════════════
#  LLM EXTRACTION COLLECTION WRAPPERS
# ═══════════════════════════════════════════════════════════════════

class NERResult(BaseModel):
    """Collection model for NER batch extraction."""
    mentions: List[Entity] = Field(default_factory=list)

class ConnectionsResult(BaseModel):
    """Collection model for extracted connections."""
    connections: List[Connection] = Field(default_factory=list)

class EntityProfilesResult(BaseModel):
    """Collection model for profile extraction results."""
    profiles: List[ProfileUpdate] = Field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════
#  LLM JUDGMENT MODELS (unchanged)
# ═══════════════════════════════════════════════════════════════════

class MergeJudgment(BaseModel):
    """Model for deciding if two entities should be merged."""
    should_merge: bool = Field(..., description="True if entities refer to the same real-world concept")
    reasoning: str = Field(..., description="Justification for the decision")
    confidence: float = Field(..., ge=0.0, le=1.0)
    new_canonical_name: Optional[str] = Field(None, description="Suggested better name if merging")


class RelevanceResult(BaseModel):
    """Structured response for a single relevance check."""
    index: int = Field(..., description="The 1-based index from the input list")
    is_relevant: bool = Field(..., description="Whether the message relates to the entity's facts")

class BulkRelevanceResult(BaseModel):
    """Collection of relevance results."""
    judgments: List[RelevanceResult] = Field(default_factory=list)


class ContradictionJudgment(BaseModel):
    """Result for a single contradiction check."""
    index: int = Field(..., description="The 1-based index from the input list")
    is_contradiction: bool = Field(..., description="Whether FACT_B contradicts FACT_A")

class BulkContradictionResult(BaseModel):
    """Collection of contradiction judgments."""
    judgments: List[ContradictionJudgment] = Field(default_factory=list)


class TopicDetail(BaseModel):
    """Model for a single topic's configuration."""
    active: bool = Field(default=True)
    labels: List[str] = Field(default_factory=list)
    aliases: List[str] = Field(default_factory=list)
    hierarchy: Dict[str, List[str]] = Field(default_factory=dict)

class TopicConfigResult(BaseModel):
    """Model for the full topic configuration."""
    topics: Dict[str, TopicDetail] = Field(..., description="Map of TopicName to its configuration")


# ═══════════════════════════════════════════════════════════════════
#  PIPELINE PROCESSING TYPES
# ═══════════════════════════════════════════════════════════════════

@dataclass
class FactMergeResult:
    to_invalidate: List[str]
    new_contents: List[str]


@dataclass
class ResolutionResult:
    """Result from EntityResolver batch resolution."""
    entity_ids: List[int]
    new_ids: Set[int]
    alias_ids: Set[int]
    entity_msg_map: Dict[int, List[int]]
    alias_updates: Dict[int, List[str]]


@dataclass
class BatchResult:
    """Result of processing a batch of messages."""
    entity_ids: List[int] = field(default_factory=list)
    new_entity_ids: Set[int] = field(default_factory=set)
    alias_updated_ids: Set[int] = field(default_factory=set)
    alias_updates: Dict[int, List[str]] = field(default_factory=dict)
    extraction_result: Optional[List[Dict]] = None
    message_embeddings: Dict[int, List[float]] = field(default_factory=dict)
    success: bool = True
    error: Optional[str] = None

    def to_dict(self) -> dict:
        """Serialize for DLQ storage."""
        return {
            "entity_ids": self.entity_ids,
            "new_entity_ids": list(self.new_entity_ids),
            "alias_updated_ids": list(self.alias_updated_ids),
            "alias_updates": {str(k): v for k, v in self.alias_updates.items()},
            "extraction_result": self.extraction_result or [],
            "message_embeddings": {
                k: (v.tolist() if hasattr(v, 'tolist') else v)
                for k, v in self.message_embeddings.items()
            },
            "success": self.success,
            "error": self.error
        }

    @classmethod
    def from_dict(cls, data: dict) -> "BatchResult":
        """Deserialize from DLQ storage."""
        return cls(
            entity_ids=data.get("entity_ids", []),
            new_entity_ids=set(data.get("new_entity_ids", [])),
            alias_updated_ids=set(data.get("alias_updated_ids", [])),
            alias_updates={int(k): v for k, v in data.get("alias_updates", {}).items() if str(k).isdigit()},
            extraction_result=data.get("extraction_result"),
            message_embeddings={int(k): v for k, v in data.get("message_embeddings", {}).items() if str(k).isdigit()},
            success=data.get("success", True),
            error=data.get("error")
        )


@dataclass
class DLQEntry:
    messages: List[Dict]
    session_text: str
    error: str
    attempt: int = 1
    timestamp: float = field(default_factory=time.time)
    batch_size: int = field(init=False)
    
    def __post_init__(self):
        self.batch_size = len(self.messages)
    
    def to_json(self) -> str:
        return json.dumps(asdict(self))
    
    @classmethod
    def from_json(cls, raw: str) -> "DLQEntry":
        data = json.loads(raw)
        data.pop("batch_size", None)
        return cls(**data)
    
    def is_transient(self, transient_errors: List[str]) -> bool:
        return any(t in self.error for t in transient_errors)


# ═══════════════════════════════════════════════════════════════════
#  AGENT CONFIG (global, stays here)
# ═══════════════════════════════════════════════════════════════════

@dataclass
class AgentConfig:
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
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
            "persona": self.persona,
            "instructions": self.instructions,
            "model": self.model,
            "temperature": self.temperature,
            "enabled_tools": self.enabled_tools,
            "is_default": self.is_default,
            "is_spawned": self.is_spawned,
            "spawned_by": self.spawned_by,
            "created_at": self.created_at.isoformat()
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "AgentConfig":
        created = data.get("created_at")
        if isinstance(created, str):
            created = datetime.fromisoformat(created)
        return cls(
            id=data["id"],
            name=data["name"],
            persona=data["persona"],
            instructions=data.get("instructions"),
            model=data.get("model"),
            temperature=data.get("temperature", 0.7),
            enabled_tools=data.get("enabled_tools"),
            is_default=data.get("is_default", False),
            is_spawned=data.get("is_spawned", False),
            spawned_by=data.get("spawned_by"),
            created_at=created or datetime.now(timezone.utc)
        )
