from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import time
from typing import Dict, List, Literal, Optional, Set, Union


@dataclass
class MessageData:
    message: str
    id: int = -1
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class EntityPair:
    entity_a: str
    entity_b: str
    confidence: float
    context: str = None

@dataclass
class MessageConnections:
    message_id: int
    entity_pairs: List[EntityPair] = field(default_factory=list)


@dataclass
class EntityItem:
    msg_id: int
    name: str
    label: str
    topic: str
    confidence: float


@dataclass
class ProfileUpdate:
    canonical_name: str
    facts: List[str]


@dataclass
class Fact:
    id: str
    source_entity_id: int
    content: str
    valid_at: datetime
    invalid_at: Optional[datetime] = None
    source_msg_id: Optional[int] = None 
    confidence: float = 1.0
    embedding: List[float] = field(default_factory=list)
    source: str = "user"

    @classmethod
    def from_record(cls, record: dict) -> "Fact":
        return cls(
            id=record["id"],
            source_entity_id=record["source_entity_id"],
            content=record["content"],
            valid_at=cls._parse_dt(record["valid_at"]),
            invalid_at=cls._parse_dt(record["invalid_at"]) if record.get("invalid_at") else None,
            confidence=record.get("confidence", 1.0),
            embedding=record.get("embedding") or [],
            source_msg_id=record.get("source_msg_id"),
            source=record.get("source", "user")
        )
    
    def to_dict(self, exclude: set = None) -> dict:
        if exclude is None:
            exclude = {"embedding"}
        result = {}
        for k in self.__dataclass_fields__:
            if k in exclude:
                continue
            val = getattr(self, k)
            if isinstance(val, datetime):
                val = val.isoformat()
            result[k] = val
        return result

    @staticmethod
    def _parse_dt(val) -> datetime:
        if isinstance(val, str):
            return datetime.fromisoformat(val)
        return val

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
    extraction_result: Optional[List[MessageConnections]] = None
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
            "extraction_result": [
                {"message_id": mc.message_id, "entity_pairs": [
                    {"entity_a": p.entity_a, "entity_b": p.entity_b, 
                    "confidence": p.confidence, "context": p.context}
                    for p in mc.entity_pairs
                ]} for mc in (self.extraction_result or [])
            ],
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
        extraction_result = None
        if data.get("extraction_result"):
            extraction_result = [
                MessageConnections(
                    message_id=mc["message_id"],
                    entity_pairs=[
                        EntityPair(
                            entity_a=p["entity_a"], 
                            entity_b=p["entity_b"], 
                            confidence=p["confidence"],
                            context=p.get("context")
                        )
                        for p in mc["entity_pairs"]
                    ]
                ) for mc in data["extraction_result"]
            ]
        
        return cls(
            entity_ids=data.get("entity_ids", []),
            new_entity_ids=set(data.get("new_entity_ids", [])),
            alias_updated_ids=set(data.get("alias_updated_ids", [])),
            alias_updates={int(k): v for k, v in data.get("alias_updates", {}).items()},
            extraction_result=extraction_result,
            message_embeddings={int(k): v for k, v in data.get("message_embeddings", {}).items()},
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


# ===== AGENT RESPONSE/RESULT TYPES =====

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

@dataclass
class BaseResult:
    status: str
    state: str
    tools_used: List[str]


@dataclass
class CompleteResult(BaseResult):
    response: str
    messages: List[Dict]
    profiles: List[Dict]
    graph: List[Dict]


@dataclass
class ClarificationResult(BaseResult):
    question: str


RunResult = Union[CompleteResult, ClarificationResult]

@dataclass
class ToolCall:
    name: str
    args: Dict = field(default_factory=dict)
    thinking: Optional[str] = None


@dataclass 
class FinalResponse:
    content: str
    usage: Optional[Dict] = None
    sources: Optional[List[Dict]] = None


@dataclass
class ClarificationRequest:
    question: str
    usage: Optional[Dict] = None


AgentResponse = Union[ToolCall, List[ToolCall], FinalResponse, ClarificationRequest]
