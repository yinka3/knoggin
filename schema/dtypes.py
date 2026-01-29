from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Literal, Optional, Union


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
class ResolutionEntry:
    verdict: Literal["EXISTING", "NEW_GROUP", "NEW_SINGLE"]
    mentions: List[str]
    entity_type: str
    canonical_name: Optional[str] = None
    topic: str = "General"
    msg_ids: List[int] = field(default_factory=list)


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
            source_msg_id=record.get("source_msg_id")
        )

    @staticmethod
    def _parse_dt(val) -> datetime:
        if isinstance(val, str):
            return datetime.fromisoformat(val)
        return val

@dataclass
class FactMergeResult:
    to_invalidate: List[str]
    new_contents: List[str]



# ===== AGENT RESPONSE/RESULT TYPES =====

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


@dataclass
class ClarificationRequest:
    question: str
    usage: Optional[Dict] = None


AgentResponse = Union[ToolCall, List[ToolCall], FinalResponse, ClarificationRequest]

@dataclass
class TraceEntry:
    step: int
    state: str
    tool: str
    args: Dict
    resolved_args: Dict
    result_summary: str
    result_count: int
    duration_ms: float
    error: Optional[str] = None

@dataclass
class QueryTrace:
    trace_id: str
    user_query: str
    started_at: datetime
    entries: List[TraceEntry] = field(default_factory=list)

