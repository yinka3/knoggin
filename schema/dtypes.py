from dataclasses import dataclass, field
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Dict, List, Literal, Optional, TypedDict, Union


class MessageData(BaseModel):
    id: int = -1
    message: str
    timestamp: datetime = Field(default_factory=datetime.now)

class EntityPair(BaseModel):
    entity_a: str = Field(..., description="First entity canonical_name (alphabetically first).")
    entity_b: str = Field(..., description="Second entity canonical_name (alphabetically second).")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score 0.0-1.0")
    reason: str = Field(..., description="Short explanation for the connection")

class MessageConnections(BaseModel):
    message_id: int = Field(..., description="Copy exactly from input.")
    entity_pairs: List[EntityPair] = Field(default_factory=list, description="Pairs of entities with meaningful connections in this message.")

class ConnectionExtractionResponse(BaseModel):
    message_results: List[MessageConnections] = Field(..., description="Per-message entity connections.")
    reasoning_trace: str = Field(..., description="Chain of thought analysis before extraction")

class ProfileUpdate(BaseModel):
    canonical_name: str
    facts: List[str] = Field(
        ..., 
        description="List of atomic facts. "
                    "Maintain existing facts. "
                    "Append new ones. "
                    "Mark invalid ones with ' [INVALIDATED: <date>]'."
    )


class BatchProfileResponse(BaseModel):
    profiles: List[ProfileUpdate] = Field(
        ..., 
        description="One ProfileUpdate per input entity, in same order as input."
    )

class ResolutionEntry(BaseModel):
    verdict: Literal["EXISTING", "NEW_GROUP", "NEW_SINGLE"] = Field(
        ..., 
        description="EXISTING: mention(s) map to an entity already in known_entities. "
                    "NEW_GROUP: multiple mentions in this batch refer to the same NEW entity. "
                    "NEW_SINGLE: a single mention representing a new entity."
    )
    mentions: List[str] = Field(
        ..., 
        description="All text spans from the input that refer to this entity. "
                    "For EXISTING, includes the mention(s) that matched. "
                    "For NEW_GROUP, all grouped mentions. "
                    "For NEW_SINGLE, the single mention."
    )
    entity_type: str = Field(
        ..., 
        description="Semantic type of the entity (e.g., person, professor, organization, place, gym). "
                    "Use the type from the original mention. If grouped mentions have mixed types, "
                    "use the type of the mention selected as canonical_name."
    )
    canonical_name: Optional[str] = Field(
        default=None, 
        description="For EXISTING: the exact canonical_name from known_entities. "
                    "For NEW_GROUP: the longest or most complete mention. "
                    "For NEW_SINGLE: the mention text verbatim."
    )
    topic: str = Field(
        default="General",
        description="The topic category this entity belongs to, from the original mention's topic. "
        "If grouped mentions have different topics, use the topic of the canonical mention."
    )


class DisambiguationResult(BaseModel):
    entries: List[ResolutionEntry] = Field(
        ..., 
        description="One entry per distinct entity. Every input mention must appear "
                    "in exactly one entry. No mention left behind, no mention duplicated."
    )

class BaseResult(TypedDict):
    status: str
    state: str
    tools_used: List[str]


class CompleteResult(BaseResult):
    response: str
    messages: List[Dict]
    profiles: List[Dict]
    graph: List[Dict]
    # web: List[Dict]


class ClarificationResult(BaseResult):
    question: str


RunResult = Union[CompleteResult, ClarificationResult]

@dataclass
class ToolCall:
    name: str
    args: Dict = field(default_factory=dict)


@dataclass 
class FinalResponse:
    content: str


@dataclass
class ClarificationRequest:
    question: str


StellaResponse = Union[ToolCall, List[ToolCall], FinalResponse, ClarificationRequest]

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