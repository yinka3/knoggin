from dataclasses import dataclass, field
from typing import Dict, List, Optional
from uuid import uuid4

from common.schema.source_reference import SourceReferenceCandidate


@dataclass(frozen=True)
class MaintenanceCandidate:
    """Python-selected autonomous maintenance work offered to an agent run."""

    id: str
    kind: str
    reason: str
    suggested_tool: str
    priority: str = "normal"
    metadata: Dict = field(default_factory=dict)
    attempts: int = 0
    cooldown_until: Optional[float] = None


@dataclass
class ToolCall:
    name: str
    args: Dict = field(default_factory=dict)
    thinking: Optional[str] = None
    call_id: str = field(default_factory=lambda: str(uuid4()))


@dataclass
class FinalResponse:
    content: str
    usage: Optional[Dict] = None
    sources: Optional[List[Dict]] = None
    sources_consulted: Optional[List[SourceReferenceCandidate]] = None
