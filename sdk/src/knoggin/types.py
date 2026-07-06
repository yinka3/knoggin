"""SDK-facing types — simple result dataclasses for Knoggin."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

@dataclass
class AgentResult:
    """Result from agent.chat()."""
    response: str
    state: str
    tools_used: List[str] = field(default_factory=list)
    evidence: Dict = field(default_factory=dict)