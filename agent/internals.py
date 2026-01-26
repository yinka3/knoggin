import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple


@dataclass(frozen=True)
class AgentConfig:
    """Immutable settings for agent run."""
    max_calls: int = 6
    max_attempts: int = 8
    max_history_turns: int = 7
    max_accumulated_messages: int = 30
    tool_limits: Tuple[Tuple[str, int], ...] = (
        ("search_messages", 2),
        ("get_connections", 4),
        ("search_entity", 4),
        ("get_activity", 5),
        ("find_path", 5),
        ("get_hierarchy", 5),
    )
    
    def get_tool_limit(self, tool_name: str, default: int = 6) -> int:
        for name, limit in self.tool_limits:
            if name == tool_name:
                return limit
        return default


@dataclass
class AgentState:
    """Mutable tracking during run."""
    call_count: int = 0
    attempt_count: int = 0
    consecutive_errors: int = 0
    tools_used: List[str] = field(default_factory=list)
    previous_calls: Set[Tuple[str, str]] = field(default_factory=set)
    last_error: Optional[str] = None
    tool_call_counts: Dict[str, int] = field(default_factory=dict)
    
    def is_duplicate(self, tool_name: str, args: Dict) -> bool:
        call_sig = (tool_name, json.dumps(args, sort_keys=True))
        return call_sig in self.previous_calls
    
    def tool_limit_reached(self, tool_name: str, config: AgentConfig) -> bool:
        limit = config.get_tool_limit(tool_name, config.max_calls)
        return self.tool_call_counts.get(tool_name, 0) >= limit
    
    def record_call(self, tool_name: str, args: Dict):
        call_sig = (tool_name, json.dumps(args, sort_keys=True))
        self.previous_calls.add(call_sig)
        self.call_count += 1
        self.tools_used.append(tool_name)
        self.tool_call_counts[tool_name] = self.tool_call_counts.get(tool_name, 0) + 1


@dataclass
class RetrievedEvidence:
    """Accumulated results from tool calls."""
    messages: List[Dict] = field(default_factory=list)
    profiles: List[Dict] = field(default_factory=list)
    graph: List[Dict] = field(default_factory=list)
    paths: List[Dict] = field(default_factory=list)
    hierarchy: List[Dict] = field(default_factory=list)
    
    def has_any(self) -> bool:
        return bool(self.profiles or self.messages or self.graph or self.paths or self.hierarchy)


@dataclass
class AgentContext:
    """Container for agent run."""
    config: AgentConfig
    state: AgentState
    evidence: RetrievedEvidence
    
    user_query: str = ""
    trace_id: str = ""
    history: List[Dict] = field(default_factory=list)
    hot_topics: List[str] = field(default_factory=list)
    active_topics: List[str] = field(default_factory=list)
    hot_topic_context: Dict[str, Dict] = field(default_factory=dict)