from datetime import datetime
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Union

from common.schema.memory import PromptContext

@dataclass(frozen=True)
class AgentRunConfig:
    """Immutable settings governing limits and timeouts for an agent run."""
    max_calls: int = 12
    max_attempts: int = 15
    max_history_turns: int = 7
    max_accumulated_messages: int = 30
    max_consecutive_errors: int = 3
    tool_timeout: float = 30.0
    tool_limits: Tuple[Tuple[str, int], ...] = (
        ("search_messages", 6),
        ("get_connections", 8),
        ("search_entity", 8),
        ("fact_check", 6),
        ("get_recent_activity", 8),
        ("find_path", 8),
        ("get_hierarchy", 8),
        ("web_search", 8),
        ("news_search", 8),
        ("save_memory", 4),
        ("save_insight", 4),
        ("forget_memory", 4),
        ("spawn_specialist", 2),
        ("mcp__*", 3),
    )
    
    def get_tool_limit(self, tool_name: str, default: int = 6) -> int:
        limits_dict = dict(self.tool_limits)
        if tool_name in limits_dict:
            return limits_dict[tool_name]
        
        for key, limit in limits_dict.items():
            if key.endswith("*") and tool_name.startswith(key[:-1]):
                return limit
        return default


@dataclass
class AgentState:
    """Mutable tracking state maintained during a single agent reasoning loop."""
    call_count: int = 0
    attempt_count: int = 0
    consecutive_errors: int = 0
    tools_used: List[str] = field(default_factory=list)
    previous_calls: Set[Tuple[str, str]] = field(default_factory=set)
    last_error: Optional[str] = None
    tool_call_counts: Dict[str, int] = field(default_factory=dict)
    usage: Dict[str, int] = field(default_factory=lambda: {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})
    
    def is_duplicate(self, tool_name: str, args: Dict) -> bool:
        call_sig = (tool_name, json.dumps(args, sort_keys=True, default=str))
        return call_sig in self.previous_calls
    
    def tool_limit_reached(self, tool_name: str, config: AgentRunConfig) -> bool:
        limit = config.get_tool_limit(tool_name, config.max_calls)
        return self.tool_call_counts.get(tool_name, 0) >= limit
    
    def record_call(self, tool_name: str, args: Dict):
        call_sig = (tool_name, json.dumps(args, sort_keys=True, default=str))
        self.previous_calls.add(call_sig)
        self.call_count += 1
        self.tools_used.append(tool_name)
        self.tool_call_counts[tool_name] = self.tool_call_counts.get(tool_name, 0) + 1


@dataclass
class RetrievedEvidence:
    """Accumulated contextual results gathered from tool executions."""
    messages: List[Dict] = field(default_factory=list)
    profiles: List[Dict] = field(default_factory=list)
    graph: List[Dict] = field(default_factory=list)
    paths: List[Dict] = field(default_factory=list)
    hierarchy: List[Dict] = field(default_factory=list)
    facts: List[Dict] = field(default_factory=list)
    sources: List[Dict] = field(default_factory=list)
    summary: Optional[str] = None
    token_count: int = 0
    
    def has_any(self) -> bool:
        return bool(self.profiles or self.messages or self.graph or self.paths or self.hierarchy or self.facts or self.sources or self.summary)


@dataclass
class AgentContext:
    """Core container aggregating configuration, state, and evidence for an agent execution."""
    config: AgentRunConfig
    state: AgentState
    evidence: RetrievedEvidence
    user_name: str = ""
    user_query: str = ""
    session_id: str = ""
    run_id: str = ""
    agent_id: str = ""
    agent_name: str = "STELLA"
    agent_persona: str = ""
    history: List[Dict] = field(default_factory=list)
    hot_topics: List[str] = field(default_factory=list)
    active_topics: List[str] = field(default_factory=list)
    hot_topic_context: Dict[str, Dict] = field(default_factory=dict)
    prompt: PromptContext = field(default_factory=PromptContext)
    is_community: bool = False
    current_participants: List[str] = field(default_factory=list)

@dataclass
class ToolCall:
    name: str
    args: Dict = field(default_factory=dict)
    thinking: Optional[str] = None
    call_id: Optional[str] = None


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
