"""Workflow-owned state for one agent execution.

``AgentRun`` keeps live agent state together for the duration of a single
reasoning loop.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4

from common.schema.agent.identity import AgentConfig
from common.schema.agent.settings import validate_tool_limit_overrides
from common.schema.agent.stream import StreamUsage
from common.schema.document import DocumentFocus
from common.schema.ingestion.contracts import ExecutionScope
from common.schema.source.references import SourceReferenceCandidate
from core.agent.maintenance import MaintenanceCandidate
from core.agent.tools.registry import (
    get_default_tool_limits,
    get_registered_tool_names,
)


def _empty_usage() -> StreamUsage:
    return {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "approximate": False,
    }


@dataclass(frozen=True, slots=True)
class AgentIdentity:
    """Resolved agent identity and effective presentation for one run."""

    config: AgentConfig
    name: str
    persona: str


@dataclass(frozen=True, slots=True)
class AgentRunLimits:
    """Immutable policy snapshot used by exactly one agent run."""

    max_calls: int = 12
    max_attempts: int = 15
    max_history_turns: int = 7
    max_accumulated_messages: int = 30
    max_accumulated_profiles: int = 20
    max_accumulated_graph: int = 40
    max_accumulated_paths: int = 8
    max_accumulated_episodes: int = 8
    max_accumulated_sources: int = 12
    max_consecutive_errors: int = 3
    empty_result_replan_threshold: int = 3
    tool_timeout: float = 30.0
    tool_limits: Tuple[Tuple[str, int], ...] = field(
        default_factory=lambda: tuple(get_default_tool_limits().items())
    )

    @classmethod
    def from_settings(cls, settings: Any) -> "AgentRunLimits":
        """Compile mutable application settings into an execution snapshot."""

        defaults = get_default_tool_limits()
        overrides = dict(getattr(settings, "tool_limit_overrides", {}))
        validate_tool_limit_overrides(settings, get_registered_tool_names())
        return cls(
            max_calls=settings.max_tool_calls,
            tool_timeout=settings.tool_timeout,
            max_attempts=settings.max_attempts,
            max_history_turns=settings.agent_history_turns,
            max_accumulated_messages=settings.max_accumulated_messages,
            max_consecutive_errors=settings.max_consecutive_errors,
            tool_limits=tuple((defaults | overrides).items()),
        )

    def get_tool_limit(self, tool_name: str, default: int = 6) -> int:
        limits = dict(self.tool_limits)
        if tool_name in limits:
            return limits[tool_name]
        for key, limit in limits.items():
            if key.endswith("*") and tool_name.startswith(key[:-1]):
                return limit
        return default


@dataclass(slots=True)
class AgentRun:
    """Mutable aggregate that owns all ephemeral state for one agent run."""

    run_id: str
    user_name: str
    project_id: str
    session_id: str
    user_query: str
    agent: AgentIdentity
    model: Optional[str]
    temperature: float
    enabled_tools: Optional[Tuple[str, ...]]
    limits: AgentRunLimits
    history: List[Dict] = field(default_factory=list)
    document_focus: Optional[DocumentFocus] = None
    hot_topics: List[str] = field(default_factory=list)
    active_topics: List[str] = field(default_factory=list)
    hot_topic_context: Dict[str, Dict] = field(default_factory=dict)
    maintenance_candidates: List[MaintenanceCandidate] = field(default_factory=list)
    is_community: bool = False
    current_participants: List[str] = field(default_factory=list)
    initial_source_candidates: List[SourceReferenceCandidate] = field(
        default_factory=list
    )
    messages: List[Dict] = field(default_factory=list)
    profiles: List[Dict] = field(default_factory=list)
    graph: List[Dict] = field(default_factory=list)
    paths: List[Dict] = field(default_factory=list)
    episodes: List[Dict] = field(default_factory=list)
    sources: List[Dict] = field(default_factory=list)
    evidence_summary: Optional[str] = None
    evidence_token_count: int = 0
    call_count: int = 0
    attempt_count: int = 0
    consecutive_errors: int = 0
    consecutive_empty_results: int = 0
    tools_used: List[str] = field(default_factory=list)
    previous_calls: Set[Tuple[str, str]] = field(default_factory=set)
    last_error: Optional[str] = None
    tool_call_counts: Dict[str, int] = field(default_factory=dict)
    short_uuid_references: Dict[str, str] = field(default_factory=dict)
    source_candidates: List[SourceReferenceCandidate] = field(default_factory=list)
    usage: StreamUsage = field(default_factory=_empty_usage)
    final_content: Optional[str] = None
    sealed: bool = False
    released: bool = False

    @classmethod
    def open(
        cls,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        user_query: str,
        agent: AgentIdentity,
        limits: AgentRunLimits,
        model: Optional[str] = None,
        temperature: float = 0.7,
        enabled_tools: Optional[List[str]] = None,
        run_id: Optional[str] = None,
        **state: Any,
    ) -> "AgentRun":
        """Open an agent run with a fixed scope and policy snapshot."""

        if not user_name or not project_id or not session_id:
            raise ValueError("AgentRun requires user, project, and session scope")
        return cls(
            run_id=run_id or str(uuid4()),
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            user_query=user_query,
            agent=agent,
            model=model,
            temperature=temperature,
            enabled_tools=tuple(enabled_tools) if enabled_tools is not None else None,
            limits=limits,
            **state,
        )

    @property
    def scope(self) -> ExecutionScope:
        """Compatibility view for scope-aware tools and helper functions."""

        return ExecutionScope(
            user_name=self.user_name,
            project_id=self.project_id,
            session_id=self.session_id,
        )

    def _require_active(self) -> None:
        if self.released:
            raise RuntimeError("AgentRun has been released")
        if self.sealed:
            raise RuntimeError("AgentRun has been finalized")

    def begin_attempt(self) -> bool:
        """Reserve the next LLM attempt if this run has capacity remaining."""

        self._require_active()
        if self.attempt_count >= self.limits.max_attempts:
            return False
        self.attempt_count += 1
        return True

    def is_duplicate(self, tool_name: str, args: Dict) -> bool:
        call_sig = (tool_name, json.dumps(args, sort_keys=True, default=str))
        return call_sig in self.previous_calls

    def tool_limit_reached(self, tool_name: str, config: Any = None) -> bool:
        del config
        limit = self.limits.get_tool_limit(tool_name, self.limits.max_calls)
        return self.tool_call_counts.get(tool_name, 0) >= limit

    def can_call_tool(self, tool_name: str, args: Dict) -> bool:
        return (
            not self.released
            and not self.sealed
            and self.call_count < self.limits.max_calls
            and not self.tool_limit_reached(tool_name)
            and not self.is_duplicate(tool_name, args)
        )

    def record_tool_call(self, tool_name: str, args: Dict) -> None:
        self._require_active()
        if not self.can_call_tool(tool_name, args):
            raise ValueError(f"Tool call is not permitted: {tool_name}")
        call_sig = (tool_name, json.dumps(args, sort_keys=True, default=str))
        self.previous_calls.add(call_sig)
        self.call_count += 1
        self.tools_used.append(tool_name)
        self.tool_call_counts[tool_name] = self.tool_call_counts.get(tool_name, 0) + 1

    def record_error(self, message: str) -> None:
        self._require_active()
        self.last_error = str(message)
        self.consecutive_errors += 1

    def note_nonfatal_error(self, message: str) -> None:
        """Expose a rejected action to the next model turn without ending it."""

        self._require_active()
        self.last_error = str(message)

    def clear_last_error(self) -> None:
        self._require_active()
        self.last_error = None

    def record_tool_success(self) -> None:
        self._require_active()
        self.consecutive_errors = 0
        self.last_error = None

    def record_tool_result(self, result: Dict) -> None:
        self._require_active()
        if result.get("error"):
            self.record_error(str(result["error"]))
            return
        self.consecutive_errors = 0
        self.last_error = None

    def record_source(self, candidate: SourceReferenceCandidate) -> None:
        self._require_active()
        self.source_candidates.append(candidate)

    def record_sources(self, candidates: List[SourceReferenceCandidate]) -> None:
        self._require_active()
        self.source_candidates.extend(candidates)

    def accumulate_tool_result(self, tool_name: str, result: Dict) -> None:
        """Apply one tool result to the aggregate's owned evidence buffers."""

        self._require_active()
        # Kept as a local import to avoid the run/internals import cycle.
        from core.agent.prompt_context import update_accumulators

        update_accumulators(self, tool_name, result)

    def record_empty_result(self) -> bool:
        """Record an empty tool turn and report whether replanning is due."""

        self._require_active()
        self.consecutive_empty_results += 1
        return (
            self.consecutive_empty_results
            >= self.limits.empty_result_replan_threshold
        )

    def clear_empty_results(self) -> None:
        self._require_active()
        self.consecutive_empty_results = 0

    def has_any(self) -> bool:
        """Whether this run has accumulated any model-visible evidence."""

        return bool(
            self.profiles
            or self.messages
            or self.graph
            or self.paths
            or self.episodes
            or self.sources
            or self.evidence_summary
        )

    def record_usage(self, usage: Optional[StreamUsage]) -> None:
        self._require_active()
        if not usage:
            return
        for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
            self.usage[key] += usage.get(key, 0)
        self.usage["approximate"] = self.usage["approximate"] or usage.get(
            "approximate", False
        )

    def clear_short_uuid_references(self) -> None:
        self.short_uuid_references.clear()

    def set_evidence_token_count(self, token_count: int) -> None:
        self._require_active()
        if not isinstance(token_count, int) or token_count < 0:
            raise ValueError("evidence token count must be a non-negative integer")
        self.evidence_token_count = token_count

    def compact_evidence(self, summary: Optional[str]) -> None:
        """Keep only the bounded evidence needed after summarization."""

        self._require_active()
        if summary:
            self.evidence_summary = summary
        self.messages = self.messages[-5:]
        self.profiles = self.profiles[-5:]
        self.graph = self.graph[-15:]
        self.episodes = []
        self.paths = []

    def finalize(self, content: str) -> None:
        self._require_active()
        if not isinstance(content, str) or not content.strip():
            raise ValueError("AgentRun final content must be a non-empty string")
        self.final_content = content
        self.sealed = True

    def finish_without_response(self) -> None:
        """Seal an execution that ended in clarification or terminal error."""

        if self.released:
            raise RuntimeError("AgentRun has been released")
        self.sealed = True

    def release(self) -> None:
        """Discard model-only, ephemeral state once no consumer needs this run."""

        if self.released:
            return
        self.short_uuid_references.clear()
        self.history.clear()
        self.initial_source_candidates.clear()
        self.maintenance_candidates.clear()
        self.messages.clear()
        self.profiles.clear()
        self.graph.clear()
        self.paths.clear()
        self.episodes.clear()
        self.sources.clear()
        self.source_candidates.clear()
        self.evidence_summary = None
        self.released = True
