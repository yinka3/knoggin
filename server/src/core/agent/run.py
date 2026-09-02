"""Workflow-owned state for one agent execution.

``AgentRun`` keeps live agent state together for the duration of a single
reasoning loop.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, cast
from uuid import uuid4

from common.schema.agent.identity import AgentConfig
from common.schema.agent.research import (
    DEFAULT_RESEARCH_PROFILES,
    ResearchProfile,
)
from common.schema.agent.settings import validate_tool_limit_overrides
from common.schema.agent.stream import StreamUsage
from common.schema.document import DocumentFocus
from common.schema.source.references import SourceReferenceCandidate
from core.agent.notebook import RunNotebook
from core.agent.tools.registry import (
    ToolRuntime,
    build_tool_runtime,
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


AAC_DIAGNOSTIC_PROJECT_ID = "__aac__"
_UNSET_AUDIT_PROJECT_ID = object()


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

    def for_research_profile(self, profile: ResearchProfile) -> "AgentRunLimits":
        """Scale the existing executor budget for a selected research mode."""

        return replace(
            self,
            max_calls=self.max_calls * profile.tool_call_budget_multiplier,
            max_attempts=self.max_attempts * profile.attempt_budget_multiplier,
            max_accumulated_sources=(
                self.max_accumulated_sources * profile.source_budget_multiplier
            ),
            tool_limits=tuple(
                (name, limit * profile.tool_call_budget_multiplier)
                for name, limit in self.tool_limits
            ),
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
    brain: str
    enabled_tools: Optional[Tuple[str, ...]]
    additional_tool_schemas: Tuple[Dict[str, Any], ...]
    tool_runtime: ToolRuntime
    limits: AgentRunLimits
    research_profile: ResearchProfile = field(
        default_factory=lambda: DEFAULT_RESEARCH_PROFILES["normal"]
    )
    history: List[Dict] = field(default_factory=list)
    document_focus: Optional[DocumentFocus] = None
    document_selection_context: Optional[Dict[str, Any]] = None
    hot_topics: List[str] = field(default_factory=list)
    hot_topic_context: Dict[str, Dict] = field(default_factory=dict)
    notebook: RunNotebook = field(default_factory=RunNotebook)
    is_community: bool = False
    current_participants: List[str] = field(default_factory=list)
    last_turn_at: Optional[datetime] = None
    initial_source_candidates: List[SourceReferenceCandidate] = field(
        default_factory=list
    )
    new_evidence_gathered: bool = False
    evidence_token_count: int = 0
    call_count: int = 0
    attempt_count: int = 0
    synthesis_attempt_count: int = 0
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
        brain: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
        additional_tool_schemas: Optional[List[Dict[str, Any]]] = None,
        run_id: Optional[str] = None,
        audit_project_id: str | None | object = _UNSET_AUDIT_PROJECT_ID,
        research_profile: ResearchProfile | None = None,
        history: Optional[List[Dict]] = None,
        document_focus: Optional[DocumentFocus] = None,
        document_selection_context: Optional[Dict[str, Any]] = None,
        hot_topics: Optional[List[str]] = None,
        hot_topic_context: Optional[Dict[str, Dict]] = None,
        notebook: Optional[RunNotebook] = None,
        is_community: bool = False,
        current_participants: Optional[List[str]] = None,
        last_turn_at: Optional[datetime] = None,
        initial_source_candidates: Optional[List[SourceReferenceCandidate]] = None,
    ) -> "AgentRun":
        """Open an agent run with a fixed scope and policy snapshot."""

        if not user_name or not project_id or not session_id:
            raise ValueError("AgentRun requires user, project, and session scope")
        effective_run_id = run_id or str(uuid4())
        effective_enabled_tools = (
            tuple(enabled_tools) if enabled_tools is not None else None
        )
        effective_additional_schemas = tuple(additional_tool_schemas or ())
        effective_audit_project_id = (
            project_id
            if audit_project_id is _UNSET_AUDIT_PROJECT_ID
            else cast(str | None, audit_project_id)
        )
        effective_last_turn_at = (
            last_turn_at
            if last_turn_at is not None
            else getattr(agent.config, "last_turn_at", None)
        )
        tool_runtime = build_tool_runtime(
            enabled_tools=effective_enabled_tools,
            additional_schemas=effective_additional_schemas,
            user_name=user_name,
            agent_id=str(getattr(agent.config, "id", "") or ""),
            project_id=project_id,
            audit_project_id=effective_audit_project_id,
            session_id=session_id,
            run_id=effective_run_id,
        )
        return cls(
            run_id=effective_run_id,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            user_query=user_query,
            agent=agent,
            model=model,
            temperature=temperature,
            brain=brain or "",
            enabled_tools=effective_enabled_tools,
            additional_tool_schemas=effective_additional_schemas,
            tool_runtime=tool_runtime,
            limits=limits,
            research_profile=research_profile or DEFAULT_RESEARCH_PROFILES["normal"],
            history=list(history or []),
            document_focus=document_focus,
            document_selection_context=document_selection_context,
            hot_topics=list(hot_topics or []),
            hot_topic_context=dict(hot_topic_context or {}),
            notebook=notebook or RunNotebook(limits=limits),
            is_community=is_community,
            current_participants=list(current_participants or []),
            last_turn_at=effective_last_turn_at,
            initial_source_candidates=list(initial_source_candidates or []),
        )

    @classmethod
    def open_aac(
        cls,
        *,
        user_name: str,
        session_id: str,
        user_query: str,
        agent: AgentIdentity,
        limits: AgentRunLimits,
        model: Optional[str] = None,
        temperature: float = 0.7,
        brain: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
        additional_tool_schemas: Optional[List[Dict[str, Any]]] = None,
        run_id: Optional[str] = None,
        research_profile: ResearchProfile | None = None,
        history: Optional[List[Dict]] = None,
        document_focus: Optional[DocumentFocus] = None,
        document_selection_context: Optional[Dict[str, Any]] = None,
        hot_topics: Optional[List[str]] = None,
        hot_topic_context: Optional[Dict[str, Dict]] = None,
        notebook: Optional[RunNotebook] = None,
        is_community: bool = False,
        current_participants: Optional[List[str]] = None,
        last_turn_at: Optional[datetime] = None,
        initial_source_candidates: Optional[List[SourceReferenceCandidate]] = None,
    ) -> "AgentRun":
        """Open a user-level AAC run with no durable project audit owner."""

        return cls.open(
            user_name=user_name,
            project_id=AAC_DIAGNOSTIC_PROJECT_ID,
            session_id=session_id,
            user_query=user_query,
            agent=agent,
            limits=limits,
            model=model,
            temperature=temperature,
            brain=brain,
            enabled_tools=enabled_tools,
            additional_tool_schemas=additional_tool_schemas,
            run_id=run_id,
            audit_project_id=None,
            research_profile=research_profile,
            history=history,
            document_focus=document_focus,
            document_selection_context=document_selection_context,
            hot_topics=hot_topics,
            hot_topic_context=hot_topic_context,
            notebook=notebook,
            is_community=is_community,
            current_participants=current_participants,
            last_turn_at=last_turn_at,
            initial_source_candidates=initial_source_candidates,
        )

    def _require_active(self) -> None:
        if self.released:
            raise RuntimeError("AgentRun has been released")
        if self.sealed:
            raise RuntimeError("AgentRun has been finalized")

    @property
    def profiles(self):
        """Compatibility view backed by the canonical entity section."""

        return self.notebook.entities

    @profiles.setter
    def profiles(self, values) -> None:
        self.notebook._replace_section("entities", list(values or []))

    @property
    def messages(self):
        """Compatibility view backed by canonical message and document evidence."""

        values = []
        for item in self.notebook.messages:
            is_topic_compatibility_item = (
                "timestamp" in item
                and item.get("message") is not None
                and item.get("score") == 1.0
                and not item.get("user_name")
                and not item.get("session_id")
            )
            if (
                "context" not in item
                and "timestamp" in item
                and item.get("message") is not None
            ) or is_topic_compatibility_item:
                values.append(
                    {
                        "id": item.get("id", item.get("message_id")),
                        "score": item.get("score", 1.0),
                        "user_name": item.get("user_name"),
                        "session_id": item.get("session_id"),
                        "context": [
                            {
                                "role": item.get("role", "assistant"),
                                "timestamp": item.get("timestamp", ""),
                                "content": item.get("message", ""),
                                "is_hit": True,
                            }
                        ],
                    }
                )
            else:
                values.append(item)
        values.extend(self.notebook.model_view()["messages"][len(values) :])
        return values

    @messages.setter
    def messages(self, values) -> None:
        self.notebook._replace_section("messages", list(values or []))

    @property
    def graph(self):
        """Compatibility view backed by canonical relationship knowledge."""

        return self.notebook.relationships

    @graph.setter
    def graph(self, values) -> None:
        self.notebook._replace_section("relationships", list(values or []))

    @property
    def paths(self):
        return self.notebook.paths

    @paths.setter
    def paths(self, values) -> None:
        self.notebook._replace_section("paths", list(values or []))

    @property
    def episodes(self):
        return self.notebook.episodes

    @episodes.setter
    def episodes(self, values) -> None:
        self.notebook._replace_section("episodes", list(values or []))

    @property
    def sources(self):
        return list(self.notebook.web_discoveries) + list(self.notebook.web_reads)

    @sources.setter
    def sources(self, values) -> None:
        self.notebook._replace_section("web_discoveries", [])
        self.notebook._replace_section("web_reads", [])
        for value in values or []:
            if not isinstance(value, dict):
                continue
            section = (
                "web_reads"
                if value.get("source_kind") in {"web_page", "web_pdf"}
                else "web_discoveries"
            )
            self.notebook._upsert(section, value)

    @property
    def evidence_summary(self) -> Optional[str]:
        return self.notebook.summary.text

    @evidence_summary.setter
    def evidence_summary(self, value: Optional[str]) -> None:
        self.notebook.summary.text = value

    def begin_attempt(self) -> bool:
        """Reserve the next LLM attempt if this run has capacity remaining."""

        self._require_active()
        if self.attempt_count >= self.limits.max_attempts:
            return False
        self.attempt_count += 1
        return True

    def begin_final_synthesis_attempt(self) -> bool:
        """Reserve the one final synthesis pass outside the normal attempt budget."""

        self._require_active()
        if self.synthesis_attempt_count:
            return False
        self.synthesis_attempt_count += 1
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

    def accumulate_tool_result(self, tool_name: str, result: Dict) -> bool:
        """Apply one tool result to the aggregate's owned evidence buffers."""

        self._require_active()
        before = self._evidence_fingerprint()
        self.notebook.apply(tool_name, result)
        gathered = before != self._evidence_fingerprint()
        self.new_evidence_gathered = self.new_evidence_gathered or gathered
        return gathered

    def _evidence_fingerprint(self) -> str:
        """Serialize evidence state for change detection within one run."""

        return self.notebook.fingerprint()

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

        return self.notebook.has_any()

    def render_notebook(self) -> str:
        """Return the strict, localized prompt view of this run's notebook."""

        return self.notebook.render()

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
        self.messages = list(self.messages)[-5:]
        self.profiles = list(self.profiles)[-5:]
        self.graph = list(self.graph)[-15:]
        self.sources = self.sources[-5:]
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
        self.notebook.clear()
        self.source_candidates.clear()
        self.released = True
