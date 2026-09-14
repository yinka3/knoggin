"""Workflow-owned state for one agent execution.

``AgentRun`` keeps live agent state together for the duration of a single
reasoning loop.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, cast
from uuid import uuid4

from common.schema.agent.identity import AgentConfig
from common.schema.agent.research import (
    DEFAULT_RESEARCH_PROFILES,
    ResearchProfile,
)
from common.schema.agent.settings import (
    ProjectBriefingMode,
    validate_tool_limit_overrides,
)
from common.schema.agent.stream import StreamUsage
from common.schema.document import DocumentFocus
from common.schema.source.references import SourceReferenceCandidate
from core.agent.notebook import (
    NotebookApplyResult,
    NotebookRolloverResult,
    RunNotebook,
)
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

_PROJECT_MEMORY_CUES = (
    "project",
    "memory",
    "remember",
    "remind",
    "previous",
    "earlier",
    "history",
    "decision",
    "decided",
    "context",
    "codebase",
    "repository",
    "repo",
    "document",
    "docs",
    "file",
    "folder",
    "note",
    "we discussed",
    "we talked",
    "last time",
)
_SIMPLE_CONVERSATIONAL_TURNS = frozenset(
    {
        "hey",
        "hi",
        "hello",
        "yo",
        "good morning",
        "good afternoon",
        "good evening",
        "nice",
        "cool",
        "great",
        "awesome",
        "okay",
        "ok",
        "sounds good",
        "got it",
        "gotcha",
        "thanks",
        "thank you",
        "yeah",
        "yep",
        "yes",
        "sure",
        "continue",
        "keep going",
        "next",
        "next one",
        "go next",
        "go to next one",
        "go to the next one",
    }
)
_CONTINUATION_TURN_RE = re.compile(
    r"(?:nice|okay|ok|cool|great|awesome|sounds good|yeah|yep|yes|sure)?"
    r"\s*(?:go\s+to\s+)?(?:the\s+)?next(?:\s+one)?"
)


def _normalized_turn_text(value: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", value.lower()))


def _has_explicit_project_memory_intent(user_query: str) -> bool:
    normalized = _normalized_turn_text(user_query)
    padded = f" {normalized} "
    return any(f" {cue} " in padded for cue in _PROJECT_MEMORY_CUES)


def _is_simple_conversational_turn(user_query: str) -> bool:
    normalized = _normalized_turn_text(user_query)
    return (
        normalized in _SIMPLE_CONVERSATIONAL_TURNS
        or _CONTINUATION_TURN_RE.fullmatch(normalized) is not None
    )


@dataclass(slots=True)
class ProjectBriefing:
    """Frozen briefing decision plus one run-local cached Project projection."""

    mode: ProjectBriefingMode
    initial_reason: Optional[str]
    requested_reason: Optional[str]
    loaded: bool = False
    load_count: int = 0
    transition_count: int = 0
    content_token_count: int = 0
    brief: str = ""
    context: str = ""
    documents_context: str = ""

    @classmethod
    def for_run(
        cls,
        *,
        mode: ProjectBriefingMode,
        user_query: str,
        research_profile: ResearchProfile,
        document_focus: Optional[DocumentFocus],
        document_selection_context: Optional[Dict[str, Any]],
        hot_topics: List[str],
        hot_topic_context: Dict[str, Dict],
    ) -> "ProjectBriefing":
        if mode not in {"always", "adaptive"}:
            raise ValueError("project briefing mode must be 'always' or 'adaptive'")
        if mode == "always":
            reason: Optional[str] = "always"
        elif research_profile.mode != "normal":
            reason = "research_mode"
        elif document_selection_context is not None:
            reason = "document_selection"
        elif document_focus is not None:
            reason = "document_focus"
        elif hot_topics or hot_topic_context:
            reason = "hot_topic_preload"
        elif _has_explicit_project_memory_intent(user_query):
            reason = "explicit_project_memory_intent"
        elif _is_simple_conversational_turn(user_query):
            reason = None
        else:
            # The fast path is intentionally small. A prompt not recognized as
            # conversational retains the Project briefing rather than guessing.
            reason = "conservative_default"
        return cls(mode=mode, initial_reason=reason, requested_reason=reason)

    @property
    def needs_load(self) -> bool:
        return self.requested_reason is not None and not self.loaded

    def request_transition(self, reason: str) -> bool:
        """Request one later briefing load after the fast path proves insufficient."""

        if self.loaded or self.requested_reason is not None:
            return False
        self.requested_reason = reason
        self.transition_count += 1
        return True

    def record_loaded(
        self,
        *,
        brief: str,
        context: str,
        documents_context: str = "",
        content_token_count: int,
    ) -> None:
        if self.loaded:
            return
        self.brief = brief
        self.context = context
        self.documents_context = documents_context
        self.content_token_count = content_token_count
        self.loaded = True
        self.load_count += 1

    def clear_content(self) -> None:
        self.brief = ""
        self.context = ""
        self.documents_context = ""


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
    max_accumulated_documents: int = 30
    max_accumulated_web_discoveries: int = 12
    max_accumulated_web_reads: int = 12
    max_accumulated_actions: int = 12
    max_accumulated_next_steps: int = 12
    max_accumulated_summary_chars: int = 4000
    max_notebook_render_tokens: int = 10000
    max_consecutive_errors: int = 3
    project_briefing_mode: ProjectBriefingMode = "adaptive"
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
        briefing_mode = cast(
            ProjectBriefingMode,
            getattr(settings, "project_briefing_mode", "adaptive"),
        )
        if briefing_mode not in {"always", "adaptive"}:
            raise ValueError("project_briefing_mode must be 'always' or 'adaptive'")
        validate_tool_limit_overrides(settings, get_registered_tool_names())
        return cls(
            max_calls=settings.max_tool_calls,
            tool_timeout=settings.tool_timeout,
            max_attempts=settings.max_attempts,
            max_history_turns=settings.agent_history_turns,
            max_accumulated_messages=settings.max_accumulated_messages,
            max_accumulated_profiles=settings.max_accumulated_profiles,
            max_accumulated_graph=settings.max_accumulated_graph,
            max_accumulated_paths=settings.max_accumulated_paths,
            max_accumulated_episodes=settings.max_accumulated_episodes,
            max_accumulated_documents=settings.max_accumulated_documents,
            max_accumulated_web_discoveries=settings.max_accumulated_web_discoveries,
            max_accumulated_web_reads=settings.max_accumulated_web_reads,
            max_accumulated_actions=settings.max_accumulated_actions,
            max_accumulated_next_steps=settings.max_accumulated_next_steps,
            max_accumulated_summary_chars=settings.max_accumulated_summary_chars,
            max_notebook_render_tokens=settings.max_notebook_render_tokens,
            max_consecutive_errors=settings.max_consecutive_errors,
            project_briefing_mode=briefing_mode,
            tool_limits=tuple((defaults | overrides).items()),
        )

    def for_research_profile(self, profile: ResearchProfile) -> "AgentRunLimits":
        """Scale the existing executor budget for a selected research mode."""

        return replace(
            self,
            max_calls=self.max_calls * profile.tool_call_budget_multiplier,
            max_attempts=self.max_attempts * profile.attempt_budget_multiplier,
            max_accumulated_web_discoveries=(
                self.max_accumulated_web_discoveries * profile.source_budget_multiplier
            ),
            max_accumulated_web_reads=(
                self.max_accumulated_web_reads * profile.source_budget_multiplier
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
    project_briefing: ProjectBriefing = field(
        default_factory=lambda: ProjectBriefing(
            mode="adaptive",
            initial_reason=None,
            requested_reason=None,
        )
    )
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
    deep_research_gap_review_count: int = 0
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
        effective_initial_source_candidates = list(initial_source_candidates or [])
        if not all(
            isinstance(candidate, SourceReferenceCandidate)
            for candidate in effective_initial_source_candidates
        ):
            raise TypeError(
                "initial_source_candidates must contain validated source references"
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
        effective_research_profile = (
            research_profile or DEFAULT_RESEARCH_PROFILES["normal"]
        )
        effective_hot_topics = list(hot_topics or [])
        effective_hot_topic_context = dict(hot_topic_context or {})
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
            research_profile=effective_research_profile,
            history=list(history or []),
            document_focus=document_focus,
            document_selection_context=document_selection_context,
            hot_topics=effective_hot_topics,
            hot_topic_context=effective_hot_topic_context,
            project_briefing=ProjectBriefing.for_run(
                mode=limits.project_briefing_mode,
                user_query=user_query,
                research_profile=effective_research_profile,
                document_focus=document_focus,
                document_selection_context=document_selection_context,
                hot_topics=effective_hot_topics,
                hot_topic_context=effective_hot_topic_context,
            ),
            notebook=notebook or RunNotebook(limits=limits),
            is_community=is_community,
            current_participants=list(current_participants or []),
            last_turn_at=effective_last_turn_at,
            initial_source_candidates=effective_initial_source_candidates,
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

    def has_grounded_investigation_evidence(self) -> bool:
        """Whether this run has evidence sufficient to finalize research work."""

        return bool(
            self.initial_source_candidates or self.notebook.has_admitted_evidence()
        )

    def needs_deep_research_gap_review(self) -> bool:
        """Whether the deep-research second-look checkpoint remains due."""

        return (
            self.research_profile.mode == "deep_research"
            and self.has_grounded_investigation_evidence()
            and not self.has_completed_deep_research_gap_review()
        )

    def has_completed_deep_research_gap_review(self) -> bool:
        """Whether this run has completed its one required second-look pass."""

        return self.deep_research_gap_review_count >= 1

    def begin_deep_research_gap_review(self) -> bool:
        """Reserve the one executor-owned deep-research review pass."""

        self._require_active()
        if not self.needs_deep_research_gap_review():
            return False
        self.deep_research_gap_review_count += 1
        self.attempt_count += 1
        return True

    def is_duplicate(self, tool_name: str, args: Dict) -> bool:
        call_sig = (tool_name, json.dumps(args, sort_keys=True, default=str))
        return call_sig in self.previous_calls

    def tool_limit_reached(self, tool_name: str) -> bool:
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

    def accumulate_tool_result(
        self, tool_name: str, result: Dict
    ) -> NotebookApplyResult:
        """Apply one tool result and retain its explicit notebook decision."""

        self._require_active()
        apply_result = self.notebook.apply(tool_name, result)
        self.new_evidence_gathered = self.new_evidence_gathered or apply_result.changed
        return apply_result

    def record_empty_result(self) -> bool:
        """Record an empty tool turn and report whether replanning is due."""

        self._require_active()
        self.consecutive_empty_results += 1
        return (
            self.consecutive_empty_results >= self.limits.empty_result_replan_threshold
        )

    def clear_empty_results(self) -> None:
        self._require_active()
        self.consecutive_empty_results = 0

    def request_project_briefing_transition(self) -> bool:
        """Request the one deferred Project load after a fast-path tool turn."""

        self._require_active()
        return self.project_briefing.request_transition("tool_followup")

    def record_project_briefing_loaded(
        self,
        *,
        brief: str,
        context: str,
        documents_context: str = "",
        content_token_count: int,
    ) -> None:
        """Cache the Project material after its single load attempt for this run."""

        self._require_active()
        self.project_briefing.record_loaded(
            brief=brief,
            context=context,
            documents_context=documents_context,
            content_token_count=content_token_count,
        )

    def has_any(self) -> bool:
        """Whether this run has accumulated any model-visible evidence."""

        return self.notebook.has_any()

    def record_usage(self, usage: Optional[StreamUsage]) -> None:
        self._require_active()
        if not usage:
            return
        for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
            self.usage[key] += usage.get(key, 0)
        self.usage["approximate"] = self.usage["approximate"] or usage.get(
            "approximate", False
        )

    def set_evidence_token_count(self, token_count: int) -> None:
        self._require_active()
        if not isinstance(token_count, int) or token_count < 0:
            raise ValueError("evidence token count must be a non-negative integer")
        self.evidence_token_count = token_count

    def rollover_notebook(
        self, summary: Optional[str] = None
    ) -> NotebookRolloverResult:
        """Start a bounded notebook generation while preserving references."""

        self._require_active()
        return self.notebook.rollover(summary)

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
        self.project_briefing.clear_content()
        self.notebook.clear()
        self.source_candidates.clear()
        self.released = True
