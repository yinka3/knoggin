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
    PromptMemoryBudget,
    validate_tool_limit_overrides,
)
from common.schema.agent.stream import StreamUsage
from common.schema.agent.tool_names import (
    RESEARCH_ONLY_TOOL_NAMES,
    expand_tool_groups,
    research_run_blocked_tool_names,
)
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


_FINDING_STRENGTHS = ("supported", "partial", "contested")
_RECORD_TEXT_FIELDS = (
    "content", "message", "excerpt", "snippet", "summary", "description", "text",
)


_MARKDOWN_LINK = re.compile(r"!?\[([^\]]*)\]\([^)]*\)")
_MARKDOWN_EMPHASIS = re.compile(r"\*\*|__|`")


def _normalized_text(value: str) -> str:
    """Text as a reader sees it: pages are stored as markdown, and models
    quote a link's words without its URL."""

    value = _MARKDOWN_EMPHASIS.sub("", _MARKDOWN_LINK.sub(r"\1", value))
    return " ".join(value.split()).casefold()


def _quote_is_in(quote: str, text: str) -> bool:
    """Whether a quote appears in text. A quote joined with an ellipsis must
    have each of its pieces there."""

    haystack = _normalized_text(text)
    pieces = [
        _normalized_text(piece) for piece in re.split(r"\.\.\.|…", quote)
        if piece.strip(" .,;:")
    ]
    return bool(pieces) and all(piece.strip(" .,;:") in haystack for piece in pieces)


def _record_text(record: Dict[str, Any]) -> str:
    """The readable text of one notebook record, where a quote must appear."""

    parts = [str(record[key]) for key in _RECORD_TEXT_FIELDS if isinstance(record.get(key), str)]
    for item in record.get("context") or ():
        if isinstance(item, dict) and isinstance(item.get("content"), str):
            parts.append(item["content"])
    return "\n".join(parts)


def _record_lines(record: Dict[str, Any]) -> str:
    start, end = record.get("start_line"), record.get("end_line")
    if type(start) is int and type(end) is int:
        return f"lines {start}-{end}"
    return ""


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
    max_passage_chars: int = 12_000
    max_research_findings: int = 30
    max_notebook_render_tokens: int = 10000
    max_consecutive_errors: int = 3
    project_briefing_mode: ProjectBriefingMode = "adaptive"
    empty_result_replan_threshold: int = 3
    empty_replans_before_answer: int = 2
    tool_timeout: float = 30.0
    tool_limits: Tuple[Tuple[str, int], ...] = field(
        default_factory=lambda: tuple(get_default_tool_limits().items())
    )

    @classmethod
    def from_settings(
        cls, settings: Any, memory_budget: PromptMemoryBudget | None = None
    ) -> "AgentRunLimits":
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
            max_history_turns=(memory_budget or PromptMemoryBudget()).input_tokens,
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
            max_passage_chars=settings.max_passage_chars,
            max_research_findings=settings.max_research_findings,
            max_notebook_render_tokens=(memory_budget or PromptMemoryBudget()).input_tokens,
            max_consecutive_errors=settings.max_consecutive_errors,
            empty_replans_before_answer=settings.empty_replans_before_answer,
            project_briefing_mode=briefing_mode,
            tool_limits=tuple((defaults | overrides).items()),
        )

    def with_extra_web_reads(self, count: int) -> "AgentRunLimits":
        """Add room for a given number of page reads, one call each."""

        if count <= 0:
            return self
        return replace(
            self,
            max_calls=self.max_calls + count,
            max_attempts=self.max_attempts + count,
            max_accumulated_web_reads=self.max_accumulated_web_reads + count,
            tool_limits=tuple(
                (name, limit + count if name == "read_web_page" else limit)
                for name, limit in self.tool_limits
            ),
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
    memory_budget: PromptMemoryBudget = field(default_factory=PromptMemoryBudget)
    research_profile: ResearchProfile = field(
        default_factory=lambda: DEFAULT_RESEARCH_PROFILES["normal"]
    )
    history: List[Dict] = field(default_factory=list)
    # Summary of this chat's turns that no longer fit the prompt.
    history_summary: str = ""
    # The last save_findings call: its requested path and result.
    saved_findings: Optional[Dict[str, Any]] = None
    document_focus: Optional[DocumentFocus] = None
    document_selection_context: Optional[Dict[str, Any]] = None
    selected_notebook: Optional[Dict[str, Any]] = None
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
    research_subquestions: Tuple[str, ...] = ()
    findings: List[Dict[str, Any]] = field(default_factory=list)
    consecutive_errors: int = 0
    consecutive_empty_results: int = 0
    # Forced replans in a row that found nothing new.
    empty_replans: int = 0
    empty_retrieval_signatures: Set[Tuple[str, str]] = field(default_factory=set)
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
        history_summary: str = "",
        document_focus: Optional[DocumentFocus] = None,
        document_selection_context: Optional[Dict[str, Any]] = None,
        selected_notebook: Optional[Dict[str, Any]] = None,
        memory_budget: PromptMemoryBudget | None = None,
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
            expand_tool_groups(enabled_tools) if enabled_tools is not None else None
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
        effective_research_profile = (
            research_profile or DEFAULT_RESEARCH_PROFILES["normal"]
        )
        research = effective_research_profile.mode != "normal"
        if research and effective_enabled_tools is not None:
            # Choosing research mode is how a chat without web tools gets them.
            effective_enabled_tools = expand_tool_groups((*effective_enabled_tools, "web"))
        # Research gathers and records findings; a confused measured run
        # rewrote its own Brain, and others started jobs of themselves.
        blocked = (
            research_run_blocked_tool_names() if research else RESEARCH_ONLY_TOOL_NAMES
        )
        if blocked:
            effective_enabled_tools = tuple(
                name for name in (
                    effective_enabled_tools
                    if effective_enabled_tools is not None
                    else sorted(get_registered_tool_names())
                )
                if name not in blocked
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
            max_graph_results=limits.max_accumulated_graph,
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
            memory_budget=memory_budget or PromptMemoryBudget(),
            research_profile=effective_research_profile,
            history=list(history or []),
            history_summary=history_summary,
            document_focus=document_focus,
            document_selection_context=document_selection_context,
            selected_notebook=selected_notebook,
            project_briefing=ProjectBriefing.for_run(
                mode=limits.project_briefing_mode,
                user_query=user_query,
                research_profile=effective_research_profile,
                document_focus=document_focus,
                document_selection_context=document_selection_context,
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

    def set_research_plan(self, subquestions: object) -> str | None:
        """Freeze the material questions that final research must cover."""

        if self.research_profile.mode == "normal":
            return "A research plan is only valid in a research mode."
        if self.research_subquestions:
            return "The research plan has already been set."
        if (
            not isinstance(subquestions, list)
            or not subquestions
            or len(subquestions) > 12
            or any(not isinstance(item, str) or not item.strip() for item in subquestions)
        ):
            return "A research plan requires 1-12 non-empty material subquestions."
        normalized = tuple(" ".join(item.split()) for item in subquestions)
        if len({item.casefold() for item in normalized}) != len(normalized):
            return "Research plan subquestions must be unique."
        self.research_subquestions = normalized
        return None

    def record_finding(
        self, *, subquestion: object, claim: object, sources: object,
        strength: object = "supported", gap: object = None,
    ) -> Dict[str, Any] | str:
        """Record one finding backed by quotes from sources this run read.

        Returns the stored finding, or the reason it was refused. Each finding
        copies its quotes and source details, so it stays meaningful after the
        notebook rolls over.
        """

        if self.research_profile.mode == "normal":
            return "record_finding is only available in research runs."
        if not self.research_subquestions:
            return "Set the research plan before recording findings."
        question = self._planned_subquestion(subquestion)
        if question is None:
            return (
                "subquestion must be one of the research plan's subquestions, "
                "by its exact text or its number."
            )
        if not isinstance(claim, str) or not claim.strip() or len(claim) > 600:
            return "claim must be one finding in 1 to 600 characters."
        if strength not in _FINDING_STRENGTHS:
            return "strength must be supported, partial, or contested."
        if gap is not None and (not isinstance(gap, str) or len(gap) > 600):
            return "gap must be text of at most 600 characters."
        if not isinstance(sources, list) or not 1 <= len(sources) <= 3:
            return "A finding needs 1 to 3 sources, each with a short exact quote."
        if len(self.findings) >= self.limits.max_research_findings:
            return (
                f"This run already has {len(self.findings)} findings, its limit "
                "(limits.max_research_findings)."
            )
        resolved = []
        for source in sources:
            reference = source.get("reference") if isinstance(source, dict) else None
            quote = source.get("quote") if isinstance(source, dict) else None
            if not isinstance(reference, str) or not reference.strip():
                return "Each source needs the notebook reference it came from, such as WR1."
            if not isinstance(quote, str) or not quote.strip() or len(quote) > 500:
                return f"The quote for {reference} must be 1 to 500 characters."
            # "WR2, WR3" names several read sources; the quote must be in one.
            candidates = []
            for name in (part.strip() for part in reference.split(",") if part.strip()):
                candidate = self.notebook.research_record(name)
                if candidate is None:
                    return (
                        f"{name} is not a page, document, or memory this run "
                        "has read; search snippets do not count."
                    )
                candidates.append((name, candidate))
            if not candidates:
                return "Each source needs the notebook reference it came from, such as WR1."
            match = next(
                (pair for pair in candidates if _quote_is_in(quote, _record_text(pair[1]))),
                None,
            )
            if match is None:
                return (
                    f"The quote for {reference} is not in what was read from it; "
                    "copy a short exact line from the passage."
                )
            reference, record = match
            resolved.append({
                "reference": reference.strip(),
                "title": str(
                    record.get("title") or record.get("document_name")
                    or record.get("original_name") or ""
                ),
                "location": str(
                    record.get("url") or record.get("relative_path")
                    or record.get("canonical_url") or ""
                ),
                "lines": _record_lines(record),
                "quote": " ".join(quote.split()),
            })
        finding = {
            "id": f"F{len(self.findings) + 1}",
            "subquestion": question,
            "claim": " ".join(claim.split()),
            "strength": strength,
            "sources": resolved,
            "gap": " ".join(gap.split()) if isinstance(gap, str) and gap.strip() else None,
        }
        self.findings.append(finding)
        return finding

    def _planned_subquestion(self, value: object) -> str | None:
        """Match a subquestion by number ("2"), text, or both ("2. When ...")."""

        if isinstance(value, int) and not isinstance(value, bool):
            number, text = value, ""
        else:
            match = re.fullmatch(r"\s*(\d+)?[.)]?\s*(.*?)\s*", str(value), re.DOTALL)
            number = int(match.group(1)) if match and match.group(1) else None
            text = " ".join(match.group(2).split()).casefold() if match else ""
        if number is not None:
            if not 1 <= number <= len(self.research_subquestions):
                return None
            question = self.research_subquestions[number - 1]
            return question if not text or text == question.casefold() else None
        return next(
            (q for q in self.research_subquestions if q.casefold() == text), None
        )

    def coverage_from_findings(self) -> List[Dict[str, Any]]:
        """research_coverage built from the recorded findings, one row per
        planned subquestion; one with no finding is reported as a gap."""

        coverage = []
        for question in self.research_subquestions:
            own = [finding for finding in self.findings if finding["subquestion"] == question]
            references = list(dict.fromkeys(
                source["reference"] for finding in own for source in finding["sources"]
            ))
            gaps = [finding["gap"] for finding in own if finding["gap"]]
            row: Dict[str, Any] = {"subquestion": question, "supporting_references": references}
            if gaps or not references:
                row["unresolved_gap"] = "; ".join(gaps) or "No finding was recorded."
            coverage.append(row)
        return coverage

    def validate_research_coverage(self, coverage: object) -> str | None:
        """Validate material research questions against admitted notebook evidence."""

        if self.research_profile.mode == "normal":
            return None
        if not self.research_subquestions:
            return "Research completion requires a plan set before investigation."
        if not isinstance(coverage, list) or not coverage or len(coverage) > 12:
            return "Research completion requires 1-12 material subquestions."
        covered_questions: set[str] = set()
        for item in coverage:
            if not isinstance(item, dict):
                return "Each research coverage entry must be an object."
            question = item.get("subquestion")
            references = item.get("supporting_references")
            gap = item.get("unresolved_gap")
            if not isinstance(question, str) or not question.strip():
                return "Each research coverage entry requires a subquestion."
            normalized_question = " ".join(question.split()).casefold()
            if normalized_question in covered_questions:
                return "Research coverage subquestions must be unique."
            covered_questions.add(normalized_question)
            if not isinstance(references, list) or any(
                not isinstance(reference, str) for reference in references
            ):
                return "Research supporting_references must be notebook references."
            if not references and (not isinstance(gap, str) or not gap.strip()):
                return "Every material subquestion needs evidence or an unresolved gap."
            finding_ids = {finding["id"] for finding in self.findings}
            invalid = [
                reference
                for reference in references
                # A recorded finding is already backed by checked quotes.
                if reference.strip() not in finding_ids
                and not self.notebook.validates_research_reference(reference)
            ]
            if invalid:
                return (
                    "Research coverage contains unknown or discovery-only references: "
                    + ", ".join(invalid[:3])
                )
        expected_questions = {
            question.casefold() for question in self.research_subquestions
        }
        if covered_questions != expected_questions:
            missing = [
                question
                for question in self.research_subquestions
                if question.casefold() not in covered_questions
            ]
            if missing:
                return "Research coverage is missing planned subquestions: " + ", ".join(
                    missing[:3]
                )
            return "Research coverage contains subquestions outside the frozen plan."
        return None

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
        if candidate not in self.source_candidates:
            self.source_candidates.append(candidate)

    def record_sources(self, candidates: List[SourceReferenceCandidate]) -> None:
        self._require_active()
        for candidate in candidates:
            self.record_source(candidate)

    def accumulate_tool_result(
        self, tool_name: str, result: Dict
    ) -> NotebookApplyResult:
        """Apply one tool result and retain its explicit notebook decision."""

        self._require_active()
        apply_result = self.notebook.apply(tool_name, result)
        self.new_evidence_gathered = self.new_evidence_gathered or apply_result.changed
        return apply_result

    def record_empty_result(self, calls: List[Tuple[str, Dict]] | None = None) -> bool:
        """Record an empty tool turn and report whether replanning is due."""

        self._require_active()
        self.consecutive_empty_results += 1
        repeated_surface = False
        for tool_name, args in calls or []:
            query = args.get("query")
            if not isinstance(query, str):
                continue
            normalized = " ".join(re.findall(r"[a-z0-9]+", query.casefold()))
            signature = (tool_name, normalized)
            repeated_surface = repeated_surface or signature in self.empty_retrieval_signatures
            self.empty_retrieval_signatures.add(signature)
        return (
            repeated_surface
            or
            self.consecutive_empty_results >= self.limits.empty_result_replan_threshold
        )

    def clear_empty_results(self) -> None:
        self._require_active()
        self.consecutive_empty_results = 0
        self.empty_retrieval_signatures.clear()

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
        self.selected_notebook = None
        self.project_briefing.clear_content()
        self.notebook.clear()
        self.source_candidates.clear()
        self.released = True
