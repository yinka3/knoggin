"""Versioned application-facing contracts.

The server's workflow objects and the existing agent stream dictionaries are
internal implementation details.  This module is the small, deliberately
boring boundary that an HTTP, SSE, or SDK adapter can depend on.  It does not
instantiate a session or know about persistence; adapters translate to and
from these models.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator

from common.exceptions import (
    ConfigurationError,
    DependencyError,
    LLMProviderError,
    LLMResponseError,
    NotFoundError,
    SessionBusyError,
    StorageError,
    ToolExecutionError,
)
from common.schema.agent.research import ResearchMode
from common.schema.artifacts import ArtifactBlock, ArtifactKind, ArtifactStatus
from common.schema.document import DocumentSelection
from common.schema.source.references import SourceConsulted

PUBLIC_CONTRACT_VERSION = "1"
EnabledToolsUpdateMode = Literal["omitted", "inherit", "disable_all", "allowlist"]


def _normalise_enabled_tools(value: list[str] | None) -> list[str] | None:
    """Validate and canonicalise the public tool allow-list.

    ``None`` means inherit/default and ``[]`` intentionally means disable all.
    The application layer performs registration checks after it resolves the
    target agent; this boundary only guarantees a deterministic shape.
    """

    if value is None:
        return None
    result: list[str] = []
    seen: set[str] = set()
    for name in value:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("enabled_tools must contain non-blank names")
        canonical = name.strip().lower()
        if canonical in seen:
            raise ValueError(f"enabled_tools contains duplicate tool: {canonical}")
        seen.add(canonical)
        result.append(canonical)
    return result


class PublicModel(BaseModel):
    """Base model for public contracts: strict fields and immutable output."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class CreateProjectRequest(PublicModel):
    name: str = Field(min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=10_000)

    @field_validator("name")
    @classmethod
    def _reject_blank_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("value must not be blank")
        return value


class ProjectResponse(PublicModel):
    id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    description: str | None = None
    status: Literal["active", "archived", "deleted"]
    session_count: int = Field(default=0, ge=0)
    allowed_projects: tuple[str, ...] = ()
    created_at: datetime | None = None
    updated_at: datetime | None = None


class CreateSessionRequest(PublicModel):
    project_id: str = Field(min_length=1)
    model: str | None = Field(default=None, min_length=1)
    agent_id: str | None = Field(default=None, min_length=1)
    enabled_tools: list[str] | None = None

    _normalise_tools = field_validator("enabled_tools")(_normalise_enabled_tools)


class SessionResponse(PublicModel):
    session_id: str = Field(min_length=1)
    project_id: str = Field(min_length=1)
    status: Literal["open", "deleted"] = "open"
    model: str | None = None
    agent_id: str | None = None
    enabled_tools: tuple[str, ...] | None = None
    created_at: datetime | None = None
    last_active_at: datetime | None = None


class UpdateAgentRequest(PublicModel):
    """Partial agent update with an explicit omitted/null distinction."""

    name: str | None = Field(default=None, min_length=1, max_length=200)
    enabled_tools: list[str] | None = None

    _normalise_tools = field_validator("enabled_tools")(_normalise_enabled_tools)

    @property
    def enabled_tools_mode(self) -> EnabledToolsUpdateMode:
        if "enabled_tools" not in self.model_fields_set:
            return "omitted"
        if self.enabled_tools is None:
            return "inherit"
        if not self.enabled_tools:
            return "disable_all"
        return "allowlist"


class RunDocumentFocusDocument(PublicModel):
    """One document optionally anchored at a request-scoped selection."""

    target_type: Literal["document"]
    document_id: str = Field(min_length=1)
    selection: DocumentSelection | None = None


class RunDocumentFocusSubtree(PublicModel):
    """One request-scoped project-relative subtree."""

    target_type: Literal["subtree"]
    path_prefix: str = Field(min_length=1)


RunDocumentFocus = Annotated[
    Union[RunDocumentFocusDocument, RunDocumentFocusSubtree],
    Field(discriminator="target_type"),
]


class SetDocumentFocusDocument(PublicModel):
    """Persisted focus for one document, deliberately without a selection."""

    target_type: Literal["document"]
    document_id: str = Field(min_length=1)


class SetDocumentFocusSubtree(PublicModel):
    """Persisted focus for one project-relative subtree."""

    target_type: Literal["subtree"]
    path_prefix: str = Field(min_length=1)


SetDocumentFocusRequest = Annotated[
    Union[SetDocumentFocusDocument, SetDocumentFocusSubtree],
    Field(discriminator="target_type"),
]


class DocumentFocusResponse(PublicModel):
    """Stable public projection of the currently pinned session focus."""

    mode: Literal["pinned"]
    created_at: datetime
    target_type: Literal["document", "subtree"]
    document_id: str | None = None
    relative_path: str | None = None
    path_prefix: str | None = None


class StartRunRequest(PublicModel):
    session_id: str = Field(min_length=1)
    query: str = Field(min_length=1, max_length=100_000)
    model: str | None = Field(default=None, min_length=1)
    agent_id: str | None = Field(default=None, min_length=1)
    enabled_tools: list[str] | None = None
    research_mode: ResearchMode = "normal"
    document_focus: RunDocumentFocus | None = None

    _normalise_tools = field_validator("enabled_tools")(_normalise_enabled_tools)

    @field_validator("query")
    @classmethod
    def _reject_blank_query(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("query must not be blank")
        return value


class Usage(PublicModel):
    prompt_tokens: int = Field(ge=0)
    completion_tokens: int = Field(ge=0)
    total_tokens: int = Field(ge=0)
    approximate: bool = False


class ArtifactResponse(PublicModel):
    """Public identity/current-revision projection for one artifact."""

    artifact_id: str = Field(min_length=1)
    project_id: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    originating_message_id: int = Field(gt=0)
    kind: ArtifactKind
    title: str = Field(min_length=1, max_length=200)
    status: ArtifactStatus
    current_revision: int = Field(ge=1)
    created_at: datetime
    updated_at: datetime


class ArtifactRevisionResponse(PublicModel):
    """Public structured artifact revision with Markdown fallback."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=False,
    )

    artifact_id: str = Field(min_length=1)
    revision: int = Field(ge=1)
    schema_version: int = Field(ge=1)
    kind: ArtifactKind
    title: str = Field(min_length=1, max_length=200)
    blocks: tuple[ArtifactBlock, ...] = Field(min_length=1, max_length=50)
    status: ArtifactStatus
    markdown: str = Field(min_length=1, max_length=100_000)
    content_hash: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
    )
    created_at: datetime


class ArtifactListResponse(PublicModel):
    artifacts: tuple[ArtifactResponse, ...]


class RunResult(PublicModel):
    run_id: str = Field(min_length=1)
    content: str
    sources: tuple[SourceConsulted, ...] = ()
    usage: Usage | None = None
    research_mode: ResearchMode = "normal"
    assistant_message_id: int | None = Field(default=None, gt=0)
    source_ref_ids: tuple[str, ...] = ()
    artifact: ArtifactResponse | None = None


class PublicError(PublicModel):
    """Safe, stable error projection for HTTP responses and stream events."""

    code: str = Field(min_length=1, max_length=64, pattern=r"^[a-z0-9_]+$")
    message: str = Field(min_length=1, max_length=500)
    retryable: bool = False
    request_id: str | None = Field(default=None, min_length=1)
    run_id: str | None = Field(default=None, min_length=1)
    details: dict[str, Any] | None = None


_PUBLIC_ERROR_PROJECTIONS: dict[type[Exception], tuple[str, str, bool]] = {
    ConfigurationError: (
        "configuration_error",
        "The server configuration is invalid.",
        False,
    ),
    DependencyError: (
        "dependency_unavailable",
        "A required service is temporarily unavailable.",
        True,
    ),
    NotFoundError: (
        "not_found",
        "The requested resource was not found.",
        False,
    ),
    SessionBusyError: (
        "session_busy",
        "This session already has an active run.",
        False,
    ),
    StorageError: (
        "storage_unavailable",
        "Storage is temporarily unavailable.",
        True,
    ),
    LLMProviderError: (
        "model_unavailable",
        "The model provider is temporarily unavailable.",
        True,
    ),
    LLMResponseError: (
        "invalid_model_response",
        "The model returned an invalid response.",
        False,
    ),
    ToolExecutionError: (
        "tool_failed",
        "A tool could not complete the request.",
        True,
    ),
}


def to_public_error(
    error: Exception,
    *,
    request_id: str | None = None,
    run_id: str | None = None,
) -> PublicError:
    """Convert an internal exception without exposing details or stack text."""

    projection = next(
        (
            value
            for error_type, value in _PUBLIC_ERROR_PROJECTIONS.items()
            if isinstance(error, error_type)
        ),
        None,
    )
    if projection is None:
        code, message, retryable = (
            ("invalid_request", "The request is invalid.", False)
            if isinstance(error, ValueError)
            else ("internal_error", "The server could not complete the request.", False)
        )
    else:
        code, message, retryable = projection
    # KnogginError.details may contain SQL, URLs, credentials, or raw model
    # output.  Public adapters may add an explicitly safe request identifier,
    # but never forward internal exception details by default.
    return PublicError(
        code=code,
        message=message,
        retryable=retryable,
        request_id=request_id,
        run_id=run_id,
    )


class _StreamEvent(PublicModel):
    version: Literal[PUBLIC_CONTRACT_VERSION] = PUBLIC_CONTRACT_VERSION
    run_id: str = Field(min_length=1)
    sequence: int = Field(ge=0)
    timestamp: datetime


class RunStartedEvent(_StreamEvent):
    type: Literal["run.started"] = "run.started"


class MessageDeltaEvent(_StreamEvent):
    type: Literal["message.delta"] = "message.delta"
    content: str


class ToolStartedEvent(_StreamEvent):
    type: Literal["tool.started"] = "tool.started"
    tool_name: str = Field(min_length=1)


class ToolCompletedEvent(_StreamEvent):
    type: Literal["tool.completed"] = "tool.completed"
    tool_name: str = Field(min_length=1)
    succeeded: bool


class SourceAddedEvent(_StreamEvent):
    type: Literal["source.added"] = "source.added"
    source: SourceConsulted


class UsageUpdatedEvent(_StreamEvent):
    type: Literal["usage.updated"] = "usage.updated"
    usage: Usage


class RunCompletedEvent(_StreamEvent):
    type: Literal["run.completed"] = "run.completed"
    result: RunResult


class RunFailedEvent(_StreamEvent):
    type: Literal["run.failed"] = "run.failed"
    error: PublicError


class RunCancelledEvent(_StreamEvent):
    type: Literal["run.cancelled"] = "run.cancelled"
    reason: str | None = None


PublicStreamEvent = Annotated[
    Union[
        RunStartedEvent,
        MessageDeltaEvent,
        ToolStartedEvent,
        ToolCompletedEvent,
        SourceAddedEvent,
        UsageUpdatedEvent,
        RunCompletedEvent,
        RunFailedEvent,
        RunCancelledEvent,
    ],
    Field(discriminator="type"),
]

_public_stream_event_adapter = TypeAdapter(PublicStreamEvent)


def validate_public_stream_event(event: object) -> PublicStreamEvent:
    """Validate an event at the public adapter boundary."""

    return _public_stream_event_adapter.validate_python(event)


def validate_public_stream(
    events: Sequence[object],
    *,
    require_terminal: bool = False,
) -> tuple[PublicStreamEvent, ...]:
    """Validate ordering and one-run ownership for a complete public stream."""

    parsed = tuple(validate_public_stream_event(event) for event in events)
    if not parsed:
        if require_terminal:
            raise ValueError("public stream must contain a terminal event")
        return parsed

    run_id = parsed[0].run_id
    previous_sequence = -1
    terminal_count = 0
    for event in parsed:
        if event.run_id != run_id:
            raise ValueError("public stream events must belong to one run")
        if event.sequence <= previous_sequence:
            raise ValueError("public stream sequence must increase monotonically")
        previous_sequence = event.sequence
        if event.type in {"run.completed", "run.failed", "run.cancelled"}:
            terminal_count += 1
    if terminal_count > 1:
        raise ValueError("public stream must contain at most one terminal event")
    if require_terminal and terminal_count != 1:
        raise ValueError("public stream must end with one terminal event")
    if terminal_count == 1 and parsed[-1].type not in {
        "run.completed",
        "run.failed",
        "run.cancelled",
    }:
        raise ValueError("public stream events cannot follow a terminal event")
    return parsed
