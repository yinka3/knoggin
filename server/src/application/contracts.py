"""Stable local application contracts consumed by the Python SDK."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, TypeAlias

RunStatus = Literal[
    "queued",
    "running",
    "awaiting_input",
    "completed",
    "failed",
    "cancelled",
]


@dataclass(frozen=True, slots=True)
class DocumentFocusDocument:
    """Select one document for a single agent turn."""

    document_id: str

    def __post_init__(self) -> None:
        if not self.document_id.strip():
            raise ValueError("document_id is required")


@dataclass(frozen=True, slots=True)
class DocumentFocusSubtree:
    """Select one subtree of a folder upload for a single agent turn."""

    folder_root_id: str
    path_prefix: str

    def __post_init__(self) -> None:
        if not self.folder_root_id.strip() or not self.path_prefix.strip():
            raise ValueError("folder_root_id and path_prefix are required")


@dataclass(frozen=True, slots=True)
class DocumentFocusFolderUpload:
    """Select all visible documents from one folder upload for one turn."""

    folder_root_id: str

    def __post_init__(self) -> None:
        if not self.folder_root_id.strip():
            raise ValueError("folder_root_id is required")


DocumentFocus: TypeAlias = (
    DocumentFocusDocument | DocumentFocusSubtree | DocumentFocusFolderUpload
)


@dataclass(frozen=True, slots=True)
class Turn:
    """One SDK request to run the agent for an existing session."""

    content: str
    document_focus: DocumentFocus | None = None
    model: str | None = None
    agent_id: str | None = None
    enabled_tools: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        if not self.content.strip():
            raise ValueError("content is required")


@dataclass(frozen=True, slots=True)
class SourceProvenance:
    """Programmatic provenance for one source consulted by an answer.

    This intentionally excludes presentation fields such as a citation label.
    The FastAPI application derives those for the browser from source metadata.
    """

    source_ref_id: str | None
    source_kind: str
    locator: dict[str, Any] | None
    excerpt: str
    document_id: str | None
    canonical_url: str | None
    source_message_id: int | None
    content_hash: str | None
    encounter_kind: str | None
    metadata: dict[str, Any]


def source_provenance_from_response(
    response: dict[str, Any],
) -> tuple[SourceProvenance, ...]:
    """Extract ordered SDK source records from one final agent response."""

    candidates = response.get("sources_consulted", [])
    source_ref_ids = response.get("source_ref_ids", [])
    if not isinstance(candidates, list):
        return ()
    if not isinstance(source_ref_ids, list):
        source_ref_ids = []

    sources: list[SourceProvenance] = []
    for index, candidate in enumerate(candidates):
        if not isinstance(candidate, dict):
            continue
        source_ref_id = (
            source_ref_ids[index]
            if index < len(source_ref_ids) and isinstance(source_ref_ids[index], str)
            else None
        )
        locator = candidate.get("locator")
        metadata = candidate.get("metadata")
        sources.append(
            SourceProvenance(
                source_ref_id=source_ref_id,
                source_kind=str(candidate.get("source_kind", "unknown")),
                locator=dict(locator) if isinstance(locator, dict) else None,
                excerpt=str(candidate.get("excerpt", "")),
                document_id=_optional_text(candidate.get("document_id")),
                canonical_url=_optional_text(candidate.get("canonical_url")),
                source_message_id=_optional_positive_int(
                    candidate.get("source_message_id")
                ),
                content_hash=_optional_text(candidate.get("content_hash")),
                encounter_kind=_optional_text(candidate.get("encounter_kind")),
                metadata=dict(metadata) if isinstance(metadata, dict) else {},
            )
        )
    return tuple(sources)


def _optional_text(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _optional_positive_int(value: Any) -> int | None:
    return value if isinstance(value, int) and value > 0 else None


@dataclass(frozen=True, slots=True)
class SessionHandle:
    """A session created through the application facade."""

    session_id: str
    project_id: str
    model: str | None


@dataclass(frozen=True, slots=True)
class RunSnapshot:
    """Current state of one SDK-owned agent run."""

    run_id: str
    session_id: str
    status: RunStatus
    created_at: datetime
    completed_at: datetime | None
    result: dict[str, Any] | None
    error: dict[str, Any] | None
    sources: tuple[SourceProvenance, ...] = ()


@dataclass(frozen=True, slots=True)
class RunEvent:
    """An SDK event, independent of HTTP/SSE presentation details."""

    run_id: str
    session_id: str
    event: str
    sequence: int
    timestamp: datetime
    data: dict[str, Any]
