"""Source-candidate construction for a user-selected document passage."""

from __future__ import annotations

from typing import Mapping

from common.schema.source.references import SourceReferenceCandidate


def build_document_selection_candidate(
    *,
    project_id: str,
    session_id: str,
    agent_run_id: str,
    selection_context: Mapping[str, object],
) -> SourceReferenceCandidate:
    """Create one answer-level source from a server-resolved selection."""
    locator = selection_context.get("locator")
    if not isinstance(locator, Mapping):
        raise ValueError("resolved document selection has no locator")
    excerpt = selection_context.get("excerpt")
    if not isinstance(excerpt, str):
        raise ValueError("resolved document selection has no excerpt")
    document_id = selection_context.get("document_id")
    source_project_id = selection_context.get("project_id")
    content_hash = selection_context.get("content_hash")
    relative_path = selection_context.get("relative_path")
    document_name = selection_context.get("document_name")
    extension = selection_context.get("extension")
    if not all(
        isinstance(value, str) and value.strip()
        for value in (
            document_id,
            source_project_id,
            content_hash,
            relative_path,
            document_name,
            extension,
        )
    ):
        raise ValueError("resolved document selection is missing document metadata")
    source_kind = (
        "pdf_document" if locator.get("kind") == "pdf_page" else "text_document"
    )
    metadata = {
        "document_name": document_name,
        "relative_path": relative_path,
        "extension": extension,
        "selection": True,
    }
    if selection_context.get("chunk_index") is not None:
        metadata["chunk_index"] = selection_context["chunk_index"]
    return SourceReferenceCandidate.model_validate(
        {
            "project_id": project_id,
            "session_id": session_id,
            "source_kind": source_kind,
            "document_id": document_id,
            "source_project_id": source_project_id,
            "content_hash": content_hash,
            "locator": dict(locator),
            "excerpt": excerpt,
            "metadata": metadata,
            "encounter_kind": "document_selection",
            "agent_run_id": agent_run_id,
            "tool_call_id": None,
            "result_position": 0,
        }
    )
