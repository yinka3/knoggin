import json

import pytest

from common.schema.document import (
    FolderScanSettings,
    FolderUploadEntry,
    WorkspaceSyncChanges,
)
from common.schema.source_reference import SourceReferenceCandidate


@pytest.mark.unit
@pytest.mark.no_network
def test_source_reference_metadata_is_recursively_immutable_and_serializable():
    metadata = {
        "document_name": "notes.pdf",
        "details": {"labels": ["planning"]},
    }
    candidate = SourceReferenceCandidate.model_validate(
        {
            "project_id": "project-1",
            "session_id": "session-1",
            "source_kind": "pdf_document",
            "document_id": "document-1",
            "content_hash": "a" * 64,
            "locator": {"kind": "pdf_page", "page": 1},
            "excerpt": "A retrieved passage.",
            "metadata": metadata,
            "encounter_kind": "document_read",
            "agent_run_id": "run-1",
            "tool_call_id": "call-1",
            "result_position": 0,
        }
    )

    with pytest.raises(TypeError, match="immutable"):
        candidate.metadata["new_key"] = "new value"
    with pytest.raises(TypeError, match="immutable"):
        candidate.metadata["details"]["labels"] += ("review",)
    metadata["details"]["labels"].append("mutated input")
    assert candidate.metadata["details"]["labels"] == ("planning",)
    assert json.loads(json.dumps(candidate.metadata))["document_name"] == "notes.pdf"


@pytest.mark.unit
@pytest.mark.no_network
def test_frozen_document_models_use_immutable_collection_types():
    changes = WorkspaceSyncChanges(
        upserts=[FolderUploadEntry(relative_path="docs/a.md", content=b"a")],
        deleted_paths=["docs/old.md"],
    )
    settings = FolderScanSettings(
        ignored_patterns=[" generated/* "],
        allowed_extensions={"md"},
        blocked_extensions={".log"},
        blocked_file_names={".env"},
        blocked_directory_names={"node_modules"},
    )

    assert changes.deleted_paths == ("docs/old.md",)
    assert settings.ignored_patterns == ("generated/*",)
    assert settings.allowed_extensions == frozenset({".md"})
    with pytest.raises(AttributeError):
        changes.deleted_paths.append("docs/other.md")
    with pytest.raises(AttributeError):
        settings.blocked_extensions.add(".tmp")
