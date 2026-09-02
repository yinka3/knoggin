import pytest
from pydantic import ValidationError

from common.schema.document import (
    DocumentFocusDocument,
    DocumentSelection,
    create_document_focus,
    dump_document_focus,
    parse_document_focus,
)


@pytest.mark.unit
@pytest.mark.no_network
def test_document_focus_uses_the_document_variant_with_only_its_selectors():
    focus = create_document_focus(
        mode="pinned",
        created_at="2026-06-22T12:00:00+00:00",
        target_type="document",
        document_id=" doc-1 ",
        relative_path=" docs/notes.md ",
    )

    assert isinstance(focus, DocumentFocusDocument)
    assert dump_document_focus(focus) == {
        "mode": "pinned",
        "behavior": "prefer",
        "created_at": "2026-06-22T12:00:00+00:00",
        "target_type": "document",
        "document_id": "doc-1",
        "relative_path": "docs/notes.md",
    }


@pytest.mark.unit
@pytest.mark.no_network
def test_request_document_focus_can_include_a_version_bound_selection():
    focus = create_document_focus(
        mode="request",
        created_at="2026-06-22T12:00:00+00:00",
        target_type="document",
        document_id="doc-1",
        relative_path="docs/notes.py",
        selection=DocumentSelection(
            content_hash="a" * 64,
            locator={
                "kind": "code_lines",
                "start_line": 4,
                "end_line": 8,
            },
        ),
    )

    assert dump_document_focus(focus)["selection"] == {
        "content_hash": "a" * 64,
        "locator": {
            "kind": "code_lines",
            "start_line": 4,
            "end_line": 8,
        },
    }


@pytest.mark.unit
@pytest.mark.no_network
def test_pinned_document_focus_rejects_a_selection():
    with pytest.raises(ValidationError, match="only valid for request focus"):
        create_document_focus(
            mode="pinned",
            created_at="2026-06-22T12:00:00+00:00",
            target_type="document",
            document_id="doc-1",
            relative_path="docs/notes.py",
            selection={
                "content_hash": "a" * 64,
                "locator": {"kind": "text_lines", "start_line": 1, "end_line": 1},
            },
        )


@pytest.mark.unit
@pytest.mark.no_network
def test_document_focus_rejects_mixed_selectors_and_naive_timestamps():
    with pytest.raises(ValidationError):
        parse_document_focus(
            {
                "mode": "pinned",
                "created_at": "2026-06-22T12:00:00+00:00",
                "target_type": "document",
                "document_id": "doc-1",
                "relative_path": "docs/notes.md",
                "folder_root_id": "folder-1",
            }
        )

    with pytest.raises(ValidationError, match="must include a timezone"):
        parse_document_focus(
            {
                "mode": "pinned",
                "created_at": "2026-06-22T12:00:00",
                "target_type": "subtree",
                "path_prefix": "docs",
            }
        )
