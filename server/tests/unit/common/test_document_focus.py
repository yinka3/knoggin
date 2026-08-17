import pytest
from pydantic import ValidationError

from common.schema.document import (
    DocumentFocusDocument,
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
        "created_at": "2026-06-22T12:00:00+00:00",
        "target_type": "document",
        "document_id": "doc-1",
        "relative_path": "docs/notes.md",
    }


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
                "target_type": "folder_upload",
                "folder_root_id": "folder-1",
            }
        )


@pytest.mark.unit
@pytest.mark.no_network
def test_document_focus_reads_legacy_null_selector_fields_without_retaining_them():
    focus = parse_document_focus(
        {
            "mode": "pinned",
            "created_at": "2026-06-22T12:00:00+00:00",
            "target_type": "folder_upload",
            "document_id": None,
            "relative_path": None,
            "folder_root_id": "folder-1",
            "path_prefix": None,
        }
    )

    assert dump_document_focus(focus) == {
        "mode": "pinned",
        "created_at": "2026-06-22T12:00:00+00:00",
        "target_type": "folder_upload",
        "folder_root_id": "folder-1",
    }
