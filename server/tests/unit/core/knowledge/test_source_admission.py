from unittest.mock import AsyncMock

import pytest

from common.schema.document import UserAttachedFile, UserAttachedUrl
from common.schema.source.references import SourceReferenceCandidate
from core.knowledge.documents import DocumentService


def _service() -> DocumentService:
    return DocumentService.__new__(DocumentService)


@pytest.mark.unit
@pytest.mark.no_network
async def test_user_sources_are_durable_by_default_and_opt_out_is_run_local():
    service = _service()
    service.submit_document = AsyncMock(return_value={"document_id": "doc-1"})
    service.save_web_link = AsyncMock(return_value={"link_id": "link-1"})

    saved_file = await service.admit_user_source(
        UserAttachedFile(original_name="notes.md", content=b"notes")
    )
    transient_url = await service.admit_user_source(
        UserAttachedUrl(url="https://example.com/notes"), durable=False
    )

    assert saved_file == {"document_id": "doc-1"}
    assert transient_url["durable"] is False
    service.submit_document.assert_awaited_once()
    service.save_web_link.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.no_network
async def test_assistant_source_promotion_is_explicit_and_bookmark_only():
    service = _service()
    service.save_web_link = AsyncMock(return_value={"link_id": "link-1"})
    candidate = SourceReferenceCandidate.model_validate(
        {
            "project_id": "project-1",
            "session_id": "session-1",
            "source_kind": "web_page",
            "canonical_url": "https://example.com/article",
            "content_hash": "a" * 64,
            "locator": {"kind": "text_lines", "start_line": 1, "end_line": 2},
            "excerpt": "A transient observation.",
            "metadata": {"title": "Article"},
            "encounter_kind": "web_read",
            "agent_run_id": "run-1",
            "tool_call_id": "call-1",
            "result_position": 0,
        }
    )

    result = await service.promote_source(candidate)

    assert result == {"link_id": "link-1"}
    service.save_web_link.assert_awaited_once_with(
        url="https://example.com/article",
        title="Article",
        summary=None,
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_promoting_a_document_source_is_rejected():
    service = _service()
    with pytest.raises(ValueError, match="only assistant-observed web sources"):
        await service.promote_source(
            {
                "project_id": "project-1",
                "session_id": "session-1",
                "source_kind": "text_document",
                "document_id": "doc-1",
                "source_project_id": "project-1",
                "content_hash": "a" * 64,
                "locator": {
                    "kind": "text_lines",
                    "start_line": 1,
                    "end_line": 1,
                },
                "excerpt": "durable document",
                "metadata": {
                    "document_name": "notes.md",
                    "relative_path": "notes.md",
                    "extension": ".md",
                },
                "encounter_kind": "document_read",
                "agent_run_id": "run-1",
                "tool_call_id": "call-1",
                "result_position": 0,
            }
        )
