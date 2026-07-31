from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from common.schema.source_reference import (
    CodeLineLocator,
    CsvRowLocator,
    DocxParagraphLocator,
    PastedTextLocator,
    PdfPageLocator,
    SearchResultLocator,
    SourceReference,
    SourceReferenceCandidate,
    TextLineLocator,
)

CONTENT_HASH = "a" * 64


def _candidate(**overrides):
    payload = {
        "project_id": "project-1",
        "session_id": "session-1",
        "source_kind": "pdf_document",
        "document_id": "document-1",
        "content_hash": CONTENT_HASH,
        "locator": {"kind": "pdf_page", "page": 2},
        "excerpt": "The second page's retrieved passage.",
        "metadata": {"document_name": "two-page-report.pdf"},
        "encounter_kind": "document_search",
        "agent_run_id": "run-1",
        "tool_call_id": "call-1",
        "result_position": 0,
    }
    payload.update(overrides)
    return payload


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("locator", "excerpt"),
    [
        ({"kind": "text_lines", "start_line": 4, "end_line": 6}, "plain text"),
        (
            {
                "kind": "text_lines",
                "start_line": 8,
                "end_line": 10,
                "section_path": ["Overview", "Risks"],
            },
            "markdown text",
        ),
        ({"kind": "csv_rows", "start_row": 2, "end_row": 4}, "CSV data"),
        (
            {
                "kind": "code_lines",
                "start_line": 12,
                "end_line": 18,
                "symbol_name": "build_report",
            },
            "source code",
        ),
        (
            {
                "kind": "docx_paragraphs",
                "start_paragraph": 4,
                "end_paragraph": 6,
                "heading_path": ["Overview", "Risks"],
            },
            "Word document text",
        ),
    ],
)
def test_document_candidates_accept_supported_reliable_locators(locator, excerpt):
    candidate = SourceReferenceCandidate.model_validate(
        _candidate(
            source_kind="text_document",
            locator=locator,
            excerpt=excerpt,
        )
    )

    assert candidate.locator.model_dump() == locator
    assert candidate.excerpt == excerpt


@pytest.mark.unit
@pytest.mark.no_network
def test_pdf_candidate_accepts_page_aware_two_page_fixture():
    candidate = SourceReferenceCandidate.model_validate(_candidate())

    assert candidate.locator == PdfPageLocator(page=2)
    assert candidate.metadata["document_name"] == "two-page-report.pdf"


@pytest.mark.unit
@pytest.mark.no_network
def test_pasted_text_candidate_requires_a_canonical_message_span():
    candidate = SourceReferenceCandidate.model_validate(
        _candidate(
            source_kind="user_pasted_text",
            document_id=None,
            source_message_id=42,
            canonical_url=None,
            locator={"kind": "character_span", "start_char": 14, "end_char": 51},
            excerpt="The explicitly pasted source material.",
            metadata={},
            encounter_kind="user_pasted_text",
            tool_call_id=None,
        )
    )

    assert candidate.locator == PastedTextLocator(start_char=14, end_char=51)
    assert candidate.source_message_id == 42


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("source_kind", "encounter_kind"),
    [
        ("web_search_result", "web_search"),
        ("news_search_result", "news_search"),
    ],
)
def test_search_candidates_are_explicitly_discovery_snippets(
    source_kind, encounter_kind
):
    candidate = SourceReferenceCandidate.model_validate(
        _candidate(
            source_kind=source_kind,
            document_id=None,
            canonical_url="https://example.test/report",
            locator={
                "kind": "search_result",
                "provider": "example-search",
                "query": "quarterly report",
                "rank": 1,
            },
            excerpt="Provider-returned result snippet.",
            metadata={"title": "Quarterly report", "discovery_snippet": True},
            encounter_kind=encounter_kind,
        )
    )

    assert candidate.locator == SearchResultLocator(
        provider="example-search", query="quarterly report", rank=1
    )
    assert candidate.metadata["discovery_snippet"] is True


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    "payload, error",
    [
        (_candidate(document_id=None), "document sources require document_id"),
        (
            _candidate(locator={"kind": "pdf_page", "page": 0}),
            "greater than or equal to 1",
        ),
        (
            _candidate(
                source_kind="text_document",
                locator={"kind": "pdf_page", "page": 1},
            ),
            "text_document sources require a text locator",
        ),
        (
            _candidate(
                source_kind="user_pasted_text",
                document_id=None,
                source_message_id=42,
                locator={"kind": "character_span", "start_char": 5, "end_char": 5},
                metadata={},
                encounter_kind="user_pasted_text",
                tool_call_id=None,
            ),
            "end_char must be greater than start_char",
        ),
        (
            _candidate(
                source_kind="web_search_result",
                document_id=None,
                canonical_url="https://example.test/report#section",
                locator={
                    "kind": "search_result",
                    "provider": "example-search",
                    "query": "quarterly report",
                    "rank": 1,
                },
                metadata={"title": "Quarterly report", "discovery_snippet": True},
                encounter_kind="web_search",
            ),
            "canonical_url must not contain a fragment",
        ),
        (
            _candidate(
                source_kind="news_search_result",
                document_id=None,
                canonical_url="https://example.test/report",
                locator={
                    "kind": "search_result",
                    "provider": "example-search",
                    "query": "quarterly report",
                    "rank": 1,
                },
                metadata={"title": "Quarterly report"},
                encounter_kind="news_search",
            ),
            "discovery_snippet",
        ),
    ],
)
def test_candidates_reject_invalid_source_shapes(payload, error):
    with pytest.raises(ValidationError, match=error):
        SourceReferenceCandidate.model_validate(payload)


@pytest.mark.unit
@pytest.mark.no_network
def test_persisted_reference_extends_a_validated_candidate():
    reference = SourceReference.model_validate(
        _candidate(
            source_ref_id="ref-1",
            message_id=99,
            idempotency_key="run-1:call-1:0",
            created_at=datetime(2026, 7, 26, tzinfo=timezone.utc),
        )
    )

    assert reference.message_id == 99
    assert reference.source_ref_id == "ref-1"


@pytest.mark.unit
@pytest.mark.no_network
def test_locator_models_preserve_their_own_invariants():
    assert TextLineLocator(start_line=1, end_line=1).kind == "text_lines"
    assert CsvRowLocator(start_row=2, end_row=2).kind == "csv_rows"
    assert CodeLineLocator(start_line=3, end_line=3).kind == "code_lines"
    assert DocxParagraphLocator(
        start_paragraph=4, end_paragraph=4
    ).kind == "docx_paragraphs"

    with pytest.raises(ValidationError, match="end_line"):
        TextLineLocator(start_line=4, end_line=3)
    with pytest.raises(ValidationError, match="end_paragraph"):
        DocxParagraphLocator(start_paragraph=4, end_paragraph=3)
