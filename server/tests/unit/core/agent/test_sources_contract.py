import hashlib

import pytest

from common.schema.source.references import SourceReferenceCandidate
from core.agent.sources.pasted_text import build_pasted_text_candidates
from core.agent.tools.search import SearchTools


@pytest.mark.no_network
def test_structured_pasted_text_span_uses_canonical_message_content_and_id():
    message = "Please use this: exact pasted passage. Thanks."
    excerpt = "exact pasted passage"
    start = message.index(excerpt)
    candidates = build_pasted_text_candidates(
        project_id="project-1",
        session_id="session-1",
        source_message_id=42,
        message_content=message,
        agent_run_id="run-1",
        spans=[{"start_char": start, "end_char": start + len(excerpt)}],
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.source_message_id == 42
    assert candidate.locator.model_dump() == {
        "kind": "character_span",
        "start_char": start,
        "end_char": start + len(excerpt),
    }
    assert candidate.excerpt == excerpt
    assert candidate.content_hash == hashlib.sha256(excerpt.encode()).hexdigest()


@pytest.mark.no_network
def test_pasted_text_fallback_recognizes_only_delimited_blocks():
    ordinary_message = "The report says revenue increased."
    fenced_message = "Please summarize:\n```text\nrevenue increased\n```"

    assert build_pasted_text_candidates(
        project_id="project-1",
        session_id="session-1",
        source_message_id=42,
        message_content=ordinary_message,
        agent_run_id="run-1",
    ) == []
    candidates = build_pasted_text_candidates(
        project_id="project-1",
        session_id="session-1",
        source_message_id=42,
        message_content=fenced_message,
        agent_run_id="run-1",
    )
    assert [candidate.excerpt for candidate in candidates] == ["revenue increased"]


@pytest.mark.no_network
def test_pasted_text_rejects_invalid_client_spans():
    with pytest.raises(ValueError, match="character spans"):
        build_pasted_text_candidates(
            project_id="project-1",
            session_id="session-1",
            source_message_id=42,
            message_content="short message",
            agent_run_id="run-1",
            spans=[{"start_char": 0, "end_char": 999}],
        )


@pytest.mark.no_network
def test_web_result_source_context_is_a_discovery_snippet_candidate():
    results = SearchTools._with_search_source_contexts(
        [
            {
                "title": "Quarterly results",
                "url": "https://Example.test/news?q=q2#fragment",
                "snippet": "Revenue rose 18 percent.",
                "_source_provider": "brave",
            }
        ],
        source_kind="web_search_result",
        query="quarterly revenue",
        fallback_provider="duckduckgo",
    )

    source_context = results[0]["source_context"]
    assert "_source_provider" not in results[0]
    assert source_context["canonical_url"] == "https://example.test/news?q=q2"
    assert source_context["excerpt"] == "Revenue rose 18 percent."
    assert source_context["locator"] == {
        "kind": "search_result",
        "provider": "brave",
        "query": "quarterly revenue",
        "rank": 1,
    }
    assert source_context["metadata"]["discovery_snippet"] is True
    candidate = SourceReferenceCandidate.model_validate(
        {
            **source_context,
            "project_id": "project-1",
            "session_id": "session-1",
            "encounter_kind": "web_search",
            "agent_run_id": "run-1",
            "tool_call_id": "call-1",
            "result_position": 0,
        }
    )
    assert candidate.source_kind == "web_search_result"


@pytest.mark.no_network
@pytest.mark.parametrize(
    "result",
    [
        {"title": "No Results", "url": "", "snippet": "Nothing found."},
        {"title": "Search Error", "url": "", "snippet": "Timed out."},
        {"title": "Useful", "url": "", "snippet": "No URL was returned."},
        {"title": "Useful", "url": "https://example.test", "snippet": "   "},
    ],
)
def test_search_adapter_excludes_notices_errors_and_incomplete_results(result):
    results = SearchTools._with_search_source_contexts(
        [result],
        source_kind="news_search_result",
        query="latest updates",
        fallback_provider="brave",
    )

    assert "source_context" not in results[0]
