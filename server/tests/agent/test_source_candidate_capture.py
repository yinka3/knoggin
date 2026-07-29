from types import SimpleNamespace

import pytest

from common.exceptions import ToolExecutionError
from common.schema.contracts import EngineScope
from common.schema.source_reference import SourceReferenceCandidate
from core.agent.executor import AgentExecutor
from core.agent.types import (
    AgentContext,
    AgentRunIdentity,
    AgentRunConfig,
    AgentState,
    FinalResponse,
    RetrievedEvidence,
    ToolCall,
)


def _pasted_candidate():
    return SourceReferenceCandidate(
        scope=EngineScope(
            user_name="ada", session_id="session-1", project_id="project-1"
        ),
        agent=AgentRunIdentity(
            config=SimpleNamespace(id="agent-1"), name="STELLA", persona=""
        ),
        source_kind="user_pasted_text",
        source_message_id=41,
        content_hash="a" * 64,
        locator={"kind": "character_span", "start_char": 0, "end_char": 12},
        excerpt="pasted notes",
        metadata={"pasted_text": True},
        encounter_kind="user_pasted_text",
        agent_run_id="run-1",
        result_position=0,
    )


def _executor(initial_source_candidates=None):
    context = AgentContext(
        config=AgentRunConfig(max_calls=4),
        state=AgentState(),
        evidence=RetrievedEvidence(),
        project_id="project-1",
        session_id="session-1",
        run_id="run-1",
        initial_source_candidates=(
            list(initial_source_candidates)
            if initial_source_candidates is not None
            else [_pasted_candidate()]
        ),
    )
    return AgentExecutor(context, llm=object(), tools=SimpleNamespace())


@pytest.mark.no_network
async def test_executor_carries_only_successful_source_contexts_to_final_response(
    monkeypatch,
):
    executor = _executor()

    async def fake_execute_tool(_tools, name, _args):
        if name == "news_search":
            raise ToolExecutionError(name, "provider unavailable")
        if name == "search_documents":
            return {
                "data": [
                    {
                        "content": "Page one exact text.",
                        "source_context": {
                            "source_kind": "pdf_document",
                            "document_id": "document-1",
                            "content_hash": "b" * 64,
                            "locator": {"kind": "pdf_page", "page": 1},
                            "excerpt": "Page one exact text.",
                            "metadata": {"document_name": "report.pdf"},
                        },
                    },
                    {"content": "not source-ready"},
                ]
            }
        return {
            "data": [
                {
                    "title": "Result title",
                    "source_context": {
                        "source_kind": "web_search_result",
                        "canonical_url": "https://example.test/result",
                        "content_hash": "c" * 64,
                        "locator": {
                            "kind": "search_result",
                            "provider": "serper",
                            "query": "release notes",
                            "rank": 1,
                        },
                        "excerpt": "Exact provider snippet.",
                        "metadata": {
                            "title": "Result title",
                            "discovery_snippet": True,
                        },
                    },
                }
            ]
        }

    monkeypatch.setattr("core.agent.executor.execute_tool", fake_execute_tool)
    results = []
    _events = [
        event
        async for event in executor._execute_tools(
            [
                ToolCall("search_documents", call_id="call-document"),
                ToolCall("web_search", call_id="call-web"),
                ToolCall("news_search", call_id="call-news"),
            ],
            results,
        )
    ]

    response = executor._wrap_final_response(FinalResponse(content="Done."))
    consulted = response["data"]["sources_consulted"]

    assert [candidate["source_kind"] for candidate in consulted] == [
        "user_pasted_text",
        "pdf_document",
        "web_search_result",
    ]
    assert [
        (candidate["tool_call_id"], candidate["result_position"])
        for candidate in consulted
    ] == [
        (None, 0),
        ("call-document", 0),
        ("call-web", 0),
    ]
    assert results[-1] == {"tool": "news_search", "error": "provider unavailable"}


@pytest.mark.no_network
async def test_executor_ignores_source_context_without_a_tool_call_id(monkeypatch):
    executor = _executor()

    async def fake_execute_tool(_tools, _name, _args):
        return {
            "data": [
                {
                    "source_context": {
                        "source_kind": "pdf_document",
                        "document_id": "document-1",
                        "content_hash": "b" * 64,
                        "locator": {"kind": "pdf_page", "page": 1},
                        "excerpt": "Page one exact text.",
                        "metadata": {"document_name": "report.pdf"},
                    }
                }
            ]
        }

    monkeypatch.setattr("core.agent.executor.execute_tool", fake_execute_tool)
    results = []
    async for _event in executor._execute_tools(
        [ToolCall("search_documents")], results
    ):
        pass

    assert executor.ctx.state.source_candidates == [_pasted_candidate()]


@pytest.mark.no_network
async def test_completed_run_keeps_two_document_calls_and_web_but_not_a_failed_call(
    monkeypatch,
):
    executor = _executor(initial_source_candidates=[])

    async def fake_execute_tool(_tools, name, _args):
        if name == "news_search":
            raise ToolExecutionError(name, "provider timeout")
        source_contexts = {
            "search_documents": {
                "source_kind": "pdf_document",
                "document_id": "document-pdf",
                "content_hash": "a" * 64,
                "locator": {"kind": "pdf_page", "page": 2},
                "excerpt": "Exact PDF passage.",
                "metadata": {"document_name": "report.pdf"},
            },
            "read_document": {
                "source_kind": "text_document",
                "document_id": "document-text",
                "content_hash": "b" * 64,
                "locator": {
                    "kind": "text_lines",
                    "start_line": 3,
                    "end_line": 5,
                },
                "excerpt": "Exact Markdown passage.",
                "metadata": {"document_name": "notes.md"},
            },
            "web_search": {
                "source_kind": "web_search_result",
                "canonical_url": "https://example.test/release",
                "content_hash": "c" * 64,
                "locator": {
                    "kind": "search_result",
                    "provider": "serper",
                    "query": "release",
                    "rank": 1,
                },
                "excerpt": "Exact provider snippet.",
                "metadata": {
                    "title": "Release note",
                    "discovery_snippet": True,
                },
            },
        }
        return {"data": [{"source_context": source_contexts[name]}]}

    monkeypatch.setattr("core.agent.executor.execute_tool", fake_execute_tool)
    results = []
    async for _event in executor._execute_tools(
        [
            ToolCall("search_documents", call_id="call-pdf"),
            ToolCall("read_document", call_id="call-text"),
            ToolCall("web_search", call_id="call-web"),
            ToolCall("news_search", call_id="call-failed-news"),
        ],
        results,
    ):
        pass

    response = executor._wrap_final_response(FinalResponse(content="Done."))
    consulted = response["data"]["sources_consulted"]

    assert [source["source_kind"] for source in consulted] == [
        "pdf_document",
        "text_document",
        "web_search_result",
    ]
    assert [source["tool_call_id"] for source in consulted] == [
        "call-pdf",
        "call-text",
        "call-web",
    ]
    assert results[-1] == {"tool": "news_search", "error": "provider timeout"}
