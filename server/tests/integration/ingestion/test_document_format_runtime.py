import asyncio
from types import SimpleNamespace

import pytest

from common.conf.manager import ConfigManager
from common.schema.agent.identity import AgentConfig
from common.schema.public import StartRunRequest, validate_public_stream
from core.agent.orchestrator import AgentOrchestrator
from core.knowledge.documents import DocumentService
from core.knowledge.documents import storage as document_storage
from core.knowledge.entity.resolver import EntityResolver
from core.knowledge.retrieval import KnowledgeRetrieval
from core.knowledge.store import KnowledgeStore
from runtime.api_port import ApplicationRuntimePort
from runtime.session_runtime import SessionRuntime as Session
from tests.fixtures.documents import build_docx_bytes, build_pdf_bytes, build_png_bytes
from tests.fixtures.factories import make_domain_config
from tests.integration.ingestion.test_server_flow import (
    _DeterministicDocumentAgentLLM,
    _DeterministicEmbeddingService,
    _session,
    _SignalCounter,
    _StaticAgentManager,
    _StaticSessionManager,
)


def _runtime_document_cases():
    return [
        (
            "launch.md",
            b"# Overview\nThe violet launch phrase is durable.\n",
            {
                "source_kind": "text_document",
                "excerpt": "# Overview\nThe violet launch phrase is durable.",
                "locator": {
                    "kind": "text_lines",
                    "start_line": 1,
                    "end_line": 2,
                    "section_path": ("Overview",),
                },
            },
        ),
        (
            "launch.pdf",
            build_pdf_bytes("The violet launch phrase is durable."),
            {
                "source_kind": "pdf_document",
                "excerpt": "The violet launch phrase is durable.",
                "locator": {"kind": "pdf_page", "page": 1},
            },
        ),
        (
            "launch.docx",
            build_docx_bytes(
                [
                    ("Overview", 1),
                    ("The violet launch phrase is durable.", None),
                ]
            ),
            {
                "source_kind": "text_document",
                "excerpt": "Overview\nThe violet launch phrase is durable.",
                "locator": {
                    "kind": "docx_paragraphs",
                    "start_paragraph": 1,
                    "end_paragraph": 2,
                    "heading_path": ("Overview",),
                },
            },
        ),
        (
            "launch.csv",
            b"topic,detail\nlaunch,The violet launch phrase is durable.\n",
            {
                "source_kind": "text_document",
                "excerpt": "topic,detail\nlaunch,The violet launch phrase is durable.",
                "locator": {"kind": "csv_rows", "start_row": 1, "end_row": 1},
            },
        ),
        (
            "launch.py",
            b'def launch():\n    return "The violet launch phrase is durable."\n',
            {
                "source_kind": "text_document",
                "excerpt": 'def launch():\n    return "The violet launch phrase is durable."',
                "locator": {
                    "kind": "code_lines",
                    "start_line": 1,
                    "end_line": 2,
                    "symbol_name": "launch",
                },
            },
        ),
        ("launch.png", build_png_bytes(), None),
    ]


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("original_name", "content", "expected_source"),
    _runtime_document_cases(),
    ids=["markdown", "pdf", "docx", "csv", "python", "ocr-image"],
)
async def test_public_runtime_preserves_format_specific_document_provenance(
    real_server_scope,
    monkeypatch,
    original_name,
    content,
    expected_source,
):
    scope = real_server_scope
    postgres = scope["postgres"]
    monkeypatch.setattr(
        document_storage.pytesseract,
        "image_to_string",
        lambda _: "The violet launch phrase is durable.\n",
    )
    embedding = _DeterministicEmbeddingService()
    llm = _DeterministicDocumentAgentLLM()
    store = KnowledgeStore(postgres, embedding)
    resources = SimpleNamespace(
        postgres=postgres,
        knowledge_store=store,
        embedding=embedding,
        llm_service=llm,
    )
    monkeypatch.setattr(
        Session,
        "current_config",
        property(
            lambda self: SimpleNamespace(
                developer_settings=SimpleNamespace(
                    limits=SimpleNamespace(conversation_context_turns=100),
                    ingestion=SimpleNamespace(message_edit_window_seconds=1),
                )
            )
        ),
    )

    documents = DocumentService(
        project_id=scope["project_id"],
        postgres_client=postgres,
        embedding_service=embedding,
        document_rerank_enabled=False,
    )
    document = await documents.submit_document(
        content=content,
        original_name=original_name,
        relative_path=f"docs/{original_name}",
        visibility_scope="project",
    )
    assert document["status"] == "indexed"

    resolver = EntityResolver(
        store,
        embedding,
        scope["project_id"],
        [scope["project_id"]],
    )
    retrieval = KnowledgeRetrieval(
        project_id=scope["project_id"],
        readable_project_ids=[scope["project_id"]],
        user_name=scope["user_name"],
        entities=resolver,
        embedding_service=embedding,
        knowledge_store=store,
        postgres=postgres,
    )
    context = _session(
        resources,
        user_name=scope["user_name"],
        project_id=scope["project_id"],
        session_id=scope["session_id"],
    )
    context.project = SimpleNamespace(
        scheduler=object(),
        record_session_activity=lambda: asyncio.sleep(0),
        readable_project_ids=[scope["project_id"]],
        entities=resolver,
        compiled_domain=make_domain_config().compile(),
        knowledge_retrieval=retrieval,
    )
    context.document_service = documents
    context.ingestion_worker = _SignalCounter()

    agent = AgentConfig(
        id="format-document-agent",
        name="Format Document Agent",
        persona={
            "attention_bias": "evidence",
            "reasoning_style": "methodical",
            "social_temperament": "calm",
            "communication_signature": "clear",
            "productive_flaw": "overexplains",
        },
        enabled_tools=["search_documents"],
    )
    context.agent_orchestrator = AgentOrchestrator(
        _StaticAgentManager(agent),
        config_provider=ConfigManager,
    )
    application = ApplicationRuntimePort(
        SimpleNamespace(
            sessions=_StaticSessionManager(context),
            resources=resources,
        )
    )

    events = [
        event
        async for event in application.run_stream(
            user_name=scope["user_name"],
            request=StartRunRequest(
                session_id=scope["session_id"],
                query="What is the violet launch phrase?",
                enabled_tools=["search_documents"],
            ),
        )
    ]

    public_events = validate_public_stream(events, require_terminal=True)
    assert public_events[-1].type == "run.completed"
    assert not [event for event in public_events if event.type == "run.failed"]
    assert any(
        event.type == "tool.completed"
        and event.tool_name == "search_documents"
        and event.succeeded
        for event in public_events
    )

    source_events = [event for event in public_events if event.type == "source.added"]
    response = public_events[-1]
    if expected_source is None:
        assert source_events == []
        assert response.result.source_ref_ids == ()
        answer = await store.get_assistant_message_with_sources(
            response.result.assistant_message_id,
            user_name=scope["user_name"],
            project_id=scope["project_id"],
            session_id=scope["session_id"],
        )
        assert answer is not None
        assert answer.sources_consulted == ()
        return

    assert len(source_events) == 1
    assert source_events[0].source.document_id == document["document_id"]
    assert source_events[0].source.source_kind == expected_source["source_kind"]
    assert source_events[0].source.excerpt == expected_source["excerpt"]
    assert source_events[0].source.locator.model_dump() == expected_source["locator"]

    assert len(response.result.source_ref_ids) == 1
    answer = await store.get_assistant_message_with_sources(
        response.result.assistant_message_id,
        user_name=scope["user_name"],
        project_id=scope["project_id"],
        session_id=scope["session_id"],
    )
    assert answer is not None
    assert len(answer.sources_consulted) == 1
    assert answer.sources_consulted[0].source_status == "available"
    assert (
        answer.sources_consulted[0].locator.model_dump() == expected_source["locator"]
    )
