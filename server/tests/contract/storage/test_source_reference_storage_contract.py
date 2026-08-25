from datetime import datetime, timezone

import pytest
from psycopg.errors import CheckViolation

from common.schema.source.references import SourceReferenceCandidate
from core.knowledge.db.readers.source_reference_reader import SourceReferenceReader
from core.knowledge.db.writers.document_writer import DocumentWriter
from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
from core.knowledge.db.writers.source_reference_writer import SourceReferenceWriter
from tests.fixtures.fakes import RecordingPostgresClient

DOCUMENT_ID = "00000000-0000-0000-0000-000000000101"
TEXT_DOCUMENT_ID = "00000000-0000-0000-0000-000000000102"
CROSS_PROJECT_DOCUMENT_ID = "00000000-0000-0000-0000-000000000103"
CROSS_PROJECT_SESSION_DOCUMENT_ID = "00000000-0000-0000-0000-000000000104"
SOURCE_REF_ID = "00000000-0000-0000-0000-000000000201"
CONTENT_HASH = "b" * 64


def document_candidate(**overrides) -> SourceReferenceCandidate:
    payload = {
        "project_id": "project-1",
        "session_id": "session-1",
        "source_kind": "pdf_document",
        "document_id": DOCUMENT_ID,
        "source_project_id": "project-1",
        "content_hash": CONTENT_HASH,
        "locator": {"kind": "pdf_page", "page": 2},
        "excerpt": "The retrieved second-page passage.",
        "metadata": {"document_name": "report.pdf"},
        "encounter_kind": "document_search",
        "agent_run_id": "run-1",
        "tool_call_id": "call-1",
        "result_position": 0,
    }
    payload.update(overrides)
    return SourceReferenceCandidate.model_validate(payload)


def pasted_text_candidate(**overrides) -> SourceReferenceCandidate:
    payload = {
        "project_id": "project-1",
        "session_id": "session-1",
        "source_kind": "user_pasted_text",
        "source_message_id": 102,
        "content_hash": CONTENT_HASH,
        "locator": {"kind": "character_span", "start_char": 4, "end_char": 22},
        "excerpt": "pasted source text",
        "encounter_kind": "user_pasted_text",
        "agent_run_id": "run-pasted",
        "result_position": 0,
    }
    payload.update(overrides)
    return SourceReferenceCandidate.model_validate(payload)


def text_document_candidate(**overrides) -> SourceReferenceCandidate:
    payload = {
        "project_id": "project-1",
        "session_id": "session-1",
        "source_kind": "text_document",
        "document_id": TEXT_DOCUMENT_ID,
        "source_project_id": "project-1",
        "content_hash": "d" * 64,
        "locator": {"kind": "text_lines", "start_line": 4, "end_line": 6},
        "excerpt": "The exact Markdown passage.",
        "metadata": {"document_name": "notes.md"},
        "encounter_kind": "document_read",
        "agent_run_id": "run-text",
        "tool_call_id": "call-text",
        "result_position": 0,
    }
    payload.update(overrides)
    return SourceReferenceCandidate.model_validate(payload)


def document_selection_candidate(**overrides) -> SourceReferenceCandidate:
    return text_document_candidate(
        locator={"kind": "code_lines", "start_line": 4, "end_line": 6},
        excerpt="4: def answer():\n5:     return 42",
        metadata={
            "document_name": "notes.py",
            "relative_path": "docs/notes.py",
            "extension": ".py",
            "selection": True,
        },
        encounter_kind="document_selection",
        agent_run_id="run-selection",
        tool_call_id=None,
        **overrides,
    )


def docx_document_candidate(**overrides) -> SourceReferenceCandidate:
    return text_document_candidate(
        locator={
            "kind": "docx_paragraphs",
            "start_paragraph": 4,
            "end_paragraph": 6,
            "heading_path": ["Overview"],
        },
        excerpt="Overview\nThe exact Word passage.",
        metadata={"document_name": "overview.docx"},
        agent_run_id="run-docx",
        tool_call_id="call-docx",
        **overrides,
    )


def web_candidate(**overrides) -> SourceReferenceCandidate:
    payload = {
        "project_id": "project-1",
        "session_id": "session-1",
        "source_kind": "web_search_result",
        "canonical_url": "https://example.test/release",
        "content_hash": "c" * 64,
        "locator": {
            "kind": "search_result",
            "provider": "serper",
            "query": "release notes",
            "rank": 1,
        },
        "excerpt": "Exact provider snippet.",
        "metadata": {"title": "Release note", "discovery_snippet": True},
        "encounter_kind": "web_search",
        "agent_run_id": "run-web",
        "tool_call_id": "call-web",
        "result_position": 0,
    }
    payload.update(overrides)
    return SourceReferenceCandidate.model_validate(payload)


def news_candidate(**overrides) -> SourceReferenceCandidate:
    payload = {
        "project_id": "project-1",
        "session_id": "session-1",
        "source_kind": "news_search_result",
        "canonical_url": "https://news.example.test/release",
        "content_hash": "e" * 64,
        "locator": {
            "kind": "search_result",
            "provider": "newsapi",
            "query": "release notes",
            "rank": 1,
        },
        "excerpt": "Exact news-provider snippet.",
        "metadata": {"title": "News release", "discovery_snippet": True},
        "encounter_kind": "news_search",
        "agent_run_id": "run-news",
        "tool_call_id": "call-news",
        "result_position": 0,
    }
    payload.update(overrides)
    return SourceReferenceCandidate.model_validate(payload)


def web_page_candidate(**overrides) -> SourceReferenceCandidate:
    payload = {
        "project_id": "project-1",
        "session_id": "session-1",
        "source_kind": "web_page",
        "canonical_url": "https://example.test/report",
        "content_hash": "f" * 64,
        "locator": {"kind": "text_lines", "start_line": 151, "end_line": 220},
        "excerpt": "Canonical webpage text observed in this exact range.",
        "metadata": {"title": "Research report"},
        "encounter_kind": "web_read",
        "agent_run_id": "run-web-read",
        "tool_call_id": "call-web-read",
        "result_position": 0,
    }
    payload.update(overrides)
    return SourceReferenceCandidate.model_validate(payload)


def web_pdf_candidate(**overrides) -> SourceReferenceCandidate:
    payload = {
        "project_id": "project-1",
        "session_id": "session-1",
        "source_kind": "web_pdf",
        "canonical_url": "https://example.test/report.pdf",
        "content_hash": "a" * 64,
        "locator": {"kind": "pdf_page", "page": 2},
        "excerpt": "Canonical external PDF text observed on page two.",
        "metadata": {"title": "Research report"},
        "encounter_kind": "web_read",
        "agent_run_id": "run-web-pdf-read",
        "tool_call_id": "call-web-pdf-read",
        "result_position": 0,
    }
    payload.update(overrides)
    return SourceReferenceCandidate.model_validate(payload)


def persisted_row(candidate: SourceReferenceCandidate, **overrides):
    row = {
        "source_ref_id": SOURCE_REF_ID,
        "project_id": candidate.project_id,
        "session_id": candidate.session_id,
        "message_id": 101,
        "source_kind": candidate.source_kind,
        "document_id": candidate.document_id,
        "source_project_id": candidate.source_project_id,
        "canonical_url": candidate.canonical_url,
        "source_message_id": candidate.source_message_id,
        "content_hash": candidate.content_hash,
        "locator": candidate.locator.model_dump(mode="json"),
        "excerpt": candidate.excerpt,
        "metadata": candidate.metadata,
        "encounter_kind": candidate.encounter_kind,
        "agent_run_id": candidate.agent_run_id,
        "tool_call_id": candidate.tool_call_id,
        "result_position": candidate.result_position,
        "idempotency_key": SourceReferenceWriter.idempotency_key(candidate),
        "created_at": datetime(2026, 7, 26, tzinfo=timezone.utc),
    }
    row.update(overrides)
    return row


@pytest.mark.storage
@pytest.mark.no_network
async def test_writer_inserts_typed_candidates_through_scoped_assistant_message():
    candidate = document_candidate()
    client = RecordingPostgresClient(fetch_one_results=[persisted_row(candidate)])
    writer = SourceReferenceWriter(client)

    references = await writer.write_for_assistant_message(
        101,
        [candidate],
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        readable_project_ids=["project-1"],
    )

    assert [reference.source_ref_id for reference in references] == [SOURCE_REF_ID]
    assert client.transaction_enters == 1
    query, params = client.calls[0][1], client.calls[0][2]
    assert "INSERT INTO public.message_source_refs" in query
    assert "message.role = 'assistant'" in query
    assert "document.visibility_scope = 'project'" in query
    assert "ON CONFLICT (idempotency_key) DO UPDATE" in query
    assert params[1:4] == ("project-1", "session-1", 101)
    assert params[17] == SourceReferenceWriter.idempotency_key(candidate)
    assert params[-11:] == (
        101,
        "project-1",
        "session-1",
        "ada",
        DOCUMENT_ID,
        DOCUMENT_ID,
        "project-1",
        CONTENT_HASH,
        ["project-1"],
        "project-1",
        "session-1",
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_writer_rejects_candidate_scope_mismatch_before_starting_a_transaction():
    client = RecordingPostgresClient()
    writer = SourceReferenceWriter(client)

    with pytest.raises(ValueError, match="candidate scope"):
        await writer.write_for_assistant_message(
            101,
            [document_candidate(session_id="other-session")],
            user_name="ada",
            project_id="project-1",
            session_id="session-1",
            readable_project_ids=["project-1"],
        )

    assert client.calls == []
    assert client.transaction_enters == 0


@pytest.mark.storage
@pytest.mark.no_network
async def test_writer_uses_stable_idempotency_for_tool_and_pasted_origins():
    document = document_candidate()
    pasted = pasted_text_candidate()

    assert SourceReferenceWriter.idempotency_key(document) == (
        SourceReferenceWriter.idempotency_key(document)
    )
    assert SourceReferenceWriter.idempotency_key(document) != (
        SourceReferenceWriter.idempotency_key(document_candidate(result_position=1))
    )
    assert SourceReferenceWriter.idempotency_key(pasted) == (
        SourceReferenceWriter.idempotency_key(pasted)
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_writer_accepts_server_resolved_document_selection_candidates():
    candidate = document_selection_candidate()
    client = RecordingPostgresClient(fetch_one_results=[persisted_row(candidate)])

    references = await SourceReferenceWriter(client).write_for_assistant_message(
        101,
        [candidate],
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        readable_project_ids=["project-1"],
    )

    assert references[0].encounter_kind == "document_selection"
    assert references[0].tool_call_id is None
    assert SourceReferenceWriter.idempotency_key(candidate) == (
        SourceReferenceWriter.idempotency_key(candidate)
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_reader_returns_only_message_scope_references_in_stable_order():
    candidate = document_candidate()
    client = RecordingPostgresClient(fetch_all_results=[[persisted_row(candidate)]])
    reader = SourceReferenceReader(client)

    references = await reader.get_message_source_refs(
        101,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert references[0].locator.page == 2
    assert references[0].source_status == "available"
    assert references[0].contributing_message_id == 101
    query, params = client.calls[0][1], client.calls[0][2]
    assert "JOIN public.sessions AS session" in query
    assert "session.user_name = %s" in query
    assert "ORDER BY ref.created_at ASC" in query
    assert params == (101, "project-1", "session-1", "ada")


@pytest.mark.storage
@pytest.mark.no_network
async def test_reader_marks_a_deleted_document_source_unavailable():
    candidate = document_candidate()
    client = RecordingPostgresClient(
        fetch_all_results=[[persisted_row(candidate, document_status="deleted")]]
    )
    reader = SourceReferenceReader(client)

    references = await reader.get_message_source_refs(
        101,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert references[0].source_status == "unavailable"


@pytest.mark.storage
@pytest.mark.no_network
async def test_reader_marks_a_replaced_document_version_unavailable():
    candidate = document_candidate()
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                persisted_row(
                    candidate,
                    document_status="indexed",
                    document_content_hash="a" * 64,
                )
            ]
        ]
    )
    reader = SourceReferenceReader(client)

    references = await reader.get_message_source_refs(
        101,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert references[0].source_status == "unavailable"
    assert references[0].excerpt == candidate.excerpt
    assert "document.content_hash AS document_content_hash" in client.calls[0][1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_reader_marks_web_results_as_search_result_snippets():
    candidate = web_candidate()
    client = RecordingPostgresClient(fetch_all_results=[[persisted_row(candidate)]])
    reader = SourceReferenceReader(client)

    references = await reader.get_message_source_refs(
        101,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert references[0].source_status == "search_result_snippet"
    assert references[0].canonical_url == "https://example.test/release"


@pytest.mark.storage
@pytest.mark.no_network
async def test_reader_marks_read_web_page_sources_available_without_refetching():
    candidate = web_page_candidate()
    client = RecordingPostgresClient(fetch_all_results=[[persisted_row(candidate)]])
    reader = SourceReferenceReader(client)

    references = await reader.get_message_source_refs(
        101,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert references[0].source_status == "available"
    assert references[0].canonical_url == "https://example.test/report"
    assert references[0].locator.model_dump(exclude_none=True) == {
        "kind": "text_lines",
        "start_line": 151,
        "end_line": 220,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_reader_marks_read_external_pdf_sources_available_without_refetching():
    candidate = web_pdf_candidate()
    client = RecordingPostgresClient(fetch_all_results=[[persisted_row(candidate)]])
    reader = SourceReferenceReader(client)

    references = await reader.get_message_source_refs(
        101,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert references[0].source_status == "available"
    assert references[0].canonical_url == "https://example.test/report.pdf"
    assert references[0].locator.model_dump(exclude_none=True) == {
        "kind": "pdf_page",
        "page": 2,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_traverses_only_episode_message_attachments():
    candidate = document_candidate()
    client = RecordingPostgresClient(fetch_all_results=[[persisted_row(candidate)]])
    reader = SourceReferenceReader(client)

    references = await reader.get_episode_source_refs(
        "episode-1",
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert references[0].contributing_message_id == 101
    query, params = client.calls[0][1], client.calls[0][2]
    assert "FROM public.episode_messages AS attachment" in query
    assert "ref.message_id = attachment.message_id" in query
    assert params == ("episode-1", "project-1", "session-1", "ada")


@pytest.mark.storage
@pytest.mark.no_network
async def test_reader_returns_one_assistant_answer_with_sources_consulted():
    candidate = document_candidate()
    client = RecordingPostgresClient(
        fetch_one_results=[{"message_id": 101, "content": "Answer"}],
        fetch_all_results=[[persisted_row(candidate)]],
    )
    reader = SourceReferenceReader(client)

    answer = await reader.get_assistant_message_with_sources(
        101,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert answer is not None
    assert answer.content == "Answer"
    assert answer.sources_consulted[0].contributing_message_id == 101
    assert "message.role = 'assistant'" in client.calls[0][1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_reader_returns_an_empty_source_collection_for_an_answer_without_refs():
    client = RecordingPostgresClient(
        fetch_one_results=[{"message_id": 101, "content": "Answer"}],
        fetch_all_results=[[]],
    )
    reader = SourceReferenceReader(client)

    answer = await reader.get_assistant_message_with_sources(
        101,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert answer is not None
    assert answer.sources_consulted == ()


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_reader_deduplicates_retries_but_keeps_other_answers():
    candidate = document_candidate()
    duplicate = persisted_row(
        candidate,
        source_ref_id="00000000-0000-0000-0000-000000000202",
    )
    other_answer = persisted_row(
        candidate,
        source_ref_id="00000000-0000-0000-0000-000000000203",
        message_id=102,
    )
    client = RecordingPostgresClient(
        fetch_all_results=[[persisted_row(candidate), duplicate, other_answer]]
    )
    reader = SourceReferenceReader(client)

    references = await reader.get_episode_source_refs(
        "episode-1",
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert [reference.contributing_message_id for reference in references] == [101, 102]


async def _seed_scope(real_postgres_client):
    await real_postgres_client.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content
        )
        VALUES
            ('ada', 'session-1', 101, 'project-1', 'assistant', 'Answer'),
            ('ada', 'session-1', 102, 'project-1', 'user', 'Pasted text')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_documents (
            document_id, project_id, visibility_scope, source_kind,
            original_name, relative_path, extension, size_bytes, content_hash
        )
        VALUES (
            %s, 'project-1', 'project', 'manual_upload',
            'report.pdf', '/report.pdf', '.pdf', 10, %s
        )
        """,
        (DOCUMENT_ID, CONTENT_HASH),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_documents (
            document_id, project_id, visibility_scope, source_kind,
            original_name, relative_path, extension, size_bytes, content_hash
        )
        VALUES (
            %s, 'project-1', 'project', 'manual_upload',
            'notes.md', '/notes.md', '.md', 10, %s
        )
        """,
        (TEXT_DOCUMENT_ID, "d" * 64),
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_real_postgres_document_tombstone_preserves_message_provenance(
    real_postgres_client,
):
    await _seed_scope(real_postgres_client)
    writer = SourceReferenceWriter(real_postgres_client)
    reader = SourceReferenceReader(real_postgres_client)

    document = document_candidate()
    first = await writer.write_for_assistant_message(
        101,
        [document],
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        readable_project_ids=["project-1"],
    )
    retried = await writer.write_for_assistant_message(
        101,
        [document],
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        readable_project_ids=["project-1"],
    )

    assert first[0].source_ref_id == retried[0].source_ref_id
    assert (
        len(
            await reader.get_message_source_refs(
                101,
                user_name="ada",
                project_id="project-1",
                session_id="session-1",
            )
        )
        == 1
    )

    deleted = await DocumentWriter(real_postgres_client, "project-1").delete_document(
        document_id=DOCUMENT_ID,
        session_id=None,
    )
    assert deleted is not None
    assert deleted["status"] == "deleted"
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.message_source_refs"
    ) == {"count": 1}
    sources = await reader.get_message_source_refs(
        101,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )
    assert sources[0].source_status == "unavailable"


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_real_postgres_provenance_uses_captured_cross_project_document_scope(
    real_postgres_client,
):
    await _seed_scope(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES ('session-2', 'ada', 'project-2')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_documents (
            document_id, project_id, visibility_scope, source_kind,
            original_name, relative_path, extension, size_bytes, content_hash
        ) VALUES (
            %s, 'project-2', 'project', 'manual_upload',
            'shared.pdf', '/shared.pdf', '.pdf', 10, %s
        )
        """,
        (CROSS_PROJECT_DOCUMENT_ID, "e" * 64),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_documents (
            document_id, project_id, session_id, visibility_scope, source_kind,
            original_name, relative_path, extension, size_bytes, content_hash
        ) VALUES (
            %s, 'project-2', 'session-2', 'session', 'manual_upload',
            'private.pdf', '/private.pdf', '.pdf', 10, %s
        )
        """,
        (CROSS_PROJECT_SESSION_DOCUMENT_ID, "f" * 64),
    )
    writer = SourceReferenceWriter(real_postgres_client)
    reader = SourceReferenceReader(real_postgres_client)
    readable_scope = ["project-1", "project-2"]
    shared = document_candidate(
        document_id=CROSS_PROJECT_DOCUMENT_ID,
        source_project_id="project-2",
        content_hash="e" * 64,
    )

    references = await writer.write_for_assistant_message(
        101,
        [shared],
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        readable_project_ids=readable_scope,
    )

    assert references[0].project_id == "project-1"
    assert references[0].source_project_id == "project-2"
    presented = await reader.get_message_source_refs(
        101,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )
    assert presented[0].source_project_id == "project-2"
    assert presented[0].source_status == "available"
    with pytest.raises(ValueError, match="not visible"):
        await writer.write_for_assistant_message(
            101,
            [
                document_candidate(
                    document_id=CROSS_PROJECT_SESSION_DOCUMENT_ID,
                    source_project_id="project-2",
                    content_hash="f" * 64,
                    agent_run_id="private-run",
                    tool_call_id="private-call",
                )
            ],
            user_name="ada",
            project_id="project-1",
            session_id="session-1",
            readable_project_ids=readable_scope,
        )
    with pytest.raises(ValueError, match="captured readable scope"):
        await writer.write_for_assistant_message(
            101,
            [shared],
            user_name="ada",
            project_id="project-1",
            session_id="session-1",
            readable_project_ids=["project-1"],
        )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_real_postgres_preserves_cross_project_provenance_after_source_deletion(
    real_postgres_client,
):
    await _seed_scope(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES ('session-2', 'ada', 'project-2')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_documents (
            document_id, project_id, visibility_scope, source_kind,
            original_name, relative_path, extension, size_bytes, content_hash
        ) VALUES (
            %s, 'project-2', 'project', 'manual_upload',
            'shared.pdf', '/shared.pdf', '.pdf', 10, %s
        )
        """,
        (CROSS_PROJECT_DOCUMENT_ID, "e" * 64),
    )
    candidate = document_candidate(
        document_id=CROSS_PROJECT_DOCUMENT_ID,
        source_project_id="project-2",
        content_hash="e" * 64,
    )
    writer = SourceReferenceWriter(real_postgres_client)
    await writer.write_for_assistant_message(
        101,
        [candidate],
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        readable_project_ids=["project-1", "project-2"],
    )

    deleted = await ProjectDeletionWriter(real_postgres_client).delete_project(
        user_name="ada",
        project_id="project-2",
    )
    sources = await SourceReferenceReader(real_postgres_client).get_message_source_refs(
        101,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert deleted == {"entities": 0, "projects": 1}
    assert len(sources) == 1
    assert sources[0].document_id == CROSS_PROJECT_DOCUMENT_ID
    assert sources[0].source_project_id == "project-2"
    assert sources[0].excerpt == candidate.excerpt
    assert sources[0].source_status == "unavailable"


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_real_postgres_marks_replaced_document_provenance_unavailable(
    real_postgres_client,
):
    await _seed_scope(real_postgres_client)
    document = document_candidate()
    await SourceReferenceWriter(real_postgres_client).write_for_assistant_message(
        101,
        [document],
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        readable_project_ids=["project-1"],
    )
    await real_postgres_client.execute(
        "UPDATE public.project_documents SET content_hash = %s WHERE document_id = %s",
        ("a" * 64, DOCUMENT_ID),
    )

    sources = await SourceReferenceReader(real_postgres_client).get_message_source_refs(
        101,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert sources[0].source_status == "unavailable"
    assert sources[0].excerpt == document.excerpt


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_real_postgres_exposes_all_source_families_on_answer_and_episode(
    real_postgres_client,
):
    await _seed_scope(real_postgres_client)
    writer = SourceReferenceWriter(real_postgres_client)
    reader = SourceReferenceReader(real_postgres_client)
    candidates = [
        document_candidate(),
        text_document_candidate(),
        docx_document_candidate(),
        pasted_text_candidate(),
        web_candidate(),
        news_candidate(),
        web_page_candidate(),
        web_pdf_candidate(),
    ]

    await writer.write_for_assistant_message(
        101,
        candidates,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        readable_project_ids=["project-1"],
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.episodes (
            episode_id, project_id, summary, source_message_count
        )
        VALUES ('episode-1', 'project-1', 'Summary', 1)
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.episode_messages (
            episode_id, project_id, session_id, message_id, message_position
        )
        VALUES ('episode-1', 'project-1', 'session-1', 101, 0)
        """
    )

    answer = await reader.get_assistant_message_with_sources(
        101,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )
    episode_sources = await reader.get_episode_source_refs(
        "episode-1",
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert answer is not None
    assert [source.source_kind for source in answer.sources_consulted] == [
        "pdf_document",
        "text_document",
        "text_document",
        "user_pasted_text",
        "web_search_result",
        "news_search_result",
        "web_page",
        "web_pdf",
    ]
    assert [source.source_status for source in answer.sources_consulted[-4:]] == [
        "search_result_snippet",
        "search_result_snippet",
        "available",
        "available",
    ]
    assert [source.contributing_message_id for source in episode_sources] == [101] * 8

    await real_postgres_client.execute(
        "DELETE FROM public.messages "
        "WHERE message_id = 101 AND project_id = 'project-1'"
    )
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.message_source_refs"
    ) == {"count": 0}
    assert (
        await reader.get_episode_source_refs(
            "episode-1",
            user_name="ada",
            project_id="project-1",
            session_id="session-1",
        )
        == []
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_real_postgres_enforces_source_shape_without_source_document_fk(
    real_postgres_client,
):
    await _seed_scope(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES ('session-2', 'ada', 'project-2')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content
        )
        VALUES ('ada', 'session-2', 201, 'project-2', 'assistant', 'Answer')
        """
    )
    valid_locator = '{"kind":"pdf_page","page":2}'
    valid_metadata = '{"document_name":"report.pdf"}'

    with pytest.raises(CheckViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO public.message_source_refs (
                source_ref_id, project_id, session_id, message_id, source_kind,
                document_id, source_project_id, content_hash, locator, excerpt, metadata,
                encounter_kind, agent_run_id, tool_call_id, result_position,
                idempotency_key
            )
            VALUES (
                '00000000-0000-0000-0000-000000000202',
                'project-1', 'session-1', 101, 'pdf_document', %s, 'project-1', %s,
                '{"kind":"search_result"}', 'excerpt', %s,
                'document_search', 'run-invalid', 'call-invalid', 0,
                'invalid-shape'
            )
            """,
            (DOCUMENT_ID, CONTENT_HASH, valid_metadata),
        )

    await real_postgres_client.execute(
        """
        INSERT INTO public.message_source_refs (
            source_ref_id, project_id, session_id, message_id, source_kind,
            document_id, source_project_id, content_hash, locator, excerpt, metadata,
            encounter_kind, agent_run_id, tool_call_id, result_position,
            idempotency_key
        )
        VALUES (
            '00000000-0000-0000-0000-000000000203',
            'project-2', 'session-2', 201, 'pdf_document', %s, 'project-2', %s, %s,
            'excerpt', %s, 'document_search', 'run-cross-project',
            'call-cross-project', 0, 'cross-project-document'
        )
        """,
        (DOCUMENT_ID, CONTENT_HASH, valid_locator, valid_metadata),
    )
    await real_postgres_client.execute(
        "DELETE FROM public.message_source_refs WHERE source_ref_id = %s",
        ("00000000-0000-0000-0000-000000000203",),
    )

    writer = SourceReferenceWriter(real_postgres_client)
    await writer.write_for_assistant_message(
        101,
        [pasted_text_candidate()],
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        readable_project_ids=["project-1"],
    )
    await real_postgres_client.execute(
        "DELETE FROM public.messages "
        "WHERE message_id = 102 AND project_id = 'project-1'"
    )
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.message_source_refs"
    ) == {"count": 0}
