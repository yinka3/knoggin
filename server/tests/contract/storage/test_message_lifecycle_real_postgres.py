import asyncio

import pytest

from common.exceptions import IdempotencyConflictError
from common.schema.source.references import SourceReferenceCandidate
from core.knowledge.db.writers.message_lifecycle_writer import (
    MessageLifecycleWriter,
)
from core.knowledge.db.writers.message_writer import MessageWriter
from core.knowledge.store import KnowledgeStore


async def _seed_session(client, session_id: str) -> None:
    await client.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES (%s, 'ada', 'project-1')
        """,
        (session_id,),
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_real_postgres_accepts_concurrent_user_message_request_once(
    real_postgres_client,
):
    await _seed_session(real_postgres_client, "session-acceptance")
    lifecycle = MessageLifecycleWriter(
        real_postgres_client,
        MessageWriter(real_postgres_client),
    )

    def message(message_id: int) -> dict:
        return {
            "id": message_id,
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": "session-acceptance",
            "role": "user",
            "content": "accept exactly once",
            "timestamp": 1_754_064_000_000,
            "metadata": {
                "idempotency_key": "request-1",
                "request_fingerprint": "a" * 64,
            },
            "acceptance_key": "request:request-1",
            "request_fingerprint": "a" * 64,
        }

    accepted = await asyncio.gather(
        *(
            lifecycle.create_editable_user_message(
                message(message_id), edit_window_seconds=600
            )
            for message_id in range(1001, 1009)
        )
    )

    accepted_ids = {result.message_id for result in accepted}
    assert len(accepted_ids) == 1
    assert sum(result.created for result in accepted) == 1
    rows = await real_postgres_client.fetch_all(
        """
        SELECT message_id, acceptance_key
        FROM public.messages
        WHERE user_name = 'ada' AND session_id = 'session-acceptance'
        """
    )
    assert rows == [
        {"message_id": next(iter(accepted_ids)), "acceptance_key": "request:request-1"}
    ]
    assert await real_postgres_client.fetch_all(
        """
        SELECT message_id, revision
        FROM public.message_revisions
        WHERE user_name = 'ada' AND session_id = 'session-acceptance'
        """
    ) == [{"message_id": next(iter(accepted_ids)), "revision": 1}]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_real_postgres_rejects_a_request_key_reused_for_a_different_payload(
    real_postgres_client,
):
    await _seed_session(real_postgres_client, "session-request-conflict")
    lifecycle = MessageLifecycleWriter(
        real_postgres_client,
        MessageWriter(real_postgres_client),
    )

    def message(message_id: int, *, fingerprint: str, content: str) -> dict:
        return {
            "id": message_id,
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": "session-request-conflict",
            "role": "user",
            "content": content,
            "timestamp": 1_754_064_000_000,
            "metadata": {"request_fingerprint": fingerprint},
            "acceptance_key": "request:retry-1",
            "request_fingerprint": fingerprint,
        }

    await lifecycle.create_editable_user_message(
        message(1101, fingerprint="a" * 64, content="first request"),
        edit_window_seconds=600,
    )
    with pytest.raises(IdempotencyConflictError):
        await lifecycle.create_editable_user_message(
            message(1102, fingerprint="b" * 64, content="changed request"),
            edit_window_seconds=600,
        )

    assert await real_postgres_client.fetch_all(
        """
        SELECT message_id, content
        FROM public.messages
        WHERE session_id = 'session-request-conflict'
        """
    ) == [{"message_id": 1101, "content": "first request"}]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_real_postgres_final_assistant_response_and_exchange_close_are_atomic_and_idempotent(
    real_postgres_client,
):
    await _seed_session(real_postgres_client, "session-final")
    lifecycle = MessageLifecycleWriter(
        real_postgres_client,
        MessageWriter(real_postgres_client),
    )
    await lifecycle.create_editable_user_message(
        {
            "id": 501,
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": "session-final",
            "role": "user",
            "content": "Keep the response grounded.",
            "timestamp": 1_000,
            "metadata": {"request_fingerprint": "b" * 64},
            "acceptance_key": "request:final-501",
            "request_fingerprint": "b" * 64,
        },
        edit_window_seconds=600,
    )
    store = KnowledgeStore(real_postgres_client, object())
    candidate = SourceReferenceCandidate(
        project_id="project-1",
        session_id="session-final",
        source_kind="user_pasted_text",
        source_message_id=501,
        content_hash="a" * 64,
        locator={"kind": "character_span", "start_char": 0, "end_char": 6},
        excerpt="source",
        metadata={"pasted_text": True},
        encounter_kind="user_pasted_text",
        agent_run_id="run-final-501",
        result_position=0,
    )
    message = {
        "id": 502,
        "role": "assistant",
        "user_name": "ada",
        "project_id": "project-1",
        "session_id": "session-final",
        "content": "The response is grounded.",
        "timestamp": 2_000,
        "metadata": {},
        "user_msg_id": 501,
        "lifecycle_state": "sealed",
        "sealed_at_ms": 2_000,
    }

    persisted_id, source_ref_ids, created = await store.finalize_assistant_exchange(
        message,
        [candidate],
        readable_project_ids=["project-1"],
    )
    duplicate_id, duplicate_source_ref_ids, duplicate_created = (
        await store.finalize_assistant_exchange(
            {**message, "id": 503, "content": "Must not be inserted."},
            [candidate],
            readable_project_ids=["project-1"],
        )
    )

    assert (persisted_id, created) == (502, True)
    assert source_ref_ids
    assert (duplicate_id, duplicate_source_ref_ids, duplicate_created) == (
        502,
        source_ref_ids,
        False,
    )
    assert await real_postgres_client.fetch_one(
        """
        SELECT lifecycle_state, exchange_state, exchange_outcome,
               exchange_closed_at_ms
        FROM public.messages
        WHERE message_id = 501
        """
    ) == {
        "lifecycle_state": "sealed",
        "exchange_state": "closed",
        "exchange_outcome": "assistant_final",
        "exchange_closed_at_ms": 2_000,
    }
    assert await real_postgres_client.fetch_all(
        """
        SELECT message_id, content
        FROM public.messages
        WHERE session_id = 'session-final' AND role = 'assistant'
        ORDER BY message_id
        """
    ) == [{"message_id": 502, "content": "The response is grounded."}]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_real_postgres_persists_and_reloads_a_clarification_exchange(
    real_postgres_client,
):
    await _seed_session(real_postgres_client, "session-clarification")
    lifecycle = MessageLifecycleWriter(
        real_postgres_client,
        MessageWriter(real_postgres_client),
    )
    await lifecycle.create_editable_user_message(
        {
            "id": 531,
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": "session-clarification",
            "role": "user",
            "content": "Help choose a profile.",
            "timestamp": 1_000,
            "metadata": {"request_fingerprint": "c" * 64},
            "acceptance_key": "request:clarification-531",
            "request_fingerprint": "c" * 64,
        },
        edit_window_seconds=600,
    )
    store = KnowledgeStore(real_postgres_client, object())
    candidate = SourceReferenceCandidate(
        project_id="project-1",
        session_id="session-clarification",
        source_kind="user_pasted_text",
        source_message_id=531,
        content_hash="c" * 64,
        locator={"kind": "character_span", "start_char": 0, "end_char": 4},
        excerpt="Help",
        metadata={"pasted_text": True},
        encounter_kind="user_pasted_text",
        agent_run_id="run-clarification-531",
        result_position=0,
    )
    message = {
        "id": 532,
        "role": "assistant",
        "user_name": "ada",
        "project_id": "project-1",
        "session_id": "session-clarification",
        "content": "Which profile should I use?",
        "timestamp": 2_000,
        "metadata": {
            "usage": {
                "prompt_tokens": 3,
                "completion_tokens": 0,
                "total_tokens": 3,
                "approximate": False,
            }
        },
        "user_msg_id": 531,
        "lifecycle_state": "sealed",
        "sealed_at_ms": 2_000,
    }

    persisted_id, source_ref_ids, created = await store.finalize_assistant_exchange(
        message,
        [candidate],
        readable_project_ids=["project-1"],
        outcome="clarification",
    )
    duplicate_id, duplicate_source_ref_ids, duplicate_created = (
        await store.finalize_assistant_exchange(
            {**message, "id": 533, "content": "Do not insert this duplicate."},
            [candidate],
            readable_project_ids=["project-1"],
            outcome="clarification",
        )
    )
    exchange = await store.get_user_agent_exchange(
        531,
        user_name="ada",
        project_id="project-1",
        session_id="session-clarification",
    )

    assert (persisted_id, created) == (532, True)
    assert source_ref_ids
    assert (duplicate_id, duplicate_source_ref_ids, duplicate_created) == (
        532,
        source_ref_ids,
        False,
    )
    assert exchange is not None
    assert exchange.exchange_state == "closed"
    assert exchange.exchange_outcome == "clarification"
    assert exchange.assistant_message_id == 532
    assert exchange.assistant_content == "Which profile should I use?"
    assert exchange.assistant_metadata == message["metadata"]
    assert exchange.source_ref_ids == tuple(source_ref_ids)
    assert await real_postgres_client.fetch_one(
        """
        SELECT exchange_state, exchange_outcome
        FROM public.messages
        WHERE message_id = 531
        """
    ) == {"exchange_state": "closed", "exchange_outcome": "clarification"}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_real_postgres_finalizes_historical_document_sources_and_rolls_back_fabrication(
    real_postgres_client,
):
    session_id = "session-final-history"
    await _seed_session(real_postgres_client, session_id)
    lifecycle = MessageLifecycleWriter(
        real_postgres_client,
        MessageWriter(real_postgres_client),
    )
    await lifecycle.create_editable_user_message(
        {
            "id": 511,
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": session_id,
            "role": "user",
            "content": "Use the captured document passage.",
            "timestamp": 1_000,
            "metadata": {"request_fingerprint": "c" * 64},
            "acceptance_key": "request:history-511",
            "request_fingerprint": "c" * 64,
        },
        edit_window_seconds=600,
    )
    document_id = "00000000-0000-0000-0000-000000000511"
    snapshot_id = "00000000-0000-0000-0000-000000001511"
    captured_hash = "a" * 64
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_documents (
            document_id, project_id, original_name, relative_path, extension,
            size_bytes, content_hash
        ) VALUES (%s, 'project-1', 'history.pdf', '/history.pdf', '.pdf', 10, %s)
        """,
        (document_id, captured_hash),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.document_parse_snapshots (
            snapshot_id, document_id, source_content_hash,
            parser_name, parser_version, parser_fingerprint, snapshot
        ) VALUES (%s, %s, %s, 'test', 'v1', %s, '{"schema_version": 1}'::jsonb)
        """,
        (snapshot_id, document_id, captured_hash, "d" * 64),
    )
    await real_postgres_client.execute(
        """
        UPDATE public.project_documents
        SET current_snapshot_id = %s, status = 'indexed'
        WHERE document_id = %s
        """,
        (snapshot_id, document_id),
    )
    candidate = SourceReferenceCandidate(
        project_id="project-1",
        session_id=session_id,
        source_kind="pdf_document",
        document_id=document_id,
        parse_snapshot_id=snapshot_id,
        source_project_id="project-1",
        content_hash=captured_hash,
        locator={
            "kind": "layout_region",
            "page": 1,
            "element_type": "page",
            "extraction_method": "native_text",
        },
        excerpt="The version-A passage.",
        metadata={"document_name": "history.pdf"},
        encounter_kind="document_search",
        agent_run_id="run-history-511",
        tool_call_id="call-history-511",
        result_position=0,
    )
    await real_postgres_client.execute(
        "UPDATE public.project_documents SET content_hash = %s WHERE document_id = %s",
        ("b" * 64, document_id),
    )
    store = KnowledgeStore(real_postgres_client, object())
    message = {
        "id": 512,
        "role": "assistant",
        "user_name": "ada",
        "project_id": "project-1",
        "session_id": session_id,
        "content": "The answer used version A.",
        "timestamp": 2_000,
        "metadata": {},
        "user_msg_id": 511,
        "lifecycle_state": "sealed",
        "sealed_at_ms": 2_000,
    }

    persisted_id, source_ref_ids, created = await store.finalize_assistant_exchange(
        message,
        [candidate],
        readable_project_ids=["project-1"],
    )
    duplicate_id, duplicate_ref_ids, duplicate_created = (
        await store.finalize_assistant_exchange(
            {**message, "id": 513, "content": "Duplicate answer."},
            [candidate],
            readable_project_ids=["project-1"],
        )
    )
    sources = await store.get_message_source_refs(
        512,
        user_name="ada",
        project_id="project-1",
        session_id=session_id,
    )

    assert (persisted_id, created) == (512, True)
    assert source_ref_ids
    assert (duplicate_id, duplicate_ref_ids, duplicate_created) == (
        512,
        source_ref_ids,
        False,
    )
    assert sources[0].source_status == "historical"
    assert sources[0].excerpt == candidate.excerpt

    await lifecycle.create_editable_user_message(
        {
            "id": 521,
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": session_id,
            "role": "user",
            "content": "Reject an invented document.",
            "timestamp": 3_000,
            "metadata": {"request_fingerprint": "d" * 64},
            "acceptance_key": "request:history-521",
            "request_fingerprint": "d" * 64,
        },
        edit_window_seconds=600,
    )
    fabricated = candidate.model_copy(
        update={
            "document_id": "00000000-0000-0000-0000-000000000599",
            "agent_run_id": "run-history-521",
            "tool_call_id": "call-history-521",
        }
    )

    with pytest.raises(ValueError, match="document is not visible"):
        await store.finalize_assistant_exchange(
            {**message, "id": 522, "user_msg_id": 521},
            [fabricated],
            readable_project_ids=["project-1"],
        )

    assert await real_postgres_client.fetch_all(
        """
        SELECT message_id, role
        FROM public.messages
        WHERE session_id = %s
        ORDER BY message_id
        """,
        (session_id,),
    ) == [
        {"message_id": 511, "role": "user"},
        {"message_id": 512, "role": "assistant"},
        {"message_id": 521, "role": "user"},
    ]
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.message_source_refs WHERE message_id = 522"
    ) == {"count": 0}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_real_postgres_failure_and_cancellation_close_user_evidence(real_postgres_client):
    await _seed_session(real_postgres_client, "session-terminal")
    lifecycle = MessageLifecycleWriter(
        real_postgres_client,
        MessageWriter(real_postgres_client),
    )
    for message_id in (601, 602):
        await lifecycle.create_editable_user_message(
            {
                "id": message_id,
                "user_name": "ada",
                "project_id": "project-1",
                "session_id": "session-terminal",
                "role": "user",
                "content": f"Turn {message_id}",
                "timestamp": message_id,
                "metadata": {"request_fingerprint": f"terminal-{message_id}"},
                "acceptance_key": f"request:terminal-{message_id}",
                "request_fingerprint": f"terminal-{message_id}",
            },
            edit_window_seconds=600,
        )
    store = KnowledgeStore(real_postgres_client, object())

    await store.close_user_exchange(
        user_name="ada",
        project_id="project-1",
        session_id="session-terminal",
        user_message_id=601,
        outcome="failed",
        closed_at_ms=3_000,
        terminal_error={"code": "llm_budget_exhausted", "retryable": False},
    )
    await store.close_user_exchange(
        user_name="ada",
        project_id="project-1",
        session_id="session-terminal",
        user_message_id=602,
        outcome="cancelled",
        closed_at_ms=3_100,
    )
    failed_exchange = await store.get_user_agent_exchange(
        601,
        user_name="ada",
        project_id="project-1",
        session_id="session-terminal",
    )

    assert failed_exchange is not None
    assert failed_exchange.terminal_error == {
        "code": "llm_budget_exhausted",
        "retryable": False,
    }

    assert await real_postgres_client.fetch_all(
        """
        SELECT message_id, lifecycle_state, exchange_state, exchange_outcome,
               exchange_closed_at_ms, metadata -> 'terminal_error' AS terminal_error
        FROM public.messages
        WHERE session_id = 'session-terminal'
        ORDER BY message_id
        """
    ) == [
        {
            "message_id": 601,
            "lifecycle_state": "sealed",
            "exchange_state": "closed",
            "exchange_outcome": "failed",
            "exchange_closed_at_ms": 3_000,
            "terminal_error": {
                "code": "llm_budget_exhausted",
                "retryable": False,
            },
        },
        {
            "message_id": 602,
            "lifecycle_state": "sealed",
            "exchange_state": "closed",
            "exchange_outcome": "cancelled",
            "exchange_closed_at_ms": 3_100,
            "terminal_error": None,
        },
    ]
