import asyncio

import pytest

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
async def test_real_postgres_resets_startup_claims_and_claims_a_partial_fifo_batch(
    real_postgres_client,
):
    """A fresh runtime releases only its own claims, then resumes FIFO work."""

    await _seed_session(real_postgres_client, "session-1")
    await _seed_session(real_postgres_client, "session-2")
    await real_postgres_client.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, lifecycle_state, ingestion_state,
            ingestion_not_before_ms, ingestion_claim_id, ingestion_claimed_at_ms
        ) VALUES
            ('ada', 'session-1', 101, 'project-1', 'user', 'first', 101,
             'sealed', 'claimed', 0, 'abandoned-session-1', 0),
            ('ada', 'session-1', 102, 'project-1', 'user', 'second', 102,
             'sealed', 'claimed', 0, 'abandoned-session-1', 0),
            ('ada', 'session-2', 201, 'project-1', 'user', 'other scope', 201,
             'sealed', 'claimed', 0, 'abandoned-session-2', 0)
        """
    )
    lifecycle = MessageLifecycleWriter(
        real_postgres_client,
        MessageWriter(real_postgres_client),
    )

    reset = await lifecycle.reset_claimed_ingestion(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert reset == [101, 102]
    assert await real_postgres_client.fetch_one(
        """
        SELECT ingestion_claim_id
        FROM public.messages
        WHERE session_id = 'session-2' AND message_id = 201
        """
    ) == {"ingestion_claim_id": "abandoned-session-2"}

    claim = await lifecycle.claim_next_batch(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        batch_size=8,
    )

    assert claim is not None
    assert [message["id"] for message in claim.messages] == [101, 102]
    assert claim.batch_id != "abandoned-session-1"
    assert await real_postgres_client.fetch_all(
        """
        SELECT message_id, ingestion_state, ingestion_claim_id
        FROM public.messages
        WHERE session_id = 'session-1'
        ORDER BY message_id
        """
    ) == [
        {
            "message_id": 101,
            "ingestion_state": "claimed",
            "ingestion_claim_id": claim.batch_id,
        },
        {
            "message_id": 102,
            "ingestion_state": "claimed",
            "ingestion_claim_id": claim.batch_id,
        },
    ]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_real_postgres_ingestion_failure_metadata_blocks_and_retries(
    real_postgres_client,
):
    await _seed_session(real_postgres_client, "session-1")
    await real_postgres_client.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, lifecycle_state, ingestion_state, ingestion_not_before_ms,
            ingestion_claim_id, ingestion_attempt_count
        ) VALUES (
            'ada', 'session-1', 101, 'project-1', 'user', 'first', 101,
            'sealed', 'claimed', 0, 'claim-1', 1
        )
        """
    )
    lifecycle = MessageLifecycleWriter(
        real_postgres_client,
        MessageWriter(real_postgres_client),
    )

    terminal = await lifecycle.fail_ingestion_claim(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        batch_id="claim-1",
        failure_stage="model",
        failure_code="llm_provider_error",
        error_summary="provider temporarily unavailable",
        retryable=True,
        max_attempts=3,
    )

    assert terminal is False
    assert await real_postgres_client.fetch_one(
        """
        SELECT ingestion_state, ingestion_attempt_count,
               ingestion_last_failure_stage, ingestion_last_failure_code,
               ingestion_last_error_summary
        FROM public.messages
        WHERE message_id = 101
        """
    ) == {
        "ingestion_state": "ready",
        "ingestion_attempt_count": 2,
        "ingestion_last_failure_stage": "model",
        "ingestion_last_failure_code": "llm_provider_error",
        "ingestion_last_error_summary": "provider temporarily unavailable",
    }

    claim = await lifecycle.claim_next_batch(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        batch_size=8,
    )
    assert claim is not None

    terminal = await lifecycle.fail_ingestion_claim(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        batch_id=claim.batch_id,
        failure_stage="model",
        failure_code="llm_provider_error",
        error_summary="provider temporarily unavailable",
        retryable=True,
        max_attempts=3,
    )
    assert terminal is True
    assert await lifecycle.retry_failed_ingestion(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        message_ids=[101],
    ) == [101]
    assert await real_postgres_client.fetch_one(
        """
        SELECT ingestion_state, ingestion_attempt_count, ingestion_last_failure_code
        FROM public.messages
        WHERE message_id = 101
        """
    ) == {
        "ingestion_state": "ready",
        "ingestion_attempt_count": 3,
        "ingestion_last_failure_code": "llm_provider_error",
    }


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
            "metadata": {"idempotency_key": "request-1"},
            "acceptance_key": "request:request-1",
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
            "metadata": {},
            "acceptance_key": "request:final-501",
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
        "ingestion_state": "excluded",
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
               exchange_closed_at_ms, ingestion_state
        FROM public.messages
        WHERE message_id = 501
        """
    ) == {
        "lifecycle_state": "sealed",
        "exchange_state": "closed",
        "exchange_outcome": "assistant_final",
        "exchange_closed_at_ms": 2_000,
        "ingestion_state": "ready",
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
                "metadata": {},
                "acceptance_key": f"request:terminal-{message_id}",
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
    )
    await store.close_user_exchange(
        user_name="ada",
        project_id="project-1",
        session_id="session-terminal",
        user_message_id=602,
        outcome="cancelled",
        closed_at_ms=3_100,
    )

    assert await real_postgres_client.fetch_all(
        """
        SELECT message_id, lifecycle_state, exchange_state, exchange_outcome,
               exchange_closed_at_ms
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
        },
        {
            "message_id": 602,
            "lifecycle_state": "sealed",
            "exchange_state": "closed",
            "exchange_outcome": "cancelled",
            "exchange_closed_at_ms": 3_100,
        },
    ]
