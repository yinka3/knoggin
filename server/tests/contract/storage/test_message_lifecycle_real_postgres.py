import asyncio

import pytest

from core.knowledge.db.writers.message_lifecycle_writer import (
    MessageLifecycleWriter,
)
from core.knowledge.db.writers.message_writer import MessageWriter


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

    blocked = await lifecycle.fail_ingestion_claim(
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

    assert blocked is False
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

    blocked = await lifecycle.fail_ingestion_claim(
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
    assert blocked is True
    assert await lifecycle.retry_blocked_ingestion(
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
