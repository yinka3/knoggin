import pytest

from core.knowledge.db.writers.graph_writer import GraphWriter
from core.knowledge.db.writers.message_lifecycle_writer import (
    MessageLifecycleWriter,
)


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
async def test_real_postgres_reclaims_expired_ingestion_claim_without_redis(
    real_postgres_client,
):
    """A fresh worker reconstructs its next full batch from Postgres alone."""

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
        GraphWriter(real_postgres_client),
    )

    claim = await lifecycle.claim_next_full_batch(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        batch_size=2,
        claim_lease_seconds=1,
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
    assert await real_postgres_client.fetch_one(
        """
        SELECT ingestion_claim_id
        FROM public.messages
        WHERE session_id = 'session-2' AND message_id = 201
        """
    ) == {"ingestion_claim_id": "abandoned-session-2"}

