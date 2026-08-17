import pytest

from core.knowledge.db.writers.parked_dlq_writer import ParkedDLQWriter


@pytest.mark.storage
@pytest.mark.requires_postgres
async def test_parked_dlq_record_survives_redis_and_transitions_in_postgres(
    real_postgres_client,
):
    writer = ParkedDLQWriter(real_postgres_client)
    await writer.park(
        dlq_id="dlq-1",
        user_name="ada",
        project_id="project-1",
        entry={
            "session_id": "session-1",
            "stage": "graph_write",
            "attempt": 2,
            "error": "TimeoutError",
        },
    )

    parked = await writer.get_parked(
        dlq_id="dlq-1", user_name="ada", project_id="project-1"
    )
    assert parked is not None
    assert parked["session_id"] == "session-1"
    assert parked["attempt"] == 2
    review = await real_postgres_client.fetch_one(
        "SELECT status, subject_type, priority FROM human_reviews "
        "WHERE kind = %s AND subject_id = %s",
        ("parked_dlq", "dlq-1"),
    )
    assert review == {
        "status": "open",
        "subject_type": "parked_dlq_item",
        "priority": "high",
    }

    assert await writer.mark_requeued(
        dlq_id="dlq-1", user_name="ada", project_id="project-1"
    )
    assert (
        await writer.get_parked(
            dlq_id="dlq-1", user_name="ada", project_id="project-1"
        )
        is None
    )
    assert await writer.mark_completed_if_requeued(
        dlq_id="dlq-1", user_name="ada", project_id="project-1"
    )
    row = await real_postgres_client.fetch_one(
        "SELECT status, requeued_at, completed_at "
        "FROM parked_dlq_items WHERE dlq_id = %s",
        ("dlq-1",),
    )
    assert row["status"] == "completed"
    assert row["requeued_at"] is not None
    assert row["completed_at"] is not None
    review = await real_postgres_client.fetch_one(
        "SELECT status FROM human_reviews WHERE kind = %s AND subject_id = %s",
        ("parked_dlq", "dlq-1"),
    )
    assert review["status"] == "resolved"
