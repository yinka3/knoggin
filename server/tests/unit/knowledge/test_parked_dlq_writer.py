import json

import pytest

from core.knowledge.db.writers.parked_dlq_writer import ParkedDLQWriter
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.unit
@pytest.mark.no_network
async def test_parked_item_is_written_as_a_durable_postgres_subject():
    client = RecordingPostgresClient()
    entry = {
        "session_id": "session-1",
        "stage": "graph_write",
        "attempt": 2,
        "error": "TimeoutError",
    }

    await ParkedDLQWriter(client).park(
        dlq_id="dlq-1",
        user_name="ada",
        project_id="project-1",
        entry=entry,
    )

    assert client.transaction_enters == 1
    calls = [call for call in client.calls if call[0] == "execute"]
    assert len(calls) == 2
    call = calls[0]
    assert "INSERT INTO public.parked_dlq_items" in call[1]
    assert "ON CONFLICT (dlq_id)" in call[1]
    payload = json.loads(call[2][-1])
    assert payload["dlq_id"] == "dlq-1"
    assert payload["user_name"] == "ada"
    assert payload["project_id"] == "project-1"
    assert "INSERT INTO public.human_reviews" in calls[1][1]


@pytest.mark.unit
@pytest.mark.no_network
async def test_parked_item_transitions_are_conditional_and_idempotent():
    client = RecordingPostgresClient(fetch_one_results=[{"dlq_id": "dlq-1"}, None])
    writer = ParkedDLQWriter(client)

    assert await writer.mark_requeued(
        dlq_id="dlq-1", user_name="ada", project_id="project-1"
    )
    assert not await writer.mark_completed_if_requeued(
        dlq_id="dlq-1", user_name="ada", project_id="project-1"
    )

    transitions = [
        call
        for call in client.calls
        if call[0] == "execute" and "UPDATE public.parked_dlq_items" in call[1]
    ]
    assert len(transitions) == 2
    assert "status = %s" in transitions[0][1]
    assert transitions[0][2][-1] == "parked"
    assert transitions[1][2][-1] == "requeued"
    assert any(
        "UPDATE public.human_reviews" in call[1]
        for call in client.calls
        if call[0] == "execute"
    )
