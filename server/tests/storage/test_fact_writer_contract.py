import json
from datetime import datetime, timezone

import pytest

from common.schema.primitives import FactRecord
from core.knowledge.db.writers.fact_writer import FactWriter
from tests.fixtures.fakes import RecordingPostgresClient

CREATE_FACTS_GRAPH_FIELDS = {
    "entity_id",
    "batch",
    "user_name",
    "session_id",
    "project_id",
}

FACT_GRAPH_FIELDS = {
    "id",
    "content",
    "valid_at",
    "invalid_at",
    "confidence",
    "source_msg_id",
    "source_user_name",
    "source_session_id",
    "source",
}


def make_fact(**overrides):
    data = {
        "id": "fact-1",
        "content": "Ada writes algorithms",
        "source_entity_id": 2,
        "valid_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "confidence": 0.8,
        "source_msg_id": None,
        "embedding": [0.1, 0.2, 0.3],
    }
    data.update(overrides)
    return FactRecord(**data)


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_writer_create_facts_batch_empty_list_skips_db():
    client = RecordingPostgresClient()
    writer = FactWriter(client)

    assert await writer.create_facts_batch(
        2,
        [],
        user_name="ada",
        project_id="project-1",
    ) == 0
    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_writer_create_facts_batch_requires_user_and_project_scope():
    client = RecordingPostgresClient()
    writer = FactWriter(client)

    with pytest.raises(ValueError, match="requires user_name scope"):
        await writer.create_facts_batch(2, [], user_name="", project_id="project-1")

    with pytest.raises(ValueError, match="requires project_id scope"):
        await writer.create_facts_batch(2, [], user_name="ada", project_id="")

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_writer_create_facts_batch_requires_source_message_scope():
    client = RecordingPostgresClient()
    writer = FactWriter(client)

    with pytest.raises(ValueError, match="without user/session scope"):
        await writer.create_facts_batch(
            2,
            [make_fact(source_msg_id=7)],
            user_name="ada",
            project_id="project-1",
        )

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_writer_create_facts_batch_writes_graph_and_fact_search():
    client = RecordingPostgresClient(
        fetch_one_results=[
            {"fact_id": "fact-1"},
            {"projected_count": 1},
        ]
    )
    writer = FactWriter(client)
    fact = make_fact(source_msg_id=7)

    count = await writer.create_facts_batch(
        2,
        [fact],
        user_name="ada",
        session_id="session-1",
        project_id="project-1",
    )

    assert count == 1
    assert len(client.calls) == 4
    facts_call, graph_call, msg_call, search_call = client.calls

    assert "INSERT INTO facts" in facts_call[1]
    assert facts_call[2] == (
        "fact-1",
        2,
        "ada",
        "project-1",
        "Ada writes algorithms",
        "2026-01-01T00:00:00+00:00",
        None,
        0.8,
        7,
        "ada",
        "session-1",
        "user",
        2,
        "project-1",
    )

    assert "MERGE (f:Fact" in graph_call[1]
    graph_params = json.loads(graph_call[2][0])
    fact_payload = graph_params["batch"][0]
    assert set(graph_params) == CREATE_FACTS_GRAPH_FIELDS
    assert set(fact_payload) == FACT_GRAPH_FIELDS
    assert graph_params == {
        "entity_id": 2,
        "batch": [
            {
                "id": "fact-1",
                "content": "Ada writes algorithms",
                "valid_at": "2026-01-01T00:00:00+00:00",
                "invalid_at": None,
                "confidence": 0.8,
                "source_msg_id": 7,
                "source_user_name": "ada",
                "source_session_id": "session-1",
                "source": "user",
            }
        ],
        "user_name": "ada",
        "session_id": "session-1",
        "project_id": "project-1",
    }

    assert "MATCH (m:Message" in msg_call[1]
    assert "MERGE (m:Message" not in msg_call[1]
    msg_params = json.loads(msg_call[2][0])
    assert msg_params["batch"][0]["source_msg_id"] == 7

    assert "INSERT INTO fact_search" in search_call[1]
    assert search_call[2] == (
        "fact-1",
        2,
        "ada",
        "project-1",
        "[0.1, 0.2, 0.3]",
        None,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_writer_create_facts_batch_without_embedding_skips_fact_search():
    client = RecordingPostgresClient(
        fetch_one_results=[
            {"fact_id": "fact-1"},
            {"projected_count": 1},
        ]
    )
    writer = FactWriter(client)

    count = await writer.create_facts_batch(
        2,
        [make_fact(embedding=[])],
        user_name="ada",
        project_id="project-1",
    )

    assert count == 1
    assert len(client.calls) == 2
    assert "INSERT INTO facts" in client.calls[0][1]
    assert "MERGE (f:Fact" in client.calls[1][1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_writer_create_facts_batch_prefers_explicit_source_scope():
    client = RecordingPostgresClient(
        fetch_one_results=[
            {"fact_id": "fact-1"},
            {"projected_count": 1},
        ]
    )
    writer = FactWriter(client)
    fact = make_fact(
        source_msg_id=9,
        source_user_name="source-user",
        source_session_id="source-session",
    )

    await writer.create_facts_batch(
        2,
        [fact],
        user_name="batch-user",
        session_id="batch-session",
        project_id="project-1",
    )

    graph_params = json.loads(client.calls[1][2][0])
    fact_payload = graph_params["batch"][0]
    assert fact_payload["source_msg_id"] == 9
    assert fact_payload["source_user_name"] == "source-user"
    assert fact_payload["source_session_id"] == "source-session"
    assert graph_params["user_name"] == "batch-user"
    assert graph_params["session_id"] == "batch-session"


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_writer_create_facts_batch_zero_created_raises():
    client = RecordingPostgresClient(fetch_one_results=[None])
    writer = FactWriter(client)

    with pytest.raises(Exception, match="parent may not exist"):
        await writer.create_facts_batch(
            2,
            [make_fact()],
            user_name="ada",
            project_id="project-1",
        )

    assert len(client.calls) == 1
    assert "INSERT INTO facts" in client.calls[0][1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_writer_invalidate_fact_updates_search_when_fact_exists():
    invalid_at = datetime(2026, 1, 2, tzinfo=timezone.utc)
    client = RecordingPostgresClient(fetch_one_results=[{"fact_id": "fact-1"}])
    writer = FactWriter(client)

    assert (
        await writer.invalidate_fact(
            "fact-1", invalid_at, project_id="project-1"
        )
        is True
    )

    assert len(client.calls) == 3
    facts_call, graph_call, search_call = client.calls
    assert "UPDATE facts" in facts_call[1]
    assert facts_call[2] == (invalid_at, "fact-1", "project-1")
    assert "SET f.invalid_at = $invalid_at" in graph_call[1]
    assert json.loads(graph_call[2][0]) == {
        "fact_id": "fact-1",
        "invalid_at": invalid_at.isoformat(),
        "project_id": "project-1",
    }
    assert "UPDATE fact_search" in search_call[1]
    assert search_call[2] == (invalid_at, "fact-1", "project-1")


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_writer_invalidate_fact_missing_graph_fact_skips_search_update():
    client = RecordingPostgresClient(fetch_one_results=[None])
    writer = FactWriter(client)

    assert (
        await writer.invalidate_fact(
            "fact-1",
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            project_id="project-1",
        )
        is False
    )
    assert len(client.calls) == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_writer_invalidate_fact_requires_project_scope():
    client = RecordingPostgresClient()
    writer = FactWriter(client)

    with pytest.raises(ValueError, match="requires project_id scope"):
        await writer.invalidate_fact(
            "fact-1",
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            project_id="",
        )

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_writer_delete_old_invalidated_facts_relies_on_search_cascade():
    cutoff = datetime(2026, 1, 3, tzinfo=timezone.utc)
    client = RecordingPostgresClient(
        fetch_all_results=[[{"fact_id": '"fact-1"'}, {"fact_id": "fact-2"}]]
    )
    writer = FactWriter(client)

    assert (
        await writer.delete_old_invalidated_facts(
            cutoff, project_id="project-1"
        )
        == 2
    )

    assert len(client.calls) == 2
    facts_call, graph_call = client.calls
    assert "DELETE FROM facts" in facts_call[1]
    assert facts_call[2] == (cutoff, "project-1")
    assert "DETACH DELETE f" in graph_call[1]
    assert json.loads(graph_call[2][0]) == {
        "fact_ids": ["fact-1", "fact-2"],
        "project_id": "project-1",
    }
    assert not any("DELETE FROM fact_search" in call[1] for call in client.calls)


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_writer_delete_old_invalidated_facts_empty_rows_skip_search_delete():
    client = RecordingPostgresClient(fetch_all_results=[[]])
    writer = FactWriter(client)

    assert (
        await writer.delete_old_invalidated_facts(
            datetime(2026, 1, 3, tzinfo=timezone.utc),
            project_id="project-1",
        )
        == 0
    )
    assert len(client.calls) == 1
    assert "DELETE FROM facts" in client.calls[0][1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_writer_delete_old_invalidated_facts_requires_project_scope():
    client = RecordingPostgresClient()
    writer = FactWriter(client)

    with pytest.raises(ValueError, match="requires project_id scope"):
        await writer.delete_old_invalidated_facts(
            datetime(2026, 1, 3, tzinfo=timezone.utc),
            project_id="",
        )

    assert client.calls == []
