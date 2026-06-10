import json
from datetime import datetime, timezone

import pytest

from common.utils.time_utils import reset_clock, set_test_clock
from knoggin_server.knowledge.db.readers.fact_reader import FactReader
from tests.fixtures.fakes import RecordingPostgresClient


class VectorLike:
    def __init__(self, values):
        self.values = values

    def tolist(self):
        return list(self.values)


def fact_row(**overrides):
    row = {
        "id": '"fact-1"',
        "source_entity_id": "2",
        "content": '"Ada writes algorithms"',
        "valid_at": '"2026-01-01T00:00:00+00:00"',
        "invalid_at": None,
        "confidence": "0.8",
        "source": '"user"',
        "source_msg_id": "7",
        "source_user_name": '"ada"',
        "source_session_id": '"session-1"',
    }
    row.update(overrides)
    return row


@pytest.mark.storage
@pytest.mark.no_network
def test_fact_reader_hydrate_fact_strips_age_strings_and_parses_dates():
    reader = FactReader(RecordingPostgresClient())

    fact = reader._hydrate_fact(fact_row(), embedding=[0.1, 0.2])

    assert fact.id == "fact-1"
    assert fact.content == "Ada writes algorithms"
    assert fact.source_entity_id == 2
    assert fact.valid_at == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert fact.source_msg_id == 7
    assert fact.source_user_name == "ada"
    assert fact.source_session_id == "session-1"
    assert fact.source == "user"
    assert fact.embedding == [0.1, 0.2]


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_search_relevant_facts_attaches_embeddings():
    client = RecordingPostgresClient(
        execute_read_results=[
            [{"fact_id": "fact-1", "embedding": VectorLike([0.1, 0.2, 0.3])}],
            [fact_row()],
        ]
    )
    reader = FactReader(client)

    facts = await reader.search_relevant_facts(2, [0.9, 0.8], limit=3)

    assert len(facts) == 1
    assert facts[0].id == "fact-1"
    assert facts[0].embedding == [0.1, 0.2, 0.3]
    vector_call, graph_call = client.calls
    assert "FROM fact_search" in vector_call[1]
    assert vector_call[2] == (2, [0.9, 0.8], 3)
    assert "WHERE f.id IN $fact_ids" in graph_call[1]
    assert json.loads(graph_call[2][0]) == {"fact_ids": ["fact-1"]}


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_search_relevant_facts_empty_vector_results_skip_graph():
    client = RecordingPostgresClient(execute_read_results=[[]])
    reader = FactReader(client)

    assert await reader.search_relevant_facts(2, [0.9, 0.8], limit=3) == []
    assert len(client.calls) == 1
    assert "FROM fact_search" in client.calls[0][1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_get_facts_for_entity_applies_active_only_filter():
    client = RecordingPostgresClient(execute_read_results=[[fact_row()]])
    reader = FactReader(client)

    facts = await reader.get_facts_for_entity(2, active_only=True)

    assert [fact.id for fact in facts] == ["fact-1"]
    assert "WHERE f.invalid_at IS NULL" in client.calls[0][1]
    assert json.loads(client.calls[0][2][0]) == {"entity_id": 2}


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_get_facts_for_entity_can_include_inactive_facts():
    client = RecordingPostgresClient(execute_read_results=[[fact_row()]])
    reader = FactReader(client)

    facts = await reader.get_facts_for_entity(2, active_only=False)

    assert [fact.id for fact in facts] == ["fact-1"]
    assert "WHERE f.invalid_at IS NULL" not in client.calls[0][1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_get_facts_for_entities_groups_and_limits_per_entity():
    rows = [
        fact_row(entity_id="2", id=f'"fact-2-{idx}"')
        for idx in range(6)
    ] + [
        fact_row(entity_id="3", id='"fact-3-0"', source_entity_id="3")
    ]
    client = RecordingPostgresClient(execute_read_results=[rows])
    reader = FactReader(client)

    grouped = await reader.get_facts_for_entities([2, 3], active_only=True)

    assert [fact.id for fact in grouped[2]] == [
        "fact-2-0",
        "fact-2-1",
        "fact-2-2",
        "fact-2-3",
        "fact-2-4",
    ]
    assert [fact.id for fact in grouped[3]] == ["fact-3-0"]
    assert "AND f.invalid_at IS NULL" in client.calls[0][1]
    assert json.loads(client.calls[0][2][0]) == {"entity_ids": [2, 3]}


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_get_facts_for_entities_empty_input_skips_db():
    client = RecordingPostgresClient()
    reader = FactReader(client)

    assert await reader.get_facts_for_entities([]) == {}
    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_get_facts_from_message_uses_user_session_scope():
    client = RecordingPostgresClient(execute_read_results=[[fact_row()]])
    reader = FactReader(client)

    facts = await reader.get_facts_from_message(
        7,
        user_name="ada",
        session_id="session-1",
    )

    assert [fact.id for fact in facts] == ["fact-1"]
    assert "MATCH (f:Fact)-[:EXTRACTED_FROM]->(m:Message" in client.calls[0][1]
    assert json.loads(client.calls[0][2][0]) == {
        "msg_id": 7,
        "user_name": "ada",
        "session_id": "session-1",
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_get_recent_facts_strips_quoted_age_strings():
    client = RecordingPostgresClient(
        execute_read_results=[
            [
                {
                    "id": '"fact-1"',
                    "content": '"Ada writes algorithms"',
                    "created_at": '"2026-01-01T00:00:00+00:00"',
                    "entity_name": '"Ada Lovelace"',
                    "entity_type": '"person"',
                }
            ]
        ]
    )
    reader = FactReader(client)
    set_test_clock("2026-01-08T12:00:00+00:00")

    try:
        facts = await reader.get_recent_facts(days=3, limit=4)
    finally:
        reset_clock()

    assert facts == [
        {
            "id": "fact-1",
            "content": "Ada writes algorithms",
            "created_at": "2026-01-01T00:00:00+00:00",
            "entity_name": "Ada Lovelace",
            "entity_type": "person",
        }
    ]
    params = json.loads(client.calls[0][2][0])
    assert params["limit"] == 4
    assert params["cutoff"] == "2026-01-05T12:00:00+00:00"


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_get_facts_from_message_refuses_missing_scope():
    client = RecordingPostgresClient()
    reader = FactReader(client)

    assert await reader.get_facts_from_message(7, user_name=None) == []
    assert await reader.get_facts_from_message(7, user_name="ada") == []
    assert client.calls == []
