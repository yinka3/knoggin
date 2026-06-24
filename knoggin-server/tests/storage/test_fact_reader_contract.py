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
        fetch_all_results=[
            [{"fact_id": "fact-1", "embedding": VectorLike([0.1, 0.2, 0.3])}],
            [fact_row()],
        ]
    )
    reader = FactReader(client)

    facts = await reader.search_relevant_facts(
        2,
        [0.9, 0.8],
        visible_project_ids=["project-1"],
        limit=3,
    )

    assert len(facts) == 1
    assert facts[0].id == "fact-1"
    assert facts[0].embedding == [0.1, 0.2, 0.3]
    vector_call, graph_call = client.calls
    assert "FROM fact_search" in vector_call[1]
    assert vector_call[2] == (2, ["project-1"], [0.9, 0.8], 3)
    assert "FROM facts" in graph_call[1]
    assert "WHERE fact_id = ANY(%s)" in graph_call[1]
    assert graph_call[2] == (["fact-1"], ["project-1"])


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_search_relevant_facts_empty_vector_results_skip_graph():
    client = RecordingPostgresClient(fetch_all_results=[[]])
    reader = FactReader(client)

    assert await reader.search_relevant_facts(
        2,
        [0.9, 0.8],
        visible_project_ids=["project-1"],
        limit=3,
    ) == []
    assert len(client.calls) == 1
    assert "FROM fact_search" in client.calls[0][1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_get_facts_for_entity_applies_active_only_filter():
    client = RecordingPostgresClient(fetch_all_results=[[fact_row()]])
    reader = FactReader(client)

    facts = await reader.get_facts_for_entity(
        2, visible_project_ids=["project-1"], active_only=True
    )

    assert [fact.id for fact in facts] == ["fact-1"]
    assert "FROM facts" in client.calls[0][1]
    assert "AND invalid_at IS NULL" in client.calls[0][1]
    assert client.calls[0][2] == (2, ["project-1"])


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_get_facts_for_entity_can_include_inactive_facts():
    client = RecordingPostgresClient(fetch_all_results=[[fact_row()]])
    reader = FactReader(client)

    facts = await reader.get_facts_for_entity(
        2, visible_project_ids=["project-1"], active_only=False
    )

    assert [fact.id for fact in facts] == ["fact-1"]
    assert "AND invalid_at IS NULL" not in client.calls[0][1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_get_facts_for_entities_groups_and_limits_per_entity():
    rows = [
        fact_row(entity_id="2", id=f'"fact-2-{idx}"')
        for idx in range(6)
    ] + [
        fact_row(entity_id="3", id='"fact-3-0"', source_entity_id="3")
    ]
    client = RecordingPostgresClient(fetch_all_results=[rows])
    reader = FactReader(client)

    grouped = await reader.get_facts_for_entities(
        [2, 3],
        visible_project_ids=["project-1"],
        active_only=True,
    )

    assert [fact.id for fact in grouped[2]] == [
        "fact-2-0",
        "fact-2-1",
        "fact-2-2",
        "fact-2-3",
        "fact-2-4",
    ]
    assert [fact.id for fact in grouped[3]] == ["fact-3-0"]
    assert "FROM facts" in client.calls[0][1]
    assert "AND invalid_at IS NULL" in client.calls[0][1]
    assert client.calls[0][2] == ([2, 3], ["project-1"])


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_get_facts_for_entities_empty_input_skips_db():
    client = RecordingPostgresClient()
    reader = FactReader(client)

    assert await reader.get_facts_for_entities(
        [], visible_project_ids=["project-1"]
    ) == {}
    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_get_facts_from_message_uses_user_session_scope():
    client = RecordingPostgresClient(fetch_all_results=[[fact_row()]])
    reader = FactReader(client)

    facts = await reader.get_facts_from_message(
        7,
        user_name="ada",
        session_id="session-1",
        visible_project_ids=["project-1"],
    )

    assert [fact.id for fact in facts] == ["fact-1"]
    assert "FROM facts" in client.calls[0][1]
    assert "source_msg_id = %s" in client.calls[0][1]
    assert client.calls[0][2] == (7, "ada", "session-1", ["project-1"])


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_get_recent_facts_strips_quoted_age_strings():
    client = RecordingPostgresClient(
        fetch_all_results=[
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
        facts = await reader.get_recent_facts(
            visible_project_ids=["project-1"],
            days=3,
            limit=4,
        )
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
    params = client.calls[0][2]
    assert params == (
        datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc),
        ["project-1"],
        4,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_get_facts_from_message_refuses_missing_scope():
    client = RecordingPostgresClient()
    reader = FactReader(client)

    with pytest.raises(ValueError, match="requires user_name scope"):
        await reader.get_facts_from_message(
            7,
            user_name=None,
            session_id="session-1",
            visible_project_ids=["project-1"],
        )
    with pytest.raises(ValueError, match="requires session_id scope"):
        await reader.get_facts_from_message(
            7,
            user_name="ada",
            session_id="",
            visible_project_ids=["project-1"],
        )
    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_reader_rejects_empty_visibility_before_empty_entity_input():
    client = RecordingPostgresClient()
    reader = FactReader(client)

    with pytest.raises(ValueError, match="requires visible_project_ids scope"):
        await reader.get_facts_for_entities([], visible_project_ids=[])

    assert client.calls == []
