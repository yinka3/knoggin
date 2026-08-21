import pytest

from common.exceptions import StorageReadError
from core.knowledge.db.readers.entity_reader import EntityReader
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_list_count_and_page_use_one_repeatable_read_snapshot():
    client = RecordingPostgresClient(
        fetch_one_results=[{"total": 1}],
        fetch_all_results=[
            [
                {
                    "id": 2,
                    "session_id": "session-1",
                    "canonical_name": "Widget",
                    "type": "concept",
                    "topic": "General",
                    "last_mentioned": 1000,
                }
            ]
        ],
    )

    entities, total = await EntityReader(client).list_entities(
        limit=20,
        offset=0,
        visible_project_ids=["project-1"],
    )

    assert total == 1
    assert [entity["id"] for entity in entities] == [2]
    assert client.transaction_enters == 1
    assert client.transaction_exits == 1
    assert (
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
        in client.calls[0][1]
    )
    assert "SELECT count(*) AS total FROM entities e" in client.calls[1][1]
    assert "SELECT" in client.calls[2][1]
    assert all(call[0] == "execute" for call in client.calls)


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("limit", "offset"),
    [
        (0, 0),
        (101, 0),
        (True, 0),
        (1, -1),
        (1, 10_001),
    ],
)
async def test_entity_list_rejects_invalid_pagination_before_opening_a_snapshot(
    limit,
    offset,
):
    client = RecordingPostgresClient()

    with pytest.raises(ValueError):
        await EntityReader(client).list_entities(
            limit=limit,
            offset=offset,
            visible_project_ids=["project-1"],
        )

    assert client.transaction_enters == 0


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_reader_rejects_invalid_bounded_query_inputs_before_querying():
    client = RecordingPostgresClient()
    reader = EntityReader(client)

    with pytest.raises(ValueError, match="get_top_connected_entities"):
        await reader.get_top_connected_entities(
            visible_project_ids=["project-1"],
            limit=0,
        )
    with pytest.raises(ValueError, match="get_recently_active_entities: days"):
        await reader.get_recently_active_entities(
            visible_project_ids=["project-1"],
            days=366,
        )

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_name_lookup_uses_valid_scoped_sql_and_returns_matches():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "id": 2,
                    "project_id": "project-1",
                    "canonical_name": "Widget",
                    "type": "concept",
                    "aliases": ["widget-service"],
                }
            ]
        ]
    )

    matches = await EntityReader(client).get_entities_by_names(
        ["Widget", "widget-service"],
        visible_project_ids=["project-1"],
    )

    assert matches == [
        {
            "id": 2,
            "project_id": "project-1",
            "canonical_name": "Widget",
            "type": "concept",
            "aliases": ["widget-service"],
        }
    ]
    query, params = client.calls[0][1], client.calls[0][2]
    assert "AS aliases,\n        FROM" not in query
    assert params == [
        ["widget", "widget-service"],
        ["widget", "widget-service"],
        ["project-1"],
        1,
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_name_lookup_does_not_report_storage_failure_as_absence():
    reader = EntityReader(
        RecordingPostgresClient(fetch_all_exceptions=[RuntimeError("database down")])
    )

    with pytest.raises(StorageReadError) as error:
        await reader.get_entities_by_names(
            ["Widget"],
            visible_project_ids=["project-1"],
        )

    assert error.value.code == "storage_read_error"
    assert error.value.details["operation"] == "get_entities_by_names"
