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
    assert "count(DISTINCT e.entity_id) AS total" in client.calls[1][1]
    assert "project_entity_contexts" in client.calls[1][1]
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
            [{"id": 2}],
            [
                {
                    "id": 2,
                    "user_name": "ada",
                    "canonical_name": "Widget",
                    "aliases": ["widget-service"],
                }
            ],
            [
                {
                    "entity_id": 2,
                    "project_id": "project-1",
                    "entity_type": "concept",
                    "topic": "General",
                    "last_mentioned_ms": 1000,
                }
            ],
        ]
    )

    matches = await EntityReader(client).get_entities_by_names(
        ["Widget", "widget-service"],
        visible_project_ids=["project-1"],
    )

    assert matches == [
        {
            "id": 2,
            "canonical_name": "Widget",
            "aliases": ["widget-service"],
            "user_name": "ada",
            "contexts": [
                {
                    "project_id": "project-1",
                    "entity_type": "concept",
                    "topic": "General",
                    "last_mentioned_ms": 1000,
                }
            ],
        }
    ]
    query, params = client.calls[0][1], client.calls[0][2]
    assert "AS aliases,\n        FROM" not in query
    assert params == [
        ["widget", "widget-service"],
        ["widget", "widget-service"],
        1,
        ["project-1"],
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


@pytest.mark.storage
@pytest.mark.no_network
async def test_resolution_catalog_loads_profiles_aliases_and_contexts_in_one_query():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "id": 2,
                    "user_name": "ada",
                    "canonical_name": "Widget",
                    "aliases": ["widget-service"],
                    "contexts": [
                        {
                            "project_id": "project-1",
                            "entity_type": "concept",
                            "topic": "General",
                            "last_mentioned_ms": 1000,
                        },
                        {
                            "project_id": "project-2",
                            "entity_type": "service",
                            "topic": "Work",
                            "last_mentioned_ms": 2000,
                        },
                    ],
                }
            ]
        ]
    )

    catalog = await EntityReader(client).get_visible_entities_for_resolution(
        visible_project_ids=["project-1", "project-2"]
    )

    assert catalog == [
        {
            "id": 2,
            "canonical_name": "Widget",
            "aliases": ["widget-service"],
            "user_name": "ada",
            "contexts": [
                {
                    "project_id": "project-1",
                    "entity_type": "concept",
                    "topic": "General",
                    "last_mentioned_ms": 1000,
                },
                {
                    "project_id": "project-2",
                    "entity_type": "service",
                    "topic": "Work",
                    "last_mentioned_ms": 2000,
                },
            ],
        }
    ]
    assert len(client.calls) == 1
    query, params = client.calls[0][1], client.calls[0][2]
    assert query.count("LEFT JOIN LATERAL") == 2
    assert "jsonb_agg" in query
    assert params == (["project-1", "project-2"], 1, ["project-1", "project-2"])


@pytest.mark.storage
@pytest.mark.no_network
async def test_resolution_catalog_does_not_report_storage_failure_as_an_empty_catalog():
    reader = EntityReader(
        RecordingPostgresClient(fetch_all_exceptions=[RuntimeError("database down")])
    )

    with pytest.raises(StorageReadError) as error:
        await reader.get_visible_entities_for_resolution(
            visible_project_ids=["project-1"]
        )

    assert error.value.code == "storage_read_error"
    assert error.value.details["operation"] == "get_visible_entities_for_resolution"


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_resolution_catalog_returns_one_scoped_durable_snapshot(
    real_postgres_client,
):
    await real_postgres_client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name, status)
        VALUES
            (40, 'ada', 'Widget', 'active'),
            (41, 'ada', 'Shared Service', 'active'),
            (42, 'ada', 'Retired Service', 'retired');
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic, last_mentioned_ms
        ) VALUES
            ('project-2', 40, 'ada', 'service', 'Archive', 2000),
            ('project-1', 40, 'ada', 'concept', 'General', 1000),
            ('project-2', 41, 'ada', 'service', 'Archive', 3000),
            ('project-1', 42, 'ada', 'service', 'Archive', 4000);
        INSERT INTO public.entity_aliases (entity_id, alias)
        VALUES
            (40, 'widget-service'),
            (40, 'widget'),
            (41, 'shared');
        """
    )

    catalog = await EntityReader(
        real_postgres_client
    ).get_visible_entities_for_resolution(
        visible_project_ids=["project-1", "project-2"]
    )

    assert [entity["id"] for entity in catalog] == [40, 41]
    assert catalog[0] == {
        "id": 40,
        "canonical_name": "Widget",
        "aliases": ["widget", "widget-service"],
        "user_name": "ada",
        "contexts": [
            {
                "project_id": "project-1",
                "entity_type": "concept",
                "topic": "General",
                "last_mentioned_ms": 1000,
            },
            {
                "project_id": "project-2",
                "entity_type": "service",
                "topic": "Archive",
                "last_mentioned_ms": 2000,
            },
        ],
    }
