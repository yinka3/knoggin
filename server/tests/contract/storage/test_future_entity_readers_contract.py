"""Readers kept for future entity browse and stats features."""

import pytest

from core.knowledge.db.readers.entity_reader import EntityReader
from core.knowledge.db.readers.graph_reader import GraphReader
from tests.fixtures.fakes import RecordingPostgresClient

VISIBLE = ["project-1", "project-2"]


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_list_search_is_literal_active_only_and_stably_ordered():
    client = RecordingPostgresClient(fetch_one_results=[{"total": 0}])

    await EntityReader(client).list_entities(visible_project_ids=VISIBLE, search="50%_off")

    _kind, count_query, params = next(call for call in client.calls if "count(DISTINCT" in call[1])
    assert "e.status = 'active'" in count_query
    assert "ESCAPE '\\'" in count_query
    assert params == (VISIBLE, "%50\\%\\_off%")


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_list_pages_break_ties_by_id():
    client = RecordingPostgresClient(fetch_one_results=[{"total": 1}], fetch_all_results=[[]])

    await EntityReader(client).list_entities(visible_project_ids=VISIBLE)

    data_query = client.calls[-1][1]
    assert "DESC NULLS LAST, e.entity_id" in data_query


@pytest.mark.storage
@pytest.mark.no_network
async def test_counts_and_stats_count_distinct_active_entities():
    client = RecordingPostgresClient(
        fetch_all_results=[[], []],
        fetch_one_results=[{"entities": 0, "episodes": 0, "relationships": 0}],
    )
    entities = EntityReader(client)

    await entities.get_entity_count_by_type(visible_project_ids=VISIBLE)
    await entities.get_entity_count_by_topic(visible_project_ids=VISIBLE)
    await GraphReader(client).get_graph_stats(visible_project_ids=VISIBLE)

    for _kind, query, _params in client.calls:
        assert "count(DISTINCT" in query and "e.status = 'active'" in query
    with pytest.raises(ValueError, match="between 1 and 100"):
        await GraphReader(client).get_neighbor_entities(2, visible_project_ids=VISIBLE, limit=0)
