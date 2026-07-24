import pytest

from core.knowledge.db.tool_queries import ToolQueries
from tests.fixtures.fakes import RecordingPostgresClient

_PATH_DEPTH_ERROR = "max_depth must be an integer between 1 and 4"


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize("max_depth", [0, 5, True, "4", "1]-(x)-[]-"])
async def test_find_path_rejects_untrusted_cypher_depth_before_querying(max_depth):
    client = RecordingPostgresClient()
    queries = ToolQueries(client)

    with pytest.raises(ValueError, match=_PATH_DEPTH_ERROR):
        await queries.find_path_filtered(
            "Ada",
            "Grace",
            visible_project_ids=["project-1"],
            max_depth=max_depth,
        )

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_internal_path_helper_enforces_the_same_cypher_depth_boundary():
    client = RecordingPostgresClient()
    queries = ToolQueries(client)

    with pytest.raises(ValueError, match=_PATH_DEPTH_ERROR):
        await queries._find_shortest_path(
            "Ada",
            "Grace",
            visible_project_ids=["project-1"],
            max_depth="1]-(x)-[]-",
        )

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_find_path_uses_a_validated_fixed_depth_in_its_cypher_query():
    client = RecordingPostgresClient(fetch_all_results=[[]])
    queries = ToolQueries(client)

    assert await queries.find_path_filtered(
        "Ada",
        "Grace",
        visible_project_ids=["project-1"],
        max_depth=4,
    ) == ([], False)

    cypher_query = client.calls[0][1]
    assert "RELATED_TO*1..4" in cypher_query
