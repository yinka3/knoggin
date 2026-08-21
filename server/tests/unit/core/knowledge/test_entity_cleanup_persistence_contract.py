import pytest

from core.knowledge.db.readers.entity_reader import EntityReader
from core.knowledge.db.writers.graph_writer import GraphWriter
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.unit
@pytest.mark.no_network
async def test_entity_cleanup_preview_is_project_scoped_and_includes_evidence_counts():
    client = RecordingPostgresClient(
        fetch_all_results=[[{"entity_id": 7, "canonical_name": "Roadmap"}]]
    )

    result = await EntityReader(client).preview_project_entity_cleanup(
        user_name="ada",
        project_id="project-1",
        limit=25,
    )

    assert result == [{"entity_id": 7, "canonical_name": "Roadmap"}]
    query, params = client.calls[0][1:]
    assert "message_reference_count" in query
    assert "relationship_count" in query
    assert "episode_reference_count" in query
    assert params == ("ada", "project-1", 1, 25)


@pytest.mark.unit
@pytest.mark.no_network
async def test_selected_entity_cleanup_validates_ownership_before_deleting():
    client = RecordingPostgresClient(
        fetch_all_results=[[{"entity_id": 2}, {"entity_id": 3}], [{"entity_id": 2}, {"entity_id": 3}]]
    )
    writer = GraphWriter(client)

    deleted = await writer.delete_selected_project_entities(
        [3, 2],
        user_name="ada",
        project_id="project-1",
    )

    assert deleted == [2, 3]
    ownership_query = client.calls[0]
    assert "FOR UPDATE" in ownership_query[1]
    assert ownership_query[2] == ([2, 3], "ada", "project-1")
    assert "DELETE FROM entities" in client.calls[1][1]


@pytest.mark.unit
@pytest.mark.no_network
async def test_selected_entity_cleanup_rejects_ids_outside_the_project():
    client = RecordingPostgresClient(fetch_all_results=[[{"entity_id": 2}]])

    with pytest.raises(ValueError, match="outside the project"):
        await GraphWriter(client).delete_selected_project_entities(
            [2, 3],
            user_name="ada",
            project_id="project-1",
        )

    assert len(client.calls) == 1
