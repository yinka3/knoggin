import pytest

from core.knowledge.db.readers.entity_reader import EntityReader
from core.knowledge.db.readers.graph_reader import GraphReader
from tests.fixtures.fakes import RecordingPostgresClient

_PATH_DEPTH_ERROR = "max_depth must be an integer between 1 and 4"


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize("max_depth", [0, 5, True, "4", "1]-(x)-[]-"])
async def test_find_path_rejects_untrusted_cypher_depth_before_querying(max_depth):
    client = RecordingPostgresClient()
    reader = GraphReader(client)

    with pytest.raises(ValueError, match=_PATH_DEPTH_ERROR):
        await reader.find_path_filtered(
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
    reader = GraphReader(client)

    with pytest.raises(ValueError, match=_PATH_DEPTH_ERROR):
        await reader._find_shortest_path(
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
    reader = GraphReader(client)

    assert await reader.find_path_filtered(
        "Ada",
        "Grace",
        visible_project_ids=["project-1"],
        max_depth=4,
    ) == ([], False)

    cypher_query = client.calls[0][1]
    assert "RELATED_TO*1..4" in cypher_query


@pytest.mark.storage
@pytest.mark.no_network
async def test_related_entities_exposes_observed_evidence_metadata():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "source": "Ade",
                    "target": "Acme",
                    "relationship_id": "project-1:1:2:works_at",
                    "relationship_type": "works_at",
                    "canonical_relationship_type": "works_at",
                    "observed_relationship_label": "works at",
                    "domain_status": "recognized",
                    "symmetric": False,
                    "connection_strength": 2,
                    "evidence_refs": [{"message_id": 101}],
                    "observation_refs": [
                        {
                            "observation_id": 12,
                            "observed_relationship_label": "works at",
                            "canonical_relationship_type": "works_at",
                            "observed_at_ms": 200,
                            "confidence": 0.9,
                            "context": "Ade joined Acme.",
                        }
                    ],
                    "evidence_message_count": 1,
                    "observation_count": 2,
                    "first_observed": 100,
                    "last_observed": 200,
                    "confidence": 0.9,
                    "last_seen": 200,
                    "context": "Ade joined Acme.",
                }
            ]
        ]
    )
    reader = EntityReader(client)

    result = await reader.get_related_entities_by_name(
        ["Ade"],
        visible_project_ids=["project-1"],
    )

    assert result == [
        {
            "source": "Ade",
            "target": "Acme",
            "relationship_id": "project-1:1:2:works_at",
            "relationship_type": "works_at",
            "symmetric": False,
            "relationship_semantics": "observed_evidence",
            "connection_strength": 2.0,
            "evidence_refs": [{"message_id": 101}],
            "observation_refs": [
                {
                    "observation_id": 12,
                    "observed_relationship_label": "works at",
                    "canonical_relationship_type": "works_at",
                    "observed_at_ms": 200,
                    "confidence": 0.9,
                    "context": "Ade joined Acme.",
                }
            ],
            "evidence_message_count": 1,
            "observation_count": 2,
            "first_observed": 100,
            "last_observed": 200,
        }
    ]
    query = client.calls[0][1]
    assert "relationship_observations" in query
    assert "observation_refs" in query
    assert "relationship_observations" in query
