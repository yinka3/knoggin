import pytest

from core.knowledge.db.readers.entity_reader import EntityReader
from core.knowledge.db.readers.graph_reader import GraphReader
from core.knowledge.db.readers.knowledge_query_reader import KnowledgeQueryReader
from tests.fixtures.fakes import RecordingPostgresClient

_PATH_DEPTH_ERROR = "max_depth must be an integer between 1 and 4"


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize("max_depth", [0, 5, True, "4", "1]-(x)-[]-"])
async def test_find_path_rejects_invalid_depth_before_querying(max_depth):
    client = RecordingPostgresClient()
    reader = GraphReader(client)

    with pytest.raises(ValueError, match=_PATH_DEPTH_ERROR):
        await reader.find_path(
            2,
            3,
            visible_project_ids=["project-1"],
            max_depth=max_depth,
        )

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_internal_path_helper_enforces_the_same_depth_boundary():
    client = RecordingPostgresClient()
    reader = GraphReader(client)

    with pytest.raises(ValueError, match=_PATH_DEPTH_ERROR):
        await reader._find_shortest_path(
            2,
            3,
            visible_project_ids=["project-1"],
            max_depth="1]-(x)-[]-",
        )

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_find_path_stops_searching_at_the_depth_limit():
    # A chain 2-10-11-12-13-3 is five hops; the search expands one side per
    # query and must give up after max_depth expansions.
    chain = {2: 10, 10: 11, 11: 12, 3: 13, 13: 12}
    edges = [[{"entity_a_id": a, "entity_b_id": b, "relationship_id": f"r{a}-{b}"}]
             for a, b in chain.items()]
    client = RecordingPostgresClient(fetch_all_results=edges)
    reader = GraphReader(client)

    assert await reader.find_path(
        2,
        3,
        visible_project_ids=["project-1"],
        max_depth=2,
    ) == []
    assert len(client.calls) == 2
    assert all("public.relationships" in call[1] for call in client.calls)


@pytest.mark.storage
@pytest.mark.no_network
async def test_path_preserves_traversal_and_stored_relationship_direction():
    relationship_id = "project-1:1:2:works_at"
    client = RecordingPostgresClient(
        fetch_all_results=[
            [{"entity_a_id": 1, "entity_b_id": 2, "relationship_id": relationship_id}],
            [
                {"entity_id": 2, "canonical_name": "Acme"},
                {"entity_id": 1, "canonical_name": "Ade"},
            ],
            [
                {
                    "relationship_id": relationship_id,
                    "evidence_refs": [
                        {
                            "project_id": "project-1",
                            "user_name": "ada",
                            "semantic_window_id": "window-1",
                            "observation_id": 101,
                        }
                    ],
                }
            ],
            [
                {
                    "relationship_id": relationship_id,
                    "project_id": "project-1",
                    "source_entity_id": 1,
                    "target_entity_id": 2,
                    "source": "Ade",
                    "target": "Acme",
                    "relationship_type": "works_at",
                    "symmetric": False,
                }
            ],
        ]
    )

    path = await GraphReader(client).find_path(
        2,
        1,
        visible_project_ids=["project-1"],
    )

    assert path == [
        {
            "step": 0,
            "entity_a_id": 2,
            "entity_b_id": 1,
            "entity_a": "Acme",
            "entity_b": "Ade",
            "relationship_id": relationship_id,
            "project_id": "project-1",
            "source_entity_id": 1,
            "target_entity_id": 2,
            "source": "Ade",
            "target": "Acme",
            "relationship_type": "works_at",
            "symmetric": False,
            "relationship_semantics": "observed_evidence",
            "evidence_refs": [
                {
                    "kind": "relationship_observation",
                    "project_id": "project-1",
                    "user_name": "ada",
                    "observation_id": 101,
                }
            ],
        }
    ]
    assert "observation.retired_at IS NULL" in client.calls[2][1]
    assert "ROW_NUMBER() OVER" in client.calls[2][1]
    assert client.calls[2][2] == (
        [relationship_id],
        ["project-1"],
        32,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_activity_uses_entity_message_refs_before_relationship_observations():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "entity_id": 2,
                    "entity": "Ade",
                    "project_id": "project-1",
                    "time": 200,
                    "evidence_refs": [{"message_id": 101}],
                    "observation_refs": [],
                }
            ]
        ]
    )

    result = await KnowledgeQueryReader(client).get_recent_activity(
        2,
        visible_project_ids=["project-1"],
    )

    assert result == [
        {
            "entity_id": 2,
            "entity": "Ade",
            "project_id": "project-1",
            "time": 200,
            "evidence_refs": [{"message_id": 101}],
            "observation_refs": [],
        }
    ]
    query = client.calls[0][1]
    assert "FROM message_entity_refs mention" in query
    assert "LEFT JOIN relationship_observations observation" in query
    assert "observation.retired_at IS NULL" in query
    assert "WHERE mention.entity_id = %s" in query


@pytest.mark.storage
@pytest.mark.no_network
async def test_hot_topic_context_is_current_project_and_uses_entity_mentions():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "topic": "Identity",
                    "name": "Ade",
                    "aliases": ["Ada"],
                    "message_refs": [
                        {
                            "project_id": "project-1",
                            "user_name": "ada",
                            "session_id": "session-1",
                            "message_id": 101,
                        }
                    ],
                }
            ]
        ]
    )

    result = await KnowledgeQueryReader(client).get_hot_topic_context_with_messages(
        ["Identity"],
        project_id="project-1",
    )

    assert result == {
        "Identity": {
            "entities": [{"name": "Ade", "aliases": ["Ada"]}],
            "message_refs": [
                {
                    "project_id": "project-1",
                    "user_name": "ada",
                    "session_id": "session-1",
                    "message_id": 101,
                }
            ],
        }
    }
    query, params = client.calls[0][1:]
    assert "LEFT JOIN message_entity_refs mention" in query
    assert "context.project_id = %s" in query
    assert params == (["Identity"], "project-1")


@pytest.mark.storage
@pytest.mark.no_network
async def test_related_entities_exposes_observed_evidence_metadata():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "project_id": "project-1",
                    "source_entity_id": 1,
                    "target_entity_id": 2,
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

    result = await reader.get_related_entities(
        [1],
        visible_project_ids=["project-1"],
    )

    assert result == [
        {
            "project_id": "project-1",
            "source_entity_id": 1,
            "target_entity_id": 2,
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
    assert "observation.retired_at IS NULL" in query
    assert "observation_refs" in query
    assert "relationship_observations" in query


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_find_path_crosses_a_hub_and_respects_project_visibility(real_postgres_client):
    """Paths come straight from the relationships table, both directions."""

    await real_postgres_client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name) VALUES
            (20, 'ada', 'Hub'), (21, 'ada', 'Alpha'), (22, 'ada', 'Beta'),
            (23, 'ada', 'Hidden');
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES
            ('project-1', 20, 'ada', 'concept', 'General'),
            ('project-1', 21, 'ada', 'concept', 'General'),
            ('project-1', 22, 'ada', 'concept', 'General'),
            ('project-2', 21, 'ada', 'concept', 'General'),
            ('project-2', 23, 'ada', 'concept', 'General');
        INSERT INTO public.relationships (
            relationship_id, user_name, project_id, entity_a_id, entity_b_id,
            relationship_type, "symmetric"
        ) VALUES
            ('project-1:21:20:uses', 'ada', 'project-1', 21, 20, 'uses', false),
            ('project-1:22:20:uses', 'ada', 'project-1', 22, 20, 'uses', false),
            ('project-2:21:23:uses', 'ada', 'project-2', 21, 23, 'uses', false);
        """
    )
    reader = GraphReader(real_postgres_client)

    path = await reader._find_shortest_path(
        21, 22, visible_project_ids=["project-1"], max_depth=4,
    )
    assert path == (
        [21, 20, 22],
        ["Alpha", "Hub", "Beta"],
        ["project-1:21:20:uses", "project-1:22:20:uses"],
    )
    # The project-2 edge is invisible from project-1.
    assert await reader._find_shortest_path(
        21, 23, visible_project_ids=["project-1"], max_depth=4,
    ) is None
    assert await reader.get_neighbor_entities(
        20, visible_project_ids=["project-1"],
    ) == [{"id": 21, "name": "Alpha"}, {"id": 22, "name": "Beta"}]
    assert await reader.get_neighbor_ids_batch(
        [21, 20], visible_project_ids=["project-1", "project-2"],
    ) == {21: {20, 23}, 20: {21, 22}}
