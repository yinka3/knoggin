import pytest

from core.knowledge.db.readers.entity_reader import EntityReader


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_entity_name_lookup_matches_canonical_names_and_aliases_in_scope(
    real_postgres_client,
):
    await real_postgres_client.execute(
        """
        INSERT INTO entities (entity_id, user_name, canonical_name) VALUES
            (2, 'ada', 'Widget'),
            (3, 'ada', 'Other Widget');
        INSERT INTO project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES
            ('project-1', 2, 'ada', 'concept', 'General'),
            ('project-2', 3, 'ada', 'concept', 'General')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO entity_aliases (entity_id, alias)
        VALUES (2, 'widget-service'), (3, 'widget-service')
        """
    )

    matches = await EntityReader(real_postgres_client).get_entities_by_names(
        ["widget", "widget-service"],
        visible_project_ids=["project-1"],
    )

    assert len(matches) == 1
    assert matches[0]["id"] == 2
    assert matches[0]["canonical_name"] == "Widget"
    assert matches[0]["aliases"] == ["widget-service"]
    assert matches[0]["contexts"] == [
        {
            "project_id": "project-1",
            "entity_type": "concept",
            "topic": "General",
            "last_mentioned_ms": None,
        }
    ]
