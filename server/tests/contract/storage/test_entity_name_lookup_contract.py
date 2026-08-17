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
        INSERT INTO entities (
            entity_id, user_name, project_id, canonical_name, topic
        ) VALUES
            (2, 'ada', 'project-1', 'Widget', 'General'),
            (3, 'ada', 'project-2', 'Other Widget', 'General')
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

    assert matches == [
        {
            "id": 2,
            "project_id": "project-1",
            "canonical_name": "Widget",
            "type": None,
            "aliases": ["widget-service"],
        }
    ]
