import os
from pathlib import Path

import pytest

from infrastructure.postgres_client import PostgresClient

DB_URL = os.environ.get(
    "KNOGGIN_TEST_DATABASE_URL",
    "postgresql://knoggin:knoggin@localhost:5432/knoggin_db",
)


@pytest.fixture
async def real_postgres_client():
    """Provides a connected PostgresClient against the real test database."""
    client = PostgresClient(dsn=DB_URL, min_size=1, max_size=2)
    await client.connect()
    try:
        await _reset_storage_db(client)
        yield client
    finally:
        await client.close()


@pytest.fixture
async def clean_db(real_postgres_client):
    """Wipes relational tables and the AGE graph before every test."""
    await _reset_storage_db(real_postgres_client)


async def _reset_storage_db(client: PostgresClient):
    """Wipe relational tables and the AGE graph, then seed baseline projects."""
    schema_sql = (
        Path(__file__).resolve().parents[3]
        / "src"
        / "infrastructure"
        / "schema.sql"
    ).read_text(encoding="utf-8")
    await client.execute(schema_sql)
    await client.execute(
        """
        TRUNCATE TABLE
            project_read_scopes,
            agent_tool_audits,
            episode_relationships,
            episode_entities,
            episode_messages,
            episode_processing_checkpoints,
            episodes,
            agent_brain_snapshots,
            agents,
            message_source_refs,
            project_artifact_revisions,
            project_artifacts,
            document_chunks,
            project_documents,
            project_document_scan_settings,
            entity_merge_audits,
            entity_merge_proposals,
            maintenance_review_events,
            maintenance_review_evidence,
            maintenance_reinterpretation_audits,
            maintenance_review_checkpoints,
            maintenance_reviews,
            relationship_observations,
            message_entity_refs,
            project_entity_contexts,
            relationships,
            entity_aliases,
            entities,
            messages,
            sessions,
            projects
        RESTART IDENTITY CASCADE;
        """
    )
    await client.execute(
        """
        INSERT INTO projects (
            project_id, user_name, name, domain_config
        )
        VALUES
            (
                'project-1', 'ada', 'Project 1',
                '{"version":1,"topics":{"Identity":{"description":"","active":true},"General":{"description":"","active":true}},"entity_types":{"Identity":{"topic":"Identity","description":"","labels":["person"]},"Concept":{"topic":"General","description":"","labels":["concept"]}},"relationships":{}}'::jsonb
            ),
            (
                'project-2', 'ada', 'Project 2',
                '{"version":1,"topics":{"Identity":{"description":"","active":true},"General":{"description":"","active":true}},"entity_types":{"Identity":{"topic":"Identity","description":"","labels":["person"]},"Concept":{"topic":"General","description":"","labels":["concept"]}},"relationships":{}}'::jsonb
            )
        ON CONFLICT (project_id) DO NOTHING;
        """
    )
    # Ensure the AGE graph exists before trying to wipe it
    graph_name = "knoggin_graph"
    row = await client.fetch_one(
        "SELECT count(*) FROM ag_graph WHERE name = %s;",
        (graph_name,),
    )
    if row["count"] == 0:
        await client.execute(
            f"SELECT create_graph('{graph_name}');"
        )

    # Wipe the AGE graph nodes and relationships
    wipe_graph_sql = (
        f"SELECT * FROM cypher('{graph_name}', "
        "$$ MATCH (n) DETACH DELETE n RETURN n $$"
        ") AS (n agtype);"
    )
    await client.execute(wipe_graph_sql)
