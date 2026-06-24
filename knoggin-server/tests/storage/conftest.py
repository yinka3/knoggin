import os
from pathlib import Path

import pytest

from infrastructure.postgres_client import PostgresClient

STORAGE_TEST_ROOT = Path(__file__).resolve().parent
DB_URL = os.environ.get(
    "KNOGGIN_TEST_DATABASE_URL",
    "postgresql://knoggin:knoggin@localhost:5432/knoggin_db",
)


def pytest_collection_modifyitems(items):
    """Attach database cleanup only to tests that opt into real Postgres."""
    for item in items:
        item_path = Path(item.path).resolve()
        if (
            item_path.is_relative_to(STORAGE_TEST_ROOT)
            and item.get_closest_marker("requires_postgres") is not None
        ):
            item.add_marker(pytest.mark.usefixtures("clean_db"))


@pytest.fixture
async def real_postgres_client():
    """Provides a connected PostgresClient against the real test database."""
    client = PostgresClient(dsn=DB_URL, min_size=1, max_size=2)
    await client.connect()
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture
async def clean_db(real_postgres_client):
    """Wipes relational tables and the AGE graph before every test."""
    await real_postgres_client.execute_write(
        """
        TRUNCATE TABLE
            document_chunks,
            project_documents,
            document_folder_uploads,
            project_document_scan_settings,
            relationship_evidence_refs,
            relationships,
            hierarchy_edges,
            facts,
            entity_aliases,
            entities,
            messages,
            entity_search,
            message_search,
            fact_search;
        """
    )
    # Ensure the AGE graph exists before trying to wipe it
    graph_name = "knoggin_graph"
    res = await real_postgres_client.execute_read(
        "SELECT count(*) FROM ag_graph WHERE name = %s;",
        (graph_name,),
    )
    if res[0]["count"] == 0:
        await real_postgres_client.execute_write(
            f"SELECT create_graph('{graph_name}');"
        )

    # Wipe the AGE graph nodes and relationships
    wipe_graph_sql = (
        f"SELECT * FROM cypher('{graph_name}', "
        "$$ MATCH (n) DETACH DELETE n RETURN n $$"
        ") AS (n agtype);"
    )
    await real_postgres_client.execute_write(
        wipe_graph_sql
    )
