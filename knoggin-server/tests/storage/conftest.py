import os
import pytest
from infrastructure.postgres_client import PostgresClient

DB_URL = os.environ.get(
    "KNOGGIN_TEST_DATABASE_URL",
    "postgresql://knoggin:knoggin@localhost:5432/knoggin_db"
)

@pytest.fixture
async def real_postgres_client():
    """Provides a connected PostgresClient against the real test database."""
    client = PostgresClient(dsn=DB_URL, min_size=1, max_size=2)
    await client.connect()
    try:
        yield client
    finally:
        await client.close()

@pytest.fixture(autouse=True)
async def clean_db(real_postgres_client):
    """Wipes the relational search tables and the AGE graph before every test."""
    # Truncate relational vector tables
    await real_postgres_client.execute_write(
        "TRUNCATE TABLE entity_search, message_search, fact_search;"
    )
    # Ensure the AGE graph exists before trying to wipe it
    graph_name = "knoggin_graph"
    res = await real_postgres_client.execute_read(
        "SELECT count(*) FROM ag_graph WHERE name = %s;",
        (graph_name,),
    )
    if res[0]['count'] == 0:
        await real_postgres_client.execute_write(f"SELECT create_graph('{graph_name}');")
        
    # Wipe the AGE graph nodes and relationships
    await real_postgres_client.execute_write(
        f"SELECT * FROM cypher('{graph_name}', $$ MATCH (n) DETACH DELETE n RETURN n $$) AS (n agtype);"
    )
