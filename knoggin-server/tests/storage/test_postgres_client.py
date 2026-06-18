import os
import pytest

from infrastructure.postgres_client import PostgresClient

DB_URL = os.environ.get(
    "KNOGGIN_TEST_DATABASE_URL",
    "postgresql://knoggin:knoggin@localhost:5432/knoggin_db"
)

@pytest.mark.storage
@pytest.mark.no_network
def test_build_cypher_wraps_query_with_graph_and_return_types():
    query = PostgresClient.build_cypher(
        "MATCH (n) RETURN n.id",
        return_types="id agtype",
        graph_name="test_graph",
    )

    assert "cypher('test_graph'" in query
    assert "MATCH (n) RETURN n.id" in query
    assert "AS (id agtype)" in query


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.slow
async def test_postgres_client_connects_when_test_database_is_configured():
    client = PostgresClient(
        DB_URL,
        min_size=0,
        max_size=2,
    )

    await client.connect()
    try:
        assert client.async_pool is not None
        assert client.sync_pool is not None
    finally:
        await client.close()
