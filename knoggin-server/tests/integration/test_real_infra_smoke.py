import json
import os

import psycopg
import pytest
from psycopg.rows import dict_row

from infrastructure.postgres_client import PostgresClient


EXPECTED_TABLES = {
    "entity_search",
    "message_search",
    "fact_search",
}

EXPECTED_INDEXES = {
    "entity_search_embedding_idx",
    "message_search_fts_idx",
    "fact_search_embedding_idx",
    "entity_search_project_idx",
    "message_search_session_idx",
    "fact_search_project_idx",
}


requires_real_infra = pytest.mark.skipif(
    not os.environ.get("KNOGGIN_TEST_DATABASE_URL")
    or not os.environ.get("KNOGGIN_TEST_REDIS_URL"),
    reason=(
        "Set KNOGGIN_TEST_DATABASE_URL and KNOGGIN_TEST_REDIS_URL to run real "
        "infra smoke tests."
    ),
)


requires_real_postgres = pytest.mark.skipif(
    not os.environ.get("KNOGGIN_TEST_DATABASE_URL"),
    reason="Set KNOGGIN_TEST_DATABASE_URL to run real Postgres infra tests.",
)


def _execute_direct_read(query, params=None, load_age=True):
    with psycopg.connect(
        os.environ["KNOGGIN_TEST_DATABASE_URL"],
        autocommit=True,
        row_factory=dict_row,
    ) as conn:
        if load_age:
            conn.execute("LOAD 'age';")
            conn.execute('SET search_path = ag_catalog, "$user", public;')
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            return cur.fetchall()


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_redis
@pytest.mark.requires_pgvector
@pytest.mark.slow
@requires_real_infra
async def test_real_postgres_and_redis_connections_are_available():
    import redis.asyncio as redis

    redis_client = redis.from_url(os.environ["KNOGGIN_TEST_REDIS_URL"])

    try:
        assert _execute_direct_read("SELECT 1 AS ok", load_age=False) == [{"ok": 1}]
        assert await redis_client.ping() is True
    finally:
        await redis_client.aclose()


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.slow
@requires_real_postgres
async def test_real_postgres_extensions_search_path_and_graph_are_ready():
    extension_rows = _execute_direct_read(
        "SELECT extname FROM pg_extension WHERE extname IN ('age', 'vector')",
        load_age=False,
    )
    extensions = {row["extname"] for row in extension_rows}
    assert {"age", "vector"}.issubset(extensions)

    search_path_rows = _execute_direct_read("SHOW search_path")
    assert "ag_catalog" in search_path_rows[0]["search_path"]

    graph_rows = _execute_direct_read(
        "SELECT name FROM ag_graph WHERE name = %s",
        ("knoggin_graph",),
    )
    assert graph_rows == [{"name": "knoggin_graph"}]


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.slow
@requires_real_postgres
async def test_real_postgres_schema_tables_and_indexes_are_present():
    table_rows = _execute_direct_read(
        """
        SELECT table_name, to_regclass('public.' || table_name) AS regclass
        FROM (
            VALUES
                ('entity_search'),
                ('message_search'),
                ('fact_search')
        ) AS expected(table_name)
        """,
        load_age=False,
    )
    present_tables = {row["table_name"] for row in table_rows if row["regclass"]}
    missing_tables = EXPECTED_TABLES - present_tables
    assert not missing_tables, (
        "Missing expected schema tables. If this is a Docker Postgres volume "
        "created before schema.sql was mounted, recreate the volume or apply "
        f"schema.sql manually. Missing: {sorted(missing_tables)}"
    )

    index_rows = _execute_direct_read(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname = ANY(%s)
        """,
        (list(EXPECTED_INDEXES),),
        load_age=False,
    )
    present_indexes = {row["indexname"] for row in index_rows}
    missing_indexes = EXPECTED_INDEXES - present_indexes
    assert not missing_indexes, (
        "Missing expected schema indexes. If tables exist but indexes are missing, "
        f"re-apply schema.sql. Missing: {sorted(missing_indexes)}"
    )


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.slow
@requires_real_postgres
async def test_real_age_cypher_query_executes_through_postgres_client():
    query = PostgresClient.build_cypher("RETURN 1 AS ok", "ok agtype")

    rows = _execute_direct_read(query, (json.dumps({}),))
    assert rows
    assert rows[0]["ok"] is not None


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.skipif(
    not os.environ.get("KNOGGIN_TEST_REDIS_URL"),
    reason="Set KNOGGIN_TEST_REDIS_URL to run real Redis infra tests.",
)
async def test_real_redis_server_metadata_is_available():
    import redis.asyncio as redis

    redis_client = redis.from_url(os.environ["KNOGGIN_TEST_REDIS_URL"])

    try:
        assert await redis_client.ping() is True
        server_info = await redis_client.info("server")
        assert server_info["redis_version"]
    finally:
        await redis_client.aclose()
