import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from infrastructure.postgres_client import PostgresClient

DB_URL = os.environ.get(
    "KNOGGIN_TEST_DATABASE_URL",
    "postgresql://knoggin:knoggin@localhost:5432/knoggin_db",
)


def _database_url(database: str) -> str:
    params = conninfo_to_dict(DB_URL)
    params["dbname"] = database
    return make_conninfo(**params)


@pytest.fixture(scope="session")
def storage_database_url():
    """Create one canonical fresh database for the storage contract session."""

    database = f"knoggin_storage_{uuid4().hex[:12]}"
    admin_url = _database_url("postgres")
    database_url = _database_url(database)
    schema_sql = (
        Path(__file__).resolve().parents[3]
        / "src"
        / "infrastructure"
        / "schema.sql"
    ).read_text(encoding="utf-8")

    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    try:
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute("CREATE EXTENSION vector")
            connection.execute("CREATE EXTENSION age")
            connection.execute("LOAD 'age'")
            connection.execute('SET search_path = ag_catalog, "$user", public')
            connection.execute(schema_sql)
            connection.execute("SELECT ag_catalog.create_graph('knoggin_graph')")
        yield database_url
    finally:
        with psycopg.connect(admin_url, autocommit=True) as admin:
            admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (database,),
            )
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database))
            )


@pytest.fixture
async def real_postgres_client(storage_database_url):
    """Provide a clean client without replaying schema migrations per test."""

    client = PostgresClient(dsn=storage_database_url, min_size=1, max_size=2)
    await client.connect()
    try:
        await _reset_storage_db(client)
        yield client
    finally:
        await client.close()


async def _reset_storage_db(client: PostgresClient):
    """Truncate canonical tables, seed projects, and wipe the AGE projection."""

    await client.execute(
        """
        DO $$
        DECLARE
            table_names TEXT;
        BEGIN
            SELECT string_agg(format('%I.%I', schemaname, tablename), ', ')
            INTO table_names
            FROM pg_tables
            WHERE schemaname = 'public';

            IF table_names IS NOT NULL THEN
                EXECUTE 'TRUNCATE TABLE ' || table_names
                    || ' RESTART IDENTITY CASCADE';
            END IF;
        END
        $$;
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
