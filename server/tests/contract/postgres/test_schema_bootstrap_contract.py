"""Fresh-database schema and extension bootstrap contracts."""

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
SCHEMA_SQL = (
    Path(__file__).resolve().parents[3] / "src" / "infrastructure" / "schema.sql"
).read_text(encoding="utf-8")


def test_schema_drops_obsolete_ingestion_tables():
    assert "DROP TABLE IF EXISTS public.ingestion_candidate_suggestions;" in SCHEMA_SQL
    assert "DROP TABLE IF EXISTS public.parked_dlq_items;" in SCHEMA_SQL


def test_schema_keeps_aac_state_user_owned_and_insights_retention_independent():
    assert "CREATE TABLE IF NOT EXISTS public.aac_discussions" in SCHEMA_SQL
    assert "CREATE TABLE IF NOT EXISTS public.aac_timeline" in SCHEMA_SQL
    assert "CREATE TABLE IF NOT EXISTS public.aac_insights" in SCHEMA_SQL
    assert "CREATE TABLE IF NOT EXISTS public.aac_insight_votes" in SCHEMA_SQL
    assert """discussion_id TEXT REFERENCES public.aac_discussions(discussion_id)
        ON DELETE SET NULL""" in SCHEMA_SQL
    assert """discussion_id TEXT NOT NULL REFERENCES public.aac_discussions(discussion_id)
        ON DELETE CASCADE""" in SCHEMA_SQL


def _conninfo_for_database(database: str) -> str:
    params = conninfo_to_dict(DB_URL)
    params["dbname"] = database
    return make_conninfo(**params)


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_async_psycopg
@pytest.mark.slow
@pytest.mark.no_network
async def test_schema_bootstraps_a_fresh_database_with_age_and_vector():
    """A new database accepts extension setup, AGE graph setup, and schema SQL."""

    database = f"knoggin_p2_bootstrap_{uuid4().hex[:12]}"
    admin_conninfo = _conninfo_for_database("postgres")
    database_conninfo = _conninfo_for_database(database)

    admin = await psycopg.AsyncConnection.connect(admin_conninfo, autocommit=True)
    try:
        await admin.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database))
        )
    finally:
        await admin.close()

    client = PostgresClient(database_conninfo, min_size=1, max_size=1)
    try:
        extension_conn = await psycopg.AsyncConnection.connect(
            database_conninfo,
            autocommit=True,
        )
        try:
            await extension_conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
            await extension_conn.execute("CREATE EXTENSION IF NOT EXISTS age")
        finally:
            await extension_conn.close()

        await client.connect()
        await client.execute("SELECT create_graph('knoggin_graph')")
        await client.execute(SCHEMA_SQL)

        extensions = await client.fetch_all(
            "SELECT extname FROM pg_extension WHERE extname IN ('age', 'vector') "
            "ORDER BY extname"
        )
        assert extensions == [{"extname": "age"}, {"extname": "vector"}]
        assert await client.fetch_one(
            "SELECT 1 AS present FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = 'messages'"
        ) == {"present": 1}
        assert await client.fetch_one(
            "SELECT is_nullable FROM information_schema.columns "
            "WHERE table_schema = 'public' "
            "AND table_name = 'messages' "
            "AND column_name = 'session_id'"
        ) == {"is_nullable": "NO"}
        assert await client.fetch_one(
            "SELECT 1 AS present FROM information_schema.columns "
            "WHERE table_schema = 'public' "
            "AND table_name = 'agent_tool_audits' "
            "AND column_name = 'confirmation_state'"
        ) is None
        assert await client.fetch_one(
            "SELECT 1 AS present FROM ag_catalog.ag_graph WHERE name = 'knoggin_graph'"
        ) == {"present": 1}
    finally:
        await client.close()
        drop_admin = await psycopg.AsyncConnection.connect(
            admin_conninfo,
            autocommit=True,
        )
        try:
            await drop_admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database))
            )
        finally:
            await drop_admin.close()
