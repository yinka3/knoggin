"""Fresh-database schema and extension bootstrap contracts."""

import asyncio
import os
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from infrastructure.postgres_client import PostgresClient
from infrastructure.schema_migrations import MIGRATIONS_DIR, migrate

DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://knoggin:knoggin@127.0.0.1:5432/postgres",
)
SCHEMA_SQL = (MIGRATIONS_DIR / "0000_baseline.sql").read_text(encoding="utf-8")


def test_schema_is_fresh_install_ddl_without_historical_upgrade_steps():
    assert "CREATE TABLE IF NOT EXISTS" not in SCHEMA_SQL
    assert "CREATE INDEX IF NOT EXISTS" not in SCHEMA_SQL
    assert "ADD COLUMN" not in SCHEMA_SQL
    assert "DROP COLUMN" not in SCHEMA_SQL
    assert "DROP TABLE" not in SCHEMA_SQL
    assert "NOT VALID" not in SCHEMA_SQL
    assert "-- Migration" not in SCHEMA_SQL


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
async def test_schema_bootstraps_a_fresh_database_with_vector():
    """The migrations build a new database, extension included."""

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
        ran = await asyncio.to_thread(migrate, database_conninfo)
        assert ran == sorted(path.stem for path in MIGRATIONS_DIR.glob("*.sql"))
        assert ran[0] == "0000_baseline"
        await client.connect()

        extension = await client.fetch_one(
            "SELECT extname FROM pg_extension WHERE extname = 'vector'"
        )
        assert extension == {"extname": "vector"}
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
            "SELECT 1 AS present FROM information_schema.columns "
            "WHERE table_schema = 'public' "
            "AND table_name = 'relationship_observations' "
            "AND column_name = 'confidence'"
        ) is None
        assert await client.fetch_one(
            "SELECT 1 AS present FROM information_schema.columns "
            "WHERE table_schema = 'public' "
            "AND table_name = 'messages' "
            "AND column_name = 'ingestion_state'"
        ) is None
        tables = await client.fetch_all(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name"
        )
        table_names = {row["table_name"] for row in tables}
        assert {
            "aac_discussions",
            "aac_insights",
            "aac_insight_votes",
            "aac_timeline",
            "entity_global_merge_audits",
            "entity_global_merge_mutations",
            "maintenance_review_resolutions",
            "project_entity_contexts",
            "project_jev_identity_decisions",
            "project_jev_extraction_decisions",
            "project_jev_classification_decisions",
        } <= table_names
        assert {
            "entity_merge_audits",
            "entity_merge_proposals",
            "ingestion_candidate_suggestions",
            "parked_dlq_items",
        }.isdisjoint(table_names)
        assert await client.fetch_one(
            "SELECT is_nullable FROM information_schema.columns "
            "WHERE table_schema = 'public' "
            "AND table_name = 'aac_discussions' AND column_name = 'end_reason'"
        ) == {"is_nullable": "YES"}
        assert await client.fetch_all(
            "SELECT conrelid::regclass::text AS table_name, confdeltype "
            "FROM pg_constraint "
            "WHERE conname IN ('aac_insights_discussion_id_fkey', "
            "'aac_timeline_discussion_id_fkey') ORDER BY conname"
        ) == [
            {"table_name": "aac_insights", "confdeltype": "n"},
            {"table_name": "aac_timeline", "confdeltype": "c"},
        ]
        assert await client.fetch_one(
            "SELECT count(*) AS invalid_constraints FROM pg_constraint "
            "WHERE connamespace = 'public'::regnamespace AND NOT convalidated"
        ) == {"invalid_constraints": 0}
        assert await client.fetch_all(
            "SELECT proname FROM pg_proc "
            "WHERE pronamespace = 'public'::regnamespace "
            "AND proname IN ('enforce_message_entity_ref_scope', "
            "'enforce_relationship_scope', 'reject_entity_identity_mutation') "
            "ORDER BY proname"
        ) == [
            {"proname": "enforce_message_entity_ref_scope"},
            {"proname": "enforce_relationship_scope"},
            {"proname": "reject_entity_identity_mutation"},
        ]
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
