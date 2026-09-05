"""Shared real-service fixtures for ingestion integration contracts."""

import json
import os
import uuid
from dataclasses import asdict
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from infrastructure.postgres_client import PostgresClient
from tests.fixtures.factories import make_domain_config

DB_URL = os.environ.get(
    "KNOGGIN_TEST_DATABASE_URL",
    "postgresql://knoggin:knoggin@localhost:5432/knoggin_db",
)


def _database_url(database: str) -> str:
    params = conninfo_to_dict(DB_URL)
    params["dbname"] = database
    return make_conninfo(**params)


@pytest.fixture
async def real_server_scope():
    database = f"knoggin_ingestion_{uuid.uuid4().hex[:12]}"
    admin_url = _database_url("postgres")
    database_url = _database_url(database)
    schema_sql = (
        Path(__file__).resolve().parents[3] / "src" / "infrastructure" / "schema.sql"
    ).read_text(encoding="utf-8")
    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute("CREATE EXTENSION vector")
        connection.execute("CREATE EXTENSION age")
        connection.execute("LOAD 'age'")
        connection.execute('SET search_path = ag_catalog, "$user", public')
        connection.execute(schema_sql)
        connection.execute("SELECT ag_catalog.create_graph('knoggin_graph')")

    postgres = PostgresClient(dsn=database_url, min_size=1, max_size=2)
    await postgres.connect()
    suffix = uuid.uuid4().hex[:12]
    user_name = "server_acceptance_test_user"
    project_id = f"server-acceptance-project-{suffix}"
    session_id = f"server-acceptance-session-{suffix}"
    await postgres.execute(
        """
        INSERT INTO projects (project_id, user_name, name, domain_config)
        VALUES (%s, %s, %s, %s)
        """,
        (
            project_id,
            user_name,
            "Server acceptance integration",
            json.dumps(asdict(make_domain_config())),
        ),
    )
    await postgres.execute(
        "INSERT INTO sessions (session_id, user_name, project_id) VALUES (%s, %s, %s)",
        (session_id, user_name, project_id),
    )
    try:
        yield {
            "postgres": postgres,
            "user_name": user_name,
            "project_id": project_id,
            "session_id": session_id,
        }
    finally:
        await postgres.close()
        with psycopg.connect(admin_url, autocommit=True) as admin:
            admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (database,),
            )
            admin.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database)))
