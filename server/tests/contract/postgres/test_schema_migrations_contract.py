"""Migrations run once each, in order, and refuse databases they did not build."""

import os
import shutil
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from infrastructure import schema_migrations
from infrastructure.schema_migrations import UnmanagedDatabaseError, migrate

DB_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql://knoggin:knoggin@127.0.0.1:5432/postgres"
)


@pytest.fixture
def empty_database():
    params = conninfo_to_dict(DB_URL)
    admin_url = make_conninfo(**{**params, "dbname": "postgres"})
    database = f"knoggin_migrations_{uuid4().hex[:12]}"
    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    try:
        yield make_conninfo(**{**params, "dbname": database})
    finally:
        with psycopg.connect(admin_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(database)
                )
            )


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.slow
@pytest.mark.no_network
def test_a_new_migration_runs_once_and_a_failed_one_leaves_no_trace(
    empty_database, tmp_path, monkeypatch
):
    shutil.copy(schema_migrations.MIGRATIONS_DIR / "0000_baseline.sql", tmp_path)
    monkeypatch.setattr(schema_migrations, "MIGRATIONS_DIR", tmp_path)
    assert migrate(empty_database) == ["0000_baseline"]
    assert migrate(empty_database) == []

    (tmp_path / "0001_add_note.sql").write_text(
        "ALTER TABLE public.projects ADD COLUMN note text;", encoding="utf-8"
    )
    (tmp_path / "0002_broken.sql").write_text(
        "ALTER TABLE public.projects ADD COLUMN later text;\nSELECT missing_function();",
        encoding="utf-8",
    )
    with pytest.raises(psycopg.Error):
        migrate(empty_database)
    with psycopg.connect(empty_database) as conn:
        versions = [row[0] for row in conn.execute(
            "SELECT version FROM public.schema_migrations ORDER BY version"
        )]
        columns = {row[0] for row in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'projects'"
        )}
    # 0001 stays applied; 0002 rolled back as a whole and will run again.
    assert versions == ["0000_baseline", "0001_add_note"]
    assert "note" in columns and "later" not in columns

    (tmp_path / "0002_broken.sql").write_text(
        "ALTER TABLE public.projects ADD COLUMN later text;", encoding="utf-8"
    )
    assert migrate(empty_database) == ["0002_broken"]


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
def test_a_database_with_unknown_tables_is_refused(empty_database):
    with psycopg.connect(empty_database, autocommit=True) as conn:
        conn.execute("CREATE TABLE public.projects (project_id text)")
    with pytest.raises(UnmanagedDatabaseError):
        migrate(empty_database)
