"""Bring a database to the current schema by running each migration once."""

from __future__ import annotations

from pathlib import Path

import psycopg

MIGRATIONS_DIR = Path(__file__).parent / "migrations"
# Any fixed key works; it only has to be the same for every Knoggin process.
_LOCK_KEY = 0x6B6E6F67


class UnmanagedDatabaseError(RuntimeError):
    """The database has tables but no record of which migrations it ran."""


def migrate(conninfo: str) -> list[str]:
    """Apply pending migrations in order and return the ones that ran.

    Each file runs in its own transaction with its ledger row, so a failure
    leaves the database at the last complete migration.
    """
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (_LOCK_KEY,))
        try:
            _ensure_ledger(conn)
            applied = {
                row[0] for row in conn.execute("SELECT version FROM public.schema_migrations")
            }
            ran = []
            for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
                if path.stem in applied:
                    continue
                with conn.transaction():
                    conn.execute(path.read_text(encoding="utf-8"))
                    conn.execute(
                        "INSERT INTO public.schema_migrations (version) VALUES (%s)",
                        (path.stem,),
                    )
                # The baseline is a pg_dump that changes session settings.
                conn.execute("RESET ALL")
                ran.append(path.stem)
            return ran
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (_LOCK_KEY,))


def _ensure_ledger(conn: psycopg.Connection) -> None:
    if conn.execute("SELECT to_regclass('public.schema_migrations')").fetchone()[0]:
        return
    tables = conn.execute(
        "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'"
    ).fetchone()[0]
    if tables:
        raise UnmanagedDatabaseError(
            "This database has tables but no schema_migrations record, so its "
            "schema version is unknown. Point DATABASE_URL at an empty database, "
            "or back this one up and recreate it."
        )
    conn.execute(
        """CREATE TABLE public.schema_migrations (
               version text PRIMARY KEY,
               applied_at timestamp with time zone DEFAULT now() NOT NULL
           )"""
    )
