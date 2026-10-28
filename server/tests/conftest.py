import sys
from pathlib import Path

SERVER_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = SERVER_ROOT / "src"

for path in (SERVER_ROOT, SRC_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import os  # noqa: E402

import psycopg  # noqa: E402
import pytest  # noqa: E402
from psycopg import sql  # noqa: E402
from psycopg.conninfo import conninfo_to_dict, make_conninfo  # noqa: E402

# TEST_DATABASE_URL only selects the Postgres server. Tests that need a
# standing database always use this name, so they can never touch the
# application database (knoggin_db) or another database on the server.
TEST_DATABASE_NAME = "knoggin_testdb"
_SERVER_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql://knoggin:knoggin@127.0.0.1:5432/postgres"
)


def server_database_url(database: str) -> str:
    params = conninfo_to_dict(_SERVER_URL)
    params["dbname"] = database
    return make_conninfo(**params)


@pytest.fixture(scope="session")
def test_database_url():
    """Recreate knoggin_testdb through the migrations once per test session."""

    from infrastructure.schema_migrations import migrate

    with psycopg.connect(server_database_url("postgres"), autocommit=True) as admin:
        admin.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(TEST_DATABASE_NAME)
            )
        )
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(TEST_DATABASE_NAME)))
    url = server_database_url(TEST_DATABASE_NAME)
    migrate(url)
    return url
