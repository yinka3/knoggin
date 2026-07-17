import json
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import Any, Optional, TypeAlias

import psycopg
from loguru import logger
from psycopg.adapt import Loader
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

QueryParams: TypeAlias = Mapping[str, Any] | Sequence[Any]


class _AgtypeLoader(Loader):
    """Decode AGE scalar, list, and map values into native Python values."""

    def load(self, data) -> Any:
        raw = bytes(data).decode("utf-8")
        value, separator, type_name = raw.rpartition("::")
        if separator and type_name in {"vertex", "edge", "path"}:
            raw = value
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            # Preserve unfamiliar AGE extensions without breaking the query.
            return raw


async def _configure_async_conn(conn: psycopg.AsyncConnection):
    """Load Apache AGE and set search path on every async connection."""
    await conn.execute("LOAD 'age';")
    await conn.execute('SET search_path = ag_catalog, "$user", public;')
    cursor = await conn.execute(
        "SELECT 'ag_catalog.agtype'::regtype::oid AS agtype_oid;"
    )
    row = await cursor.fetchone()
    if row is None:
        raise RuntimeError("Apache AGE agtype is unavailable after loading AGE")
    conn.adapters.register_loader(int(row["agtype_oid"]), _AgtypeLoader)
    await conn.commit()


class PostgresClient:
    """
    Asynchronous Postgres connection-pool client.
    Supports Apache AGE (graph) and pgvector/tsvector (hybrid storage).
    """

    def __init__(
        self,
        dsn: str,
        min_size: int = 1,
        max_size: int = 10,
        startup_timeout: float = 30.0,
    ):
        if min_size < 1:
            raise ValueError("PostgresClient min_size must be at least 1")
        if max_size < min_size:
            raise ValueError(
                "PostgresClient max_size must be greater than or equal to min_size"
            )
        if startup_timeout <= 0:
            raise ValueError("PostgresClient startup_timeout must be greater than 0")

        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self.startup_timeout = startup_timeout
        self._pool: Optional[AsyncConnectionPool] = None

    async def connect(self):
        """Open the pool after its minimum connections are ready for use."""
        if self._pool is not None:
            raise RuntimeError("PostgresClient is already connected")

        pool: Optional[AsyncConnectionPool] = None

        try:
            pool = AsyncConnectionPool(
                conninfo=self.dsn,
                min_size=self.min_size,
                max_size=self.max_size,
                kwargs={"autocommit": False, "row_factory": dict_row},
                configure=_configure_async_conn,
                open=False,
            )
            self._pool = pool
            await pool.open(wait=True, timeout=self.startup_timeout)
            logger.info("Connected to Postgres (async pool ready with AGE loaded)")
        except Exception as e:
            logger.error(f"Failed to connect to Postgres: {e}")
            if pool is not None:
                try:
                    await pool.close()
                except Exception as cleanup_error:
                    logger.error(
                        "Failed to close partially initialized Postgres pool: "
                        f"{cleanup_error}"
                    )
            self._pool = None
            raise

    async def close(self):
        """Close the asynchronous connection pool."""
        pool = self._pool
        self._pool = None
        if pool is not None:
            await pool.close()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[psycopg.AsyncCursor]:
        """Yield a cursor inside a managed connection and transaction."""
        if self._pool is None:
            raise RuntimeError("PostgresClient is not connected")

        async with self._pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    yield cur

    async def fetch_all(
        self, query: str, params: Optional[QueryParams] = None
    ) -> list[dict[str, Any]]:
        """Execute a statement and return all result rows."""
        async with self.transaction() as cur:
            await cur.execute(query, params)
            return await cur.fetchall()

    async def fetch_one(
        self, query: str, params: Optional[QueryParams] = None
    ) -> Optional[dict[str, Any]]:
        """Execute a statement and return its first row, if present."""
        async with self.transaction() as cur:
            await cur.execute(query, params)
            return await cur.fetchone()

    async def execute(self, query: str, params: Optional[QueryParams] = None) -> int:
        """Execute a statement and return its affected-row count."""
        async with self.transaction() as cur:
            await cur.execute(query, params)
            return cur.rowcount

    # --- Cypher Helpers ---

    @staticmethod
    def build_cypher(
        cypher_query: str,
        return_types: str = "result agtype",
        graph_name: str = "knoggin_graph",
    ) -> str:
        """
        Wraps a Cypher query in the required Apache AGE SQL syntax.
        Parameters should be passed to psycopg execution as `%s` (a JSON string).
        `return_types` dictates the expected output columns, such as
        ``id agtype, name agtype``.
        """
        return (
            f"SELECT * FROM cypher('{graph_name}', $${cypher_query}$$, %s) "
            f"AS ({return_types})"  # noqa: S608
        )
