import asyncio
from typing import Any, Dict, List, Optional

import psycopg
from loguru import logger
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, ConnectionPool


def _configure_sync_conn(conn: psycopg.Connection):
    """Load Apache AGE and set search path on every sync connection."""
    conn.execute("LOAD 'age';")
    conn.execute('SET search_path = ag_catalog, "$user", public;')
    conn.commit()


async def _configure_async_conn(conn: psycopg.AsyncConnection):
    """Load Apache AGE and set search path on every async connection."""
    await conn.execute("LOAD 'age';")
    await conn.execute('SET search_path = ag_catalog, "$user", public;')
    await conn.commit()


class PostgresClient:
    """
    Unified Postgres client managing both Async and Sync connection pools.
    Supports Apache AGE (graph) and pgvector/tsvector (hybrid storage).
    """

    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 10):
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self.async_pool: Optional[AsyncConnectionPool] = None
        self.sync_pool: Optional[ConnectionPool] = None

    async def connect(self):
        """Initialize both connection pools asynchronously."""
        try:
            self.async_pool = AsyncConnectionPool(
                conninfo=self.dsn,
                min_size=self.min_size,
                max_size=self.max_size,
                kwargs={"autocommit": False, "row_factory": dict_row},
                configure=_configure_async_conn,
                open=False,
            )
            await self.async_pool.open()

            # Sync pool initialized in thread to avoid blocking event loop
            def _init_sync():
                pool = ConnectionPool(
                    conninfo=self.dsn,
                    min_size=self.min_size,
                    max_size=self.max_size,
                    kwargs={"autocommit": False, "row_factory": dict_row},
                    configure=_configure_sync_conn,
                    open=False,
                )
                pool.open()
                return pool

            self.sync_pool = await asyncio.to_thread(_init_sync)

            logger.info(
                "Connected to Postgres (Async and Sync pools initialized with AGE loaded)"
            )
        except Exception as e:
            logger.error(f"Failed to connect to Postgres: {e}")
            raise

    async def close(self):
        """Close both connection pools."""
        if self.async_pool:
            await self.async_pool.close()
        if self.sync_pool:
            await asyncio.to_thread(self.sync_pool.close)

    # --- Async Helpers ---

    async def execute_read(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a read-only SQL or AGE query asynchronously."""
        if not self.async_pool:
            raise RuntimeError("PostgresClient async_pool is not initialized")

        async with self.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(query, params or {})
                    return await cur.fetchall()

    async def execute_write(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> int:
        """Execute a write SQL or AGE query asynchronously. Returns rowcount."""
        if not self.async_pool:
            raise RuntimeError("PostgresClient async_pool is not initialized")

        async with self.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(query, params or {})
                    return cur.rowcount

    # --- Sync Helpers (for Background Threads) ---

    def execute_read_sync(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a read-only SQL or AGE query synchronously."""
        if not self.sync_pool:
            raise RuntimeError("PostgresClient sync_pool is not initialized")

        with self.sync_pool.connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(query, params or {})
                    return cur.fetchall()

    def execute_write_sync(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> int:
        """Execute a write SQL or AGE query synchronously. Returns rowcount."""
        if not self.sync_pool:
            raise RuntimeError("PostgresClient sync_pool is not initialized")

        with self.sync_pool.connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(query, params or {})
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
        `return_types` dictates the expected output columns, e.g., 'id agtype, name agtype'.
        """
        return f"SELECT * FROM cypher('{graph_name}', $${cypher_query}$$, %s) AS ({return_types})"  # noqa: S608
