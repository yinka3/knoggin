from typing import Any, Dict, List, Optional

import psycopg
from loguru import logger
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


async def _configure_async_conn(conn: psycopg.AsyncConnection):
    """Load Apache AGE and set search path on every async connection."""
    await conn.execute("LOAD 'age';")
    await conn.execute('SET search_path = ag_catalog, "$user", public;')
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
        self.async_pool: Optional[AsyncConnectionPool] = None

    async def connect(self):
        """Open the pool after its minimum connections are ready for use."""
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
            self.async_pool = pool
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
            self.async_pool = None
            raise

    async def close(self):
        """Close the asynchronous connection pool."""
        if self.async_pool:
            await self.async_pool.close()

    # --- Query Helpers ---

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
