from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import Any, Optional, TypeAlias

import psycopg
from loguru import logger
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

QueryParams: TypeAlias = Mapping[str, Any] | Sequence[Any]


class PostgresClient:
    """
    Asynchronous Postgres connection-pool client (pgvector and full-text search).
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
                open=False,
            )
            self._pool = pool
            await pool.open(wait=True, timeout=self.startup_timeout)
            logger.info("Connected to Postgres (async pool ready)")
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
                else:
                    self._pool = None
            raise

    async def close(self):
        """Close the asynchronous connection pool."""
        pool = self._pool
        if pool is not None:
            await pool.close()
            if self._pool is pool:
                self._pool = None

    def pool_snapshot(self) -> dict[str, bool | int]:
        """Return a bounded, read-only snapshot of pool state.

        ``get_stats`` is an in-process counter read and does not perform SQL or
        alter pool state. Only the small allowlist below is returned; in
        particular, the configured DSN is never exposed to callers.
        """

        snapshot: dict[str, bool | int] = {
            "connected": self._pool is not None,
            "pool_min": self.min_size,
            "pool_max": self.max_size,
            "pool_size": 0,
            "pool_available": 0,
            "requests_waiting": 0,
            "stats_available": False,
        }
        pool = self._pool
        get_stats = getattr(pool, "get_stats", None) if pool is not None else None
        if not callable(get_stats):
            return snapshot
        try:
            stats = get_stats()
        except Exception:
            return snapshot
        if not isinstance(stats, Mapping):
            return snapshot

        for key in ("pool_size", "pool_available", "requests_waiting"):
            value = stats.get(key)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                snapshot[key] = value
        snapshot["stats_available"] = True
        return snapshot

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

