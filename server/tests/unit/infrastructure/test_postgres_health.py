import pytest

from infrastructure.postgres_client import PostgresClient


@pytest.mark.unit
@pytest.mark.no_network
def test_pool_snapshot_when_disconnected_is_safe() -> None:
    client = PostgresClient(
        "postgresql://user:password@example.invalid/db",
        min_size=2,
        max_size=5,
    )

    snapshot = client.pool_snapshot()

    assert snapshot == {
        "connected": False,
        "pool_min": 2,
        "pool_max": 5,
        "pool_size": 0,
        "pool_available": 0,
        "requests_waiting": 0,
        "stats_available": False,
    }
    assert "dsn" not in snapshot


@pytest.mark.unit
@pytest.mark.no_network
def test_pool_snapshot_allowlists_stats_and_does_not_mutate_pool() -> None:
    class FakePool:
        def __init__(self) -> None:
            self.stats = {
                "pool_size": 4,
                "pool_available": 2,
                "requests_waiting": 1,
                "dsn": "postgresql://secret",
                "unknown": object(),
            }
            self.calls = 0

        def get_stats(self) -> dict[str, object]:
            self.calls += 1
            return dict(self.stats)

    client = PostgresClient("postgresql://user:password@example.invalid/db")
    pool = FakePool()
    client._pool = pool  # noqa: SLF001 - private pool is isolated in this unit test.
    before = dict(pool.stats)

    snapshot = client.pool_snapshot()

    assert snapshot == {
        "connected": True,
        "pool_min": 1,
        "pool_max": 10,
        "pool_size": 4,
        "pool_available": 2,
        "requests_waiting": 1,
        "stats_available": True,
    }
    assert pool.calls == 1
    assert pool.stats == before
    assert "dsn" not in snapshot


@pytest.mark.unit
@pytest.mark.no_network
def test_pool_snapshot_degrades_safely_when_stats_are_unavailable() -> None:
    class BrokenPool:
        def get_stats(self) -> dict[str, object]:
            raise RuntimeError("stats unavailable")

    client = PostgresClient("postgresql://user:password@example.invalid/db")
    client._pool = BrokenPool()  # noqa: SLF001 - private pool is isolated in this unit test.

    snapshot = client.pool_snapshot()

    assert snapshot["connected"] is True
    assert snapshot["stats_available"] is False
    assert snapshot["pool_size"] == 0


@pytest.mark.unit
@pytest.mark.no_network
def test_resource_work_snapshot_includes_postgres_without_exposing_internal_state() -> None:
    from runtime.resources import ResourceManager

    class FakePostgres:
        def pool_snapshot(self) -> dict[str, bool | int]:
            return {
                "connected": True,
                "pool_min": 1,
                "pool_max": 2,
                "pool_size": 1,
                "pool_available": 1,
                "requests_waiting": 0,
                "stats_available": True,
            }

    manager = object.__new__(ResourceManager)
    manager.postgres = FakePostgres()
    manager.background_work = None
    manager.model_work = None

    snapshot = manager.work_snapshot()

    assert snapshot["postgres"] == manager.postgres.pool_snapshot()
    assert snapshot["background_work"] is None
    assert snapshot["model_work"] is None
