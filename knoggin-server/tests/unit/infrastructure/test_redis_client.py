import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from common.exceptions import ConfigurationError, DependencyError
from common.schema.settings import RedisConnectionSettings
from infrastructure import redis_client as redis_module
from infrastructure.redis_client import AsyncRedisClient


def settings(**overrides):
    return RedisConnectionSettings(**overrides)


def make_client(*, ping_side_effect=None):
    client = MagicMock()
    client.ping = AsyncMock(side_effect=ping_side_effect)
    client.aclose = AsyncMock()
    return client


@pytest.mark.unit
@pytest.mark.no_network
def test_settings_read_environment_at_call_time(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "rediss://user:secret@redis.example:6380/4")
    monkeypatch.setenv("REDIS_MAX_CONNECTIONS", "25")
    monkeypatch.setenv("REDIS_HEALTH_CHECK_INTERVAL", "45")
    monkeypatch.setenv("REDIS_CONNECT_TIMEOUT", "1.5")
    monkeypatch.setenv("REDIS_STARTUP_ATTEMPTS", "4")
    monkeypatch.setenv("REDIS_STARTUP_BACKOFF_SECONDS", "0.1")

    configured = RedisConnectionSettings.from_env()

    assert configured.url == "rediss://user:secret@redis.example:6380/4"
    assert configured.max_connections == 25
    assert configured.health_check_interval == 45
    assert configured.connect_timeout == 1.5
    assert configured.startup_attempts == 4
    assert configured.startup_backoff_seconds == 0.1


@pytest.mark.unit
@pytest.mark.no_network
def test_settings_default_to_local_redis(monkeypatch):
    for name in (
        "REDIS_URL",
        "REDIS_MAX_CONNECTIONS",
        "REDIS_HEALTH_CHECK_INTERVAL",
        "REDIS_CONNECT_TIMEOUT",
        "REDIS_STARTUP_ATTEMPTS",
        "REDIS_STARTUP_BACKOFF_SECONDS",
    ):
        monkeypatch.delenv(name, raising=False)

    configured = RedisConnectionSettings.from_env()

    assert configured.url == "redis://localhost:6379/0"


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("REDIS_MAX_CONNECTIONS", "0"),
        ("REDIS_CONNECT_TIMEOUT", "0"),
        ("REDIS_STARTUP_ATTEMPTS", "0"),
        ("REDIS_STARTUP_BACKOFF_SECONDS", "-1"),
    ],
)
def test_invalid_environment_settings_raise_configuration_error(
    monkeypatch,
    name,
    value,
):
    monkeypatch.setenv(name, value)

    with pytest.raises(ConfigurationError, match="Invalid Redis"):
        RedisConnectionSettings.from_env()


@pytest.mark.unit
@pytest.mark.no_network
def test_url_is_passed_through_without_application_validation(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "custom-redis-value")

    configured = RedisConnectionSettings.from_env()

    assert configured.url == "custom-redis-value"


@pytest.mark.unit
@pytest.mark.no_network
def test_build_client_uses_validated_pool_settings(monkeypatch):
    from_url = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(redis_module.aioredis.Redis, "from_url", from_url)
    configured = settings(
        url="rediss://user:secret@redis.example:6380/3",
        max_connections=20,
        health_check_interval=15,
        connect_timeout=1.25,
    )
    manager = AsyncRedisClient(configured)

    client = manager._build_client()

    assert client is from_url.return_value
    from_url.assert_called_once_with(
        configured.url,
        decode_responses=True,
        max_connections=20,
        health_check_interval=15,
        socket_connect_timeout=1.25,
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_client_property_requires_connection():
    manager = AsyncRedisClient(settings())

    with pytest.raises(RuntimeError, match="not connected"):
        _ = manager.client


@pytest.mark.unit
@pytest.mark.no_network
async def test_connect_validates_once_and_reuses_stable_client(monkeypatch):
    candidate = make_client()
    manager = AsyncRedisClient(settings())
    build_client = MagicMock(return_value=candidate)
    monkeypatch.setattr(manager, "_build_client", build_client)

    first = await manager.connect()
    second = await manager.connect()

    assert first is candidate
    assert second is candidate
    assert manager.client is candidate
    build_client.assert_called_once_with()
    candidate.ping.assert_awaited_once_with()


@pytest.mark.unit
@pytest.mark.no_network
async def test_concurrent_connect_constructs_one_client(monkeypatch):
    started = asyncio.Event()
    release = asyncio.Event()

    async def delayed_ping():
        started.set()
        await release.wait()
        return True

    candidate = make_client(ping_side_effect=delayed_ping)
    manager = AsyncRedisClient(settings())
    build_client = MagicMock(return_value=candidate)
    monkeypatch.setattr(manager, "_build_client", build_client)

    first_task = asyncio.create_task(manager.connect())
    await started.wait()
    second_task = asyncio.create_task(manager.connect())
    release.set()

    first, second = await asyncio.gather(first_task, second_task)

    assert first is candidate
    assert second is candidate
    build_client.assert_called_once_with()
    candidate.ping.assert_awaited_once_with()


@pytest.mark.unit
@pytest.mark.no_network
async def test_startup_retries_same_candidate_with_exponential_backoff(monkeypatch):
    candidate = make_client(
        ping_side_effect=[
            ConnectionError("first"),
            ConnectionError("second"),
            True,
        ]
    )
    manager = AsyncRedisClient(
        settings(startup_attempts=3, startup_backoff_seconds=0.2)
    )
    sleep = AsyncMock()
    monkeypatch.setattr(manager, "_build_client", lambda: candidate)
    monkeypatch.setattr(redis_module.asyncio, "sleep", sleep)

    result = await manager.connect()

    assert result is candidate
    assert candidate.ping.await_count == 3
    assert [call.args[0] for call in sleep.await_args_list] == [0.2, 0.4]
    candidate.aclose.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.no_network
async def test_exhausted_startup_closes_candidate_without_exposing_credentials(
    monkeypatch,
):
    candidate = make_client(ping_side_effect=ConnectionError("unavailable"))
    manager = AsyncRedisClient(
        settings(
            url="redis://user:secret@redis.example:6379/2",
            startup_attempts=2,
            startup_backoff_seconds=0,
        )
    )
    monkeypatch.setattr(manager, "_build_client", lambda: candidate)
    monkeypatch.setattr(redis_module.asyncio, "sleep", AsyncMock())

    with pytest.raises(DependencyError) as exc_info:
        await manager.connect()

    with pytest.raises(RuntimeError, match="not connected"):
        _ = manager.client
    candidate.aclose.assert_awaited_once_with(close_connection_pool=True)
    assert exc_info.value.details == {
        "error_type": "ConnectionError",
        "endpoint": "redis://redis.example:6379/2",
        "attempts": 2,
    }
    assert "secret" not in str(exc_info.value)
    assert "secret" not in str(exc_info.value.details)


@pytest.mark.unit
@pytest.mark.no_network
async def test_timed_out_startup_ping_uses_failure_cleanup(monkeypatch):
    cancelled = asyncio.Event()

    async def hanging_ping():
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    candidate = make_client(ping_side_effect=hanging_ping)
    manager = AsyncRedisClient(
        settings(
            connect_timeout=0.001,
            startup_attempts=2,
            startup_backoff_seconds=0,
        )
    )
    monkeypatch.setattr(manager, "_build_client", lambda: candidate)
    monkeypatch.setattr(redis_module.asyncio, "sleep", AsyncMock())

    with pytest.raises(DependencyError):
        await manager.connect()

    assert cancelled.is_set()
    candidate.aclose.assert_awaited_once_with(close_connection_pool=True)


@pytest.mark.unit
@pytest.mark.no_network
async def test_cancelled_startup_closes_candidate_and_propagates(monkeypatch):
    started = asyncio.Event()

    async def hanging_ping():
        started.set()
        await asyncio.Event().wait()

    candidate = make_client(ping_side_effect=hanging_ping)
    manager = AsyncRedisClient(settings())
    monkeypatch.setattr(manager, "_build_client", lambda: candidate)

    startup = asyncio.create_task(manager.connect())
    await started.wait()
    startup.cancel()

    with pytest.raises(asyncio.CancelledError):
        await startup

    candidate.aclose.assert_awaited_once_with(close_connection_pool=True)


@pytest.mark.unit
@pytest.mark.no_network
async def test_close_is_idempotent_and_clears_client():
    client = make_client()
    manager = AsyncRedisClient(settings())
    manager._client = client

    await manager.close()
    await manager.close()

    with pytest.raises(RuntimeError, match="not connected"):
        _ = manager.client
    client.aclose.assert_awaited_once_with(close_connection_pool=True)


@pytest.mark.unit
@pytest.mark.no_network
async def test_close_failure_still_clears_client():
    client = make_client()
    client.aclose.side_effect = RuntimeError("close failed")
    manager = AsyncRedisClient(settings())
    manager._client = client

    await manager.close()

    with pytest.raises(RuntimeError, match="not connected"):
        _ = manager.client


@pytest.mark.unit
@pytest.mark.no_network
def test_client_exposes_connection_lifecycle_only():
    domain_methods = {
        "log_conversation_turn",
        "update_message_mapping",
        "delete_message_turn",
        "refresh_session_ttls",
        "load_formatted_memories",
    }

    assert domain_methods.isdisjoint(vars(AsyncRedisClient))
