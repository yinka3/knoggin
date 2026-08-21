"""Real PostgreSQL/Redis acceptance and idempotency contracts."""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from common.schema.primitives import Message
from core.knowledge.store import KnowledgeStore
from runtime.session_runtime import SessionRuntime as Session
from infrastructure.redis_client import RedisKeys
from tests.integration.ingestion.test_server_flow import (
    _DeterministicEmbeddingService,
    _session,
    _SignalCounter,
)


def _configure_context(monkeypatch) -> None:
    monkeypatch.setattr(
        Session,
        "current_config",
        property(
            lambda self: type(
                "Config",
                (),
                {
                    "developer_settings": type(
                        "DeveloperSettings",
                        (),
                        {
                            "limits": type(
                                "Limits", (), {"conversation_context_turns": 100}
                            )(),
                            "ingestion": type(
                                "Ingestion", (), {"message_edit_window_seconds": 1}
                            )(),
                        },
                    )()
                },
            )()
        ),
    )


def _context(scope, *, postgres, redis):
    store = KnowledgeStore(postgres, _DeterministicEmbeddingService())
    resources = SimpleNamespace(
        postgres=postgres,
        redis=redis,
        knowledge_store=store,
        embedding=_DeterministicEmbeddingService(),
    )
    context = _session(
        resources,
        user_name=scope["user_name"],
        project_id=scope["project_id"],
        session_id=scope["session_id"],
    )
    context.consumer = _SignalCounter()
    return context


async def _message_rows(scope):
    return await scope["postgres"].fetch_all(
        "SELECT message_id, content FROM messages "
        "WHERE session_id = %s AND role = 'user' ORDER BY message_id",
        (scope["session_id"],),
    )


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_concurrent_identical_submissions_are_accepted_once(
    real_server_scope, monkeypatch
):
    """Independent runtime instances converge on one durable message signal."""

    _configure_context(monkeypatch)
    scope = real_server_scope
    contexts = [
        _context(scope, postgres=scope["postgres"], redis=scope["redis"])
        for _ in range(8)
    ]
    timestamp = datetime(2026, 8, 1, 16, 0, tzinfo=timezone.utc)

    results = await asyncio.gather(
        *(
            context.add(
                Message(content="same concurrent submission", timestamp=timestamp)
            )
            for context in contexts
        )
    )

    assert {result.id for result in results} == {results[0].id}
    assert await _message_rows(scope) == [
        {"message_id": results[0].id, "content": "same concurrent submission"}
    ]
    assert await scope["redis"].get(
        RedisKeys.heartbeat_counter(scope["user_name"], scope["session_id"])
    ) == "1"


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_enqueue_failure_keeps_durable_pending_claim_for_retry(
    real_server_scope, monkeypatch
):
    """A durable PostgreSQL write survives a Redis signal outage."""

    _configure_context(monkeypatch)
    scope = real_server_scope
    context = _context(scope, postgres=scope["postgres"], redis=scope["redis"])
    timestamp = datetime(2026, 8, 1, 16, 1, tzinfo=timezone.utc)
    original_incr = scope["redis"].incr
    calls = 0

    async def fail_once(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("simulated signal outage")
        return await original_incr(*args, **kwargs)

    monkeypatch.setattr(scope["redis"], "incr", fail_once)
    message = Message(content="signal retry", timestamp=timestamp)

    with pytest.raises(ConnectionError, match="signal outage"):
        await context.add(message)

    dedup_keys = []
    async for key in scope["redis"].scan_iter(
        match=RedisKeys.message_dedup_pattern(scope["user_name"], scope["session_id"])
    ):
        dedup_keys.append(key)
    assert len(dedup_keys) == 1
    assert await scope["redis"].get(dedup_keys[0]) == f"pending:{message.id}"
    assert await _message_rows(scope) == [
        {"message_id": message.id, "content": "signal retry"}
    ]

    retried = await context.add(Message(content="signal retry", timestamp=timestamp))
    assert retried.id == message.id
    assert await scope["redis"].get(dedup_keys[0]) == f"accepted:{message.id}"
    assert await scope["redis"].get(
        RedisKeys.heartbeat_counter(scope["user_name"], scope["session_id"])
    ) == "1"


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_lost_acceptance_response_is_safe_to_retry(
    real_server_scope, monkeypatch
):
    """A caller retry after losing the response receives the same acceptance."""

    _configure_context(monkeypatch)
    scope = real_server_scope
    first_runtime = _context(scope, postgres=scope["postgres"], redis=scope["redis"])
    second_runtime = _context(scope, postgres=scope["postgres"], redis=scope["redis"])
    timestamp = datetime(2026, 8, 1, 16, 2, tzinfo=timezone.utc)

    first = await first_runtime.add(
        Message(content="response was lost", timestamp=timestamp)
    )
    retry = await second_runtime.add(
        Message(content="response was lost", timestamp=timestamp)
    )

    assert retry.id == first.id
    assert await _message_rows(scope) == [
        {"message_id": first.id, "content": "response was lost"}
    ]
    assert await scope["redis"].get(
        RedisKeys.heartbeat_counter(scope["user_name"], scope["session_id"])
    ) == "1"


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_restart_reuses_a_pending_acceptance_claim(
    real_server_scope, monkeypatch
):
    """A new runtime can finish a durable acceptance left pending by a crash."""

    _configure_context(monkeypatch)
    scope = real_server_scope
    first_runtime = _context(scope, postgres=scope["postgres"], redis=scope["redis"])
    timestamp = datetime(2026, 8, 1, 16, 3, tzinfo=timezone.utc)
    original_incr = scope["redis"].incr

    async def fail_signal(*_args, **_kwargs):
        raise ConnectionError("runtime stopped before signal")

    monkeypatch.setattr(scope["redis"], "incr", fail_signal)
    with pytest.raises(ConnectionError, match="runtime stopped"):
        await first_runtime.add(
            Message(content="restart pending claim", timestamp=timestamp)
        )
    monkeypatch.setattr(scope["redis"], "incr", original_incr)

    restarted = _context(scope, postgres=scope["postgres"], redis=scope["redis"])
    retried = await restarted.add(
        Message(content="restart pending claim", timestamp=timestamp)
    )

    assert await _message_rows(scope) == [
        {"message_id": retried.id, "content": "restart pending claim"}
    ]
    assert await scope["redis"].get(
        RedisKeys.heartbeat_counter(scope["user_name"], scope["session_id"])
    ) == "1"


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_acceptance_claim_expiry_is_reclaimable(
    real_server_scope, monkeypatch
):
    """An expired pending claim can be reclaimed by a later runtime."""

    _configure_context(monkeypatch)
    scope = real_server_scope
    context = _context(scope, postgres=scope["postgres"], redis=scope["redis"])
    timestamp = datetime(2026, 8, 1, 16, 4, tzinfo=timezone.utc)
    original_incr = scope["redis"].incr

    async def fail_signal(*_args, **_kwargs):
        raise ConnectionError("claim owner stopped")

    monkeypatch.setattr(scope["redis"], "incr", fail_signal)
    with pytest.raises(ConnectionError, match="claim owner stopped"):
        await context.add(Message(content="expired claim", timestamp=timestamp))
    monkeypatch.setattr(scope["redis"], "incr", original_incr)

    dedup_keys = [
        key
        async for key in scope["redis"].scan_iter(
            match=RedisKeys.message_dedup_pattern(
                scope["user_name"], scope["session_id"]
            )
        )
    ]
    assert len(dedup_keys) == 1
    digest = dedup_keys[0]
    await scope["redis"].expire(digest, 1)
    await asyncio.sleep(1.2)
    assert await scope["redis"].get(digest) is None

    # The five-minute acceptance claim is an intentionally bounded Redis
    # idempotency window. Once it expires, the deterministic key is reclaimable
    # and the caller receives a new durable acceptance identity.
    retried = await context.add(Message(content="expired claim", timestamp=timestamp))
    assert retried.id > 0
    assert len(await _message_rows(scope)) == 2


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_same_timestamp_scopes_and_content_remain_distinct(
    real_server_scope, monkeypatch
):
    """Scope and content are part of the deterministic acceptance identity."""

    _configure_context(monkeypatch)
    scope = real_server_scope
    first = _context(scope, postgres=scope["postgres"], redis=scope["redis"])

    other_session = f"session-{scope['project_id']}"
    await scope["postgres"].execute(
        "INSERT INTO sessions (session_id, user_name, project_id) VALUES (%s, %s, %s)",
        (other_session, scope["user_name"], scope["project_id"]),
    )
    other_scope = {**scope, "session_id": other_session}
    second = _context(other_scope, postgres=scope["postgres"], redis=scope["redis"])
    timestamp = datetime(2026, 8, 1, 16, 5, tzinfo=timezone.utc)

    first_message, second_message, changed_message = await asyncio.gather(
        first.add(Message(content="same timestamp", timestamp=timestamp)),
        second.add(Message(content="same timestamp", timestamp=timestamp)),
        first.add(Message(content="different content", timestamp=timestamp)),
    )

    assert len({first_message.id, second_message.id, changed_message.id}) == 3
    assert first_message.id != changed_message.id
    assert second_message.id != first_message.id
    assert await scope["postgres"].fetch_one(
        "SELECT count(*) AS count FROM messages WHERE project_id = %s AND role = 'user'",
        (scope["project_id"],),
    ) == {"count": 3}
