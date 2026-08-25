"""Real durable acceptance and idempotency contracts."""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from common.schema.primitives import Message
from core.knowledge.store import KnowledgeStore
from runtime.session_runtime import SessionRuntime as Session
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


def _context(scope, *, postgres):
    store = KnowledgeStore(postgres, _DeterministicEmbeddingService())
    resources = SimpleNamespace(
        postgres=postgres,
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
@pytest.mark.no_network
async def test_real_concurrent_identical_submissions_are_accepted_once(
    real_server_scope, monkeypatch
):
    """Independent runtime instances converge on one durable message signal."""

    _configure_context(monkeypatch)
    scope = real_server_scope
    contexts = [
        _context(scope, postgres=scope["postgres"])
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


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_real_local_wake_failure_keeps_durable_acceptance_for_retry(
    real_server_scope, monkeypatch
):
    """A durable PostgreSQL write survives a local worker wake failure."""

    _configure_context(monkeypatch)
    scope = real_server_scope
    context = _context(scope, postgres=scope["postgres"])
    timestamp = datetime(2026, 8, 1, 16, 1, tzinfo=timezone.utc)
    original_signal = context.consumer.signal
    calls = 0

    def fail_once():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("simulated local wake outage")
        original_signal()

    monkeypatch.setattr(context.consumer, "signal", fail_once)
    message = Message(content="signal retry", timestamp=timestamp)

    with pytest.raises(RuntimeError, match="local wake outage"):
        await context.add(message)

    assert await _message_rows(scope) == [
        {"message_id": message.id, "content": "signal retry"}
    ]

    retried = await context.add(Message(content="signal retry", timestamp=timestamp))
    assert retried.id == message.id
    assert context.consumer.calls == 1


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_real_lost_acceptance_response_is_safe_to_retry(
    real_server_scope, monkeypatch
):
    """A caller retry after losing the response receives the same acceptance."""

    _configure_context(monkeypatch)
    scope = real_server_scope
    first_runtime = _context(scope, postgres=scope["postgres"])
    second_runtime = _context(scope, postgres=scope["postgres"])
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


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_real_restart_reuses_durable_acceptance(
    real_server_scope, monkeypatch
):
    """A new runtime receives the original durable acceptance ID."""

    _configure_context(monkeypatch)
    scope = real_server_scope
    first_runtime = _context(scope, postgres=scope["postgres"])
    timestamp = datetime(2026, 8, 1, 16, 3, tzinfo=timezone.utc)
    first = await first_runtime.add(
        Message(content="restart durable acceptance", timestamp=timestamp)
    )

    restarted = _context(scope, postgres=scope["postgres"])
    retried = await restarted.add(
        Message(content="restart durable acceptance", timestamp=timestamp)
    )

    assert retried.id == first.id
    assert await _message_rows(scope) == [
        {"message_id": retried.id, "content": "restart durable acceptance"}
    ]
    assert restarted.consumer.calls == 1


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_real_durable_acceptance_does_not_expire(
    real_server_scope, monkeypatch
):
    """A retry remains bound to its durable acceptance indefinitely."""

    _configure_context(monkeypatch)
    scope = real_server_scope
    context = _context(scope, postgres=scope["postgres"])
    timestamp = datetime(2026, 8, 1, 16, 4, tzinfo=timezone.utc)
    first = await context.add(Message(content="durable acceptance", timestamp=timestamp))
    retried = await context.add(
        Message(content="durable acceptance", timestamp=timestamp)
    )

    assert retried.id == first.id
    assert await _message_rows(scope) == [
        {"message_id": first.id, "content": "durable acceptance"}
    ]


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_real_same_timestamp_scopes_and_content_remain_distinct(
    real_server_scope, monkeypatch
):
    """Scope and content are part of the deterministic acceptance identity."""

    _configure_context(monkeypatch)
    scope = real_server_scope
    first = _context(scope, postgres=scope["postgres"])

    other_session = f"session-{scope['project_id']}"
    await scope["postgres"].execute(
        "INSERT INTO sessions (session_id, user_name, project_id) VALUES (%s, %s, %s)",
        (other_session, scope["user_name"], scope["project_id"]),
    )
    other_scope = {**scope, "session_id": other_session}
    second = _context(other_scope, postgres=scope["postgres"])
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
