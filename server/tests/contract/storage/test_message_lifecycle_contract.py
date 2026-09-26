import pytest

from core.knowledge.db.writers.message_lifecycle_writer import MessageLifecycleWriter
from tests.fixtures.fakes import RecordingPostgresClient


class _UnusedMessageWriter:
    pass


def _writer(client=None):
    return MessageLifecycleWriter(client or RecordingPostgresClient(), _UnusedMessageWriter())


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    "message",
    [
        {},
        {"acceptance_key": ""},
        {"acceptance_key": "request:abc"},
    ],
)
async def test_editable_message_requires_complete_idempotency_identity(message):
    with pytest.raises(ValueError):
        await _writer().create_editable_user_message(message, edit_window_seconds=30)


@pytest.mark.storage
@pytest.mark.no_network
async def test_assistant_finalization_rejects_invalid_or_conflicting_outcomes():
    writer = _writer()
    with pytest.raises(ValueError, match="assistant outcome"):
        async with writer.client.transaction() as cur:
            await writer.prepare_assistant_exchange_finalization(
                user_name="ada",
                project_id="project-1",
                session_id="session-1",
                user_message_id=7,
                outcome="failed",
                cur=cur,
            )

    client = RecordingPostgresClient(
        fetch_one_results=[
            {
                "message_id": 7,
                "exchange_state": "closed",
                "exchange_outcome": "clarification",
                "exchange_closed_at_ms": 100,
            }
        ]
    )
    with pytest.raises(ValueError, match="already closed as clarification"):
        async with client.transaction() as cur:
            await _writer(client).prepare_assistant_exchange_finalization(
                user_name="ada",
                project_id="project-1",
                session_id="session-1",
                user_message_id=7,
                outcome="assistant_final",
                cur=cur,
            )


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("outcome", "terminal_error"),
    [
        ("assistant_final", {"code": "run_failed", "retryable": True}),
        ("failed", {"code": "unknown", "retryable": True}),
        ("failed", {"code": "run_cancelled", "retryable": False}),
        ("cancelled", {"code": "run_failed", "retryable": True}),
        ("failed", {"code": "run_failed", "retryable": False}),
    ],
)
async def test_exchange_closure_rejects_inconsistent_terminal_errors(
    outcome, terminal_error
):
    with pytest.raises(ValueError, match="terminal error"):
        await _writer().close_user_exchange(
            user_name="ada",
            project_id="project-1",
            session_id="session-1",
            user_message_id=7,
            outcome=outcome,
            terminal_error=terminal_error,
        )


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize("closed_at_ms", [True, -1, "100"])
async def test_exchange_closure_requires_a_non_negative_integer_time(closed_at_ms):
    with pytest.raises(ValueError, match="closed_at_ms"):
        await _writer().close_user_exchange(
            user_name="ada",
            project_id="project-1",
            session_id="session-1",
            user_message_id=7,
            outcome="failed",
            closed_at_ms=closed_at_ms,
        )


@pytest.mark.storage
@pytest.mark.no_network
async def test_closed_assistant_exchange_requires_its_assistant_row():
    client = RecordingPostgresClient(
        fetch_one_results=[
            {
                "message_id": 7,
                "exchange_state": "closed",
                "exchange_outcome": "assistant_final",
                "exchange_closed_at_ms": 100,
            },
            None,
        ]
    )

    with pytest.raises(RuntimeError, match="missing its assistant row"):
        async with client.transaction() as cur:
            await _writer(client).close_user_exchange(
                user_name="ada",
                project_id="project-1",
                session_id="session-1",
                user_message_id=7,
                outcome="assistant_final",
                closed_at_ms=100,
                cur=cur,
            )


@pytest.mark.storage
@pytest.mark.no_network
async def test_orphan_repair_rechecks_open_state_and_inspection_timestamp():
    client = RecordingPostgresClient(
        fetch_one_results=[{"timestamp_ms": 51, "exchange_state": "open"}]
    )

    with pytest.raises(ValueError, match="changed since it was inspected"):
        await _writer(client).close_orphaned_user_exchange(
            user_name="ada",
            project_id="project-1",
            session_id="session-1",
            user_message_id=7,
            expected_opened_at_ms=50,
            stale_before_ms=100,
        )


@pytest.mark.storage
@pytest.mark.no_network
async def test_orphan_repair_closes_through_canonical_failed_exchange_path():
    client = RecordingPostgresClient(
        fetch_one_results=[
            {"timestamp_ms": 50, "exchange_state": "open"},
            {
                "message_id": 7,
                "exchange_state": "open",
                "exchange_outcome": None,
                "exchange_closed_at_ms": None,
            },
            {"message_id": 7},
        ]
    )

    result = await _writer(client).close_orphaned_user_exchange(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        user_message_id=7,
        expected_opened_at_ms=50,
        stale_before_ms=100,
    )

    assert result.outcome == "failed"
    update_call = next(call for call in client.calls if "UPDATE public.messages" in call[1])
    assert '"code": "run_failed"' in update_call[2][3]
