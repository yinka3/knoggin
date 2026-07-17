from datetime import datetime, timedelta, timezone

import pytest

from common.schema.agent_contracts import AgentConfig
from common.schema.contracts import DLQEntry, EngineScope, EngineWorkUnit
from common.schema.primitives import Message
from common.utils.time_utils import (
    SystemClock,
    TestClock,
    frozen_time,
    get_now,
    get_now_iso,
    get_now_ms,
    get_now_unix,
    parse_iso_time_or_now,
    reset_clock,
    set_test_clock,
)


@pytest.fixture(autouse=True)
def restore_system_clock():
    reset_clock()
    yield
    reset_clock()


@pytest.mark.unit
@pytest.mark.no_network
def test_system_clock_returns_utc_aware_datetime():
    now = SystemClock().now()

    assert now.tzinfo is not None
    assert now.utcoffset() == timedelta(0)


@pytest.mark.unit
@pytest.mark.no_network
def test_frozen_clock_formats_use_canonical_utc_values():
    set_test_clock("2024-01-02T03:04:05.123000+00:00")

    assert get_now() == datetime(2024, 1, 2, 3, 4, 5, 123000, tzinfo=timezone.utc)
    assert get_now_iso() == "2024-01-02T03:04:05.123000+00:00"
    assert get_now_ms() == 1704164645123
    assert get_now_unix() == 1704164645.123


@pytest.mark.unit
@pytest.mark.no_network
def test_test_clock_advance_updates_all_delegate_formats():
    clock = TestClock("2024-01-02T03:04:05+00:00")
    set_test_clock(clock)

    clock.advance(minutes=2, seconds=3)

    assert get_now_iso() == "2024-01-02T03:06:08+00:00"
    assert get_now_ms() == 1704164768000
    assert get_now_unix() == 1704164768.0


@pytest.mark.unit
@pytest.mark.no_network
def test_frozen_time_restores_previous_clock():
    set_test_clock("2024-01-02T00:00:00+00:00")

    with frozen_time("2025-03-04T05:06:07+00:00"):
        assert get_now_iso() == "2025-03-04T05:06:07+00:00"

    assert get_now_iso() == "2024-01-02T00:00:00+00:00"


@pytest.mark.unit
@pytest.mark.no_network
def test_parse_iso_time_or_now_uses_active_clock_fallback():
    set_test_clock("2024-01-02T03:04:05+00:00")

    assert parse_iso_time_or_now("not a timestamp") == get_now()


@pytest.mark.unit
@pytest.mark.no_network
def test_schema_default_factories_use_active_clock():
    set_test_clock("2024-01-02T03:04:05+00:00")

    message = Message(content="hello")
    agent = AgentConfig(id="agent-1", name="Ada", persona="curious")
    work = EngineWorkUnit.for_graph_write(
        EngineScope(user_name="ada", session_id="session-1")
    )
    dlq = DLQEntry(messages=[{"id": 1}], session_text="hello", error="boom")

    assert message.timestamp == get_now()
    assert agent.created_at == get_now()
    assert work.trace.created_at == get_now()
    assert dlq.timestamp == get_now_unix()
