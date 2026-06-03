import pytest

from tests.fixtures.factories import make_project_state
from tests.fixtures.fakes import FakeScheduler


@pytest.mark.unit
@pytest.mark.no_network
async def test_project_state_shutdown_unsubscribes_and_stops_scheduler():
    scheduler = FakeScheduler()
    state = make_project_state(scheduler=scheduler)
    calls = []

    state.add_config_unsubscriber(lambda: calls.append("first"))
    state.add_config_unsubscriber(lambda: calls.append("second"))

    await state.shutdown()

    assert calls == ["first", "second"]
    assert state.config_unsubscribers == []
    assert scheduler.stopped == 1
