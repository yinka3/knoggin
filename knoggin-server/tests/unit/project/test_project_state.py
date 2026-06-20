import pytest

from tests.fixtures.factories import make_project_state
from tests.fixtures.fakes import FakeScheduler


@pytest.mark.unit
@pytest.mark.no_network
def test_project_state_owns_distinct_file_rag_service():
    first = make_project_state(project_id="project-1")
    second = make_project_state(project_id="project-2")

    assert first.file_rag.project_id == "project-1"
    assert second.file_rag.project_id == "project-2"
    assert first.file_rag is not second.file_rag
    assert first.file_rag._postgres is first.postgres_client
    assert first.file_rag._storage_root == first.file_storage_root.resolve()


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
