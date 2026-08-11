import asyncio

import pytest

from tests.fixtures.factories import make_project_state
from tests.fixtures.fakes import FakeScheduler


@pytest.mark.unit
@pytest.mark.no_network
def test_project_state_owns_distinct_document_services():
    first = make_project_state(project_id="project-1")
    second = make_project_state(project_id="project-2")

    assert first.document_service.project_id == "project-1"
    assert second.document_service.project_id == "project-2"
    assert first.document_service is not second.document_service


@pytest.mark.unit
@pytest.mark.no_network
async def test_project_state_shutdown_unsubscribes_and_stops_scheduler():
    scheduler = FakeScheduler()
    state = make_project_state(scheduler=scheduler)
    calls = []

    state.add_config_unsubscriber(lambda: calls.append("first"))
    state.add_config_unsubscriber(lambda: calls.append("second"))

    await state.shutdown()
    await state.shutdown()

    assert calls == ["first", "second"]
    assert state.config_unsubscribers == []
    assert scheduler.stopped == 1


@pytest.mark.unit
@pytest.mark.no_network
async def test_project_state_shutdown_cancels_tracked_community_task():
    state = make_project_state()
    cancelled = asyncio.Event()

    async def discussion():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    task = asyncio.create_task(discussion())
    state.track_community_task(task)
    await asyncio.sleep(0)

    await state.shutdown()

    assert cancelled.is_set()
    assert task.cancelled()


@pytest.mark.unit
@pytest.mark.no_network
async def test_project_state_rejects_missing_domain_configuration():
    state = make_project_state()

    class EmptyDomainStore:
        async def load(self, user_name, project_id):
            assert (user_name, project_id) == ("ada", "project-1")
            return None

    state.domain_config_store = EmptyDomainStore()

    with pytest.raises(
        RuntimeError,
        match="domain configuration is required",
    ):
        await state.load_domain_config()
