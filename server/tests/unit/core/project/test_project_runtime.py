import pytest

from tests.fixtures.factories import make_project_state
from tests.fixtures.fakes import FakeScheduler


@pytest.mark.unit
@pytest.mark.no_network
def test_project_runtime_owns_distinct_document_services():
    first = make_project_state(project_id="project-1")
    second = make_project_state(project_id="project-2")

    assert first.document_service.project_id == "project-1"
    assert second.document_service.project_id == "project-2"
    assert first.document_service is not second.document_service


@pytest.mark.unit
@pytest.mark.no_network
async def test_project_runtime_shutdown_unsubscribes_and_stops_scheduler():
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
async def test_project_runtime_shutdown_cancels_project_work_after_scheduler_stop():
    calls = []

    class RecordingScheduler:
        async def stop(self):
            calls.append("scheduler")

    class RecordingBackgroundWork:
        async def cancel_project(self, project_id):
            calls.append(f"background:{project_id}")

    class RecordingIndexer:
        async def shutdown(self):
            calls.append("document-indexer")

    state = make_project_state(
        scheduler=RecordingScheduler(),
        background_work=RecordingBackgroundWork(),
    )
    state.document_indexer = RecordingIndexer()
    state.add_config_unsubscriber(lambda: calls.append("unsubscribe"))

    await state.shutdown()

    assert calls == [
        "scheduler",
        "document-indexer",
        "background:project-1",
        "unsubscribe",
    ]


@pytest.mark.unit
@pytest.mark.no_network
async def test_project_runtime_shutdown_finishes_cleanup_after_a_phase_failure():
    calls = []

    class FailingScheduler:
        async def stop(self):
            calls.append("scheduler")
            raise RuntimeError("scheduler failed")

    class RecordingBackgroundWork:
        async def cancel_project(self, project_id):
            calls.append(f"background:{project_id}")

    class RecordingIndexer:
        async def shutdown(self):
            calls.append("document-indexer")

    state = make_project_state(
        scheduler=FailingScheduler(),
        background_work=RecordingBackgroundWork(),
    )
    state.document_indexer = RecordingIndexer()
    state.add_config_unsubscriber(lambda: calls.append("unsubscribe"))

    with pytest.raises(RuntimeError, match="ProjectRuntime shutdown failed"):
        await state.shutdown()

    assert calls == [
        "scheduler",
        "document-indexer",
        "background:project-1",
        "unsubscribe",
    ]
