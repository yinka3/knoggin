import pytest

from common.conf.domain_config import DomainConfig
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
        registered_job_names = ("episode",)

        async def stop(self):
            calls.append("scheduler")

    class RecordingBackgroundWork:
        async def cancel_owner(self, owner):
            calls.append(f"background:{owner}")

    class RecordingIndexer:
        async def shutdown(self):
            calls.append("document-indexer")

    state = make_project_state(
        scheduler=RecordingScheduler(),
        background_work=RecordingBackgroundWork(),
    )
    state.document_service._indexer = RecordingIndexer()
    state.add_config_unsubscriber(lambda: calls.append("unsubscribe"))

    await state.shutdown()

    assert calls == [
        "scheduler",
        "document-indexer",
        "background:project:project-1:document-index",
        "background:project:project-1:episode",
        "unsubscribe",
    ]


@pytest.mark.unit
@pytest.mark.no_network
async def test_project_runtime_shutdown_cancels_each_registered_project_job_only():
    calls = []

    class RecordingScheduler:
        registered_job_names = ("episode", "project_semantic")

        async def stop(self):
            calls.append("scheduler")

    class RecordingBackgroundWork:
        async def cancel_owner(self, owner):
            calls.append(owner)

    class RecordingIndexer:
        async def shutdown(self):
            calls.append("document-indexer")

    state = make_project_state(
        scheduler=RecordingScheduler(),
        background_work=RecordingBackgroundWork(),
    )
    state.document_service._indexer = RecordingIndexer()

    await state.shutdown()

    assert calls == [
        "scheduler",
        "document-indexer",
        "project:project-1:document-index",
        "project:project-1:episode",
        "project:project-1:project_semantic",
    ]


@pytest.mark.unit
@pytest.mark.no_network
async def test_project_runtime_shutdown_finishes_cleanup_after_a_phase_failure():
    calls = []

    class FailingScheduler:
        registered_job_names = ("episode",)

        async def stop(self):
            calls.append("scheduler")
            raise RuntimeError("scheduler failed")

    class RecordingBackgroundWork:
        async def cancel_owner(self, owner):
            calls.append(f"background:{owner}")

    class RecordingIndexer:
        async def shutdown(self):
            calls.append("document-indexer")

    state = make_project_state(
        scheduler=FailingScheduler(),
        background_work=RecordingBackgroundWork(),
    )
    state.document_service._indexer = RecordingIndexer()
    state.add_config_unsubscriber(lambda: calls.append("unsubscribe"))

    with pytest.raises(RuntimeError, match="ProjectRuntime shutdown failed"):
        await state.shutdown()

    assert calls == [
        "scheduler",
        "document-indexer",
        "background:project:project-1:document-index",
        "background:project:project-1:episode",
        "unsubscribe",
    ]


@pytest.mark.unit
@pytest.mark.no_network
def test_project_runtime_exposes_one_semantic_wake_edge_only_when_registered():
    class RecordingScheduler:
        def __init__(self):
            self.wakes = 0

        def wake(self):
            self.wakes += 1
            return True

    scheduler = RecordingScheduler()
    state = make_project_state(scheduler=scheduler)

    assert state.signal_semantic_work() is False
    state.project_semantic_job = object()
    assert state.signal_semantic_work() is True
    assert scheduler.wakes == 1


@pytest.mark.unit
@pytest.mark.no_network
async def test_project_runtime_selects_the_vp01_adapter_for_a_new_domain_language():
    selected_languages = []
    installed = []

    class RecordingProcessor:
        def set_vp01(self, adapter):
            installed.append(adapter)

    async def get_vp01(language):
        selected_languages.append(language)
        return f"adapter:{language}"

    runtime = make_project_state(text_processor=RecordingProcessor())
    runtime._get_vp01 = get_vp01
    multilingual = DomainConfig.from_mapping(
        {
            "version": 2,
            "topics": {"Work": {"active": True}},
            "entity_types": {
                "Company": {"topic": "Work", "labels": ["company"]}
            },
            "vp01_language": "multilingual",
        }
    ).compile()

    await runtime._select_vp01(multilingual)

    assert selected_languages == ["multilingual"]
    assert installed == ["adapter:multilingual"]
