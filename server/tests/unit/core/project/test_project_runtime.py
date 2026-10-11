import asyncio

import pytest

from common.conf.domain_config import DomainConfig
from core.project.domain_config_store import DomainActivation
from tests.fixtures.factories import make_domain_config, make_project_state
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
    assert state._closed is False


@pytest.mark.unit
@pytest.mark.no_network
async def test_project_runtime_shutdown_retries_only_failed_cleanup():
    calls = []
    scheduler_attempts = 0
    unsubscribe_attempts = 0

    class RetryScheduler:
        registered_job_names = ()

        async def stop(self):
            nonlocal scheduler_attempts
            scheduler_attempts += 1
            calls.append("scheduler")
            if scheduler_attempts == 1:
                raise RuntimeError("retry scheduler")

    class RecordingIndexer:
        async def shutdown(self):
            calls.append("document-indexer")

    def retry_unsubscribe():
        nonlocal unsubscribe_attempts
        unsubscribe_attempts += 1
        calls.append("unsubscribe")
        if unsubscribe_attempts == 1:
            raise RuntimeError("retry unsubscribe")

    state = make_project_state(scheduler=RetryScheduler())
    state.document_service._indexer = RecordingIndexer()
    state.add_config_unsubscriber(retry_unsubscribe)

    with pytest.raises(RuntimeError, match="ProjectRuntime shutdown failed"):
        await state.shutdown()
    await state.shutdown()

    assert calls == [
        "scheduler",
        "document-indexer",
        "unsubscribe",
        "scheduler",
        "unsubscribe",
    ]
    assert state._closed is True


@pytest.mark.unit
@pytest.mark.no_network
async def test_project_runtime_serializes_concurrent_shutdown_calls():
    scheduler_started = asyncio.Event()
    finish_scheduler = asyncio.Event()
    calls = []

    class BlockingScheduler:
        registered_job_names = ()

        async def stop(self):
            calls.append("scheduler")
            scheduler_started.set()
            await finish_scheduler.wait()

    class RecordingIndexer:
        async def shutdown(self):
            calls.append("document-indexer")

    state = make_project_state(scheduler=BlockingScheduler())
    state.document_service._indexer = RecordingIndexer()
    state.add_config_unsubscriber(lambda: calls.append("unsubscribe"))

    first = asyncio.create_task(state.shutdown())
    await scheduler_started.wait()
    second = asyncio.create_task(state.shutdown())
    await asyncio.sleep(0)
    finish_scheduler.set()
    await asyncio.gather(first, second)

    assert calls == ["scheduler", "document-indexer", "unsubscribe"]


@pytest.mark.unit
@pytest.mark.no_network
def test_project_runtime_exposes_one_semantic_wake_edge_only_when_registered():
    class RecordingScheduler:
        def __init__(self):
            self.wakes = 0
            self.requested_jobs = []

        def wake_job(self, job_name):
            self.wakes += 1
            self.requested_jobs.append(job_name)
            return True

    class SemanticProcessor:
        name = "project_semantic"

    scheduler = RecordingScheduler()
    state = make_project_state(scheduler=scheduler)

    assert state.signal_semantic_work() is False
    state.project_semantic_processor = SemanticProcessor()
    assert state.signal_semantic_work() is True
    assert scheduler.wakes == 1
    assert scheduler.requested_jobs == ["project_semantic"]


@pytest.mark.unit
@pytest.mark.no_network
async def test_project_runtime_preloads_vp01_before_domain_commit():
    calls = []

    class RecordingProcessor:
        def set_vp01(self, adapter):
            calls.append(("installed", adapter))

    class Adapter:
        def extract_entities(self, *_args, **_kwargs):
            return []

    adapter = Adapter()

    async def get_vp01(language):
        calls.append(("loaded", language))
        return adapter

    class Store:
        async def activate(self, *, candidate, expected_version, **_kwargs):
            calls.append(("committed", candidate.vp01_language))
            assert expected_version == 1
            config = candidate.with_version(2)
            return DomainActivation(
                config=config, compiled=config.compile(), previous_version=1
            )

    runtime = make_project_state(text_processor=RecordingProcessor())
    runtime._get_vp01 = get_vp01
    runtime.domain_config_store = Store()
    candidate = make_domain_config(version=0).to_dict()
    candidate["vp01_language"] = "multilingual"

    activation = await runtime.activate_domain_config(
        DomainConfig.from_mapping(candidate), expected_version=1
    )

    assert calls == [
        ("loaded", "multilingual"),
        ("committed", "multilingual"),
        ("installed", adapter),
    ]
    assert activation.config.version == runtime.domain_config.version == 2


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize("invalid_adapter", [False, True])
async def test_project_runtime_does_not_commit_when_vp01_is_unavailable_or_invalid(
    invalid_adapter,
):
    class RecordingProcessor:
        def __init__(self):
            self.installed = []

        def set_vp01(self, adapter):
            self.installed.append(adapter)

    class Store:
        def __init__(self):
            self.commits = 0

        async def activate(self, **_kwargs):
            self.commits += 1
            raise AssertionError("model preparation must finish before commit")

    processor = RecordingProcessor()
    runtime = make_project_state(text_processor=processor)
    store = Store()
    runtime.domain_config_store = store

    async def get_vp01(_language):
        if invalid_adapter:
            return object()
        raise RuntimeError("model unavailable")

    runtime._get_vp01 = get_vp01
    candidate = make_domain_config(version=0).to_dict()
    candidate["vp01_language"] = "multilingual"

    with pytest.raises(
        TypeError if invalid_adapter else RuntimeError,
        match="VP-01 adapter" if invalid_adapter else "model unavailable",
    ):
        await runtime.activate_domain_config(
            DomainConfig.from_mapping(candidate), expected_version=1
        )

    assert store.commits == 0
    assert runtime.domain_config.version == 1
    assert runtime.compiled_domain.vp01_language == "en"
    assert processor.installed == []
