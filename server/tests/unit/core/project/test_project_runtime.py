import asyncio
from types import SimpleNamespace

import pytest

from runtime.project_factory import ProjectFactory
from tests.fixtures.factories import make_domain_config, make_project_state
from tests.fixtures.fakes import (
    FakeEmbeddingService,
    FakePipeline,
    FakePostgresClient,
    FakeRedis,
    FakeScheduler,
)


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

    assert calls == ["first", "second"]
    assert state.config_unsubscribers == []
    assert scheduler.stopped == 1


@pytest.mark.unit
@pytest.mark.no_network
async def test_project_runtime_shutdown_cancels_tracked_community_task():
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
def test_project_factory_owns_document_service_environment(monkeypatch):
    captured = {}

    class RecordingDocumentService:
        def __init__(self, **kwargs):
            captured.update(kwargs)
            self.project_id = kwargs["project_id"]
            self._reader = object()
            self._writer = object()

    monkeypatch.setenv("KNOGGIN_DOCUMENT_RERANK_ENABLED", "false")
    monkeypatch.setenv("KNOGGIN_DOCUMENT_RERANK_CANDIDATES", "7")
    monkeypatch.setattr(
        "runtime.project_factory.DocumentService",
        RecordingDocumentService,
    )
    monkeypatch.setattr(
        "runtime.project_factory.ResourceProfile.from_environment",
        classmethod(
            lambda cls: SimpleNamespace(workspace_prepare_concurrency=9)
        ),
    )

    runtime = ProjectFactory.create_runtime(
        project_id="project-1",
        domain_config=make_domain_config(),
        entities=object(),
        pipeline=FakePipeline(),
        scheduler=FakeScheduler(),
        user_name="ada",
        redis_client=FakeRedis(),
        readable_project_ids=["project-1"],
        postgres_client=FakePostgresClient(),
        embedding_service=FakeEmbeddingService(),
    )

    assert runtime.document_service.__class__ is RecordingDocumentService
    assert captured["document_rerank_enabled"] is False
    assert captured["document_rerank_candidates"] == 7
    assert captured["workspace_prepare_concurrency"] == 9
