import json
from types import SimpleNamespace

import pytest

from core.health.service import RuntimeHealthService


class SessionRuntimeReader:
    def __init__(self, active_sessions):
        self._active_sessions = active_sessions

    def get_runtime_session(self, session_id):
        return self._active_sessions.get(session_id)

    def active_runtime_count(self):
        return len(self._active_sessions)


class FakePostgres:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.fetch_calls = 0

    def pool_snapshot(self) -> dict[str, bool | int]:
        return {
            "connected": not self.fail,
            "pool_min": 1,
            "pool_max": 4,
            "pool_size": 2,
            "pool_available": 1,
            "requests_waiting": 0,
            "stats_available": not self.fail,
        }

    async def fetch_one(self, query: str):
        self.fetch_calls += 1
        assert query == "SELECT 1 AS ok"
        if self.fail:
            raise RuntimeError("postgresql://secret-host/knoggin")
        return {"ok": 1}


class FakeCoordinator:
    def __init__(self, snapshot: dict):
        self.snapshot = snapshot
        self.calls = []

    def snapshot_for_health(self, **kwargs):
        self.calls.append(kwargs)
        return dict(self.snapshot)


def resources(*, postgres=None):
    return SimpleNamespace(
        postgres=postgres or FakePostgres(),
        model_work=FakeCoordinator(
            {
                "foreground_concurrency": 1,
                "background_concurrency": 2,
                "queued_by_priority": {"foreground": 0, "background": 1},
                "in_flight_by_priority": {"foreground": 1, "background": 0},
                "work_by_name": {"embedding": {"queued": 1}},
            }
        ),
        background_work=FakeCoordinator(
            {
                "max_concurrency": 2,
                "max_queued_global": 8,
                "queued": 2,
                "queued_for_project": 1,
            }
        ),
        knowledge_store=object(),
        executor=object(),
        embedding=object(),
        llm_service=object(),
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_ingestion_health_prefers_durable_postgres_queue_state():
    class Store:
        async def get_ingestion_queue_health(self, **_kwargs):
            return {
                "pending_count": 2,
                "claimed_count": 1,
                "blocked_count": 0,
                "oldest_pending_ms": None,
                "last_processed_ms": 1,
            }

    resource_set = resources()
    resource_set.knowledge_store = Store()
    service = RuntimeHealthService(
        resources=resource_set,
        projects=SimpleNamespace(active_projects={}),
        sessions=SessionRuntimeReader(
            {
                    "session-a": SimpleNamespace(
                        project_id="project-a", ingestion_worker=FakeWorker()
                    )
            }
        ),
    )

    payload = (
        await service.get_ingestion_health(
            user_name="ada", project_id="project-a", session_id="session-a"
        )
    ).model_dump(mode="json")

    assert payload["details"]["queue"]["pending_count"] == 2
    assert payload["details"]["queue"]["claimed_count"] == 1
    assert payload["details"]["queue"]["available"] is True
    assert "postgres" not in payload["details"]


class FakeWorker:
    def __init__(self, state="running", *, timeout=10.0, failures=0):
        self.state = state
        self.timeout = timeout
        self.failures = failures

    def health_snapshot(self):
        return {
            "state": self.state,
            "current_batch_size": 0,
            "consecutive_failures": self.failures,
            "batch_timeout_seconds": self.timeout,
        }


class FakeScheduler:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.calls = 0

    def snapshot_for_health(self):
        self.calls += 1
        return dict(self.snapshot)


class FakeDocumentService:
    def __init__(self, *, pending=0):
        self.pending = pending
        self.calls = 0

    def indexing_snapshot_for_health(self):
        return {"local_submission_tasks": 0, "recovered_count": 0}

    async def pending_index_count(self):
        self.calls += 1
        return self.pending


@pytest.mark.unit
@pytest.mark.no_network
async def test_engine_health_is_healthy_and_does_not_mutate_dependencies():
    postgres = FakePostgres()
    service = RuntimeHealthService(
        resources=resources(postgres=postgres),
        projects=SimpleNamespace(
            active_projects={"project-a": object(), "project-b": object()}
        ),
        sessions=SessionRuntimeReader({"session-a": object()}),
    )

    snapshot = await service.get_engine_health()
    payload = snapshot.model_dump(mode="json")

    json.dumps(payload)
    assert payload["status"] == "healthy"
    assert payload["activity"] == "busy"
    assert payload["details"]["loaded_project_count"] == 2
    assert payload["details"]["active_runtime_session_count"] == 1
    assert postgres.fetch_calls == 1
    assert "secret" not in json.dumps(payload)


@pytest.mark.unit
@pytest.mark.no_network
async def test_failed_dependency_probes_are_bounded_and_redacted():
    service = RuntimeHealthService(
        resources=resources(postgres=FakePostgres(fail=True)),
        projects=SimpleNamespace(active_projects={}),
        sessions=SessionRuntimeReader({}),
    )

    payload = (await service.get_engine_health()).model_dump(mode="json")

    assert payload["status"] == "failed"
    assert payload["activity"] == "idle"
    assert payload["details"]["postgres"]["reason"] == "unavailable"
    assert "postgresql://" not in json.dumps(payload)


@pytest.mark.unit
@pytest.mark.no_network
async def test_resource_health_projects_current_queue_without_other_project_ids():
    resource_set = resources()
    service = RuntimeHealthService(
        resources=resource_set,
        projects=SimpleNamespace(
            active_projects={"project-a": object(), "project-b": object()}
        ),
        sessions=SessionRuntimeReader({"session-a": object()}),
    )

    payload = (await service.get_resource_health(project_id="project-a")).model_dump(
        mode="json"
    )

    assert payload["status"] == "degraded"
    assert payload["details"]["background_work"]["queued_for_project"] == 1
    assert payload["details"]["model_work"]["foreground"]["active"] == 1
    assert resource_set.background_work.calls == [{"project_id": "project-a"}]
    serialized = json.dumps(payload)
    assert "project-a" not in serialized
    assert "project-b" not in serialized


@pytest.mark.unit
@pytest.mark.no_network
async def test_ingestion_health_reports_durable_queue_delay():
    class Store:
        async def get_ingestion_queue_health(self, **_kwargs):
            return {
                "pending_count": 1,
                "claimed_count": 0,
                "blocked_count": 0,
                "oldest_pending_ms": 1,
                "last_processed_ms": None,
            }

    resource_set = resources()
    resource_set.knowledge_store = Store()
    service = RuntimeHealthService(
        resources=resource_set,
            projects=SimpleNamespace(active_projects={}),
            sessions=SessionRuntimeReader(
                {
                    "session-a": SimpleNamespace(
                        project_id="project-a", ingestion_worker=FakeWorker()
                    )
                }
        ),
    )

    payload = (
        await service.get_ingestion_health(
            user_name="ada", project_id="project-a", session_id="session-a"
        )
    ).model_dump(mode="json")

    assert payload["status"] == "degraded"
    assert payload["activity"] == "delayed"
    assert payload["details"]["queue"]["pending_count"] == 1
    assert payload["details"]["queue"]["delay_state"] == "delayed"
    assert payload["details"]["progress"]["message_state"] == "pending"
    assert "postgres" not in payload["details"]


@pytest.mark.unit
@pytest.mark.no_network
async def test_ingestion_health_degrades_when_durable_queue_metrics_fail():
    class FailingStore:
        async def get_ingestion_queue_health(self, **_kwargs):
            raise RuntimeError("postgresql://secret")

    resource_set = resources()
    resource_set.knowledge_store = FailingStore()
    service = RuntimeHealthService(
        resources=resource_set,
        projects=SimpleNamespace(active_projects={}),
        sessions=SessionRuntimeReader(
            {
                "session-a": SimpleNamespace(
                    project_id="project-a", ingestion_worker=FakeWorker()
                )
            }
        ),
    )

    payload = (
        await service.get_ingestion_health(
            user_name="ada", project_id="project-a", session_id="session-a"
        )
    ).model_dump(mode="json")

    assert payload["status"] == "degraded"
    assert payload["details"]["queue"]["available"] is False
    assert payload["details"]["queue"]["pending_count"] == 0
    assert "postgresql://" not in json.dumps(payload)


@pytest.mark.unit
@pytest.mark.no_network
async def test_background_health_combines_scheduler_queue_and_document_indexing():
    scheduler = FakeScheduler(
        {
            "state": "running",
            "registered_jobs": ["document_index_recovery", "episode_generation"],
            "enabled_jobs": ["document_index_recovery"],
            "active_jobs": [
                {
                    "name": "document_index_recovery",
                    "state": "running",
                    "elapsed_seconds": 2.0,
                    "execution_timeout_seconds": 300.0,
                    "lease_seconds": 360,
                }
            ],
            "queued_jobs": 0,
            "running_jobs": 1,
            "stalled_jobs": 0,
            "recent_failed_jobs": 0,
            "recent_outcomes": [],
        }
    )
    document_service = FakeDocumentService(pending=3)
    project = SimpleNamespace(
        project_id="project-a",
        scheduler=scheduler,
        document_service=document_service,
    )
    resource_set = resources()
    resource_set.background_work = FakeCoordinator(
        {
            "max_concurrency": 2,
            "max_queued_global": 8,
            "queued": 1,
            "queued_for_project": 1,
            "active_for_project": True,
            "queued_operation_categories": ["document-index"],
            "active_operation_categories": ["episode-generation"],
        }
    )
    service = RuntimeHealthService(
        resources=resource_set,
        projects=SimpleNamespace(active_projects={"project-a": project}),
        sessions=SessionRuntimeReader({}),
    )

    payload = (await service.get_background_health(project_id="project-a")).model_dump(
        mode="json"
    )

    assert payload["status"] == "healthy"
    assert payload["activity"] == "busy"
    assert payload["details"]["scheduler"]["running_jobs"] == 1
    assert payload["details"]["document_indexing"]["pending_document_count"] == 3
    assert payload["details"]["background_work"]["queued_for_project"] == 1
    assert scheduler.calls == 1
    assert document_service.calls == 1
    serialized = json.dumps(payload)
    assert "project-a" not in serialized


@pytest.mark.unit
@pytest.mark.no_network
async def test_background_health_does_not_claim_a_stopped_scheduler_is_healthy():
    project = SimpleNamespace(
        project_id="project-a",
        scheduler=FakeScheduler(
            {
                "state": "stopped",
                "registered_jobs": ["episode_generation"],
                "enabled_jobs": [],
                "active_jobs": [],
                "queued_jobs": 0,
                "running_jobs": 0,
                "stalled_jobs": 0,
                "recent_failed_jobs": 0,
                "recent_outcomes": [],
            }
        ),
        document_service=FakeDocumentService(pending=0),
    )
    resource_set = resources()
    resource_set.background_work = FakeCoordinator(
        {
            "max_concurrency": 2,
            "max_queued_global": 8,
            "queued": 0,
            "queued_for_project": 0,
            "active_for_project": False,
        }
    )
    service = RuntimeHealthService(
        resources=resource_set,
        projects=SimpleNamespace(active_projects={"project-a": project}),
        sessions=SessionRuntimeReader({}),
    )

    payload = (await service.get_background_health(project_id="project-a")).model_dump(
        mode="json"
    )

    assert payload["status"] == "degraded"
    assert payload["activity"] == "idle"
    assert "background scheduler is stopped" in payload["warnings"]
