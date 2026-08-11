import json
from types import SimpleNamespace

import pytest

from core.health.service import RuntimeHealthService


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


class FakeRedis:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.ping_calls = 0
        self.strings = {}
        self.lists = {}

    async def ping(self):
        self.ping_calls += 1
        if self.fail:
            raise RuntimeError("redis://:secret@example.invalid")
        return True

    async def llen(self, key):
        return len(self.lists.get(key, []))

    async def lrange(self, key, start, end):
        items = self.lists.get(key, [])
        return items[start : (len(items) if end == -1 else end + 1)]

    async def get(self, key):
        return self.strings.get(key)

class FakeCoordinator:
    def __init__(self, snapshot: dict):
        self.snapshot = snapshot
        self.calls = []

    def snapshot_for_health(self, **kwargs):
        self.calls.append(kwargs)
        return dict(self.snapshot)


def resources(*, postgres=None, redis=None):
    return SimpleNamespace(
        postgres=postgres or FakePostgres(),
        redis=redis or FakeRedis(),
        redis_manager=None,
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
    redis = FakeRedis()
    service = RuntimeHealthService(
        resources=resources(postgres=postgres, redis=redis),
        projects=SimpleNamespace(
            active_projects={"project-a": object(), "project-b": object()}
        ),
        active_sessions={"session-a": object()},
    )

    snapshot = await service.get_engine_health()
    payload = snapshot.model_dump(mode="json")

    json.dumps(payload)
    assert payload["status"] == "healthy"
    assert payload["activity"] == "busy"
    assert payload["details"]["loaded_project_count"] == 2
    assert payload["details"]["active_runtime_session_count"] == 1
    assert postgres.fetch_calls == 1
    assert redis.ping_calls == 1
    assert "secret" not in json.dumps(payload)


@pytest.mark.unit
@pytest.mark.no_network
async def test_failed_dependency_probes_are_bounded_and_redacted():
    service = RuntimeHealthService(
        resources=resources(
            postgres=FakePostgres(fail=True), redis=FakeRedis(fail=True)
        ),
        projects=SimpleNamespace(active_projects={}),
        active_sessions={},
    )

    payload = (await service.get_engine_health()).model_dump(mode="json")

    assert payload["status"] == "failed"
    assert payload["activity"] == "idle"
    assert payload["details"]["postgres"]["reason"] == "unavailable"
    assert payload["details"]["redis"]["reason"] == "probe_failed"
    assert "postgresql://" not in json.dumps(payload)
    assert "redis://" not in json.dumps(payload)


@pytest.mark.unit
@pytest.mark.no_network
async def test_resource_health_projects_current_queue_without_other_project_ids():
    resource_set = resources()
    service = RuntimeHealthService(
        resources=resource_set,
        projects=SimpleNamespace(
            active_projects={"project-a": object(), "project-b": object()}
        ),
        active_sessions={"session-a": object()},
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
async def test_ingestion_health_reads_fixed_keys_and_classifies_pending_work():
    from infrastructure.redis_client import RedisKeys

    redis = FakeRedis()
    redis.lists[RedisKeys.buffer("ada", "session-a")] = [
        '{"id": 1, "message": "hidden", "timestamp": "2020-01-01T00:00:00+00:00"}'
    ]
    redis.strings[RedisKeys.last_processed("ada", "session-a")] = "0"
    redis.strings[RedisKeys.project_last_processed("ada", "project-a")] = "0"
    redis.strings[RedisKeys.checkpoint("ada", "session-a")] = "4"
    resource_set = resources(redis=redis)
    service = RuntimeHealthService(
        resources=resource_set,
        projects=SimpleNamespace(active_projects={}),
        active_sessions={
            "session-a": SimpleNamespace(
                project_id="project-a", consumer=FakeWorker()
            )
        },
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
    assert payload["details"]["progress"]["checkpoint_count"] == 4
    serialized = json.dumps(payload)
    assert "hidden" not in serialized
    assert '"id"' not in serialized


@pytest.mark.unit
@pytest.mark.no_network
async def test_ingestion_health_degrades_without_failing_when_redis_reads_fail():
    class FailingRedis(FakeRedis):
        async def llen(self, _key):
            raise RuntimeError("redis://secret")

        async def lrange(self, _key, _start, _end):
            raise RuntimeError("redis://secret")

        async def get(self, _key):
            raise RuntimeError("redis://secret")

    service = RuntimeHealthService(
        resources=resources(redis=FailingRedis()),
        projects=SimpleNamespace(active_projects={}),
        active_sessions={
            "session-a": SimpleNamespace(
                project_id="project-a", consumer=FakeWorker()
            )
        },
    )

    payload = (
        await service.get_ingestion_health(
            user_name="ada", project_id="project-a", session_id="session-a"
        )
    ).model_dump(mode="json")

    assert payload["status"] == "degraded"
    assert payload["details"]["redis_available"] is False
    assert payload["details"]["queue"]["pending_count"] == 0
    assert "redis://" not in json.dumps(payload)


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
        active_sessions={},
    )

    payload = (
        await service.get_background_health(project_id="project-a")
    ).model_dump(mode="json")

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
        active_sessions={},
    )

    payload = (
        await service.get_background_health(project_id="project-a")
    ).model_dump(mode="json")

    assert payload["status"] == "degraded"
    assert payload["activity"] == "idle"
    assert "background scheduler is stopped" in payload["warnings"]
