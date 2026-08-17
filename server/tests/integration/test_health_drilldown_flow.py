import json
from types import SimpleNamespace

import pytest

from core.agent.tools.health import HealthTools
from core.health.service import RuntimeHealthService


class HealthPostgres:
    def __init__(self):
        self.fetch_one_calls = 0
        self.write_calls = 0

    def pool_snapshot(self):
        return {
            "connected": True,
            "pool_min": 1,
            "pool_max": 4,
            "pool_size": 2,
            "pool_available": 1,
            "requests_waiting": 0,
            "stats_available": True,
        }

    async def fetch_one(self, _query):
        self.fetch_one_calls += 1
        return {"ok": 1}

    async def execute(self, *_args, **_kwargs):
        self.write_calls += 1


class HealthRedis:
    def __init__(self):
        self.ping_calls = 0
        self.write_calls = 0

    async def ping(self):
        self.ping_calls += 1
        return True

    async def llen(self, _key):
        return 0

    async def lrange(self, _key, _start, _end):
        return []

    async def get(self, _key):
        return None

    async def set(self, *_args, **_kwargs):
        self.write_calls += 1


class HealthCoordinator:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.calls = []

    def snapshot_for_health(self, **kwargs):
        self.calls.append(kwargs)
        return dict(self.snapshot)


class HealthToolHarness(HealthTools):
    def __init__(self, service):
        self.health_service = service
        self.user_name = "ada"
        self.project_id = "project-a"
        self.session_id = "session-a"


@pytest.mark.integration
@pytest.mark.no_network
async def test_health_drilldown_is_bounded_scoped_and_read_only():
    postgres = HealthPostgres()
    redis = HealthRedis()
    resources = SimpleNamespace(
        postgres=postgres,
        redis=redis,
        redis_manager=None,
        model_work=HealthCoordinator(
            {
                "foreground_concurrency": 1,
                "background_concurrency": 1,
                "queued_by_priority": {"foreground": 2, "background": 0},
                "in_flight_by_priority": {"foreground": 1, "background": 0},
                "work_by_name": {"embedding": {"queued": 1}},
            }
        ),
        background_work=HealthCoordinator(
            {
                "max_concurrency": 2,
                "max_queued_global": 8,
                "queued": 1,
                "queued_for_project": 1,
            }
        ),
        knowledge_store=object(),
        executor=object(),
        embedding=object(),
        llm_service=object(),
    )
    project = SimpleNamespace(
        project_id="project-a",
        scheduler=HealthCoordinator(
            {
                "state": "running",
                "queued_jobs": 0,
                "running_jobs": 0,
                "stalled_jobs": 0,
                "recent_failed_jobs": 0,
            }
        ),
        document_service=SimpleNamespace(
            indexing_snapshot_for_health=lambda: {"local_submission_tasks": 0},
            pending_index_count=lambda: 0,
        ),
    )
    service = RuntimeHealthService(
        resources=resources,
        projects=SimpleNamespace(active_projects={"project-a": project}),
        active_sessions={"session-a": SimpleNamespace(project_id="project-a")},
    )
    tools = HealthToolHarness(service)

    engine = await tools.get_engine_health()
    assert engine["status"] == "healthy"
    resources_health = await tools.get_resource_health()
    ingestion_health = await tools.get_ingestion_health()
    background_health = await tools.get_background_health()

    assert resources_health["status"] == "degraded"
    assert ingestion_health["status"] == "degraded"
    assert background_health["status"] == "healthy"
    serialized = json.dumps(resources_health)
    assert "project-a" not in serialized
    assert postgres.fetch_one_calls == 1
    assert redis.ping_calls == 1
    assert postgres.write_calls == 0
    assert redis.write_calls == 0
