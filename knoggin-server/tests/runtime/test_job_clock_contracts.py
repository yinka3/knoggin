import asyncio
from datetime import timedelta
from types import SimpleNamespace

import pytest

from common.schema.settings import CommunitySettings, DeveloperSettings, RootConfig
from common.utils.time_utils import (
    frozen_time,
    get_now,
    get_now_ms,
    get_now_unix,
)
from infrastructure.job.base import JobContext, JobResult
from infrastructure.job.scheduler import Scheduler
from infrastructure.redis_client import RedisKeys
from knoggin_server.community.community_job import AACJob
from knoggin_server.ingestion.jobs.archive_job import FactArchivalJob
from knoggin_server.ingestion.jobs.cleaner_job import EntityCleanupJob
from knoggin_server.ingestion.jobs.dlq_job import DLQReplayJob
from knoggin_server.knowledge.jobs.profile_job import ProfileRefinementJob
from tests.fixtures.fakes import FakeRedis, FakeResources

FROZEN_AT = "2024-01-01T00:00:00+00:00"


def job_context() -> JobContext:
    return JobContext(user_name="ada", scope_id="project-1", project_id="project-1")


class ProcessorWithGraphClient:
    graph_client = object()

    async def run(self, messages, session_text):
        raise AssertionError("processor.run should not be reached in clock tests")


class FakeSchedulerJob:
    name = "clocked_job"

    async def execute(self, ctx):
        return JobResult(success=True, summary="done")


class CleanupGraph:
    def __init__(self, orphan_ids=None):
        self.orphan_ids = orphan_ids or []
        self.cleanup_calls = []
        self.orphan_calls = []
        self.bulk_delete_calls = []

    async def cleanup_null_entities(self, project_id=None):
        self.cleanup_calls.append(project_id)

    async def get_orphan_entities(
        self, user_id, orphan_cutoff, junk_cutoff, project_id=None
    ):
        self.orphan_calls.append((user_id, orphan_cutoff, junk_cutoff, project_id))
        return self.orphan_ids

    async def bulk_delete_entities(self, entity_ids, project_id=None):
        self.bulk_delete_calls.append((list(entity_ids), project_id))
        return len(entity_ids)


class CleanupEntities:
    def __init__(self, user_id=1):
        self.user_id = user_id
        self.removed = []

    async def get_id(self, name):
        return self.user_id

    def remove_entities(self, entity_ids):
        self.removed.extend(entity_ids)


class ArchivalGraph:
    def __init__(self, deleted_count=0):
        self.deleted_count = deleted_count
        self.calls = []

    async def delete_old_invalidated_facts(self, cutoff, project_id=None):
        self.calls.append((cutoff, project_id))
        return self.deleted_count


class ProfileEntities:
    entity_profiles = {
        1: {"canonical_name": "ada", "type": "person"},
        2: {"canonical_name": "Recent", "type": "concept"},
        3: {"canonical_name": "Ready", "type": "concept"},
    }

    async def get_id(self, name):
        return 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_scheduler_uses_frozen_wall_clock_for_activity_idle_and_last_run():
    redis = FakeRedis()
    scheduler = Scheduler("ada", "project-1", redis, project_id="project-1")
    job = FakeSchedulerJob()
    ctx = job_context()

    with frozen_time(FROZEN_AT) as clock:
        await scheduler.record_activity()
        activity_key = RedisKeys.last_activity("ada", "project-1")
        assert await redis.get(activity_key) == "2024-01-01T00:00:00+00:00"

        clock.advance(seconds=45)
        assert await scheduler._get_idle_seconds() == 45.0

        await scheduler._execute_job(job, ctx)
        assert scheduler._last_runs[job.name] == get_now()


@pytest.mark.runtime
@pytest.mark.no_network
async def test_scheduler_pending_checks_use_scope_id():
    redis = FakeRedis()
    scheduler = Scheduler("ada", "project-1", redis, project_id="project-1")
    job = FakeSchedulerJob()
    scheduler.register(job)
    captured_contexts = []

    async def fake_execute_job(job_arg, ctx_arg):
        captured_contexts.append(ctx_arg)

    scheduler._execute_job = fake_execute_job
    await redis.set(RedisKeys.job_pending("ada", "project-1", job.name), "1")
    await redis.set(RedisKeys.job_pending("ada", "session-1", job.name), "1")

    await scheduler._run_pending_checks()
    await asyncio.sleep(0)

    assert await redis.get(RedisKeys.job_pending("ada", "project-1", job.name)) is None
    assert await redis.get(RedisKeys.job_pending("ada", "session-1", job.name)) == "1"
    assert captured_contexts[0].scope_id == "project-1"
    assert captured_contexts[0].project_id == "project-1"

@pytest.mark.runtime
@pytest.mark.no_network
async def test_dlq_job_should_run_and_empty_execute_use_frozen_unix_time():
    redis = FakeRedis()
    job = DLQReplayJob(
        entities=object(),
        processor=ProcessorWithGraphClient(),
        write_to_graph=lambda result: (True, None),
        redis_client=redis,
        interval=60,
    )
    ctx = job_context()
    last_run_key = RedisKeys.job_last_run(job.name, "ada", "project-1")

    with frozen_time(FROZEN_AT) as clock:
        assert await job.should_run(ctx) is False
        assert await redis.get(last_run_key) == str(get_now_unix())

        clock.advance(seconds=59)
        assert await job.should_run(ctx) is False

        clock.advance(seconds=1)
        assert await job.should_run(ctx) is True

        await redis.set(last_run_key, "not-a-number")
        assert await job.should_run(ctx) is False
        assert await redis.get(last_run_key) == str(get_now_unix())

        clock.advance(seconds=10)
        result = await job.execute(ctx)
        assert result.success is True
        assert result.summary == "DLQ empty"
        assert await redis.get(last_run_key) == str(get_now_unix())


@pytest.mark.runtime
@pytest.mark.no_network
async def test_entity_cleanup_uses_frozen_cutoffs_and_updates_last_run():
    redis = FakeRedis()
    graph = CleanupGraph()
    entities = CleanupEntities(user_id=1)
    job = EntityCleanupJob(
        user_name="ada",
        graph_client=graph,
        entities=entities,
        redis_client=redis,
        interval_hours=1,
        orphan_age_hours=2,
        stale_junk_days=3,
    )
    ctx = job_context()
    last_run_key = RedisKeys.job_last_run(job.name, "ada", "project-1")

    with frozen_time(FROZEN_AT):
        assert await job.should_run(ctx) is False
        assert await redis.get(last_run_key) == str(get_now_unix())

        result = await job.execute(ctx)

        assert result.success is True
        assert result.summary == "No orphans found"
        expected_orphan_cutoff = get_now_ms() - (2 * 3600 * 1000)
        expected_junk_cutoff = get_now_ms() - (3 * 24 * 3600 * 1000)
        assert graph.cleanup_calls == ["project-1"]
        assert graph.orphan_calls == [
            (1, expected_orphan_cutoff, expected_junk_cutoff, "project-1")
        ]
        assert await redis.get(last_run_key) == str(get_now_unix())


@pytest.mark.runtime
@pytest.mark.no_network
async def test_entity_cleanup_user_missing_still_updates_last_run():
    redis = FakeRedis()
    graph = CleanupGraph()
    entities = CleanupEntities(user_id=None)
    job = EntityCleanupJob("ada", graph, entities, redis)
    ctx = job_context()
    last_run_key = RedisKeys.job_last_run(job.name, "ada", "project-1")

    with frozen_time(FROZEN_AT):
        result = await job.execute(ctx)

        assert result.summary == "User entity not initialized"
        assert graph.cleanup_calls == ["project-1"]
        assert graph.orphan_calls == []
        assert await redis.get(last_run_key) == str(get_now_unix())


@pytest.mark.runtime
@pytest.mark.no_network
async def test_fact_archival_uses_frozen_interval_marker_and_cutoff():
    redis = FakeRedis()
    graph = ArchivalGraph(deleted_count=2)
    job = FactArchivalJob(
        user_name="ada",
        graph_client=graph,
        redis_client=redis,
        retention_days=14,
        fallback_interval_hours=1,
    )
    ctx = job_context()
    last_run_key = RedisKeys.job_last_run(job.name, "ada", "project-1")
    profile_complete_key = RedisKeys.profile_complete("ada", "project-1")

    with frozen_time(FROZEN_AT) as clock:
        assert await job.should_run(ctx) is False
        assert await redis.get(last_run_key) == str(get_now_unix())

        clock.advance(minutes=59)
        assert await job.should_run(ctx) is False

        clock.advance(minutes=1)
        assert await job.should_run(ctx) is True

        await redis.set(profile_complete_key, str(get_now_unix()))
        assert await job.should_run(ctx) is True
        assert await redis.get(profile_complete_key) is None

        result = await job.execute(ctx)

        assert result.summary == "Archived 2 invalidated facts"
        expected_cutoff = get_now() - timedelta(days=14)
        assert graph.calls == [(expected_cutoff, "project-1")]
        assert await redis.get(last_run_key) == str(get_now_unix())


@pytest.mark.runtime
@pytest.mark.no_network
async def test_aac_job_uses_frozen_interval_and_last_run(monkeypatch):
    class FakeCommunityManager:
        def __init__(self, project_state, user_name, resources):
            self.project_state = project_state
            self.user_name = user_name
            self.resources = resources

        async def trigger_discussion(self):
            self.resources.triggered_discussions += 1

    class FakeCommunityGraph:
        async def delete_old_discussions(self, days):
            self.deleted_days = days

    def patch_config(enabled, interval_minutes=30):
        root = RootConfig(developer_settings=DeveloperSettings())
        root.developer_settings.community = CommunitySettings(
            enabled=enabled,
            interval_minutes=interval_minutes,
        )
        monkeypatch.setattr(
            "knoggin_server.community.community_job.ConfigManager.get",
            staticmethod(lambda: SimpleNamespace(config=root)),
        )

    monkeypatch.setattr(
        "knoggin_server.community.community_job.CommunityManager",
        FakeCommunityManager,
    )

    resources = FakeResources()
    resources.triggered_discussions = 0
    resources.graph_client.community = FakeCommunityGraph()
    project_state = SimpleNamespace(project_id="project-1")
    job = AACJob(project_state, resources)
    ctx = job_context()
    last_run_key = RedisKeys.job_last_run(job.name, "ada", "project-1")

    with frozen_time(FROZEN_AT) as clock:
        patch_config(enabled=False)
        assert await job.should_run(ctx) is False

        patch_config(enabled=True, interval_minutes=30)
        assert await job.should_run(ctx) is True

        await resources.redis.set(last_run_key, get_now_unix())
        clock.advance(minutes=29)
        assert await job.should_run(ctx) is False

        clock.advance(minutes=1)
        assert await job.should_run(ctx) is True

        result = await job.execute(ctx)
        assert result.success is True
        assert resources.triggered_discussions == 1
        assert await resources.redis.get(last_run_key) == str(get_now_unix())

@pytest.mark.runtime
@pytest.mark.no_network
async def test_profile_refinement_targeted_recency_and_markers_use_frozen_unix(
    monkeypatch,
):
    root = RootConfig(developer_settings=DeveloperSettings())
    root.developer_settings.jobs.merger.enabled = False
    monkeypatch.setattr(
        "knoggin_server.knowledge.jobs.profile_job.ConfigManager.get",
        staticmethod(lambda: SimpleNamespace(config=root)),
    )

    redis = FakeRedis()
    job = ProfileRefinementJob(
        llm=object(),
        entities=ProfileEntities(),
        graph_client=object(),
        executor=None,
        embedding_service=object(),
        redis_client=redis,
    )
    ctx = job_context()
    seen_entity_ids = []
    written_updates = []

    async def fake_get_conversation_context(ctx_arg, num_turns, **kwargs):
        return [{"formatted": "[MSG_1] hello", "user_msg_id": 1}]

    async def fake_run_updates(ctx_arg, entity_ids, conversation):
        seen_entity_ids.extend(entity_ids)
        return (
            [
                {
                    "id": eid,
                    "canonical_name": f"Entity {eid}",
                    "embedding": [0.1],
                    "last_msg_id": 1,
                    "project_id": ctx_arg.project_id,
                }
                for eid in entity_ids
            ],
            list(entity_ids),
        )

    async def fake_write_updates(updates, project_id):
        written_updates.extend(updates)

    async def fake_maybe_refine_user(ctx_arg, current_msg_id):
        return False

    job._get_conversation_context = fake_get_conversation_context
    job._run_updates = fake_run_updates
    job._write_updates = fake_write_updates
    job._maybe_refine_user = fake_maybe_refine_user

    recent_key = RedisKeys.last_profile_update("ada", "project-1", 2)
    old_key = RedisKeys.last_profile_update("ada", "project-1", 3)
    profile_complete_key = RedisKeys.profile_complete("ada", "project-1")

    with frozen_time(FROZEN_AT):
        await redis.set(recent_key, get_now_unix() - 30)
        await redis.set(old_key, get_now_unix() - 61)

        result = await job.execute(ctx, target_ids=[2, 3])

        assert result.success is True
        assert seen_entity_ids == [3]
        assert [update["id"] for update in written_updates] == [3]
        assert await redis.get(recent_key) == str(get_now_unix() - 30)
        assert await redis.get(old_key) == str(get_now_unix())
        assert await redis.get(profile_complete_key) == str(get_now_unix())
        assert (old_key, 3600) in redis.expirations
        assert (profile_complete_key, 300) in redis.expirations
