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
from knoggin_server.ingestion.jobs.profile_job import ProfileRefinementJob
from tests.fixtures.fakes import FakeRedis, FakeResources

FROZEN_AT = "2024-01-01T00:00:00+00:00"


def job_context() -> JobContext:
    return JobContext(user_name="ada", project_id="project-1")


class ProcessorWithKnowledgeStore:
    knowledge_store = object()

    async def run(self, messages, session_text, *, session_id):
        raise AssertionError("processor.run should not be reached in clock tests")


class FakeSchedulerJob:
    name = "clocked_job"
    enabled = True

    async def should_run(self, ctx):
        return True

    async def execute(self, ctx):
        return JobResult(success=True, summary="done")


class CleanupKnowledgeStore:
    def __init__(
        self,
        orphan_ids=None,
        null_deleted_ids=None,
        bulk_deleted_ids=None,
    ):
        self.orphan_ids = orphan_ids or []
        self.null_deleted_ids = null_deleted_ids or []
        self.bulk_deleted_ids = bulk_deleted_ids
        self.cleanup_calls = []
        self.orphan_calls = []
        self.bulk_delete_calls = []

    async def cleanup_null_entities(self, project_id=None):
        self.cleanup_calls.append(project_id)
        return list(self.null_deleted_ids)

    async def get_orphan_entities(
        self, user_id, orphan_cutoff, junk_cutoff, project_id=None
    ):
        self.orphan_calls.append((user_id, orphan_cutoff, junk_cutoff, project_id))
        return self.orphan_ids

    async def bulk_delete_entities(self, entity_ids, project_id=None):
        self.bulk_delete_calls.append((list(entity_ids), project_id))
        if self.bulk_deleted_ids is None:
            return list(entity_ids)
        return [
            entity_id
            for entity_id in entity_ids
            if entity_id in self.bulk_deleted_ids
        ]


class CleanupEntities:
    def __init__(self, user_id=1):
        self.user_id = user_id
        self.removed = []

    async def get_id(self, name):
        return self.user_id

    def remove_entities(self, entity_ids):
        self.removed.extend(entity_ids)


class ArchivalKnowledgeStore:
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
async def test_scheduler_uses_frozen_wall_clock_for_activity_and_idle():
    redis = FakeRedis()
    scheduler = Scheduler("ada", "project-1", redis)
    job = FakeSchedulerJob()
    ctx = job_context()

    with frozen_time(FROZEN_AT) as clock:
        await scheduler.record_activity()
        activity_key = RedisKeys.project_last_activity("ada", "project-1")
        assert await redis.get(activity_key) == "2024-01-01T00:00:00+00:00"

        clock.advance(seconds=45)
        assert await scheduler._get_idle_seconds() == 45.0

        await scheduler._execute_job(job, ctx)
        assert (
            await redis.get(RedisKeys.job_lease("ada", "project-1", job.name))
            is None
        )
    assert not hasattr(scheduler, "session_id")
    assert not hasattr(ctx, "session_id")
    assert not hasattr(ctx, "scope_id")

@pytest.mark.runtime
@pytest.mark.no_network
async def test_dlq_job_exposes_cadence_and_leaves_clock_to_scheduler():
    redis = FakeRedis()
    job = DLQReplayJob(
        entities=object(),
        processor=ProcessorWithKnowledgeStore(),
        write_to_graph=lambda result: (True, None),
        redis_client=redis,
        interval=60,
    )
    ctx = job_context()
    assert job.cadence_seconds == 60
    assert await job.should_run(ctx) is False

    result = await job.execute(ctx)

    assert result.success is True
    assert result.summary == "DLQ empty"
    assert await redis.get(
        RedisKeys.job_last_run(job.name, "ada", "project-1")
    ) is None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_entity_cleanup_uses_frozen_cutoffs_without_owning_cadence():
    redis = FakeRedis()
    knowledge_store = CleanupKnowledgeStore()
    entities = CleanupEntities(user_id=1)
    job = EntityCleanupJob(
        user_name="ada",
        knowledge_store=knowledge_store,
        entities=entities,
        redis_client=redis,
        interval_hours=1,
        orphan_age_hours=2,
        stale_junk_days=3,
    )
    ctx = job_context()
    with frozen_time(FROZEN_AT):
        assert await job.should_run(ctx) is False
        assert job.cadence_seconds == 3600

        result = await job.execute(ctx)

        assert result.success is True
        assert result.summary == "No orphans found"
        expected_orphan_cutoff = get_now_ms() - (2 * 3600 * 1000)
        expected_junk_cutoff = get_now_ms() - (3 * 24 * 3600 * 1000)
        assert knowledge_store.cleanup_calls == ["project-1"]
        assert knowledge_store.orphan_calls == [
            (1, expected_orphan_cutoff, expected_junk_cutoff, "project-1")
        ]
        assert await redis.get(
            RedisKeys.job_last_run(job.name, "ada", "project-1")
        ) is None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_entity_cleanup_user_missing_returns_success_without_clock_write():
    redis = FakeRedis()
    knowledge_store = CleanupKnowledgeStore()
    entities = CleanupEntities(user_id=None)
    job = EntityCleanupJob("ada", knowledge_store, entities, redis)
    ctx = job_context()
    with frozen_time(FROZEN_AT):
        result = await job.execute(ctx)

        assert result.summary == "User entity not initialized"
        assert knowledge_store.cleanup_calls == ["project-1"]
        assert knowledge_store.orphan_calls == []
        assert await redis.get(
            RedisKeys.job_last_run(job.name, "ada", "project-1")
        ) is None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_entity_cleanup_evicts_only_ids_confirmed_deleted():
    redis = FakeRedis()
    knowledge_store = CleanupKnowledgeStore(
        orphan_ids=[2, 3, 4],
        null_deleted_ids=[5],
        bulk_deleted_ids=[2, 4],
    )
    entities = CleanupEntities(user_id=1)
    job = EntityCleanupJob("ada", knowledge_store, entities, redis)

    result = await job.execute(job_context())

    assert result.summary == "Cleaned 3 entities"
    assert knowledge_store.bulk_delete_calls == [([2, 3, 4], "project-1")]
    assert entities.removed == [5, 2, 4]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_fact_archival_exposes_fallback_cadence_and_consumes_marker_on_success():
    redis = FakeRedis()
    knowledge_store = ArchivalKnowledgeStore(deleted_count=2)
    job = FactArchivalJob(
        user_name="ada",
        knowledge_store=knowledge_store,
        redis_client=redis,
        retention_days=14,
        fallback_interval_hours=1,
    )
    ctx = job_context()
    profile_complete_key = RedisKeys.project_profile_complete("ada", "project-1")

    with frozen_time(FROZEN_AT):
        assert await job.should_run(ctx) is False
        assert job.cadence_seconds == 3600

        await redis.set(profile_complete_key, str(get_now_unix()))
        assert await job.should_run(ctx) is True
        assert await redis.get(profile_complete_key) is not None

        result = await job.execute(ctx)

        assert result.summary == "Archived 2 invalidated facts"
        expected_cutoff = get_now() - timedelta(days=14)
        assert knowledge_store.calls == [(expected_cutoff, "project-1")]
        assert await redis.get(profile_complete_key) is None
        assert await redis.get(
            RedisKeys.job_last_run(job.name, "ada", "project-1")
        ) is None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_fact_archival_preserves_profile_marker_when_execution_fails():
    redis = FakeRedis()
    knowledge_store = ArchivalKnowledgeStore()
    job = FactArchivalJob("ada", knowledge_store, redis)
    ctx = job_context()
    profile_complete_key = RedisKeys.project_profile_complete("ada", "project-1")
    await redis.set(profile_complete_key, "profile-run-1")

    async def fail_archival(cutoff, project_id=None):
        raise RuntimeError("database unavailable")

    knowledge_store.delete_old_invalidated_facts = fail_archival

    with pytest.raises(RuntimeError, match="database unavailable"):
        await job.execute(ctx)

    assert await redis.get(profile_complete_key) == "profile-run-1"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_aac_job_exposes_live_cadence_and_leaves_clock_to_scheduler(monkeypatch):
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
    resources.knowledge_store.community = FakeCommunityGraph()
    project_state = SimpleNamespace(project_id="project-1")
    job = AACJob(project_state, resources)
    ctx = job_context()
    with frozen_time(FROZEN_AT):
        patch_config(enabled=False)
        assert await job.should_run(ctx) is False
        assert job.cadence_seconds is None

        patch_config(enabled=True, interval_minutes=30)
        assert await job.should_run(ctx) is False
        assert job.cadence_seconds == 30 * 60
        assert job.run_immediately_on_first_check is True

        result = await job.execute(ctx)
        assert result.success is True
        assert resources.triggered_discussions == 1
        assert await resources.redis.get(
            RedisKeys.job_last_run(job.name, "ada", "project-1")
        ) is None

@pytest.mark.runtime
@pytest.mark.no_network
async def test_profile_refinement_targeted_recency_and_markers_use_frozen_unix(
    monkeypatch,
):
    redis = FakeRedis()
    job = ProfileRefinementJob(
        llm=object(),
        entities=ProfileEntities(),
        knowledge_store=object(),
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
    profile_complete_key = RedisKeys.project_profile_complete("ada", "project-1")

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
        assert await redis.smembers(
            RedisKeys.merge_queue("ada", "project-1")
        ) == {"3"}
        assert (old_key, 3600) in redis.expirations
        assert (profile_complete_key, 300) in redis.expirations
