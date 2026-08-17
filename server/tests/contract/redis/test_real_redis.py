from types import SimpleNamespace
from uuid import uuid4

import pytest

from common.schema.settings import DLQSettings, RedisConnectionSettings
from core.ingestion.recovery import replay_job as dlq_job
from core.ingestion.recovery.dlq_state import ensure_dlq_id, serialize_dlq_entry
from core.ingestion.recovery.replay_job import DLQReplayJob
from infrastructure.job.base import JobContext
from infrastructure.redis_client import AsyncRedisClient, RedisKeys


class _EmptyStore:
    def __init__(self):
        self.parked = {}

    async def get_requeued_dlq_items(self, **_kwargs):
        return []

    async def park_dlq_item(self, *, dlq_id, user_name, project_id, entry):
        self.parked[(user_name, project_id, dlq_id)] = {
            **dict(entry),
            "status": "parked",
        }

    async def get_parked_dlq_item(self, *, dlq_id, user_name, project_id):
        return self.parked.get((user_name, project_id, dlq_id))

    async def mark_parked_dlq_item_requeued(self, *, dlq_id, user_name, project_id):
        entry = self.parked.get((user_name, project_id, dlq_id))
        if entry is None or entry["status"] != "parked":
            return False
        entry["status"] = "requeued"
        return True

    async def mark_parked_dlq_item_completed_if_requeued(
        self, *, dlq_id, user_name, project_id
    ):
        entry = self.parked.get((user_name, project_id, dlq_id))
        if entry is None or entry["status"] != "requeued":
            return False
        entry["status"] = "completed"
        return True


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
async def test_async_redis_client_connects_and_round_trips_values():
    manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    client = await manager.connect()
    key = f"knoggin:test:redis:{uuid4()}"

    try:
        assert await client.ping()
        assert await client.set(key, "ready", ex=30)
        assert await client.get(key) == "ready"
    finally:
        await client.delete(key)
        await manager.close()


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_dlq_park_requeue_and_repark_lifecycle(monkeypatch):
    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(dlq_job, "emit", emit_nothing)
    user_name = f"redis-test-{uuid4()}"
    project_id = f"project-{uuid4()}"
    manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    client = await manager.connect()
    job = DLQReplayJob(
        entities=SimpleNamespace(project_id=project_id),
        processor=SimpleNamespace(knowledge_store=_EmptyStore()),
        write_to_graph=None,
        redis_client=client,
        settings=DLQSettings(max_attempts=2),
    )
    context = JobContext(user_name=user_name, project_id=project_id)
    entry = {
        "error": "unsupported DLQ schema",
        "attempt": 1,
        "stage": "checkpoint",
        "user_name": user_name,
        "project_id": project_id,
        "session_id": "session-1",
        "messages": [{"id": 7, "message": "Redis recovery"}],
        "batch_result": {"schema_version": 999},
    }
    dlq_id = ensure_dlq_id(entry)
    dlq_key = RedisKeys.dlq(user_name, project_id)
    parked_key = RedisKeys.dlq_parked(user_name, project_id)
    state_key = RedisKeys.dlq_state(user_name, project_id)

    try:
        await client.rpush(dlq_key, serialize_dlq_entry(entry))

        first = await job.execute(context)

        assert first.summary == "Processed 1: 0 retried, 1 parked"
        assert await client.llen(dlq_key) == 0
        assert await client.llen(parked_key) == 1
        assert await client.hget(state_key, dlq_id) == "parked"

        assert await job.requeue_parked_dlq_item(user_name, project_id, dlq_id)
        assert await client.llen(dlq_key) == 1
        assert await client.llen(parked_key) == 0
        assert await client.hget(state_key, dlq_id) == "queued"

        second = await job.execute(context)

        assert second.summary == "Processed 1: 0 retried, 1 parked"
        assert await client.llen(dlq_key) == 0
        assert await client.llen(parked_key) == 1
        assert await client.hget(state_key, dlq_id) == "parked"
    finally:
        await client.delete(*RedisKeys.project_cleanup_keys(user_name, project_id))
        await manager.close()
