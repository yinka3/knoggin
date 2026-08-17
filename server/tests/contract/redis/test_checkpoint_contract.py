"""Real-Redis contracts for durable ingestion checkpoint commits."""

import asyncio
from uuid import uuid4

import pytest

from common.schema.settings import IngestionSettings, RedisConnectionSettings
from core.ingestion.batch import IngestionBatch
from core.ingestion.recovery import checkpoint as checkpoint_module
from core.ingestion.recovery.checkpoint import (
    CheckpointCommit,
    commit_ingestion_checkpoint,
)
from infrastructure.redis_client import AsyncRedisClient, RedisKeys
from infrastructure.work_record import WorkRecord
from tests.fixtures.ingestion import ingestion_policy


def _batch(
    *,
    user_name: str,
    project_id: str,
    session_id: str,
    message_ids: list[int],
    checkpoint_interval: int = 32,
) -> IngestionBatch:
    batch = IngestionBatch.open(
        user_name=user_name,
        project_id=project_id,
        session_id=session_id,
        messages=[
            {"id": message_id, "message": f"Checkpoint message {message_id}."}
            for message_id in message_ids
        ],
        session_text="\n".join(
            f"[USER]: Checkpoint message {message_id}." for message_id in message_ids
        ),
        policy=ingestion_policy(
            ingestion=IngestionSettings(checkpoint_interval=checkpoint_interval)
        ),
        batch_id=f"batch-{uuid4()}",
    )
    # Checkpointing is the final durable step and only accepts a graph-committed
    # aggregate, matching the worker/replay call sites exercised by production.
    batch.validate_input()
    batch.mark_extracted()
    batch.set_resolution(
        entity_ids=[],
        new_entity_ids=set(),
        alias_updated_ids=set(),
        entity_message_map={},
        alias_updates={},
        candidate_suggestions=[],
    )
    batch.set_relationship_observations([])
    batch.complete()
    batch.mark_message_logs_handled()
    batch.mark_candidate_suggestions_handled()
    batch.set_graph_write_buffers(
        graph_work_unit=WorkRecord.for_graph_write(batch.scope),
        safe_entity_ids=set(),
        graph_alias_updates=[],
        entity_writes=[],
        relationship_writes=[],
        message_entity_refs=[],
        eligible_messages=[],
        skipped_relationships=[],
        zombie_entity_ids=set(),
        dirty_entity_ids=set(),
    )
    batch.seal_for_commit()
    batch.graph_work_unit.start()
    batch.graph_work_unit.succeed()
    batch.mark_graph_committed()
    return batch


async def _connect() -> tuple[AsyncRedisClient, object]:
    manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    return manager, await manager.connect()


async def _cleanup_scope(
    client,
    *,
    user_name: str,
    project_id: str,
    session_ids: set[str],
) -> None:
    keys = {RedisKeys.project_last_processed(user_name, project_id)}
    for session_id in session_ids:
        keys.update(RedisKeys.session_keys(user_name, session_id))
        async for key in client.scan_iter(
            match=f"checkpoint_commit:{user_name}:{session_id}:*"
        ):
            keys.add(key)
    if keys:
        await client.delete(*keys)


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_checkpoint_returns_lua_count_and_threshold_response():
    """The production Lua script returns the count and threshold flag intact."""

    user_name = f"checkpoint-lua-{uuid4()}"
    project_id = f"project-{uuid4()}"
    session_id = f"session-{uuid4()}"
    manager, client = await _connect()
    try:
        first = await commit_ingestion_checkpoint(
            client,
            _batch(
                user_name=user_name,
                project_id=project_id,
                session_id=session_id,
                message_ids=[11, 12],
                checkpoint_interval=3,
            ),
        )
        second = await commit_ingestion_checkpoint(
            client,
            _batch(
                user_name=user_name,
                project_id=project_id,
                session_id=session_id,
                message_ids=[13],
                checkpoint_interval=3,
            ),
        )

        assert first == CheckpointCommit(count_before_reset=2, threshold_reached=False)
        assert first.current_count == 2
        assert second == CheckpointCommit(count_before_reset=3, threshold_reached=True)
        assert second.current_count == 0
        assert await client.get(RedisKeys.checkpoint(user_name, session_id)) == "0"
        assert await client.get(RedisKeys.last_processed(user_name, session_id)) == "13"
        assert (
            await client.get(RedisKeys.project_last_processed(user_name, project_id))
            == "13"
        )
    finally:
        await _cleanup_scope(
            client,
            user_name=user_name,
            project_id=project_id,
            session_ids={session_id},
        )
        await manager.close()


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_same_batch_commit_is_atomic_and_idempotent():
    """Concurrent workers for one batch increment the checkpoint only once."""

    user_name = f"checkpoint-concurrent-{uuid4()}"
    project_id = f"project-{uuid4()}"
    session_id = f"session-{uuid4()}"
    manager, client = await _connect()
    try:
        batches = [
            _batch(
                user_name=user_name,
                project_id=project_id,
                session_id=session_id,
                message_ids=[21, 22],
                checkpoint_interval=10,
            )
            for _ in range(8)
        ]
        results = await asyncio.gather(
            *(
                commit_ingestion_checkpoint(
                    client,
                    batch,
                )
                for batch in batches
            )
        )

        assert set(results) == {
            CheckpointCommit(count_before_reset=2, threshold_reached=False)
        }
        assert await client.get(RedisKeys.checkpoint(user_name, session_id)) == "2"
        assert await client.get(RedisKeys.last_processed(user_name, session_id)) == "22"
        assert (
            await client.get(RedisKeys.project_last_processed(user_name, project_id))
            == "22"
        )
    finally:
        await _cleanup_scope(
            client,
            user_name=user_name,
            project_id=project_id,
            session_ids={session_id},
        )
        await manager.close()


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_concurrent_batches_cross_threshold_once():
    """Two distinct batches crossing the threshold produce one reset."""

    user_name = f"checkpoint-threshold-{uuid4()}"
    project_id = f"project-{uuid4()}"
    session_id = f"session-{uuid4()}"
    manager, client = await _connect()
    try:
        first, second = await asyncio.gather(
            commit_ingestion_checkpoint(
                client,
                _batch(
                    user_name=user_name,
                    project_id=project_id,
                session_id=session_id,
                message_ids=[31, 32],
                checkpoint_interval=4,
                ),
            ),
            commit_ingestion_checkpoint(
                client,
                _batch(
                    user_name=user_name,
                    project_id=project_id,
                session_id=session_id,
                message_ids=[33, 34],
                checkpoint_interval=4,
                ),
            ),
        )

        assert {first, second} == {
            CheckpointCommit(count_before_reset=2, threshold_reached=False),
            CheckpointCommit(count_before_reset=4, threshold_reached=True),
        }
        assert await client.get(RedisKeys.checkpoint(user_name, session_id)) == "0"
        assert await client.get(RedisKeys.last_processed(user_name, session_id)) == "34"
        assert (
            await client.get(RedisKeys.project_last_processed(user_name, project_id))
            == "34"
        )
    finally:
        await _cleanup_scope(
            client,
            user_name=user_name,
            project_id=project_id,
            session_ids={session_id},
        )
        await manager.close()


class _LostResponseRedis:
    """Proxy that drops the response after the first real Lua execution."""

    def __init__(self, client):
        self._client = client
        self._lost = False

    async def eval(self, *args, **kwargs):
        response = await self._client.eval(*args, **kwargs)
        if not self._lost:
            self._lost = True
            raise ConnectionError("response lost after Redis executed the script")
        return response


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_reconstructed_retry_after_lost_response_commits_once():
    """A retry after a post-execution response loss reuses the commit token."""

    user_name = f"checkpoint-retry-{uuid4()}"
    project_id = f"project-{uuid4()}"
    session_id = f"session-{uuid4()}"
    manager, client = await _connect()
    proxy = _LostResponseRedis(client)
    batch = _batch(
        user_name=user_name,
        project_id=project_id,
        session_id=session_id,
        message_ids=[41],
        checkpoint_interval=10,
    )
    try:
        with pytest.raises(ConnectionError, match="response lost"):
            await commit_ingestion_checkpoint(proxy, batch)

        retry = await commit_ingestion_checkpoint(proxy, batch)

        assert retry == CheckpointCommit(count_before_reset=1, threshold_reached=False)
        assert await client.get(RedisKeys.checkpoint(user_name, session_id)) == "1"
        assert await client.get(RedisKeys.last_processed(user_name, session_id)) == "41"
    finally:
        await _cleanup_scope(
            client,
            user_name=user_name,
            project_id=project_id,
            session_ids={session_id},
        )
        await manager.close()


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_expired_commit_token_allows_explicit_replay(monkeypatch):
    """After the idempotency TTL expires, a replay is a new checkpoint commit."""

    monkeypatch.setattr(checkpoint_module, "CHECKPOINT_COMMIT_TTL_SECONDS", 1)
    user_name = f"checkpoint-expiry-{uuid4()}"
    project_id = f"project-{uuid4()}"
    session_id = f"session-{uuid4()}"
    manager, client = await _connect()
    batch = _batch(
        user_name=user_name,
        project_id=project_id,
        session_id=session_id,
        message_ids=[51],
        checkpoint_interval=10,
    )
    token_key = RedisKeys.checkpoint_commit(
        user_name,
        session_id,
        checkpoint_module._checkpoint_commit_token(batch),
    )
    try:
        first = await commit_ingestion_checkpoint(client, batch)
        assert first.count_before_reset == 1
        assert 0 < await client.ttl(token_key) <= 1

        await asyncio.sleep(1.2)
        assert await client.exists(token_key) == 0

        replay = await commit_ingestion_checkpoint(
            client,
            _batch(
                user_name=user_name,
                project_id=project_id,
                session_id=session_id,
                message_ids=[51],
            ),
        )
        assert replay == CheckpointCommit(count_before_reset=2, threshold_reached=False)
        assert await client.get(RedisKeys.checkpoint(user_name, session_id)) == "2"
    finally:
        await _cleanup_scope(
            client,
            user_name=user_name,
            project_id=project_id,
            session_ids={session_id},
        )
        await manager.close()


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_session_and_project_cursors_are_monotonic():
    """Out-of-order workers cannot move session or project cursors backwards."""

    user_name = f"checkpoint-order-{uuid4()}"
    project_id = f"project-{uuid4()}"
    first_session = f"session-{uuid4()}"
    second_session = f"session-{uuid4()}"
    manager, client = await _connect()
    try:
        await commit_ingestion_checkpoint(
            client,
            _batch(
                user_name=user_name,
                project_id=project_id,
                session_id=first_session,
                message_ids=[100],
                checkpoint_interval=100,
            ),
        )
        await commit_ingestion_checkpoint(
            client,
            _batch(
                user_name=user_name,
                project_id=project_id,
                session_id=first_session,
                message_ids=[10],
                checkpoint_interval=100,
            ),
        )
        await commit_ingestion_checkpoint(
            client,
            _batch(
                user_name=user_name,
                project_id=project_id,
                session_id=second_session,
                message_ids=[50],
                checkpoint_interval=100,
            ),
        )

        assert (
            await client.get(RedisKeys.last_processed(user_name, first_session))
            == "100"
        )
        assert (
            await client.get(RedisKeys.last_processed(user_name, second_session))
            == "50"
        )
        assert (
            await client.get(RedisKeys.project_last_processed(user_name, project_id))
            == "100"
        )
    finally:
        await _cleanup_scope(
            client,
            user_name=user_name,
            project_id=project_id,
            session_ids={first_session, second_session},
        )
        await manager.close()
