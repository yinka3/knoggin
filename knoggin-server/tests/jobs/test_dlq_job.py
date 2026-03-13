"""Tests for jobs/dlq.py — DLQ replay with stage-aware retry. All dependencies mocked."""

import json
import time
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from src.jobs.dlq import DLQReplayJob
from src.jobs.base import JobContext
from src.common.schema.dtypes import BatchResult, EntityPair, MessageConnections


def make_dlq_entry(stage="processing", error="TimeoutError", attempt=1, messages=None, session_text="", batch_result=None):
    if messages is None:
        messages = [{"id": 1, "message": "test"}]
    entry = {
        "timestamp": time.time(),
        "error": error,
        "attempt": attempt,
        "stage": stage,
        "batch_size": len(messages),
        "messages": messages,
    }
    if stage == "processing":
        entry["session_text"] = session_text
    elif stage == "graph_write" and batch_result:
        entry["batch_result"] = batch_result
    return entry


def make_dlq_job(**overrides):
    resolver = MagicMock()
    resolver.entity_profiles = {1: {"canonical_name": "Alice"}, 2: {"canonical_name": "Bob"}}

    processor = MagicMock()
    processor.run = AsyncMock(return_value=BatchResult(success=True))

    write_to_graph = AsyncMock(return_value=(True, None))

    defaults = dict(
        ent_resolver=resolver,
        processor=processor,
        write_to_graph=write_to_graph,
        interval=60,
        batch_size=50,
        max_attempts=3,
    )
    defaults.update(overrides)
    job = DLQReplayJob(**defaults)
    return job, resolver, processor, write_to_graph


def make_ctx(dlq_items=None):
    redis = MagicMock()

    encoded = [json.dumps(item) for item in (dlq_items or [])]
    redis.llen = AsyncMock(return_value=len(encoded))
    redis.lpop = AsyncMock(side_effect=encoded + [None])
    redis.rpush = AsyncMock()
    redis.set = AsyncMock()
    redis.get = AsyncMock(return_value=None)

    return JobContext(
        user_name="Yinka",
        session_id="test-session",
        redis=redis,
    )


class TestDLQReplayJob:

    @pytest.mark.asyncio
    async def test_first_run_seeds_and_returns_false(self):
        job, *_ = make_dlq_job()
        redis = MagicMock()
        redis.get = AsyncMock(return_value=None)
        redis.set = AsyncMock()
        ctx = JobContext(user_name="Yinka", session_id="s1", redis=redis)

        result = await job.should_run(ctx)
        assert result is False
        redis.set.assert_called_once()

    @pytest.mark.asyncio
    async def test_interval_elapsed(self):
        job, *_ = make_dlq_job(interval=60)
        redis = MagicMock()
        redis.get = AsyncMock(return_value=str(time.time() - 120))
        ctx = JobContext(user_name="Yinka", session_id="s1", redis=redis)

        assert await job.should_run(ctx) is True

    @pytest.mark.asyncio
    async def test_interval_not_elapsed(self):
        job, *_ = make_dlq_job(interval=60)
        redis = MagicMock()
        redis.get = AsyncMock(return_value=str(time.time() - 10))
        ctx = JobContext(user_name="Yinka", session_id="s1", redis=redis)

        assert await job.should_run(ctx) is False

    def test_network_error_is_transient(self):
        job, *_ = make_dlq_job()
        assert job._is_transient("ConnectionError: server refused") is True

    def test_memgraph_error_is_transient(self):
        job, *_ = make_dlq_job()
        assert job._is_transient("Cannot get shared access to storage") is True

    def test_rate_limit_is_transient(self):
        job, *_ = make_dlq_job()
        assert job._is_transient("429 Too Many Requests") is True

    def test_key_error_is_not_transient(self):
        job, *_ = make_dlq_job()
        assert job._is_transient("KeyError: 'canonical_name'") is False

    def test_transient_check_case_insensitive(self):
        job, *_ = make_dlq_job()
        assert job._is_transient("TIMEOUTERROR") is True

    def test_validate_all_valid(self):
        job, resolver, *_ = make_dlq_job()
        resolver.entity_profiles = {1: {}, 2: {}, 3: {}}

        result = BatchResult(
            entity_ids=[1, 2, 3],
            new_entity_ids={2},
            alias_updated_ids={3},
        )
        validated = job._validate_batch_result(result)
        assert validated.entity_ids == [1, 2, 3]

    def test_validate_stale_entities_filtered(self):
        job, resolver, *_ = make_dlq_job()
        resolver.entity_profiles = {1: {}}

        result = BatchResult(
            entity_ids=[1, 2, 3],
            new_entity_ids={2, 3},
            alias_updated_ids={1, 3},
        )
        validated = job._validate_batch_result(result)
        assert validated.entity_ids == [1]
        assert validated.new_entity_ids == set()
        assert validated.alias_updated_ids == {1}

    def test_validate_all_stale(self):
        job, resolver, *_ = make_dlq_job()
        resolver.entity_profiles = {}

        result = BatchResult(entity_ids=[1, 2, 3])
        validated = job._validate_batch_result(result)
        assert validated.entity_ids == []

    @pytest.mark.asyncio
    async def test_retry_graph_write_success(self):
        job, resolver, _, write_to_graph = make_dlq_job()
        resolver.entity_profiles = {1: {}, 2: {}}

        entry = make_dlq_entry(
            stage="graph_write",
            batch_result=BatchResult(
                entity_ids=[1, 2],
                extraction_result=[
                    MessageConnections(message_id=1, entity_pairs=[
                        EntityPair(entity_a="A", entity_b="B", confidence=0.9)
                    ])
                ],
            ).to_dict(),
        )
        ctx = make_ctx()

        with patch("src.jobs.dlq.emit", new_callable=AsyncMock):
            result = await job._retry_graph_write(entry, ctx)

        assert result is True
        write_to_graph.assert_called_once()

    @pytest.mark.asyncio
    async def test_retry_graph_write_all_stale(self):
        job, resolver, _, write_to_graph = make_dlq_job()
        resolver.entity_profiles = {}

        entry = make_dlq_entry(
            stage="graph_write",
            batch_result=BatchResult(entity_ids=[99, 100]).to_dict(),
        )
        ctx = make_ctx()

        with patch("src.jobs.dlq.emit", new_callable=AsyncMock):
            result = await job._retry_graph_write(entry, ctx)

        assert result is True
        write_to_graph.assert_not_called()

    @pytest.mark.asyncio
    async def test_retry_processing_success(self):
        job, _, processor, write_to_graph = make_dlq_job()
        processor.run = AsyncMock(return_value=BatchResult(
            success=True,
            entity_ids=[1],
            extraction_result=[
                MessageConnections(message_id=1, entity_pairs=[
                    EntityPair(entity_a="A", entity_b="B", confidence=0.9)
                ])
            ],
        ))

        entry = make_dlq_entry(
            stage="processing",
            messages=[{"id": 1, "message": "test msg"}],
            session_text="[USER]: test msg",
        )
        ctx = make_ctx()

        with patch("src.jobs.dlq.emit", new_callable=AsyncMock):
            result = await job._retry_processing(entry, ctx)

        assert result is True
        processor.run.assert_called_once()
        write_to_graph.assert_called_once()

    @pytest.mark.asyncio
    async def test_retry_processing_no_messages_skips(self):
        job, _, processor, _ = make_dlq_job()

        entry = make_dlq_entry(stage="processing", messages=[])
        ctx = make_ctx()

        with patch("src.jobs.dlq.emit", new_callable=AsyncMock):
            result = await job._retry_processing(entry, ctx)

        assert result is True
        processor.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_retry_processing_fails(self):
        job, _, processor, _ = make_dlq_job()
        processor.run = AsyncMock(return_value=BatchResult(success=False, error="NLP_CRASH"))

        entry = make_dlq_entry(stage="processing")
        ctx = make_ctx()

        with patch("src.jobs.dlq.emit", new_callable=AsyncMock):
            result = await job._retry_processing(entry, ctx)

        assert result is False

    @pytest.mark.asyncio
    async def test_empty_dlq(self):
        job, *_ = make_dlq_job()
        ctx = make_ctx(dlq_items=[])

        with patch("src.jobs.dlq.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert result.success is True
        assert "empty" in result.summary.lower()

    @pytest.mark.asyncio
    async def test_transient_graph_write_retried(self):
        job, resolver, _, write_to_graph = make_dlq_job()
        resolver.entity_profiles = {1: {}}

        entry = make_dlq_entry(
            stage="graph_write",
            error="Connection refused",
            attempt=1,
            batch_result=BatchResult(entity_ids=[1]).to_dict(),
        )
        ctx = make_ctx(dlq_items=[entry])

        with patch("src.jobs.dlq.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert "1 retried" in result.summary

    @pytest.mark.asyncio
    async def test_transient_processing_retried(self):
        job, _, processor, write_to_graph = make_dlq_job()
        processor.run = AsyncMock(return_value=BatchResult(success=True))

        entry = make_dlq_entry(
            stage="processing",
            error="TimeoutError",
            attempt=1,
            messages=[{"id": 1, "message": "test"}],
            session_text="[USER]: test",
        )
        ctx = make_ctx(dlq_items=[entry])

        with patch("src.jobs.dlq.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert "1 retried" in result.summary

    @pytest.mark.asyncio
    async def test_retry_fails_requeued_with_incremented_attempt(self):
        job, _, processor, _ = make_dlq_job()
        processor.run = AsyncMock(return_value=BatchResult(success=False, error="still broken"))

        entry = make_dlq_entry(
            stage="processing",
            error="TimeoutError",
            attempt=1,
        )
        ctx = make_ctx(dlq_items=[entry])

        with patch("src.jobs.dlq.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        rpush_calls = ctx.redis.rpush.call_args_list
        requeued = [c for c in rpush_calls if "dlq:" in str(c[0][0]) and "parked" not in str(c[0][0])]
        assert len(requeued) >= 1
        requeued_entry = json.loads(requeued[0][0][1])
        assert requeued_entry["attempt"] == 2

    @pytest.mark.asyncio
    async def test_max_attempts_parks(self):
        job, *_ = make_dlq_job(max_attempts=2)

        entry = make_dlq_entry(
            stage="processing",
            error="TimeoutError",
            attempt=2,
        )
        ctx = make_ctx(dlq_items=[entry])

        with patch("src.jobs.dlq.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert "1 parked" in result.summary

    @pytest.mark.asyncio
    async def test_non_transient_parks_immediately(self):
        job, *_ = make_dlq_job()

        entry = make_dlq_entry(
            stage="processing",
            error="KeyError: 'canonical_name'",
            attempt=1,
        )
        ctx = make_ctx(dlq_items=[entry])

        with patch("src.jobs.dlq.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert "1 parked" in result.summary

    @pytest.mark.asyncio
    async def test_corrupt_json_parked(self):
        job, *_ = make_dlq_job()

        redis = MagicMock()
        redis.llen = AsyncMock(return_value=1)
        redis.lpop = AsyncMock(side_effect=["not valid json{{{", None])
        redis.rpush = AsyncMock()
        redis.set = AsyncMock()
        ctx = JobContext(user_name="Yinka", session_id="s1", redis=redis)

        with patch("src.jobs.dlq.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert result.success is True
        assert "1 parked" in result.summary

    @pytest.mark.asyncio
    async def test_circuit_breaker_halts_after_consecutive_failures(self):
        job, _, processor, _ = make_dlq_job()
        processor.run = AsyncMock(return_value=BatchResult(success=False, error="broken"))

        entries = [
            make_dlq_entry(stage="processing", error="TimeoutError", attempt=1)
            for _ in range(10)
        ]
        ctx = make_ctx(dlq_items=entries)

        with patch("src.jobs.dlq.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        processed_count = int(result.summary.split("Processed ")[1].split(":")[0])
        assert processed_count <= 6

    def test_updates_all(self):
        job, *_ = make_dlq_job()
        job.update_settings(interval=120, batch_size=100, max_attempts=5)
        assert job.interval == 120
        assert job.batch_size == 100
        assert job.max_attempts == 5

    def test_partial_update(self):
        job, *_ = make_dlq_job(interval=60, batch_size=50)
        job.update_settings(interval=120)
        assert job.interval == 120
        assert job.batch_size == 50