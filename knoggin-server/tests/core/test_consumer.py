"""Tests for src/core/batch_consumer.py — BatchConsumer buffer drain and lifecycle."""

import asyncio
import json
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from src.core.batch_consumer import BatchConsumer
from src.core.batch_processor import BatchResult
from src.common.schema.dtypes import EntityPair, MessageConnections




def make_msg(msg_id: int, text: str = "test message") -> dict:
    return {"id": msg_id, "message": text, "role": "user", "timestamp": "2025-01-01T00:00:00"}


def make_raw_msgs(msg_ids: list[int]) -> list[bytes]:
    return [json.dumps(make_msg(mid)).encode() for mid in msg_ids]


def make_consumer(**overrides) -> tuple:
    store = MagicMock()
    store.save_message_logs.return_value = True

    processor = MagicMock()
    processor.run = AsyncMock(return_value=BatchResult())
    processor.move_to_dead_letter = AsyncMock(return_value=True)

    redis = MagicMock()
    redis.llen = AsyncMock(return_value=0)
    redis.lrange = AsyncMock(return_value=[])
    redis.ltrim = AsyncMock()
    redis.incrby = AsyncMock(return_value=0)
    redis.set = AsyncMock()
    redis.get = AsyncMock(return_value=None)

    get_session_ctx = AsyncMock(return_value=[
        {"role_label": "USER", "content": "test context"},
    ])
    run_session_jobs = AsyncMock()
    write_to_graph = AsyncMock(return_value=(True, None))

    defaults = dict(
        user_name="Yinka",
        session_id="test-session",
        store=store,
        processor=processor,
        redis=redis,
        get_session_context=get_session_ctx,
        run_session_jobs=run_session_jobs,
        write_to_graph=write_to_graph,
        batch_size=4,
        batch_timeout=300.0,
        checkpoint_interval=8,
        session_window=6,
    )
    defaults.update(overrides)

    consumer = BatchConsumer(**defaults)
    return consumer, store, processor, redis, run_session_jobs, write_to_graph


class TestKeyPatterns:

    def test_buffer_key(self):
        consumer, *_ = make_consumer()
        assert "buffer" in consumer._buffer_key
        assert "Yinka" in consumer._buffer_key
        assert "test-session" in consumer._buffer_key

    def test_checkpoint_key(self):
        consumer, *_ = make_consumer()
        assert "checkpoint" in consumer._checkpoint_key
        assert "Yinka" in consumer._checkpoint_key


class TestFormatSessionText:

    def test_formats_conversation(self):
        consumer, *_ = make_consumer()
        conversation = [
            {"role_label": "USER", "content": "Hey what's up"},
            {"role_label": "AGENT", "content": "Not much, how can I help?"},
        ]
        result = consumer._format_session_text(conversation)
        assert "[USER]: Hey what's up" in result
        assert "[AGENT]: Not much, how can I help?" in result

    def test_empty_conversation(self):
        consumer, *_ = make_consumer()
        assert consumer._format_session_text([]) == ""


class TestDrainBufferSuccess:

    @pytest.mark.asyncio
    async def test_empty_buffer(self):
        consumer, _, processor, redis, _, _ = make_consumer()
        redis.llen = AsyncMock(return_value=0)
        redis.lrange = AsyncMock(return_value=[])

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        processor.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_full_batch_processed(self):
        consumer, store, processor, redis, _, _ = make_consumer()
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])
        redis.incrby = AsyncMock(return_value=4)

        processor.run = AsyncMock(return_value=BatchResult(
            message_embeddings={1: [0.1], 2: [0.2], 3: [0.3], 4: [0.4]},
        ))

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        processor.run.assert_called_once()
        redis.ltrim.assert_called()
        set_calls = [c for c in redis.set.call_args_list if "last_processed" in str(c)]
        assert len(set_calls) >= 1

    @pytest.mark.asyncio
    async def test_partial_flush_drains_below_batch_size(self):
        consumer, _, processor, redis, _, _ = make_consumer()
        raw = make_raw_msgs([1, 2])  # only 2 msgs, batch_size=4

        redis.llen = AsyncMock(return_value=2)
        redis.lrange = AsyncMock(side_effect=[raw, []])
        redis.incrby = AsyncMock(return_value=2)

        processor.run = AsyncMock(return_value=BatchResult(
            message_embeddings={1: [0.1], 2: [0.2]},
        ))

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=True)

        processor.run.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_flush_below_batch_size(self):
        consumer, _, processor, redis, _, _ = make_consumer()

        redis.llen = AsyncMock(return_value=2)  # < batch_size of 4

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        processor.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_message_logs_saved(self):
        consumer, store, processor, redis, _, _ = make_consumer()
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])
        redis.incrby = AsyncMock(return_value=4)

        processor.run = AsyncMock(return_value=BatchResult(
            message_embeddings={1: [0.1], 2: [0.2], 3: [0.3], 4: [0.4]},
        ))

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        store.save_message_logs.assert_called_once()

    @pytest.mark.asyncio
    async def test_drains_multiple_batches(self):
        consumer, store, processor, redis, _, _ = make_consumer()

        batch_1 = make_raw_msgs([1, 2, 3, 4])
        batch_2 = make_raw_msgs([5, 6, 7, 8])

        redis.llen = AsyncMock(side_effect=[8, 4, 0])
        redis.lrange = AsyncMock(side_effect=[batch_1, batch_2, []])
        redis.incrby = AsyncMock(return_value=4)

        processor.run = AsyncMock(return_value=BatchResult(
            message_embeddings={i: [0.1] for i in range(1, 9)},
        ))

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        assert processor.run.call_count == 2
        assert redis.ltrim.call_count == 2

    @pytest.mark.asyncio
    async def test_no_extraction_result_skips_graph_write(self):
        consumer, store, processor, redis, _, write_to_graph = make_consumer()
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])
        redis.incrby = AsyncMock(return_value=4)

        processor.run = AsyncMock(return_value=BatchResult(
            message_embeddings={i: [0.1] for i in [1, 2, 3, 4]},
            extraction_result=None,
        ))

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        write_to_graph.assert_not_called()
        store.save_message_logs.assert_called_once()
        redis.ltrim.assert_called()

    @pytest.mark.asyncio
    async def test_empty_extraction_result_skips_graph_write(self):
        consumer, _, processor, redis, _, write_to_graph = make_consumer()
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])
        redis.incrby = AsyncMock(return_value=4)

        processor.run = AsyncMock(return_value=BatchResult(
            message_embeddings={i: [0.1] for i in [1, 2, 3, 4]},
            extraction_result=[],
        ))

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        write_to_graph.assert_not_called()


class TestDrainBufferFailure:

    @pytest.mark.asyncio
    async def test_processor_failure_sends_to_dlq(self):
        consumer, _, processor, redis, _, _ = make_consumer()
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])

        processor.run = AsyncMock(return_value=BatchResult(
            success=False, error="NLP_CRASH"
        ))

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        processor.move_to_dead_letter.assert_called_once()
        call_kwargs = processor.move_to_dead_letter.call_args
        assert call_kwargs[1]["stage"] == "processing"

    @pytest.mark.asyncio
    async def test_processor_failure_dlq_failure_breaks(self):
        consumer, _, processor, redis, _, _ = make_consumer()
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])

        processor.run = AsyncMock(return_value=BatchResult(
            success=False, error="NLP_CRASH"
        ))
        processor.move_to_dead_letter = AsyncMock(return_value=False)

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        redis.ltrim.assert_not_called()

    @pytest.mark.asyncio
    async def test_graph_write_failure_sends_to_dlq(self):
        consumer, _, processor, redis, _, write_to_graph = make_consumer()
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])
        redis.incrby = AsyncMock(return_value=4)

        result = BatchResult(
            message_embeddings={1: [0.1], 2: [0.2], 3: [0.3], 4: [0.4]},
            extraction_result=[
                MessageConnections(message_id=1, entity_pairs=[
                    EntityPair(entity_a="A", entity_b="B", confidence=0.9)
                ])
            ],
        )
        processor.run = AsyncMock(return_value=result)
        write_to_graph.return_value = (False, "MEMGRAPH_DOWN")

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        processor.move_to_dead_letter.assert_called_once()
        call_kwargs = processor.move_to_dead_letter.call_args
        assert call_kwargs[1]["stage"] == "graph_write"
        assert call_kwargs[1]["batch_result"] is result

    @pytest.mark.asyncio
    async def test_graph_write_timeout_sends_to_dlq(self):
        consumer, _, processor, redis, _, write_to_graph = make_consumer()
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])
        redis.incrby = AsyncMock(return_value=4)

        result = BatchResult(
            message_embeddings={1: [0.1], 2: [0.2], 3: [0.3], 4: [0.4]},
            extraction_result=[
                MessageConnections(message_id=1, entity_pairs=[
                    EntityPair(entity_a="A", entity_b="B", confidence=0.9)
                ])
            ],
        )
        processor.run = AsyncMock(return_value=result)

        # Must match write_to_graph(batch_result=...) signature
        async def hang(batch_result):
            await asyncio.sleep(999)
        write_to_graph.side_effect = hang
        consumer.batch_timeout = 0.1

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        processor.move_to_dead_letter.assert_called_once()
        call_kwargs = processor.move_to_dead_letter.call_args
        assert "TIMEOUT" in call_kwargs[0][1] or "TIMEOUT" in str(call_kwargs[1].get("error", ""))

    @pytest.mark.asyncio
    async def test_save_failure_does_not_break_drain(self):
        consumer, store, processor, redis, _, write_to_graph = make_consumer()
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])
        redis.incrby = AsyncMock(return_value=4)

        result = BatchResult(
            message_embeddings={i: [0.1] for i in [1, 2, 3, 4]},
            extraction_result=[
                MessageConnections(message_id=1, entity_pairs=[
                    EntityPair(entity_a="A", entity_b="B", confidence=0.9)
                ])
            ],
        )
        processor.run = AsyncMock(return_value=result)
        store.save_message_logs.side_effect = Exception("disk full")

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        write_to_graph.assert_not_called()
        processor.move_to_dead_letter.assert_called_once()
        redis.ltrim.assert_called_once()

    @pytest.mark.asyncio
    async def test_malformed_json_raises(self):
        consumer, _, processor, redis, _, _ = make_consumer()

        redis.llen = AsyncMock(return_value=1)
        redis.lrange = AsyncMock(return_value=[b"not valid json{{{"])

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            with pytest.raises(json.JSONDecodeError):
                await consumer._drain_buffer(flush_partial=True)

        processor.run.assert_not_called()


class TestCheckpoint:

    @pytest.mark.asyncio
    async def test_below_interval_no_jobs(self):
        consumer, _, processor, redis, run_jobs, _ = make_consumer(checkpoint_interval=8)
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])
        redis.incrby = AsyncMock(return_value=4)  # 4 < 8

        processor.run = AsyncMock(return_value=BatchResult(
            message_embeddings={i: [0.1] for i in [1, 2, 3, 4]},
        ))

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        run_jobs.assert_not_called()

    @pytest.mark.asyncio
    async def test_reaches_interval_triggers_jobs(self):
        consumer, _, processor, redis, run_jobs, _ = make_consumer(checkpoint_interval=4)
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])
        redis.incrby = AsyncMock(return_value=4)  # 4 >= 4

        processor.run = AsyncMock(return_value=BatchResult(
            message_embeddings={i: [0.1] for i in [1, 2, 3, 4]},
        ))

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        run_jobs.assert_called_once()
        reset_calls = [c for c in redis.set.call_args_list if "checkpoint" in str(c[0][0])]
        assert len(reset_calls) >= 1

    @pytest.mark.asyncio
    async def test_checkpoint_counter_reset_to_zero(self):
        consumer, _, processor, redis, run_jobs, _ = make_consumer(checkpoint_interval=4)
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])
        redis.incrby = AsyncMock(return_value=4)

        processor.run = AsyncMock(return_value=BatchResult(
            message_embeddings={i: [0.1] for i in [1, 2, 3, 4]},
        ))

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        checkpoint_reset_calls = [
            c for c in redis.set.call_args_list
            if "checkpoint" in str(c[0][0]) and c[0][1] == 0
        ]
        assert len(checkpoint_reset_calls) == 1

    @pytest.mark.asyncio
    async def test_below_checkpoint_no_reset(self):
        consumer, _, processor, redis, run_jobs, _ = make_consumer(checkpoint_interval=8)
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])
        redis.incrby = AsyncMock(return_value=4)  # 4 < 8

        processor.run = AsyncMock(return_value=BatchResult(
            message_embeddings={i: [0.1] for i in [1, 2, 3, 4]},
        ))

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        run_jobs.assert_not_called()
        checkpoint_reset_calls = [
            c for c in redis.set.call_args_list
            if "checkpoint" in str(c[0][0]) and c[0][1] == 0
        ]
        assert len(checkpoint_reset_calls) == 0


class TestLifecycle:

    @pytest.mark.asyncio
    async def test_start_creates_task(self):
        consumer, *_ = make_consumer()

        with patch("src.core.batch_consumer.emit_sync"):
            consumer.start()

        assert consumer._task is not None
        assert not consumer._shutdown_requested

        consumer._task.cancel()
        try:
            await consumer._task
        except asyncio.CancelledError:
            pass

    def test_signal_sets_wake_event(self):
        consumer, *_ = make_consumer()
        assert not consumer._wake_event.is_set()

        consumer.signal()
        assert consumer._wake_event.is_set()

    @pytest.mark.asyncio
    async def test_stop_runs_final_drain_and_jobs(self):
        consumer, _, processor, redis, run_jobs, _ = make_consumer()

        redis.lrange = AsyncMock(return_value=[])
        redis.llen = AsyncMock(return_value=0)

        with patch("src.core.batch_consumer.emit_sync"):
            consumer.start()

        await asyncio.sleep(0.05)

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer.stop()

        assert consumer._task is None
        assert consumer._shutdown_requested is True
        run_jobs.assert_called()

    @pytest.mark.asyncio
    async def test_start_twice_is_noop(self):
        consumer, *_ = make_consumer()

        with patch("src.core.batch_consumer.emit_sync"):
            consumer.start()

        first_task = consumer._task
        assert first_task is not None

        with patch("src.core.batch_consumer.emit_sync"):
            consumer.start()

        assert consumer._task is first_task

        consumer._task.cancel()
        try:
            await consumer._task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_stop_when_not_running(self):
        consumer, *_ = make_consumer()
        assert consumer._task is None

        with patch("src.core.batch_consumer.emit", new_callable=AsyncMock):
            await consumer.stop()

        assert consumer._task is None


class TestUpdateSettings:

    def test_updates_all_fields(self):
        consumer, *_ = make_consumer()

        consumer.update_ingestion_settings(
            batch_size=16,
            batch_timeout=600.0,
            checkpoint_interval=32,
            session_window=24,
        )

        assert consumer.batch_size == 16
        assert consumer.batch_timeout == 600.0
        assert consumer.checkpoint_interval == 32
        assert consumer.session_window == 24

    def test_partial_update(self):
        consumer, *_ = make_consumer()
        original_timeout = consumer.batch_timeout

        consumer.update_ingestion_settings(batch_size=16)

        assert consumer.batch_size == 16
        assert consumer.batch_timeout == original_timeout

    def test_none_values_ignored(self):
        consumer, *_ = make_consumer()
        original = consumer.batch_size

        consumer.update_ingestion_settings(batch_size=None)
        assert consumer.batch_size == original