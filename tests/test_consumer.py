"""Tests for main/consumer.py — BatchConsumer buffer drain and lifecycle.

This module contains unit tests for the BatchConsumer class, focusing on its
buffer draining logic, lifecycle management (start/stop), and ingestion settings.
All dependencies (Redis, Memgraph, LLM) are mocked to ensure deterministic behavior.
"""

import asyncio
import json
import pytest
from unittest.mock import MagicMock, AsyncMock, patch, PropertyMock

from main.consumer import BatchConsumer
from main.processor import BatchResult
from shared.models.schema.dtypes import EntityPair, MessageConnections


# --- Helpers ---

def make_msg(msg_id: int, text: str = "test message") -> dict:
    """Create a mock message dictionary for testing."""
    return {"id": msg_id, "message": text, "role": "user", "timestamp": "2025-01-01T00:00:00"}


def make_raw_msgs(msg_ids: list[int]) -> list[bytes]:
    """Simulate Redis lrange returning JSON-encoded message dicts."""
    return [json.dumps(make_msg(mid)).encode() for mid in msg_ids]


def make_consumer(**overrides) -> tuple:
    """Build a BatchConsumer with fully mocked dependencies."""
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
    """Tests for BatchConsumer Redis key pattern generation."""

    def test_buffer_key(self):
        """Verify the buffer key includes user name and session ID."""
        consumer, *_ = make_consumer()
        assert "buffer" in consumer._buffer_key
        assert "Yinka" in consumer._buffer_key
        assert "test-session" in consumer._buffer_key

    def test_checkpoint_key(self):
        """Verify the checkpoint key includes user name."""
        consumer, *_ = make_consumer()
        assert "checkpoint" in consumer._checkpoint_key
        assert "Yinka" in consumer._checkpoint_key


class TestFormatSessionText:
    """Tests for the _format_session_text helper method."""

    def test_formats_conversation(self):
        """Verify messages are formatted correctly into a single string."""
        consumer, *_ = make_consumer()
        conversation = [
            {"role_label": "USER", "content": "Hey what's up"},
            {"role_label": "AGENT", "content": "Not much, how can I help?"},
        ]
        result = consumer._format_session_text(conversation)
        assert "[USER]: Hey what's up" in result
        assert "[AGENT]: Not much, how can I help?" in result

    def test_empty_conversation(self):
        """Return an empty string for an empty conversation."""
        consumer, *_ = make_consumer()
        assert consumer._format_session_text([]) == ""


class TestDrainBufferSuccess:
    """Tests for successful _drain_buffer execution paths."""

    @pytest.mark.asyncio
    async def test_empty_buffer(self):
        """Should not call the processor if the buffer is empty."""
        consumer, _, processor, redis, _, _ = make_consumer()
        redis.llen = AsyncMock(return_value=0)
        redis.lrange = AsyncMock(return_value=[])

        with patch("main.consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        processor.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_full_batch_processed(self):
        """Buffer has >= batch_size messages — should drain one batch."""
        consumer, store, processor, redis, _, _ = make_consumer()
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])
        redis.incrby = AsyncMock(return_value=4)

        processor.run = AsyncMock(return_value=BatchResult(
            message_embeddings={1: [0.1], 2: [0.2], 3: [0.3], 4: [0.4]},
        ))

        with patch("main.consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        processor.run.assert_called_once()
        redis.ltrim.assert_called()
        # last_processed should be set to max msg id
        set_calls = [c for c in redis.set.call_args_list if "last_processed" in str(c)]
        assert len(set_calls) >= 1

    @pytest.mark.asyncio
    async def test_partial_flush_drains_below_batch_size(self):
        """Timeout flush: drains even when buffer < batch_size."""
        consumer, _, processor, redis, _, _ = make_consumer()
        raw = make_raw_msgs([1, 2])  # only 2 msgs, batch_size=4

        redis.llen = AsyncMock(return_value=2)
        redis.lrange = AsyncMock(side_effect=[raw, []])
        redis.incrby = AsyncMock(return_value=2)

        processor.run = AsyncMock(return_value=BatchResult(
            message_embeddings={1: [0.1], 2: [0.2]},
        ))

        with patch("main.consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=True)

        processor.run.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_flush_below_batch_size(self):
        """Without flush_partial, buffer < batch_size should NOT drain."""
        consumer, _, processor, redis, _, _ = make_consumer()

        redis.llen = AsyncMock(return_value=2)  # < batch_size of 4

        with patch("main.consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        processor.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_message_logs_saved(self):
        """After successful processing, message logs should be saved to store."""
        consumer, store, processor, redis, _, _ = make_consumer()
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])
        redis.incrby = AsyncMock(return_value=4)

        processor.run = AsyncMock(return_value=BatchResult(
            message_embeddings={1: [0.1], 2: [0.2], 3: [0.3], 4: [0.4]},
        ))

        with patch("main.consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        store.save_message_logs.assert_called_once()


class TestDrainBufferFailure:
    """Tests for failure paths in _drain_buffer."""

    @pytest.mark.asyncio
    async def test_processor_failure_sends_to_dlq(self):
        """Failure in processing stage should send current batch to DLQ."""
        consumer, _, processor, redis, _, _ = make_consumer()
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])

        processor.run = AsyncMock(return_value=BatchResult(
            success=False, error="NLP_CRASH"
        ))

        with patch("main.consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        processor.move_to_dead_letter.assert_called_once()
        call_kwargs = processor.move_to_dead_letter.call_args
        assert call_kwargs[1]["stage"] == "processing"

    @pytest.mark.asyncio
    async def test_processor_failure_dlq_failure_breaks(self):
        """If both processing AND DLQ write fail, the loop should break and preserve buffer."""
        consumer, _, processor, redis, _, _ = make_consumer()
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])

        processor.run = AsyncMock(return_value=BatchResult(
            success=False, error="NLP_CRASH"
        ))
        processor.move_to_dead_letter = AsyncMock(return_value=False)

        with patch("main.consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        # Buffer should NOT be trimmed — messages left for retry
        redis.ltrim.assert_not_called()

    @pytest.mark.asyncio
    async def test_graph_write_failure_sends_to_dlq(self):
        """Failure writing to graph should send batch and extraction results to DLQ."""
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

        with patch("main.consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        processor.move_to_dead_letter.assert_called_once()
        call_kwargs = processor.move_to_dead_letter.call_args
        assert call_kwargs[1]["stage"] == "graph_write"
        assert call_kwargs[1]["batch_result"] is result

    @pytest.mark.asyncio
    async def test_graph_write_timeout_sends_to_dlq(self):
        """A timeout during graph write should be caught and sent to DLQ."""
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
        # Simulate timeout — must accept the BatchResult argument
        async def hang(batch_result):
            await asyncio.sleep(999)
        write_to_graph.side_effect = hang
        consumer.batch_timeout = 0.1  # very short timeout for test

        with patch("main.consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        processor.move_to_dead_letter.assert_called_once()
        call_kwargs = processor.move_to_dead_letter.call_args
        assert "TIMEOUT" in call_kwargs[0][1] or "TIMEOUT" in str(call_kwargs[1].get("error", ""))


class TestCheckpoint:
    """Tests for checkpointing logic (triggering periodic session jobs)."""

    @pytest.mark.asyncio
    async def test_below_interval_no_jobs(self):
        """Don't trigger jobs if the checkpoint interval hasn't been reached."""
        consumer, _, processor, redis, run_jobs, _ = make_consumer(checkpoint_interval=8)
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])
        redis.incrby = AsyncMock(return_value=4)  # 4 < 8

        processor.run = AsyncMock(return_value=BatchResult(
            message_embeddings={i: [0.1] for i in [1, 2, 3, 4]},
        ))

        with patch("main.consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        run_jobs.assert_not_called()

    @pytest.mark.asyncio
    async def test_reaches_interval_triggers_jobs(self):
        """Trigger session jobs when the message counter hits the interval."""
        consumer, _, processor, redis, run_jobs, _ = make_consumer(checkpoint_interval=4)
        raw = make_raw_msgs([1, 2, 3, 4])

        redis.llen = AsyncMock(return_value=4)
        redis.lrange = AsyncMock(side_effect=[raw, []])
        redis.incrby = AsyncMock(return_value=4)  # 4 >= 4

        processor.run = AsyncMock(return_value=BatchResult(
            message_embeddings={i: [0.1] for i in [1, 2, 3, 4]},
        ))

        with patch("main.consumer.emit", new_callable=AsyncMock):
            await consumer._drain_buffer(flush_partial=False)

        run_jobs.assert_called_once()
        # Counter should be reset to 0
        reset_calls = [c for c in redis.set.call_args_list if "checkpoint" in str(c[0][0])]
        assert len(reset_calls) >= 1


class TestLifecycle:
    """Tests for the BatchConsumer lifecycle (start, signal, stop)."""

    @pytest.mark.asyncio
    async def test_start_creates_task(self):
        """Starting the consumer should initialize an asyncio task."""
        consumer, *_ = make_consumer()

        with patch("main.consumer.emit_sync"):
            consumer.start()

        assert consumer._task is not None
        assert not consumer._shutdown_requested

        # Cleanup
        consumer._task.cancel()
        try:
            await consumer._task
        except asyncio.CancelledError:
            pass

    def test_signal_sets_wake_event(self):
        """Signaling the consumer should set the wake event to trigger a drain."""
        consumer, *_ = make_consumer()
        assert not consumer._wake_event.is_set()

        consumer.signal()
        assert consumer._wake_event.is_set()

    @pytest.mark.asyncio
    async def test_stop_runs_final_drain_and_jobs(self):
        """Stopping should perform a final drain and run pending session jobs."""
        consumer, _, processor, redis, run_jobs, _ = make_consumer()

        redis.lrange = AsyncMock(return_value=[])
        redis.llen = AsyncMock(return_value=0)

        with patch("main.consumer.emit_sync"):
            consumer.start()

        # Let the loop enter _run
        await asyncio.sleep(0.05)

        with patch("main.consumer.emit", new_callable=AsyncMock):
            await consumer.stop()

        assert consumer._task is None
        assert consumer._shutdown_requested is True
        run_jobs.assert_called()


class TestUpdateSettings:
    """Tests for runtime ingestion settings updates."""

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
        assert consumer.batch_timeout == original_timeout  # unchanged

    def test_none_values_ignored(self):
        consumer, *_ = make_consumer()
        original = consumer.batch_size

        consumer.update_ingestion_settings(batch_size=None)
        assert consumer.batch_size == original