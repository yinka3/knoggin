import json

import pytest

from common.schema.contracts import BatchResult
from infrastructure.job.base import JobContext
from infrastructure.redis_client import RedisKeys
from knoggin_server.ingestion.jobs.dlq_job import DLQReplayJob
from tests.fixtures.fakes import FakeGraphClient, FakeRedis


class ProcessorWithoutGraphClient:
    graph_client = None


class ProcessorWithGraphClient:
    graph_client = object()


class RecordingProcessor:
    def __init__(self):
        self.graph_client = FakeGraphClient()
        self.run_calls = []

    async def run(self, messages, session_text, *, session_id):
        self.run_calls.append((messages, session_text, session_id))
        result = BatchResult()
        result.set_scope("ada", session_id, "project-1")
        return result


async def successful_write_to_graph(result):
    return True, None


@pytest.mark.ingestion
@pytest.mark.no_network
def test_dlq_job_requires_processor_graph_client():
    with pytest.raises(ValueError, match="requires a BatchProcessor with graph_client"):
        DLQReplayJob(
            entities=object(),
            processor=ProcessorWithoutGraphClient(),
            write_to_graph=lambda result: (True, None),
            redis_client=FakeRedis(),
        )


@pytest.mark.ingestion
@pytest.mark.no_network
def test_dlq_job_accepts_processor_with_graph_client():
    job = DLQReplayJob(
        entities=object(),
        processor=ProcessorWithGraphClient(),
        write_to_graph=lambda result: (True, None),
        redis_client=FakeRedis(),
    )

    assert job.name == "dlq_auto_replay"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_dlq_job_parks_entries_missing_session_id():
    redis = FakeRedis()
    job = DLQReplayJob(
        entities=object(),
        processor=RecordingProcessor(),
        write_to_graph=successful_write_to_graph,
        redis_client=redis,
    )
    ctx = JobContext(user_name="ada", project_id="project-1")
    dlq_key = RedisKeys.dlq("ada", "project-1")
    parked_key = RedisKeys.dlq_parked("ada", "project-1")

    await redis.rpush(
        dlq_key,
        json.dumps(
            {
                "error": "TimeoutError",
                "attempt": 1,
                "stage": "processing",
                "messages": [{"id": 1, "message": "hello"}],
            }
        ),
    )

    result = await job.execute(ctx)

    assert result.summary == "Processed 1: 0 retried, 1 parked"
    assert await redis.llen(dlq_key) == 0
    assert await redis.llen(parked_key) == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_dlq_processing_replay_uses_entry_session_id():
    redis = FakeRedis()
    processor = RecordingProcessor()
    job = DLQReplayJob(
        entities=object(),
        processor=processor,
        write_to_graph=successful_write_to_graph,
        redis_client=redis,
    )
    ctx = JobContext(user_name="ada", project_id="project-1")
    dlq_key = RedisKeys.dlq("ada", "project-1")
    message = {"id": 1, "message": "hello"}

    await redis.rpush(
        dlq_key,
        json.dumps(
            {
                "error": "TimeoutError",
                "attempt": 1,
                "stage": "processing",
                "messages": [message],
                "session_text": "[USER]: hello",
                "user_name": "ada",
                "session_id": "session-1",
                "project_id": "project-1",
            }
        ),
    )

    result = await job.execute(ctx)

    assert result.summary == "Processed 1: 1 retried, 0 parked"
    assert processor.run_calls == [([message], "[USER]: hello", "session-1")]
