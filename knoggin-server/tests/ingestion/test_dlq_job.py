import pytest

from knoggin_server.ingestion.jobs.dlq_job import DLQReplayJob
from tests.fixtures.fakes import FakeRedis


class ProcessorWithoutGraphClient:
    graph_client = None


class ProcessorWithGraphClient:
    graph_client = object()


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
