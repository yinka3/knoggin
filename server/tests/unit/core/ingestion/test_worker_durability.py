import pytest

from common.exceptions import ConfigurationError, LLMProviderError, LLMResponseError
from common.schema.settings import IngestionSettings
from core.ingestion.batch import IngestionBatch
from core.ingestion.worker import IngestionWorker
from tests.fixtures.ingestion import ingestion_policy


class _Claim:
    batch_id = "claim-1"
    messages = [{"id": 7, "message": "Ada met Grace.", "role": "user"}]


class _Store:
    def __init__(self):
        self.claims = [_Claim()]
        self.failed = []
        self.released = []
        self.sealed = 0

    async def seal_due_user_messages(self, **_kwargs):
        self.sealed += 1

    async def claim_next_ingestion_batch(self, **_kwargs):
        return self.claims.pop(0) if self.claims else None

    async def fail_ingestion_claim(self, **kwargs):
        self.failed.append(kwargs)

    async def release_ingestion_claim(self, **kwargs):
        self.released.append(kwargs)


class _Processor:
    project_id = "project-1"

    def capture_policy(self):
        return ingestion_policy()

    def open_batch(self, messages, session_text, **kwargs):
        return IngestionBatch.open(
            user_name="ada",
            project_id="project-1",
            messages=messages,
            session_text=session_text,
            session_id=kwargs["session_id"],
            policy=kwargs["policy"],
            batch_id=kwargs["batch_id"],
        )

    async def process(self, batch):
        batch.success = True


class _InvalidProcessor(_Processor):
    async def process(self, batch):
        batch.fail(ValueError("invalid extraction"))


class _FailureProcessor(_Processor):
    def __init__(self, failure):
        self.failure = failure

    async def process(self, batch):
        batch.fail(self.failure)


async def _context(*_args):
    return [{"role_label": "USER", "content": "Ada met Grace."}]


def _worker(store, processor, write):
    return IngestionWorker(
        "ada",
        "session-1",
        store,
        processor,
        _context,
        write,
        IngestionSettings(),
    )


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_durable_worker_claims_and_completes_without_runtime_recovery():
    store = _Store()

    async def write(batch):
        assert batch.batch_id == "claim-1"
        return None

    worker = _worker(store, _Processor(), write)
    await worker._drain_durable_queue()

    assert store.sealed == 1
    assert store.failed == []
    assert worker.health_snapshot()["last_success_at"] is not None


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_durable_worker_records_failure_on_the_claim():
    store = _Store()

    async def write(_batch):
        raise RuntimeError("graph unavailable")

    worker = _worker(store, _Processor(), write)
    await worker._drain_durable_queue()

    assert store.failed[0]["batch_id"] == "claim-1"
    assert store.failed[0]["retryable"] is True


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_durable_worker_pauses_for_programming_boundary_failure():
    store = _Store()
    worker = _worker(store, _InvalidProcessor(), lambda _batch: None)

    await worker._drain_durable_queue()

    assert store.failed == []
    assert worker.health_snapshot()["state"] == "paused"
    assert worker.health_snapshot()["pause_reason"] == "subsystem:ValueError"

    assert await worker.resume() is True
    assert store.released == [
        {
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": "session-1",
            "batch_id": "claim-1",
        }
    ]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_durable_worker_pauses_for_configuration_repair():
    store = _Store()
    worker = _worker(
        store,
        _FailureProcessor(ConfigurationError("missing model configuration")),
        lambda _batch: None,
    )

    await worker._drain_durable_queue()

    assert store.failed == []
    assert worker.health_snapshot()["pause_reason"] == "subsystem:configuration_error"


@pytest.mark.ingestion
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("failure", "retryable", "stage"),
    [
        (LLMProviderError("provider unavailable"), True, "runtime"),
        (LLMResponseError("invalid structured response"), True, "model"),
    ],
)
async def test_durable_worker_retries_transient_model_failures(failure, retryable, stage):
    store = _Store()
    worker = _worker(store, _FailureProcessor(failure), lambda _batch: None)

    await worker._drain_durable_queue()

    assert store.failed[0]["retryable"] is retryable
    assert store.failed[0]["failure_stage"] == stage
    assert store.failed[0]["failure_code"] == failure.code
