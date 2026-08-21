import pytest

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
        self.sealed = 0

    async def seal_due_user_messages(self, **_kwargs):
        self.sealed += 1

    async def claim_next_ingestion_batch(self, **_kwargs):
        return self.claims.pop(0) if self.claims else None

    async def fail_ingestion_claim(self, **kwargs):
        self.failed.append(kwargs)

    async def release_ingestion_claim(self, **_kwargs):
        raise AssertionError("success must not release a durable claim")


class _Processor:
    project_id = "project-1"

    def capture_policy(self, _settings):
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


async def _context(*_args):
    return [{"role_label": "USER", "content": "Ada met Grace."}]


def _worker(store, processor, write):
    return IngestionWorker(
        "ada",
        "session-1",
        store,
        processor,
        None,
        _context,
        write,
        IngestionSettings(),
    )


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_durable_worker_claims_and_completes_without_redis_recovery():
    store = _Store()

    async def write(batch):
        assert batch.batch_id == "claim-1"
        return True, None

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
        return False, "graph unavailable"

    worker = _worker(store, _Processor(), write)
    await worker._drain_durable_queue()

    assert store.failed[0]["batch_id"] == "claim-1"
    assert store.failed[0]["retryable"] is True
