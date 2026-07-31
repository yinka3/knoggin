import pytest

from core.ingestion.batch import IngestionBatch, IngestionMilestone
from core.ingestion.services.batch_consumer import IngestionWorker


class _KnowledgeStore:
    async def save_message_logs(self, _rows):
        assert IngestionMilestone.MESSAGE_LOGS_HANDLED not in self.batch.milestones


class _Processor:
    project_id = "project-1"


def _completed_batch() -> IngestionBatch:
    batch = IngestionBatch.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        messages=[{"id": 7, "message": "Ada met Grace."}],
        session_text="[USER]: Ada met Grace.",
    )
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
    return batch


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_consumer_marks_message_logs_only_after_the_store_succeeds():
    batch = _completed_batch()
    worker = object.__new__(IngestionWorker)
    worker.user_name = "ada"
    worker.session_id = "session-1"
    worker.processor = _Processor()
    worker.knowledge_store = _KnowledgeStore()
    worker.knowledge_store.batch = batch

    success, dlq_written = await worker._save_message_logs_or_dlq(batch)

    assert (success, dlq_written) == (True, False)
    assert IngestionMilestone.MESSAGE_LOGS_HANDLED in batch.milestones
