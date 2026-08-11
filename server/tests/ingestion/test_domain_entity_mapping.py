import pytest

from common.conf.topics_config import TopicConfig
from common.schema.settings import TopicSchema
from core.ingestion.batch import IngestionBatch
from tests.fixtures.ingestion import ingestion_policy
from tests.ingestion.test_batch_processor_entity_resolution_contract import (
    make_harness,
)


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_pipeline_derives_topic_from_canonical_entity_type():
    topics = TopicConfig(
        {
            "General": TopicSchema(active=True),
            "Software Development": TopicSchema(
                active=True,
                labels=["software project"],
            ),
        }
    )
    policy = ingestion_policy(topics=topics)
    pipeline, _, _, _ = make_harness()

    class Processor:
        async def extract_mentions(self, _batch):
            return [(1, "Knoggin", "Software Development", "Wrong Topic")]

    pipeline.processor = Processor()
    batch = IngestionBatch.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        messages=[{"id": 1, "message": "Knoggin"}],
        session_text="",
        policy=policy,
    )

    mentions = await pipeline._extract_mentions(batch)

    assert mentions == [(1, "Knoggin", "Software Development", "Software Development")]
    assert [issue.code for issue in batch.issues] == ["derived_topic_override"]

