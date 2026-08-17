import pytest

from common.conf.domain_config import DomainConfig
from core.ingestion.batch import IngestionBatch
from tests.fixtures.ingestion import ingestion_policy
from tests.unit.core.ingestion.test_pipeline_entity_resolution_contract import (
    make_harness,
)


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_pipeline_derives_topic_from_canonical_entity_type():
    policy = ingestion_policy(
        compiled_domain=DomainConfig.from_mapping(
            {
                "version": 0,
                "topics": {
                    "General": {"active": True},
                    "Software Development": {"active": True},
                },
                "entity_types": {
                    "Software Project": {
                        "topic": "Software Development",
                        "labels": ["software project"],
                    }
                },
            }
        ).compile()
    )
    pipeline, _, _, _ = make_harness()

    class Processor:
        async def extract_mentions(self, _batch):
            return [(1, "Knoggin", "Software Project", "Wrong Topic")]

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

    assert mentions == [(1, "Knoggin", "Software Project", "Software Development")]
    assert [issue.code for issue in batch.issues] == ["derived_topic_override"]
