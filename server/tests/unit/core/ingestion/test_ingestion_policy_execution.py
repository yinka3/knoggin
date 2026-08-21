"""Regression coverage for policy-only ingestion execution paths."""

import pytest

from common.schema.settings import EntityResolutionSettings
from core.ingestion.batch import IngestionBatch
from tests.fixtures.ingestion import ingestion_policy
from tests.unit.core.ingestion.test_pipeline_entity_resolution_contract import (
    make_harness,
)


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_candidate_lookup_uses_the_threshold_captured_by_the_batch_policy():
    pipeline, entities, knowledge_store, _ = make_harness()
    entities.candidate_vector_threshold = 0.99
    policy = ingestion_policy(
        entity_resolution=EntityResolutionSettings(
            candidate_fuzzy_threshold=77,
            candidate_vector_threshold=0.23,
        )
    )
    batch = IngestionBatch.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        messages=[{"id": 1, "message": "Alice"}],
        session_text="",
        policy=policy,
    )

    await entities.candidate_entries_for_mentions(
        [(1, "Alice", "person", "Identity")],
        policy=batch.policy,
        parent_work_record=batch.work_unit,
    )

    assert knowledge_store.vector_searches[-1]["score_threshold"] == 0.23
