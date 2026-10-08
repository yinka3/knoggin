"""A bounded topic rebuild must commit only the final entity IDs."""

from dataclasses import replace

import pytest

from common.schema.semantic_window import SemanticWindowStage
from core.ingestion.context_entity_build import ContextEntityBuildService
from core.knowledge.db.readers.entity_reader import EntityReader
from core.knowledge.db.readers.semantic_window_reader import SemanticWindowReader
from core.knowledge.db.writers.semantic_commit_writer import SemanticCommitWriter
from core.knowledge.db.writers.semantic_window_writer import SemanticWindowWriter
from core.knowledge.entity.resolver import EntityResolver
from tests.contract.storage.test_semantic_commit_contract import (
    _block,
    _build,
    _classification_policy,
    _commit_context,
    _membership,
    _seed_message,
    _window,
    _WindowContext,
)
from tests.unit.core.ingestion.test_context_entity_build import (
    FakeVP01,
    VP01EntitySpan,
    _async_value,
    processor,
)
from tests.unit.core.ingestion.test_jev_classification_rebuild import BudgetedTopicJev


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_rebuilt_topic_commits_once_with_current_entity_id(real_postgres_client):
    await _seed_message(real_postgres_client)
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    block = _block("Delta manages the finance budget.")
    context = await _commit_context(real_postgres_client, window, (block,))
    assert await window_writer.advance_stage(
        window_id=window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=context.revision_id,
    )
    frozen = _classification_policy()
    frozen = replace(
        frozen,
        llm_ner_mode="disabled",
        jev=frozen.jev.model_copy(
            update={
                "classification_mode": "active",
                "classification_acceptance_policy_version": "override-positive-v1",
                "max_calls_per_window": 1,
            }
        ),
    )
    current = _build(
        _WindowContext(window.window_id, context),
        impact=(block.block_id,),
        entity_ids=(),
        entities={},
        associations=(),
        ingestion_policy=frozen,
    )
    jev = BudgetedTopicJev()
    counter = iter(range(1000, 1010))
    service = ContextEntityBuildService(
        processor=processor(
            FakeVP01([VP01EntitySpan(text="Delta", label="company", start=0, end=5)]),
            llm_ner_mode="disabled",
        ),
        resolver=EntityResolver(
            EntityReader(real_postgres_client),
            "project-1",
            ["project-1"],
            jev_client=jev,
        ),
        allocate_entity_id=lambda: _async_value(next(counter)),
    )
    await service.build(current)
    assert current.entity_result.project_classifications[1000].topic == "Finance"
    current.jev_work_budget.max_observations = 1
    await service.build(current)
    assert current.jev_work_budget.calls == current.jev_work_budget.observations == 1
    assert current.entity_result.project_classifications[1001].topic == "Finance"
    writer = SemanticCommitWriter(real_postgres_client)
    assert not (await writer.commit(current)).resumed
    assert (await writer.commit(current)).resumed
    records = await SemanticWindowReader(
        real_postgres_client
    ).list_jev_classification_decisions(
        window.window_id,
        user_name="ada",
        project_id="project-1",
    )
    assert len(records) == 1
    assert records[0]["entity_id"] == 1001
    assert records[0]["accepted_topic"] == "Finance"
    assert records[0]["record"]["pass_number"] == 2
    assert records[0]["reused_from_pass"] == 1
