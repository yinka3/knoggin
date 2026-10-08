"""Accepted supporting-only extraction survives rebuild and atomic storage."""

import pytest

from common.schema.jev import JevPolicy
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
    _commit_context,
    _membership,
    _policy,
    _seed_message,
    _window,
    _WindowContext,
)
from tests.unit.core.ingestion.test_context_entity_build import (
    BudgetedExtractionJev,
    FakeVP01,
    VP01EntitySpan,
    _async_value,
    processor,
)


@pytest.mark.storage
@pytest.mark.requires_postgres
async def test_supporting_extractions_commit_once_after_budget_exhaustion(
    real_postgres_client,
):
    await _seed_message(real_postgres_client)
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    first = _block("Orion manages the rollout.")
    second = _block("Vega manages the budget.")
    context = await _commit_context(real_postgres_client, window, (first, second))
    assert await window_writer.advance_stage(
        window_id=window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=context.revision_id,
    )
    current = _build(
        _WindowContext(window.window_id, context),
        impact=(first.block_id, second.block_id),
        entity_ids=(),
        entities={},
        associations=(),
        ingestion_policy=_policy(
            JevPolicy(extraction_mode="active", max_calls_per_window=2)
        ),
    )
    offset = len(first.markdown) + 2
    jev = BudgetedExtractionJev(choice="type_2")
    proc = processor(
        FakeVP01(
            [
                VP01EntitySpan(text="Orion", label="company", start=0, end=5),
                VP01EntitySpan(
                    text="Vega", label="company", start=offset, end=offset + 4
                ),
            ]
        ),
        jev_client=jev,
    )
    proc.get_known_aliases = lambda: {"Delta": 701}
    counter = iter(range(1000, 1010))
    service = ContextEntityBuildService(
        processor=proc,
        resolver=EntityResolver(
            EntityReader(real_postgres_client), "project-1", ["project-1"]
        ),
        allocate_entity_id=lambda: _async_value(next(counter)),
    )
    await service.build(current)
    budget = current.jev_work_budget
    budget.max_observations = 2
    await service.build(current)
    assert budget.calls == budget.observations == len(jev.calls) == 2
    assert len(current.trace.extraction_decisions) == 2
    assert {
        mention.block_ids for mention in current.mentions if mention.name == "Delta"
    } == {
        (first.block_id,),
        (second.block_id,),
    }
    writer = SemanticCommitWriter(real_postgres_client)
    assert not (await writer.commit(current)).resumed
    assert (await writer.commit(current)).resumed
    reader = SemanticWindowReader(real_postgres_client)
    records = await reader.list_jev_extraction_decisions(
        window.window_id, user_name="ada", project_id="project-1"
    )
    assert len(records) == 2
    assert all(
        item["accepted_entity_type"] == "Company"
        and not item["has_context_offsets"]
        and item["reused_for_pass"] == 2
        for item in records
    )
    assert (
        await reader.list_jev_extraction_decisions(
            window.window_id, user_name="other", project_id="project-1"
        )
        == []
    )
