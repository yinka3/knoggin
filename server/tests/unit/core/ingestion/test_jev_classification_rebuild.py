"""Topic proposals must survive bounded rebuilds with current entity IDs."""

from dataclasses import replace

import pytest

from common.schema.jev import JevResult
from core.ingestion.context_entity_build import ContextEntityBuildService
from tests.unit.core.ingestion.test_context_entity_build import (
    FakeVP01,
    VP01EntitySpan,
    _async_value,
    block,
    build,
    processor,
    resolver,
)
from tests.unit.core.knowledge.test_jev_topic_observe import TopicJev, policy


class BudgetedTopicJev(TopicJev):
    async def evaluate(self, **kwargs):
        if not kwargs["work_budget"].take():
            return JevResult(outcome="unavailable", reason="call_budget_exhausted")
        return await super().evaluate(**kwargs)


@pytest.mark.parametrize("mode", ["active", "observe"])
@pytest.mark.parametrize("changed", [None, "evidence", "policy"])
async def test_rebuild_remaps_unchanged_topic_proposals_without_spending_budget(
    mode, changed
):
    frozen = policy(mode, acceptance_policy="override-positive-v1")
    frozen = replace(
        frozen,
        llm_ner_mode="disabled",
        jev=frozen.jev.model_copy(update={"max_calls_per_window": 1}),
    )
    current_block = block("Acme manages the project budget.")
    current = build(blocks=(current_block,), compiled_domain=frozen.domain)
    current.policy = frozen
    jev = BudgetedTopicJev()
    entity_resolver = resolver()
    entity_resolver._jev_client = jev
    counter = iter(range(900, 910))
    service = ContextEntityBuildService(
        processor=processor(
            FakeVP01([VP01EntitySpan(text="Acme", label="company", start=0, end=4)]),
            llm_ner_mode="disabled",
        ),
        resolver=entity_resolver,
        allocate_entity_id=lambda: _async_value(next(counter)),
    )
    await service.build(current)
    assert current.trace.classification_decisions[0]["entity_id"] == 900
    budget = current.jev_work_budget
    if changed == "evidence":
        current_block.markdown = "Acme manages a different project."
    elif changed == "policy":
        current.policy = replace(
            frozen,
            jev=frozen.jev.model_copy(
                update={"classification_min_evidence_noul": 0.99}
            ),
        )
    await service.build(current)
    assert current.jev_work_budget is budget
    assert budget.calls == 1
    assert len(jev.calls) == 1
    assert len(current.trace.classification_decisions) == 1
    observation = current.trace.classification_decisions[0]
    assert observation["entity_id"] == 901
    assert len(current.trace.classification_aggregates) == 1
    assert current.trace.classification_aggregates[0]["entity_id"] == 901
    expected = "Finance" if changed is None and mode == "active" else "Work"
    assert current.entity_result.project_classifications[901].topic == expected
    if changed is None:
        assert observation["reused_from_pass"] == 1
        assert observation["suggested_topic"] == "Finance"
        assert budget.observations == 1
    else:
        assert "reused_from_pass" not in observation
        assert observation["record"]["result"]["outcome"] == "unavailable"
