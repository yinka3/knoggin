"""Occurrence coverage and accepted extraction across bounded rebuilds."""

import json
from dataclasses import replace

import pytest

from common.schema.ingestion.contracts import UnknownEndpointDiagnostic
from common.schema.jev import JevPolicy
from core.ingestion.context_entity_build import ContextEntityBuildService
from tests.unit.core.ingestion.test_context_entity_build import (
    BudgetedExtractionJev,
    FakeEntityLLM,
    FakeVP01,
    VP01EntitySpan,
    _async_value,
    block,
    build,
    domain,
    policy,
    processor,
    resolver,
    support,
)


@pytest.mark.parametrize("mode", ["active", "observe", "disabled"])
@pytest.mark.parametrize("call_limit", [1, 2])
async def test_supporting_occurrences_and_residual_gaps_stay_block_scoped(
    mode, call_limit
):
    first = block("Orion manages the rollout.")
    second = block("Vega manages the budget.")
    current = build(
        blocks=(first, second),
        compiled_domain=domain(),
        supports={
            first.block_id: (support(first.block_id, 91),),
            second.block_id: (support(second.block_id, 92),),
        },
        message_texts={
            91: "Delta manages the rollout.",
            92: "Delta manages the budget.",
        },
    )
    current.policy = policy(
        domain(), jev=JevPolicy(extraction_mode=mode, max_calls_per_window=call_limit)
    )
    offset = len(first.markdown) + 2
    llm = FakeEntityLLM([])
    proc = processor(
        FakeVP01(
            [
                VP01EntitySpan(text="Orion", label="company", start=0, end=5),
                VP01EntitySpan(
                    text="Vega", label="company", start=offset, end=offset + 4
                ),
            ]
        ),
        llm=llm,
        jev_client=BudgetedExtractionJev(),
    )
    proc.get_known_aliases = lambda: {"Delta": 701}
    if mode != "active":
        llm.mentions = [
            {"block_id": "b1", "name": "Delta", "type": "Company"},
            {"block_id": "b2", "name": "Delta", "type": "Company"},
        ]
    mentions = await proc.extract_context_mentions(current)
    recovered = [mention for mention in mentions if mention.name == "Delta"]
    if mode == "active" and call_limit == 1:
        assert [mention.block_ids for mention in recovered] == [(first.block_id,)]
        assert len(llm.calls) == 1
        residual = json.loads(llm.calls[0]["user"])["context_blocks"]
        assert [item["markdown"] for item in residual] == [second.markdown]
        assert current.trace.jev_extraction_avoided_fallback_blocks == 1
        assert current.trace.jev_extraction_avoided_fallback_calls == 0
    else:
        assert {mention.block_ids for mention in recovered} == {
            (first.block_id,),
            (second.block_id,),
        }
        assert all(
            mention.source_start is None and mention.source_end is None
            for mention in recovered
        )
        assert len(llm.calls) == (0 if mode == "active" else 1)
    if mode == "active" and call_limit == 2:
        assert (
            sum(
                item["accepted_entity_type"] is not None
                for item in current.trace.extraction_decisions
            )
            == 2
        )


@pytest.mark.parametrize("changed", [None, "evidence", "policy", "removed_gap"])
async def test_rebuild_carries_only_unchanged_accepted_extractions(changed):
    current_block = block("Orion works with Zephyr Dynamics.")
    current = build(
        blocks=(current_block,),
        compiled_domain=domain(),
        supports={current_block.block_id: (support(current_block.block_id, 91),)},
        message_texts={91: "Orion works with Zephyr Dynamics."},
    )
    current.policy = policy(
        domain(),
        llm_ner_mode="disabled",
        jev=JevPolicy(extraction_mode="active", max_calls_per_window=1),
    )
    current.set_unknown_endpoint_diagnostics(
        (
            UnknownEndpointDiagnostic(
                block_id=current_block.block_id,
                name="Zephyr Dynamics",
                entity_type="Company",
            ),
        )
    )
    jev = BudgetedExtractionJev()
    counter = iter(range(900, 910))
    service = ContextEntityBuildService(
        processor=processor(
            FakeVP01([VP01EntitySpan(text="Orion", label="company", start=0, end=5)]),
            llm_ner_mode="disabled",
            jev_client=jev,
        ),
        resolver=resolver(),
        allocate_entity_id=lambda: _async_value(next(counter)),
    )
    await service.build(current)
    assert any(mention.name == "Zephyr Dynamics" for mention in current.mentions)
    budget = current.jev_work_budget
    budget.max_observations = 1
    if changed == "evidence":
        current.message_text_by_id[91] = "Zephyr Dynamics is under review."
    elif changed == "policy":
        current.policy = replace(
            current.policy,
            jev=current.policy.jev.model_copy(
                update={"extraction_min_entity_noul": 0.9}
            ),
        )
    elif changed == "removed_gap":
        current.set_unknown_endpoint_diagnostics(())
    await service.build(current)
    assert current.jev_work_budget is budget
    assert budget.calls == budget.observations == 1
    assert len(jev.calls) == 1
    accepted = [
        item
        for item in current.trace.extraction_decisions
        if item["accepted_entity_type"] is not None
    ]
    if changed is None:
        assert any(mention.name == "Zephyr Dynamics" for mention in current.mentions)
        assert len(accepted) == 1
        assert accepted[0]["reused_for_pass"] == 2
        assert len(current.trace.extraction_decisions) == 1
    else:
        assert not any(
            mention.name == "Zephyr Dynamics" for mention in current.mentions
        )
        assert accepted == []
        assert current.trace.extraction_decisions[0]["superseded_by_pass"] == 2
