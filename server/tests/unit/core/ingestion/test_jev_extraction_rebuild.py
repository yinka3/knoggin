"""Occurrence coverage and accepted extraction across bounded rebuilds."""

import json
from dataclasses import replace

import pytest

from common.schema.ingestion.contracts import (
    ContextBlockMention,
    UnknownEndpointDiagnostic,
)
from common.schema.jev import JevPolicy
from core.ingestion.context_entity_build import ContextEntityBuildService
from core.ingestion.text_processor import _ContextBlockText, _FallbackGap
from core.knowledge.entity.profile import EntityProfile
from infrastructure.jev_client import JevWorkBudget
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
async def test_support_only_names_stay_unresolved_and_block_scoped(mode, call_limit):
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
    llm = FakeEntityLLM(
        [
            {
                "block_id": "b1",
                "name": "Delta",
                "type": "Company",
                "supporting_message_id": "m1",
                "evidence_quote": "Delta manages the rollout.",
            },
            {
                "block_id": "b2",
                "name": "Delta",
                "type": "Company",
                "supporting_message_id": "m2",
                "evidence_quote": "Delta manages the budget.",
            },
        ]
    )
    jev = BudgetedExtractionJev()
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
        jev_client=jev,
    )
    proc.get_known_aliases = lambda: {"Delta": 701}
    mentions = await proc.extract_context_mentions(current)
    assert [mention.name for mention in mentions] == ["Orion", "Vega"]
    assert jev.calls == []
    assert current.trace.jev_extraction_candidates == 0
    assert current.trace.extraction_decisions == []
    assert len(llm.calls) == 1
    residual = json.loads(llm.calls[0]["user"])["context_blocks"]
    assert [item["markdown"] for item in residual] == [
        first.markdown,
        second.markdown,
    ]
    unresolved = [
        issue
        for issue in current.issues
        if issue.metadata.get("reason") == "supporting_message_name_unresolved"
    ]
    assert [
        (issue.metadata["block_id"], issue.metadata["supporting_message_id"])
        for issue in unresolved
    ] == [
        (str(first.block_id), "m1"),
        (str(second.block_id), "m2"),
    ]


async def test_exhausted_budget_reuses_prior_acceptance_behind_new_gap():
    current_block = block("Orion works with Zephyr Dynamics and Nova Systems.")
    current = build(blocks=(current_block,), compiled_domain=domain())
    current.policy = policy(
        domain(),
        llm_ner_mode="disabled",
        jev=JevPolicy(extraction_mode="active", max_calls_per_window=1),
    )
    zephyr = UnknownEndpointDiagnostic(
        block_id=current_block.block_id,
        name="Zephyr Dynamics",
        entity_type="Company",
    )
    nova = UnknownEndpointDiagnostic(
        block_id=current_block.block_id,
        name="Nova Systems",
        entity_type="Company",
    )
    current.set_unknown_endpoint_diagnostics((zephyr,))
    jev = BudgetedExtractionJev()
    proc = processor(
        FakeVP01([VP01EntitySpan(text="Orion", label="company", start=0, end=5)]),
        llm_ner_mode="disabled",
        jev_client=jev,
    )

    first = await proc.extract_context_mentions(current)
    assert [mention.name for mention in first] == ["Orion", "Zephyr Dynamics"]
    budget = current.jev_work_budget
    budget.max_observations = 1
    current.set_unknown_endpoint_diagnostics((nova, zephyr))

    replayed = await proc.extract_context_mentions(current)

    assert [mention.name for mention in replayed] == ["Orion", "Zephyr Dynamics"]
    assert budget.observations == budget.calls == 1
    assert len(jev.calls) == 1
    assert len(current.trace.extraction_decisions) == 1
    assert current.trace.extraction_decisions[0]["accepted_entity_type"] == "Company"
    assert "reused_for_pass" in current.trace.extraction_decisions[0]


async def test_exhausted_budget_replays_acceptance_after_preparation_limit():
    current_block = block("Orion works with Zephyr Dynamics.")
    current = build(blocks=(current_block,), compiled_domain=domain())
    current.identity_pass_number = 1
    current.policy = policy(
        domain(),
        llm_ner_mode="disabled",
        jev=JevPolicy(extraction_mode="active", max_calls_per_window=1),
    )
    zephyr = UnknownEndpointDiagnostic(
        block_id=current_block.block_id,
        name="Zephyr Dynamics",
        entity_type="Company",
    )
    current.set_unknown_endpoint_diagnostics((zephyr,))
    jev = BudgetedExtractionJev()
    proc = processor(
        FakeVP01([VP01EntitySpan(text="Orion", label="company", start=0, end=5)]),
        llm_ner_mode="disabled",
        jev_client=jev,
    )

    first = await proc.extract_context_mentions(current)
    assert [mention.name for mention in first] == ["Orion", "Zephyr Dynamics"]
    budget = current.jev_work_budget
    budget.max_observations = 1
    earlier_diagnostics = tuple(
        UnknownEndpointDiagnostic(
            block_id=current_block.block_id,
            name=f"Absent Endpoint {index}",
            entity_type="Company",
        )
        for index in range(512)
    )
    current.set_unknown_endpoint_diagnostics((*earlier_diagnostics, zephyr))
    current.identity_pass_number += 1

    replayed = await proc.extract_context_mentions(current)

    assert [mention.name for mention in replayed] == ["Orion", "Zephyr Dynamics"]
    assert budget.observations == budget.calls == 1
    assert len(jev.calls) == 1
    assert current.trace.extraction_decisions[0]["reused_for_pass"] == 2


@pytest.mark.parametrize("alias", ["Zephyr Dynamics", "Zephyr   Dynamics"])
async def test_exhausted_budget_replays_alias_after_earlier_diagnostics(alias):
    current_block = block("Orion works with Zephyr Dynamics.")
    current = build(blocks=(current_block,), compiled_domain=domain())
    current.policy = policy(domain())
    current.jev_work_budget = JevWorkBudget(
        max_calls=1, max_observations=1, observations=1
    )
    earlier_diagnostics = tuple(
        UnknownEndpointDiagnostic(
            block_id=current_block.block_id,
            name=f"Absent Endpoint {index}",
            entity_type="Company",
        )
        for index in range(512)
    )
    current.set_unknown_endpoint_diagnostics(
        (
            *earlier_diagnostics,
            UnknownEndpointDiagnostic(
                block_id=current_block.block_id,
                name="Zephyr Dynamics",
                entity_type="Company",
            ),
        )
    )
    proc = processor(FakeVP01([]))
    proc.get_known_aliases = lambda: {alias: 701}
    proc.get_profile = lambda _entity_id: _async_value(None)

    candidates = await proc._prepare_jev_extraction_candidates(
        current,
        assembled=_ContextBlockText.from_blocks((current_block,)),
        gaps=[
            _FallbackGap(
                block=current_block,
                reasons=("known_alias_missing_from_extraction",),
                support_text="",
            )
        ],
        mentions=[],
        previous_acceptances=[
            {
                "candidate_name": "Zephyr Dynamics",
                "evidence_origin": "known_alias_gap",
                "record": {"evidence_block_ids": [str(current_block.block_id)]},
            }
        ],
    )

    assert [(item.name, item.evidence_origin) for item in candidates] == [
        ("Zephyr Dynamics", "known_alias_gap")
    ]
    assert current.jev_work_budget.observations == 1


async def test_exhausted_budget_replays_only_literal_candidates_in_current_gaps():
    current_block = block("Acme works with Vega.")
    removed_block = block("Ghost was removed.")
    current = build(blocks=(current_block, removed_block), compiled_domain=domain())
    current.jev_work_budget = JevWorkBudget(
        max_calls=1, max_observations=1, observations=1
    )
    current.set_unknown_endpoint_diagnostics(
        (
            UnknownEndpointDiagnostic(block_id=removed_block.block_id, name="Ghost", entity_type="Company"),
            UnknownEndpointDiagnostic(block_id=current_block.block_id, name="Vega", entity_type="Company"),
        )
    )
    proc = processor(FakeVP01([]))
    proc.get_known_aliases = lambda: {"Acme": 701, "Missing": 702}
    proc.get_profile = lambda _entity_id: _async_value(
        EntityProfile(
            canonical_name="Acme",
            entity_type="Company",
            topic="Work",
            project_id="project-1",
        )
    )
    acceptances = [
        {
            "candidate_name": name,
            "evidence_origin": origin,
            "record": {"evidence_block_ids": [str(block_id)]},
        }
        for name, origin, block_id in (
            ("Ghost", "unknown_relationship_endpoint", removed_block.block_id),
            ("Vega", "unknown_relationship_endpoint", current_block.block_id),
            ("Acme", "known_alias_gap", current_block.block_id),
            ("Missing", "known_alias_gap", current_block.block_id),
        )
    ]

    candidates = await proc._prepare_jev_extraction_candidates(
        current,
        assembled=_ContextBlockText.from_blocks((current_block, removed_block)),
        gaps=[_FallbackGap(current_block, ("known_alias_missing_from_extraction",), "")],
        mentions=[],
        previous_acceptances=acceptances,
    )

    assert {(item.name, item.evidence_origin) for item in candidates} == {
        ("Vega", "unknown_relationship_endpoint"),
        ("Acme", "known_alias_gap"),
    }
    assert next(item for item in candidates if item.name == "Acme").proposed_type == "Company"


async def test_fresh_candidate_scan_skips_represented_and_nonliteral_endpoints():
    current_block = block("Acme works with Vega.")
    current = build(blocks=(current_block,), compiled_domain=domain())
    current.jev_work_budget = JevWorkBudget(max_calls=1, max_observations=1)
    current.set_unknown_endpoint_diagnostics(
        tuple(
            UnknownEndpointDiagnostic(block_id=current_block.block_id, name=name, entity_type="Company")
            for name in ("Acme", "Absent", "Vega")
        )
    )
    proc = processor(FakeVP01([]))
    proc.get_known_aliases = lambda: {"Acme": 701}
    represented = ContextBlockMention(
        block_ids=(current_block.block_id,),
        name="Acme",
        entity_type="Company",
        topic="Work",
        origin="vp01",
        source_start=0,
        source_end=4,
    )

    candidates = await proc._prepare_jev_extraction_candidates(
        current,
        assembled=_ContextBlockText.from_blocks((current_block,)),
        gaps=[_FallbackGap(current_block, ("unknown_relationship_endpoint",), "")],
        mentions=[represented],
    )

    assert [(item.name, item.evidence_origin) for item in candidates] == [
        ("Vega", "unknown_relationship_endpoint")
    ]


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
