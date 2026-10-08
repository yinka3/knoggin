"""Capture deterministic/fake-backed baselines; never makes provider calls.

Run from server: python -m tests.fixtures.capture_jev_baseline
This is a contract baseline, not a live extraction-quality benchmark.
"""

import asyncio
import json
from pathlib import Path

from common.conf.domain_config import DomainConfig
from common.schema.ingestion.contracts import (
    ContextBlockMention,
    UnknownEndpointDiagnostic,
)
from core.ingestion.vp01 import VP01EntitySpan
from tests.unit.core.ingestion.test_context_entity_build import (
    FakeEntityLLM,
    FakeKnowledgeStore,
    FakeVP01,
    _async_value,
    block,
    build,
    domain,
    identity_domain,
    policy,
    processor,
    resolver,
)


async def capture():
    records = []
    for case_id, text in (
        ("ambiguous_bob", "Bob joined the project planning meeting."),
        (
            "explicit_bob",
            "Bob, whose full name is Robert Chen, joined the planning meeting.",
        ),
    ):
        current = block(text)
        store = FakeKnowledgeStore(
            [
                {
                    "id": entity_id,
                    "user_name": "ada",
                    "canonical_name": name,
                    "aliases": ["Bob"],
                    "contexts": [
                        {
                            "project_id": "project-1",
                            "entity_type": "Person",
                            "topic": "Work",
                        }
                    ],
                }
                for entity_id, name in ((701, "Robert Chen"), (702, "Bob Smith"))
            ]
        )
        result = await resolver(knowledge_store=store).resolve_context_block_mentions(
            [
                ContextBlockMention(
                    block_ids=(current.block_id,),
                    name="Bob",
                    entity_type="Person",
                    topic="Work",
                    origin="vp01",
                )
            ],
            block_text_by_id={current.block_id: text},
            policy=policy(identity_domain()),
            allocate_entity_id=lambda: _async_value(703),
        )
        records.append(
            {
                "case_id": case_id,
                "entity_ids": result.entity_ids,
                "new_entity_ids": sorted(result.new_entity_ids),
                "identity_outcome": result.identity_decisions[0]["outcome"],
            }
        )

    review_cases = json.loads(
        Path(__file__).with_name("jev_identity_review_cases.json").read_text(
            encoding="utf-8"
        )
    )["cases"]
    review_domain = DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"Work": {"active": True}},
            "entity_types": {
                entity_type: {"topic": "Work", "labels": [entity_type.lower()]}
                for entity_type in ("Person", "Company", "Project", "Database")
            },
        }
    ).compile()
    for case in review_cases:
        candidates = [*case.get("candidates", [])]
        if case.get("stored_identity") is not None:
            candidates.append(case["stored_identity"])
        rows = [
            {
                "id": candidate["entity_id"],
                "user_name": "ada",
                "canonical_name": candidate["canonical_name"],
                "aliases": candidate.get("aliases", []),
                "contexts": [
                    {
                        "project_id": "project-2"
                        if "foreign_type" in candidate
                        else "project-1",
                        "entity_type": candidate.get(
                            "type", candidate.get("foreign_type", "Person")
                        ),
                        "topic": "Work",
                    }
                ],
            }
            for candidate in candidates
        ]
        mention_type = (
            "Person"
            if "foreign_type" in candidates[0]
            else candidates[0].get("type", "Person")
        )
        current = block(case["context"])
        result = await resolver(
            knowledge_store=FakeKnowledgeStore(rows),
            readable_project_ids=["project-1", "project-2"],
        ).resolve_context_block_mentions(
            [
                ContextBlockMention(
                    block_ids=(current.block_id,),
                    name=case["mention"],
                    entity_type=mention_type,
                    topic="Work",
                    origin="vp01",
                )
            ],
            block_text_by_id={current.block_id: case["context"]},
            policy=policy(review_domain),
            allocate_entity_id=lambda: _async_value(900),
        )
        records.append(
            {
                "case_id": case["id"],
                "entity_ids": result.entity_ids,
                "new_entity_ids": sorted(result.new_entity_ids),
                "identity_outcome": (
                    result.identity_decisions[0]["outcome"]
                    if result.identity_decisions
                    else "new_id_no_candidates"
                ),
            }
        )

    for endpoint_present in (False, True):
        current = block("Orion works with Zephyr Dynamics.")
        semantic_build = build(blocks=(current,), compiled_domain=domain())
        if endpoint_present:
            semantic_build.set_unknown_endpoint_diagnostics(
                (
                    UnknownEndpointDiagnostic(
                        block_id=current.block_id,
                        name="Zephyr Dynamics",
                        entity_type="Company",
                    ),
                )
            )
        llm = FakeEntityLLM(
            [{"block_id": "b1", "name": "Zephyr Dynamics", "type": "Company"}]
        )
        mentions = await processor(
            FakeVP01(
                [
                    VP01EntitySpan(text="Orion", label="company", start=0, end=5),
                ]
            ),
            llm=llm,
        ).extract_context_mentions(semantic_build)
        records.append(
            {
                "case_id": "endpoint_gap"
                if endpoint_present
                else "one_mention_covers_block",
                "mentions": [
                    {
                        "name": m.name,
                        "type": m.entity_type,
                        "topic": m.topic,
                        "origin": m.origin,
                    }
                    for m in mentions
                ],
                "generative_calls": len(llm.calls),
            }
        )
    destination = Path(__file__).with_name("jev_baseline_outputs.json")
    destination.write_text(
        json.dumps(
            {
                "version": 1,
                "kind": "deterministic_and_fake_backed_baseline",
                "human_review_status": "pending",
                "live_provider_calls": 0,
                "records": records,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"Captured {len(records)} baseline cases in {destination.name}")


if __name__ == "__main__":
    asyncio.run(capture())
