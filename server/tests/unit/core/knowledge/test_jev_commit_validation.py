"""Forged private provenance must fail validation before any SQL insert."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from common.schema.ingestion.contracts import ContextBlockEntityAssociation
from core.knowledge.db.writers.semantic_commit_writer import SemanticCommitWriter
from tests.contract.storage.test_semantic_commit_contract import (
    _add_jev_classification_observation,
    _add_jev_extraction_observation,
    _add_jev_observation,
    _build,
    _classification_policy,
    _entity,
)
from tests.unit.core.ingestion.test_context_entity_build import block, build


def staged(capability):
    frozen = _classification_policy()
    frozen = replace(
        frozen,
        jev=frozen.jev.model_copy(
            update={
                "identity_mode": "observe",
                "extraction_mode": "active",
            }
        ),
    )
    current_block = block("Delta manages the budget.")
    current = build(blocks=(current_block,), compiled_domain=frozen.domain)
    current = _build(
        SimpleNamespace(window_id=current.window_id, context=current.context),
        impact=(current_block.block_id,),
        entity_ids=(10,),
        entities={10: _entity(10, "Delta", "Company")},
        associations=(
            ContextBlockEntityAssociation(
                block_id=current_block.block_id, entity_id=10, mention_text="Delta"
            ),
        ),
        ingestion_policy=frozen,
    )
    helpers = {
        "identity": _add_jev_observation,
        "extraction": _add_jev_extraction_observation,
        "classification": _add_jev_classification_observation,
    }
    helpers[capability](current, current_block.block_id)
    return current, {current_block.block_id}


@pytest.mark.parametrize(
    "mutation,error",
    [
        ("scope", "scope"),
        ("type", "invalid entity type"),
        ("offset", "offset provenance"),
        ("truncation_flag", "candidate-set provenance"),
        ("truncated", "Truncated"),
        ("status", "acceptance status"),
        ("response", "valid response"),
        ("mapping", "committed mention"),
        ("mention", "committed mention"),
        ("size", "record limit"),
    ],
)
async def test_extraction_rejects_forged_provenance_before_insert(mutation, error):
    current, eligible = staged("extraction")
    observation = current.trace.extraction_decisions[0]
    if mutation == "scope":
        observation["record"]["project_id"] = "other-project"
    elif mutation == "type":
        observation["suggested_entity_type"] = "Unconfigured"
    elif mutation == "offset":
        observation["has_context_offsets"] = None
    elif mutation == "truncation_flag":
        observation["candidate_set_truncated"] = None
    elif mutation == "truncated":
        observation["candidate_set_truncated"] = True
    elif mutation == "status":
        observation["accepted_entity_type"] = None
    elif mutation == "response":
        observation["record"]["result"]["response"] = None
    elif mutation == "mapping":
        observation["record"]["option_mapping"] = {"type_1": "Company"}
    elif mutation == "mention":
        association = current.entity_result.block_entity_associations[0]
        current.entity_result = replace(
            current.entity_result,
            block_entity_associations=(replace(association, mention_text="Different"),),
        )
    elif mutation == "size":
        observation["candidate_name"] = "x" * 256_000
        observation["accepted_entity_type"] = None
        observation["record"]["acceptance_status"] = "rejected"
    cursor = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(ValueError, match=error):
        await SemanticCommitWriter._write_jev_extraction_decisions(
            cursor, current, eligible_blocks=eligible
        )
    cursor.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "mutation,error",
    [
        ("id", "resolved entity"),
        ("aggregate_id", "aggregate identity"),
        ("duplicate_aggregate", "aggregate identity"),
        ("missing_aggregate", "cover decisions"),
        ("membership", "first-entry"),
        ("baseline", "disagree on baseline"),
        ("aggregate", "aggregate is inconsistent"),
        ("scope", "scope"),
        ("options", "decision is inconsistent"),
        ("response", "requires a response"),
        ("size", "record limit"),
    ],
)
async def test_topic_rejects_forged_provenance_before_insert(mutation, error):
    current, eligible = staged("classification")
    observation = current.trace.classification_decisions[0]
    aggregate = current.trace.classification_aggregates[0]
    if mutation == "id":
        observation["entity_id"] = True
    elif mutation == "aggregate_id":
        aggregate["entity_id"] = True
    elif mutation == "duplicate_aggregate":
        current.trace.classification_aggregates.append(dict(aggregate))
    elif mutation == "missing_aggregate":
        current.trace.classification_aggregates.clear()
    elif mutation == "membership":
        current.entity_result.project_classifications[10] = replace(
            current.entity_result.project_classifications[10], membership="existing"
        )
    elif mutation == "baseline":
        current.trace.classification_decisions.append(
            {**observation, "baseline_topic": "Finance"}
        )
    elif mutation == "aggregate":
        aggregate["observation_count"] = 2
    elif mutation == "scope":
        observation["record"]["evidence_block_ids"] = [str(uuid4())]
    elif mutation == "options":
        observation["allowed_topics"] = ["Finance"]
    elif mutation == "response":
        observation["record"]["result"]["response"] = None
    elif mutation == "size":
        observation["extra"] = "x" * 256_000
    cursor = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(ValueError, match=error):
        await SemanticCommitWriter._write_jev_classification_decisions(
            cursor, current, eligible_blocks=eligible
        )
    cursor.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "mutation,error",
    [
        ("scope", "scope"),
        ("counts", "candidate counts"),
        ("catalog", "candidate catalog"),
        ("size", "record limit"),
        ("accepted_response", "valid response"),
    ],
)
async def test_identity_rejects_forged_provenance_before_insert(mutation, error):
    current, eligible = staged("identity")
    observation = current.trace.identity_decisions[0]["jev"]
    if mutation == "scope":
        observation["record"]["window_id"] = str(uuid4())
    elif mutation == "counts":
        observation["offered_candidate_count"] = 2
    elif mutation == "catalog":
        observation["eligible_candidate_ids"] = []
    elif mutation == "size":
        observation["extra"] = "x" * 512_000
    elif mutation == "accepted_response":
        observation["accepted_entity_id"] = 42
    cursor = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(ValueError, match=error):
        await SemanticCommitWriter._write_jev_identity_decisions(
            cursor, current, eligible_blocks=eligible
        )
    cursor.execute.assert_not_awaited()


@pytest.mark.parametrize("capability", ["identity", "extraction", "classification"])
async def test_valid_private_provenance_inserts_once(capability):
    current, eligible = staged(capability)
    cursor = SimpleNamespace(execute=AsyncMock())
    await getattr(SemanticCommitWriter, f"_write_jev_{capability}_decisions")(
        cursor,
        current,
        eligible_blocks=eligible,
    )
    cursor.execute.assert_awaited_once()
    query, params = cursor.execute.await_args.args
    assert f"project_jev_{capability}_decisions" in query
    assert params[:2] == (current.window_id, current.project_id)


@pytest.mark.parametrize("weak", [False, True])
async def test_active_topic_commit_rechecks_evidence_gate(weak):
    current, eligible = staged("classification")
    current.policy = replace(
        current.policy,
        jev=current.policy.jev.model_copy(
            update={
                "classification_mode": "active",
                "classification_acceptance_policy_version": "override-positive-v1",
            }
        ),
    )
    current.entity_result.project_classifications[10] = replace(
        current.entity_result.project_classifications[10], topic="Finance"
    )
    current.entity_result.pending_entity_writes[10] = replace(
        current.entity_result.pending_entity_writes[10], topic="Finance"
    )
    observation = current.trace.classification_decisions[0]
    observation["accepted_topic"] = "Finance"
    observation["record"].update(
        mode="active",
        acceptance_status="accepted",
        acceptance_policy_version="override-positive-v1",
    )
    observation["aggregation"]["operational_topic"] = "Finance"
    if weak:
        observation["record"]["result"]["response"]["answers"]["topic_evidence"][
            "noul"
        ] = 0.1
    cursor = SimpleNamespace(execute=AsyncMock())
    if weak:
        with pytest.raises(ValueError, match="operational topic"):
            await SemanticCommitWriter._write_jev_classification_decisions(
                cursor, current, eligible_blocks=eligible
            )
        cursor.execute.assert_not_awaited()
    else:
        await SemanticCommitWriter._write_jev_classification_decisions(
            cursor, current, eligible_blocks=eligible
        )
        cursor.execute.assert_awaited_once()
