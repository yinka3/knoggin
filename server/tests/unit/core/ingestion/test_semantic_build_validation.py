"""Reject corrupt frozen-window input before extraction or staging."""

from dataclasses import replace
from uuid import uuid4

import pytest

from common.schema.ingestion.contracts import ContextBlockMention
from common.schema.semantic_window import SemanticWindowStage
from core.ingestion.batch import SemanticWindowBuild
from tests.contract.storage.test_semantic_commit_contract import _window
from tests.unit.core.knowledge.test_jev_commit_validation import staged


@pytest.mark.parametrize(
    "field,value,error",
    [
        ("window_id", "bad", "window_id"),
        ("context", None, "ContextSnapshot"),
        ("policy", None, "IngestionPolicy"),
        ("policy_snapshot", [], "mapping"),
        ("impact_block_ids", set(), "frozenset"),
        ("impact_block_ids", frozenset({"bad"}), "UUIDs"),
        ("message_text_by_id", {0: "text"}, "positive IDs"),
        ("message_text_by_id", {1: None}, "positive IDs"),
    ],
)
def test_build_rejects_invalid_frozen_inputs(field, value, error):
    current, _ = staged("identity")
    with pytest.raises((TypeError, ValueError), match=error):
        replace(current, **{field: value})


@pytest.mark.parametrize(
    "mutation,error",
    [
        ("outside", "current Context blocks"),
        ("untyped", "typed support"),
        ("mismatched", "mapping key"),
        ("domain", "domain versions"),
    ],
)
def test_build_rejects_invalid_evidence_catalog(mutation, error):
    current, eligible = staged("identity")
    block_id = next(iter(eligible))
    supports = current.block_supports
    if mutation == "outside":
        update = {"block_supports": {uuid4(): supports[block_id]}}
    elif mutation == "untyped":
        update = {"block_supports": {block_id: [supports[block_id][0]]}}
    elif mutation == "mismatched":
        update = {
            "block_supports": {
                block_id: (
                    supports[block_id][0].model_copy(update={"block_id": uuid4()}),
                )
            }
        }
    else:
        update = {"context": current.context.model_copy(update={"domain_version": 99})}
    with pytest.raises((TypeError, ValueError), match=error):
        replace(current, **update)


@pytest.mark.parametrize(
    "mutation,error",
    [
        ("type", "SemanticWindowRecord"),
        ("stage", "context_committed"),
        ("revision", "checkpoint"),
    ],
)
def test_reopen_rejects_invalid_checkpoint(mutation, error):
    current, _ = staged("identity")
    window = _window().model_copy(
        update={
            "stage": SemanticWindowStage.CONTEXT_COMMITTED,
            "context_revision_id": current.context.revision_id,
            "policy_snapshot": {
                "ingestion_policy": current.policy.semantic_window_snapshot()
            },
        }
    )
    if mutation == "type":
        window = None
    elif mutation == "stage":
        window = window.model_copy(update={"stage": SemanticWindowStage.CLAIMED})
    else:
        window = window.model_copy(update={"context_revision_id": uuid4()})
    with pytest.raises((TypeError, ValueError), match=error):
        SemanticWindowBuild.from_committed_window(
            window=window,
            context=current.context,
            impact_block_ids=current.impact_block_ids,
            block_supports={},
            message_text_by_id={},
        )


@pytest.mark.parametrize(
    "mutation,error",
    [
        ("mentions", "ContextBlockMention"),
        ("mention_scope", "eligible input"),
        ("result", "ContextEntityResult"),
        ("association_scope", "eligible current"),
        ("empty", "empty effective impact"),
        ("writes", "must be typed"),
        ("unresolved", "requires resolved entities"),
    ],
)
def test_staging_rejects_invalid_outputs(mutation, error):
    current, _ = staged("identity")
    with pytest.raises((TypeError, ValueError), match=error):
        if mutation == "mentions":
            current.set_mentions(["invalid"])
        elif mutation == "mention_scope":
            current.set_mentions(
                [
                    ContextBlockMention(
                        block_ids=(uuid4(),),
                        name="Delta",
                        entity_type="Company",
                        topic="Work",
                        origin="vp01",
                    )
                ]
            )
        elif mutation == "result":
            current.set_entity_result(None)
        elif mutation == "association_scope":
            association = current.entity_result.block_entity_associations[0]
            current.set_entity_result(
                replace(
                    current.entity_result,
                    block_entity_associations=(replace(association, block_id=uuid4()),),
                )
            )
        elif mutation == "empty":
            current.set_empty_knowledge_result()
        elif mutation == "writes":
            current.set_relationship_writes(["invalid"])
        else:
            current.entity_result = None
            current.set_relationship_writes([])
