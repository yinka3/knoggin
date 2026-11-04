"""Reject corrupt frozen-window input before extraction or staging."""

from dataclasses import replace
from uuid import uuid4

import pytest

from common.conf.domain_config import DomainConfig
from common.schema.ingestion.contracts import (
    EntityMention,
    EntityResult,
    EntityWrite,
    ProjectEntityClassification,
    RelationshipWrite,
    ResolvedEntityMention,
)
from common.schema.semantic_window import (
    SemanticWindowOrigin,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from common.schema.settings import EntityResolutionSettings, ExtractionSettings
from core.ingestion.batch import EpisodeEvidence, EvidenceMessage, SemanticWindowBuild
from core.ingestion.policy import IngestionPolicy


def _policy():
    return IngestionPolicy.capture(
        extraction=ExtractionSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=DomainConfig.from_mapping({
            "version": 1,
            "topics": {"Work": {"active": True}},
            "entity_types": {"Company": {"topic": "Work", "labels": ["company"]}},
        }).compile(),
    )


def _episode(episode_id, *message_ids):
    return EpisodeEvidence(
        episode_id=episode_id, summary="A summary.",
        messages=tuple(
            EvidenceMessage(message_id=m, role="user", content="Delta.", timestamp_ms=1)
            for m in message_ids
        ),
    )


def _build():
    return SemanticWindowBuild(
        window_id=uuid4(), user_name="ada", project_id="project-1",
        policy=_policy(), policy_snapshot={}, episodes=(_episode("ep-1", 1, 2),),
    )


def _mention(*message_ids):
    return EntityMention(
        message_ids=message_ids, name="Delta", entity_type="Company", topic="Work",
        origin="vp01",
    )


def _result(*message_ids):
    return EntityResult(
        entity_ids=(10,),
        new_entity_ids=frozenset({10}),
        alias_updated_ids=frozenset(),
        alias_updates={},
        pending_entity_writes={10: EntityWrite(
            entity_id=10, is_new=True, canonical_name="Delta", entity_type="Company",
            topic="Work",
        )},
        project_classifications={10: ProjectEntityClassification(
            entity_id=10, entity_type="Company", topic="Work", membership="missing",
        )},
        resolved_mentions=(ResolvedEntityMention(
            mention=_mention(*message_ids), entity_id=10, occurrence_index=0,
        ),),
    )


@pytest.mark.parametrize(
    "field,value,error",
    [
        ("window_id", "bad", "window_id"),
        ("policy", None, "IngestionPolicy"),
        ("policy_snapshot", [], "mapping"),
        ("episodes", ("bad",), "EpisodeEvidence"),
        ("episodes", (_episode("ep-1", 1), _episode("ep-2", 1)), "share a message"),
    ],
)
def test_build_rejects_invalid_frozen_inputs(field, value, error):
    with pytest.raises((TypeError, ValueError), match=error):
        replace(_build(), **{field: value})


def test_an_episode_needs_messages():
    with pytest.raises(ValueError, match="requires messages"):
        _episode("ep-1")


@pytest.mark.parametrize("mutation,error", [
    ("type", "SemanticWindowRecord"), ("stage", "context_committed"),
])
def test_reopen_rejects_invalid_checkpoint(mutation, error):
    window = SemanticWindowRecord(
        window_id=uuid4(), user_name="ada", project_id="project-1",
        origin=SemanticWindowOrigin.CONVERSATION, stage=SemanticWindowStage.CLAIMED,
        domain_version=1,
        policy_snapshot={"ingestion_policy": _policy().semantic_window_snapshot()},
        source_token_count=1, token_estimator="test", token_estimator_version="1",
    )
    with pytest.raises((TypeError, ValueError), match=error):
        SemanticWindowBuild.from_committed_window(
            window=None if mutation == "type" else window, episodes=(),
        )


@pytest.mark.parametrize(
    "mutation,error",
    [
        ("mentions", "EntityMention"),
        ("mention_scope", "this window's Episode messages"),
        ("result", "EntityResult"),
        ("resolved_scope", "this window's Episode messages"),
        ("writes", "must be typed"),
        ("unresolved", "requires resolved entities"),
        ("write_scope", "this window's messages"),
    ],
)
def test_staging_rejects_invalid_outputs(mutation, error):
    current = _build()
    with pytest.raises((TypeError, ValueError), match=error):
        if mutation == "mentions":
            current.set_mentions(["invalid"])
        elif mutation == "mention_scope":
            current.set_mentions([_mention(99)])
        elif mutation == "result":
            current.set_entity_result(None)
        elif mutation == "resolved_scope":
            current.set_entity_result(_result(99))
        elif mutation == "writes":
            current.set_entity_result(_result(1))
            current.set_relationship_writes(["invalid"])
        elif mutation == "unresolved":
            current.set_relationship_writes([])
        else:
            current.set_entity_result(_result(1))
            current.set_relationship_writes([RelationshipWrite(
                support_message_ids=(99,), entity_a_id=1, entity_b_id=10,
                relationship_type="owns",
            )])
