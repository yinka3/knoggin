"""VP-02: relationships per Episode over the entities its messages name."""

from uuid import uuid4

import pytest

from common.conf.domain_config import DomainConfig
from common.exceptions import LLMRefusalError
from common.schema.ingestion.contracts import (
    EntityMention,
    EntityResult,
    EntityWrite,
    ProjectEntityClassification,
    ResolvedEntityMention,
)
from common.schema.settings import EntityResolutionSettings, ExtractionSettings
from common.scoping import IDENTITY_ENTITY_ID
from core.ingestion.batch import EpisodeEvidence, EvidenceMessage, SemanticWindowBuild
from core.ingestion.policy import IngestionPolicy
from core.ingestion.relationship_extractor import RelationshipExtractor
from core.knowledge.entity.profile import EntityProfile


def _domain():
    return DomainConfig.from_mapping({
        "version": 1,
        "topics": {"Work": {"active": True}},
        "entity_types": {
            "Person": {"topic": "Work", "labels": ["person"]},
            "Company": {"topic": "Work", "labels": ["company"]},
        },
        "relationships": {
            "OWNS": {
                "source_types": ["Person"],
                "target_types": ["Company"],
                "labels": ["owns", "has ownership of"],
            }
        },
    }).compile()


def _episode(episode_id, *texts, first_id=1):
    return EpisodeEvidence(
        episode_id=episode_id,
        summary=f"Summary of {episode_id}.",
        messages=tuple(
            EvidenceMessage(message_id=first_id + n, role="user", content=text,
                            timestamp_ms=1_000)
            for n, text in enumerate(texts)
        ),
    )


def _build(*episodes, entities, mentions, earlier=(), context_hints=None):
    """``entities``: id -> (name, type); ``mentions``: (entity_id, name, message ids)."""

    build = SemanticWindowBuild(
        window_id=uuid4(),
        user_name="ada",
        project_id="project-1",
        policy=IngestionPolicy.capture(
            extraction=ExtractionSettings(),
            entity_resolution=EntityResolutionSettings(),
            compiled_domain=_domain(),
        ),
        policy_snapshot={},
        episodes=tuple(episodes),
        earlier_summaries=tuple(earlier),
        context_hints=context_hints or {},
    )
    pending = {
        entity_id: EntityWrite(
            entity_id=entity_id, is_new=True, canonical_name=name,
            entity_type=entity_type, topic="Work", aliases=(name,),
        )
        for entity_id, (name, entity_type) in entities.items()
    }
    build.set_entity_result(EntityResult(
        entity_ids=tuple(entities),
        new_entity_ids=frozenset(pending),
        alias_updated_ids=frozenset(),
        alias_updates={},
        pending_entity_writes=pending,
        project_classifications={
            entity_id: ProjectEntityClassification(
                entity_id=entity_id, entity_type=entity_type, topic="Work",
                membership="missing",
            )
            for entity_id, (_, entity_type) in entities.items()
        },
        resolved_mentions=tuple(
            ResolvedEntityMention(
                mention=EntityMention(
                    message_ids=message_ids, name=name,
                    entity_type=entities[entity_id][1], topic="Work", origin="vp01",
                ),
                entity_id=entity_id,
                occurrence_index=index,
            )
            for index, (entity_id, name, message_ids) in enumerate(mentions)
        ),
    ))
    return build


class _Entities:
    async def get_profile(self, _entity_id):
        raise AssertionError("new entities do not need a profile")

    def get_mentions_for_id(self, _entity_id):
        return []


class _LLM:
    """Answers each Episode's call in order; an exception is raised instead."""

    extraction_model = "test-vp02"

    def __init__(self, *answers):
        self.answers = list(answers)
        self.calls = []

    async def generate_structured(self, *, response_model, **kwargs):
        self.calls.append(kwargs)
        answer = self.answers.pop(0) if self.answers else []
        if isinstance(answer, Exception):
            raise answer
        return response_model.model_validate({"connections": answer})


def _owns(a, b, *messages, label="owns", context=None):
    return {
        "messages": list(messages or ["M1"]), "entity_a": a, "entity_b": b,
        "relationship": label, **({"context": context} if context else {}),
    }


ALICE_DELTA = {10: ("Alice", "Person"), 11: ("Delta", "Company")}


@pytest.mark.unit
@pytest.mark.no_network
async def test_a_relationship_cites_the_episode_messages_that_state_it():
    build = _build(
        _episode("ep-1", "Alice is the owner.", "She owns Delta."),
        entities=ALICE_DELTA,
        mentions=[(10, "Alice", (1,)), (11, "Delta", (2,))],
    )
    llm = _LLM([
        _owns("e2", "e3", "M1", "M2", context="Alice owns Delta."),
        _owns("e2", "e3", "M2", "M1", context="duplicate"),
        _owns("e2", "e3", "M9"),
    ])

    writes = await RelationshipExtractor(user_name="ada", llm=llm, entities=_Entities()).extract(build)

    assert [(w.support_message_ids, w.entity_a_id, w.entity_b_id, w.canonical_type) for w in writes] == [
        ((1, 2), 10, 11, "OWNS"),
    ]
    assert build.relationship_writes == writes
    assert build.trace.relationships_seen == 3
    assert build.trace.relationships_rejected == 1
    assert [issue.code for issue in build.issues] == ["invalid_relationship"]


@pytest.mark.unit
@pytest.mark.no_network
async def test_separate_support_sets_and_labels_stay_separate_observations():
    build = _build(
        _episode("ep-1", "Alice owns Delta.", "Alice also controls Delta."),
        entities=ALICE_DELTA,
        mentions=[(10, "Alice", (1, 2)), (11, "Delta", (1, 2))],
    )
    llm = _LLM([
        _owns("e2", "e3", "M1"),
        _owns("e2", "e3", "M2"),
        _owns("e2", "e3", "M1", label="has ownership of"),
    ])

    writes = await RelationshipExtractor(user_name="ada", llm=llm, entities=_Entities()).extract(build)

    assert {(w.observed_label, w.support_message_ids) for w in writes} == {
        ("owns", (1,)), ("owns", (2,)), ("has ownership of", (1,)),
    }


@pytest.mark.unit
@pytest.mark.no_network
async def test_each_episode_sees_only_the_entities_its_messages_name():
    build = _build(
        _episode("ep-1", "Alice owns Delta."),
        _episode("ep-2", "Bob started at Delta.", first_id=2),
        _episode("ep-3", "Nothing named here.", first_id=3),
        entities={**ALICE_DELTA, 12: ("Bob", "Person")},
        mentions=[(10, "Alice", (1,)), (11, "Delta", (1, 2)), (12, "Bob", (2,))],
        earlier=("Older.", "Previous."),
    )
    llm = _LLM([], [])

    await RelationshipExtractor(user_name="ada", llm=llm, entities=_Entities()).extract(build)

    first, second = (call["user"] for call in llm.calls)
    assert "e2 = Alice [Person]" in first and "Bob" not in first
    assert "e2 = Delta [Company]" in second and "e3 = Bob [Person]" in second
    assert "Alice" not in second
    # vp02_episode_lookback is 2.
    assert "- Older.\n- Previous." in first
    assert "- Previous.\n- Summary of ep-1." in second
    # An Episode that names no entity makes no call.
    assert len(llm.calls) == 2


@pytest.mark.unit
@pytest.mark.no_network
async def test_context_blocks_the_messages_support_are_a_hint():
    build = _build(
        _episode("ep-1", "Alice owns Delta."),
        entities=ALICE_DELTA,
        mentions=[(10, "Alice", (1,)), (11, "Delta", (1,))],
        context_hints={1: ("Alice runs Delta day to day.",), 99: ("Unrelated.",)},
    )
    llm = _LLM([])

    await RelationshipExtractor(user_name="ada", llm=llm, entities=_Entities()).extract(build)

    assert "CONTEXT (hint, not evidence):\n- Alice runs Delta day to day." in llm.calls[0]["user"]
    assert "Unrelated." not in llm.calls[0]["user"]


@pytest.mark.unit
@pytest.mark.no_network
async def test_the_user_keeps_the_reserved_handle_when_a_candidate_shares_the_name():
    build = _build(
        _episode("ep-1", "Ada knows Delta.", "Ada owns Delta."),
        entities={10: ("Ada", "Person"), 11: ("Delta", "Company")},
        mentions=[(10, "Ada", (2,)), (11, "Delta", (1, 2))],
    )
    llm = _LLM([_owns("e1", "e3", "M1", label="knows"), _owns("e2", "e3", "M2")])

    writes = await RelationshipExtractor(user_name="Ada", llm=llm, entities=_Entities()).extract(build)

    assert {(w.entity_a_id, w.entity_b_id) for w in writes} == {(IDENTITY_ENTITY_ID, 11), (10, 11)}
    assert "e1 = Ada [Identity] (reserved user identity)" in llm.calls[0]["user"]
    assert "reserved user identity" in llm.calls[0]["system"]


@pytest.mark.unit
@pytest.mark.no_network
async def test_same_name_candidates_get_distinct_handles():
    build = _build(
        _episode("ep-1", "Alex owns Delta.", "The other Alex owns Delta."),
        entities={10: ("Alex", "Person"), 11: ("Alex", "Person"), 12: ("Delta", "Company")},
        mentions=[(10, "Alex", (1,)), (11, "Alex", (2,)), (12, "Delta", (1, 2))],
    )
    llm = _LLM([_owns("e2", "e4", "M1"), _owns("e3", "e4", "M2")])

    writes = await RelationshipExtractor(user_name="ada", llm=llm, entities=_Entities()).extract(build)

    assert {(w.entity_a_id, w.support_message_ids) for w in writes} == {(10, (1,)), (11, (2,))}
    assert llm.calls[0]["user"].count("Alex [Person]") == 2


class _ForeignEntities:
    async def get_profile(self, entity_id):
        return EntityProfile(
            canonical_name="Alice", entity_type="Company", topic="Archive",
            project_id="project-2",
        )

    def get_mentions_for_id(self, entity_id):
        return ["Alice"]


@pytest.mark.unit
@pytest.mark.no_network
async def test_a_reused_identity_uses_its_type_in_this_project():
    build = _build(
        _episode("ep-1", "Alice owns Delta."),
        entities=ALICE_DELTA,
        mentions=[(10, "Alice", (1,)), (11, "Delta", (1,))],
    )
    result = build.entity_result
    build.set_entity_result(EntityResult(
        entity_ids=result.entity_ids,
        new_entity_ids=frozenset({11}),
        alias_updated_ids=frozenset(),
        alias_updates={},
        pending_entity_writes={11: result.pending_entity_writes[11]},
        project_classifications=result.project_classifications,
        resolved_mentions=result.resolved_mentions,
    ))

    writes = await RelationshipExtractor(
        user_name="ada", llm=_LLM([_owns("e2", "e3")]), entities=_ForeignEntities(),
    ).extract(build)

    assert [(w.source_type, w.target_type, w.canonical_type) for w in writes] == [
        ("Person", "Company", "OWNS"),
    ]


@pytest.mark.unit
@pytest.mark.no_network
async def test_unknown_handles_and_self_relations_are_rejected():
    build = _build(
        _episode("ep-1", "Alice owns Delta."),
        entities=ALICE_DELTA,
        mentions=[(10, "Alice", (1,)), (11, "Delta", (1,))],
    )
    llm = _LLM([_owns("e99", "e3"), _owns("e2", "e2")])

    writes = await RelationshipExtractor(user_name="ada", llm=llm, entities=_Entities()).extract(build)

    assert writes == ()
    assert build.trace.relationships_rejected == 2


@pytest.mark.unit
@pytest.mark.no_network
async def test_a_refused_episode_loses_only_its_own_relationships():
    build = _build(
        _episode("ep-1", "Alice owns Delta."),
        _episode("ep-2", "Alice still owns Delta.", first_id=2),
        entities=ALICE_DELTA,
        mentions=[(10, "Alice", (1, 2)), (11, "Delta", (1, 2))],
    )
    llm = _LLM(LLMRefusalError("refused"), [_owns("e2", "e3")])

    writes = await RelationshipExtractor(user_name="ada", llm=llm, entities=_Entities()).extract(build)

    assert [w.support_message_ids for w in writes] == [(2,)]
    assert build.trace.episodes_refused == 1
