"""Per-Episode VP-01, entity resolution, and the window's entity result."""

import json
from uuid import uuid4

import pytest
import spacy

from common.conf.domain_config import DomainConfig
from common.exceptions import LLMRefusalError
from common.schema.ingestion.contracts import (
    EntityMention,
    EntityResult,
    ProjectEntityClassification,
)
from common.schema.semantic_window import (
    SemanticWindowOrigin,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from common.schema.settings import EntityResolutionSettings, ExtractionSettings
from core.ingestion.batch import EpisodeEvidence, EvidenceMessage, SemanticWindowBuild
from core.ingestion.entity_build import EntityBuildService, _log_jev_observations
from core.ingestion.policy import IngestionPolicy
from core.ingestion.text_processor import TextProcessor
from core.knowledge.entity.profile import EntityProfile
from core.knowledge.entity.resolver import EntityResolver


class InlineModelWork:
    async def run_blocking(self, operation, **_kwargs):
        return operation()


class FakeKnowledgeStore:
    def __init__(self, entities=()):
        self.entities = {int(entity["id"]): entity for entity in entities}
        self.catalog_lookups = []
        self.name_lookups = []
        self.profile_lookups = []
        self.fail_catalog_lookup = False

    async def get_entity_by_id(self, entity_id, *_args, **_kwargs):
        self.profile_lookups.append(entity_id)
        return self.entities.get(entity_id)

    async def get_entities_by_names(self, names, *, visible_project_ids):
        self.name_lookups.append(list(names))
        return []

    async def get_entities_by_ids(self, entity_ids, *, visible_project_ids):
        return [self.entities[i] for i in entity_ids if i in self.entities]

    async def get_visible_entities_for_resolution(self, *, visible_project_ids):
        if self.fail_catalog_lookup:
            raise RuntimeError("candidate catalog lookup failed")
        self.catalog_lookups.append(list(visible_project_ids))
        return [
            entity
            for entity in self.entities.values()
            if entity.get("status", "active") == "active"
            and any(
                context.get("project_id") in visible_project_ids
                for context in entity.get("contexts", ())
            )
        ]


class FakeEntityLLM:
    """Answers each Episode's call in order; an exception is raised instead."""

    extraction_model = "fake-extraction"

    def __init__(self, *answers):
        self.answers = list(answers)
        self.calls = []

    async def generate_structured(self, *, response_model, **kwargs):
        self.calls.append(kwargs)
        answer = self.answers.pop(0) if self.answers else []
        if isinstance(answer, Exception):
            raise answer
        return response_model.model_validate({"mentions": answer})


def domain():
    return DomainConfig.from_mapping({
        "version": 4,
        "topics": {"Work": {"active": True}},
        "entity_types": {
            "Person": {"topic": "Work", "labels": ["person"],
                       "description": "A person involved in this project."},
            "Company": {"topic": "Work", "labels": ["company"],
                        "description": "A company involved in this project."},
        },
    }).compile()


def policy(compiled_domain=None, **extraction):
    return IngestionPolicy.capture(
        extraction=ExtractionSettings(**extraction),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=compiled_domain or domain(),
    )


def message(message_id, content, role="user"):
    return EvidenceMessage(
        message_id=message_id, role=role, content=content, timestamp_ms=1_000 * message_id,
    )


def episode(episode_id, *messages, summary=None):
    return EpisodeEvidence(
        episode_id=episode_id, summary=summary or f"Summary of {episode_id}.",
        messages=tuple(messages),
    )


def build(*episodes, earlier=(), **extraction):
    return SemanticWindowBuild(
        window_id=uuid4(),
        user_name="ada",
        project_id="project-1",
        policy=policy(**extraction),
        policy_snapshot={},
        episodes=tuple(episodes),
        earlier_summaries=tuple(earlier),
    )


def processor(llm, *, aliases=None, profiles=None, nlp=None):
    async def get_profile(entity_id):
        return (profiles or {}).get(entity_id)

    return TextProcessor(
        get_known_aliases=lambda: aliases or {},
        get_alias_version=lambda: 1,
        get_profile=get_profile,
        spacy=nlp or spacy.blank("en"),
        settings=ExtractionSettings(),
        llm=llm,
        user_name="ada",
        model_work=InlineModelWork(),
    )


def resolver(*, knowledge_store=None, readable_project_ids=None):
    return EntityResolver(
        knowledge_store=knowledge_store or FakeKnowledgeStore(),
        project_id="project-1",
        readable_project_ids=readable_project_ids or ["project-1"],
    )


def mention(name, *message_ids, entity_type="Company", origin="vp01"):
    return EntityMention(
        message_ids=message_ids or (1,), name=name, entity_type=entity_type,
        topic="Work", origin=origin,
    )


async def _async_value(value):
    return value


def project_profile(name, entity_type="Company", project_id="project-1"):
    return EntityProfile(
        canonical_name=name, entity_type=entity_type, topic="Work", project_id=project_id,
    )


# VP-01 per Episode


@pytest.mark.unit
@pytest.mark.no_network
async def test_each_episode_gets_one_call_with_its_own_messages_and_lookback():
    llm = FakeEntityLLM([], [])
    semantic_build = build(
        episode("ep-1", message(1, "I met Dana at Acme.")),
        episode("ep-2", message(2, "Later Dana called."), message(3, "Noted.", "assistant")),
        earlier=("Older summary.", "Previous summary."),
    )

    await processor(llm).extract_mentions(semantic_build)

    first, second = (call["user"] for call in llm.calls)
    assert "[M1] USER: I met Dana at Acme." in first
    assert "Later Dana called." not in first
    assert "[M1] USER: Later Dana called." in second
    assert "[M2] ASSISTANT: Noted." in second
    # vp01_episode_lookback is 1: only the summary just before each Episode.
    assert "- Previous summary." in first and "Older summary." not in first
    assert "- Summary of ep-1." in second and "Previous summary." not in second


@pytest.mark.unit
@pytest.mark.no_network
async def test_lookback_zero_sends_no_earlier_episodes():
    llm = FakeEntityLLM([])
    semantic_build = build(
        episode("ep-1", message(1, "Hello.")), earlier=("Older.",),
        vp01_episode_lookback=0,
    )

    await processor(llm).extract_mentions(semantic_build)

    assert "Older." not in llm.calls[0]["user"]


@pytest.mark.unit
@pytest.mark.no_network
async def test_a_mention_keeps_only_the_cited_messages_that_contain_its_name():
    llm = FakeEntityLLM([
        {"messages": ["M1", "M2"], "name": "Acme Labs", "type": "Company"},
        {"messages": ["M2"], "name": "Zephyr", "type": "Company"},
        {"messages": ["M1"], "name": "Acme Labs", "type": "Planet"},
        {"messages": ["M9"], "name": "Acme Labs", "type": "Person"},
    ])
    semantic_build = build(episode(
        "ep-1",
        message(11, "We picked ACME labs today."),
        message(12, "The contract is ready."),
    ))

    mentions = await processor(llm).extract_mentions(semantic_build)

    assert [(m.name, m.message_ids, m.origin) for m in mentions] == [
        ("Acme Labs", (11,), "vp01")
    ]
    reasons = [issue.metadata["reason"] for issue in semantic_build.issues]
    assert reasons == ["not_in_cited_messages", "unknown_type", "not_in_cited_messages"]
    assert semantic_build.trace.llm_mentions_accepted == 1
    assert semantic_build.trace.llm_mentions_rejected == 3


@pytest.mark.unit
@pytest.mark.no_network
async def test_known_names_are_mentions_and_the_model_is_told_to_skip_them():
    llm = FakeEntityLLM([
        {"messages": ["M1"], "name": "Acme Labs", "type": "Company"},
        {"messages": ["M2"], "name": "Dana", "type": "Person"},
    ])
    semantic_build = build(episode(
        "ep-1", message(1, "ACME LABS signed."), message(2, "Dana and Acme Labs met."),
    ))
    text_processor = processor(
        llm, aliases={"acme labs": 701, "orion": 702},
        profiles={701: project_profile("Acme Labs")},
    )

    mentions = await text_processor.extract_mentions(semantic_build)

    assert [(m.name, m.message_ids, m.origin) for m in mentions] == [
        ("ACME LABS", (1, 2), "known_alias"),
        ("Dana", (2,), "vp01"),
    ]
    assert "KNOWN NAMES (already in the graph; do not return them):\n- ACME LABS" in (
        llm.calls[0]["user"]
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_a_name_known_only_in_another_project_is_left_for_the_model_to_type():
    llm = FakeEntityLLM([{"messages": ["M1"], "name": "Acme Labs", "type": "Company"}])
    semantic_build = build(episode("ep-1", message(1, "Acme Labs signed.")))
    text_processor = processor(
        llm, aliases={"acme labs": 701},
        profiles={701: project_profile("Acme Labs", "Vendor", project_id="project-2")},
    )

    mentions = await text_processor.extract_mentions(semantic_build)

    assert [(m.name, m.origin) for m in mentions] == [("Acme Labs", "vp01")]


@pytest.mark.unit
@pytest.mark.no_network
async def test_a_refused_episode_keeps_its_known_names_and_the_window_goes_on():
    llm = FakeEntityLLM(
        LLMRefusalError("refused"),
        [{"messages": ["M1"], "name": "Dana", "type": "Person"}],
    )
    semantic_build = build(
        episode("ep-1", message(1, "Acme Labs signed.")),
        episode("ep-2", message(2, "Dana called.")),
    )
    text_processor = processor(
        llm, aliases={"acme labs": 701}, profiles={701: project_profile("Acme Labs")},
    )

    mentions = await text_processor.extract_mentions(semantic_build)

    assert [(m.name, m.origin) for m in mentions] == [
        ("Acme Labs", "known_alias"), ("Dana", "vp01"),
    ]
    assert semantic_build.trace.episodes_refused == 1


@pytest.mark.unit
@pytest.mark.no_network
async def test_common_words_are_not_entities():
    llm = FakeEntityLLM([
        {"messages": ["M1"], "name": "the", "type": "Company"},
        {"messages": ["M1"], "name": "acme labs", "type": "Company"},
    ])
    semantic_build = build(episode("ep-1", message(1, "the current vendor is acme labs.")))

    mentions = await processor(llm).extract_mentions(semantic_build)

    assert [m.name for m in mentions] == ["acme labs"]


# The window's entity result


@pytest.mark.unit
@pytest.mark.no_network
async def test_the_entity_result_refs_exactly_the_messages_that_name_each_entity():
    llm = FakeEntityLLM([
        {"messages": ["M1", "M2"], "name": "Acme Labs", "type": "Company"},
    ])
    semantic_build = build(episode(
        "ep-1", message(11, "We selected Acme Labs."), message(12, "Acme Labs signed."),
        message(13, "The contract is ready."),
    ))

    result = await EntityBuildService(
        processor=processor(llm), resolver=resolver(),
        allocate_entity_id=lambda: _async_value(701),
    ).build(semantic_build)

    assert [(r.message_id, r.entity_id) for r in result.message_entity_refs] == [
        (11, 701), (12, 701),
    ]
    assert result.pending_entity_writes[701].canonical_name == "Acme Labs"
    assert semantic_build.entity_result is result


@pytest.mark.unit
@pytest.mark.no_network
def test_every_resolved_entity_needs_a_mention():
    with pytest.raises(ValueError, match="needs a mention"):
        EntityResult(
            entity_ids=(10,),
            new_entity_ids=frozenset(),
            alias_updated_ids=frozenset(),
            alias_updates={},
            pending_entity_writes={},
            project_classifications={
                10: ProjectEntityClassification(
                    entity_id=10, entity_type="Person", topic="Work",
                    membership="existing",
                )
            },
        )


@pytest.mark.unit
@pytest.mark.no_network
def test_a_mention_must_cite_this_windows_messages():
    semantic_build = build(episode("ep-1", message(1, "Acme.")))

    with pytest.raises(ValueError, match="this window's Episode messages"):
        semantic_build.set_mentions([mention("Acme", 99)])


@pytest.mark.unit
@pytest.mark.no_network
def test_a_committed_window_reopens_with_its_frozen_policy():
    frozen = policy()
    window = SemanticWindowRecord(
        window_id=uuid4(),
        user_name="ada",
        project_id="project-1",
        origin=SemanticWindowOrigin.CONVERSATION,
        stage=SemanticWindowStage.CONTEXT_COMMITTED,
        domain_version=frozen.domain.version,
        context_revision_id=uuid4(),
        policy_snapshot={"ingestion_policy": frozen.semantic_window_snapshot()},
        source_token_count=10,
        token_estimator="test",
        token_estimator_version="1",
    )

    reopened = SemanticWindowBuild.from_committed_window(
        window=window, episodes=[episode("ep-1", message(1, "Hi."))],
        earlier_summaries=["Before."],
    )

    assert reopened.window_id == window.window_id
    assert reopened.policy == frozen
    assert reopened.background_for(0, 2) == ("Before.",)


def test_jev_identity_observations_are_emitted_as_bounded_json(monkeypatch):
    emitted = []

    class BoundLogger:
        def info(self, message, payload):
            emitted.append((message, payload))

    monkeypatch.setattr(
        "core.ingestion.entity_build.logger.bind", lambda **_fields: BoundLogger(),
    )
    observation = {
        "record": {"window_id": "window-1", "result": {"outcome": "available"}},
        "candidate_set_truncated": False,
    }

    _log_jev_observations("identity", (None, observation))

    assert emitted == [("jev_identity_observation {}", json.dumps(
        observation, sort_keys=True, separators=(",", ":"),
    ))]


# Identity resolution


@pytest.mark.unit
@pytest.mark.no_network
async def test_a_known_name_alone_resolves_without_a_model_candidate():
    store = FakeKnowledgeStore()
    entity_resolver = resolver(knowledge_store=store)

    resolution = await entity_resolver.resolve_mentions(
        [mention("Acme", origin="known_alias")],
        message_text_by_id={1: "Acme is selected."},
        policy=policy(),
        allocate_entity_id=lambda: _async_value(701),
    )

    assert resolution.entity_ids == (701,)
    assert resolution.resolved_mentions[0].mention.message_ids == (1,)
    assert store.catalog_lookups == [["project-1"]]
    assert entity_resolver.has_cached_entity(701) is False
    assert entity_resolver.get_alias_version() == 0


@pytest.mark.unit
@pytest.mark.no_network
async def test_no_mentions_load_no_candidate_catalog():
    store = FakeKnowledgeStore()

    resolution = await resolver(knowledge_store=store).resolve_mentions(
        [], message_text_by_id={}, policy=policy(),
        allocate_entity_id=lambda: _async_value(701),
    )

    assert resolution.entity_ids == ()
    assert store.catalog_lookups == []


@pytest.mark.unit
@pytest.mark.no_network
async def test_a_candidate_catalog_failure_propagates():
    store = FakeKnowledgeStore()
    store.fail_catalog_lookup = True

    with pytest.raises(RuntimeError, match="candidate catalog lookup failed"):
        await resolver(knowledge_store=store).resolve_mentions(
            [mention("Acme")], message_text_by_id={}, policy=policy(),
            allocate_entity_id=lambda: _async_value(701),
        )


def _entity(entity_id, name, *, aliases=(), project_id="project-1", entity_type="Company"):
    return {
        "id": entity_id, "user_name": "ada", "canonical_name": name,
        "aliases": list(aliases),
        "contexts": [{"project_id": project_id, "entity_type": entity_type, "topic": "Work"}],
    }


@pytest.mark.unit
@pytest.mark.no_network
async def test_a_foreign_visible_identity_gets_a_target_project_classification():
    store = FakeKnowledgeStore([
        _entity(701, "Acme Labs", aliases=["Acme Labs"], project_id="project-2",
                entity_type="Vendor"),
    ])

    resolution = await resolver(
        knowledge_store=store, readable_project_ids=["project-1", "project-2"],
    ).resolve_mentions(
        [mention("Acme Labs")],
        message_text_by_id={1: "Acme Labs sponsors the project."},
        policy=policy(), allocate_entity_id=lambda: _async_value(702),
    )

    assert resolution.entity_ids == (701,)
    assert resolution.new_entity_ids == frozenset()
    classification = resolution.project_classifications[701]
    assert (classification.entity_type, classification.topic, classification.membership) == (
        "Company", "Work", "missing",
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_standalone_and_batch_candidate_matching_agree():
    store = FakeKnowledgeStore([_entity(701, "Acme Labs", aliases=["Acme"])])
    entity_resolver = resolver(knowledge_store=store)

    standalone = await entity_resolver.get_candidate_ids("Acme Labs")
    resolution = await entity_resolver.resolve_mentions(
        [mention("Acme Labs")],
        message_text_by_id={1: "Acme Labs sponsors the project."},
        policy=policy(), allocate_entity_id=lambda: _async_value(702),
    )

    assert standalone == [(701, 1.0)]
    assert resolution.entity_ids == (701,)
    assert store.catalog_lookups == [["project-1"], ["project-1"]]


@pytest.mark.unit
@pytest.mark.no_network
async def test_distinct_names_share_one_candidate_catalog():
    store = FakeKnowledgeStore([_entity(701, "Acme Labs"), _entity(702, "Beta Corp")])

    resolution = await resolver(knowledge_store=store).resolve_mentions(
        [mention("Acme Labs"), mention("Beta Corp")],
        message_text_by_id={1: "Acme Labs and Beta Corp sponsor the project."},
        policy=policy(), allocate_entity_id=lambda: _async_value(703),
    )

    assert resolution.entity_ids == (701, 702)
    assert store.catalog_lookups == [["project-1"]]
    assert store.name_lookups == []
    assert store.profile_lookups == []


@pytest.mark.unit
@pytest.mark.no_network
async def test_a_foreign_identity_keeps_one_type_per_project():
    store = FakeKnowledgeStore([
        _entity(701, "Acme Labs", aliases=["Acme Labs"], project_id="project-2",
                entity_type="Vendor"),
    ])

    resolution = await resolver(
        knowledge_store=store, readable_project_ids=["project-1", "project-2"],
    ).resolve_mentions(
        [mention("Acme Labs"), mention("Acme Labs", entity_type="Person")],
        message_text_by_id={1: "Acme Labs sponsors the project."},
        policy=policy(), allocate_entity_id=lambda: _async_value(702),
    )

    assert resolution.entity_ids == (701, 702)
    assert {
        entity_id: (item.entity_type, item.membership)
        for entity_id, item in resolution.project_classifications.items()
    } == {701: ("Company", "missing"), 702: ("Person", "missing")}


@pytest.mark.unit
@pytest.mark.no_network
async def test_an_ambiguous_alias_abstains_without_a_clear_lead():
    store = FakeKnowledgeStore([
        _entity(701, "Robert Chen", aliases=["Bob"], entity_type="Person"),
        _entity(702, "Bob Smith", aliases=["Bob"], entity_type="Person"),
    ])

    resolution = await resolver(knowledge_store=store).resolve_mentions(
        [mention("Bob", entity_type="Person")],
        message_text_by_id={1: "Bob joined the project planning meeting."},
        policy=policy(), allocate_entity_id=lambda: _async_value(703),
    )

    assert resolution.entity_ids == (703,)
    assert resolution.identity_decisions[0]["outcome"] == "abstained"


@pytest.mark.unit
@pytest.mark.no_network
async def test_an_ambiguous_alias_uses_a_full_name_in_the_message():
    store = FakeKnowledgeStore([
        _entity(701, "Robert Chen", aliases=["Bob"], entity_type="Person"),
        _entity(702, "Bob Smith", aliases=["Bob"], entity_type="Person"),
    ])

    resolution = await resolver(knowledge_store=store).resolve_mentions(
        [mention("Bob", entity_type="Person")],
        message_text_by_id={1: "Bob, whose full name is Robert Chen, joined the meeting."},
        policy=policy(), allocate_entity_id=lambda: _async_value(703),
    )

    decision = resolution.identity_decisions[0]
    assert resolution.entity_ids == (701,)
    assert (decision["outcome"], decision["candidate_id"]) == ("reused", 701)
    assert "context_name_support" in decision["signals"]


@pytest.mark.unit
@pytest.mark.no_network
async def test_new_same_name_mentions_of_different_types_stay_apart():
    ids = iter((701, 702))

    resolution = await resolver().resolve_mentions(
        [mention("Alex", 1, entity_type="Person"), mention("Alex", 2)],
        message_text_by_id={1: "Alex is the project sponsor.", 2: "Alex is the project sponsor."},
        policy=policy(), allocate_entity_id=lambda: _async_value(next(ids)),
    )

    assert {i: w.entity_type for i, w in resolution.pending_entity_writes.items()} == {
        701: "Person", 702: "Company",
    }


@pytest.mark.unit
@pytest.mark.no_network
async def test_a_repeated_new_name_with_the_same_context_is_one_identity():
    resolution = await resolver().resolve_mentions(
        [mention("Acme Labs", 1), mention("Acme Labs", 2)],
        message_text_by_id={
            1: "Acme Labs sponsors the project.", 2: "Acme Labs sponsors the project.",
        },
        policy=policy(), allocate_entity_id=lambda: _async_value(701),
    )

    assert resolution.entity_ids == (701,)
    assert [item.entity_id for item in resolution.resolved_mentions] == [701, 701]


@pytest.mark.unit
@pytest.mark.no_network
async def test_new_homonyms_without_shared_context_stay_separate():
    ids = iter((701, 702))

    resolution = await resolver().resolve_mentions(
        [mention("Alex", 1, entity_type="Person"), mention("Alex", 2, entity_type="Person")],
        message_text_by_id={
            1: "Alex manages the design review for the mobile application.",
            2: "Alex approves the payroll budget for the finance team.",
        },
        policy=policy(), allocate_entity_id=lambda: _async_value(next(ids)),
    )

    assert resolution.entity_ids == (701, 702)


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("surface", "entity_type", "reused"),
    [
        # Written exactly like the stored name: the same entity, even when the
        # sentence is too short to give other context.
        ("Delta", "Company", True),
        ("Zephyr", "Company", True),
        # A lowercase common word may just be the word.
        ("delta", "Company", False),
        # A different type is a different thing.
        ("Delta", "Person", False),
    ],
)
async def test_a_one_word_name_written_like_the_entity_is_reused(
    surface, entity_type, reused,
):
    store = FakeKnowledgeStore([
        _entity(701, "Delta"), _entity(702, "Zephyr"),
    ])
    existing = 701 if surface.casefold() == "delta" else 702

    resolution = await resolver(knowledge_store=store).resolve_mentions(
        [mention(surface, entity_type=entity_type)],
        message_text_by_id={1: f"John owns {surface}."},
        policy=policy(), allocate_entity_id=lambda: _async_value(900),
    )

    assert (resolution.entity_ids == (existing,)) is reused
