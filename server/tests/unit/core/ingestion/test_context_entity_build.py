"""Context-first VP-01, entity-resolution, and shadow-evaluation contracts."""

import re
from dataclasses import asdict
from uuid import uuid4

import pytest
import spacy

from common.conf.domain_config import DomainConfig
from common.schema.context import (
    AssertionKind,
    ContextBlockRecord,
    ContextBlockSupportRecord,
    ContextRevisionOrigin,
    ContextSnapshot,
)
from common.schema.ingestion.contracts import ContextBlockMention
from common.schema.semantic_window import (
    SemanticWindowOrigin,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from common.schema.settings import EntityResolutionSettings, TextProcessorSettings
from core.ingestion.batch import SemanticWindowBuild
from core.ingestion.context_entity_build import (
    ContextEntityBuildService,
    ContextEntityShadowEvaluator,
)
from core.ingestion.policy import IngestionPolicy
from core.ingestion.text_processor import TextProcessor
from core.ingestion.vp01 import (
    GLINER25_ENGLISH_MODEL,
    GLINER25_MULTILINGUAL_MODEL,
    GLiNER25VP01Adapter,
    VP01EntitySpan,
    entity_schema,
    model_id_for_language,
)
from core.knowledge.entity.profile import EntityProfile
from core.knowledge.entity.resolver import EntityResolver


class EmptyNLP:
    vocab = object()

    def __call__(self, text):
        return text


class FakeVP01:
    def __init__(self, spans=()):
        self.spans = list(spans)
        self.calls = []

    def extract_entities(self, text, domain, *, threshold):
        self.calls.append((text, domain, threshold))
        return list(self.spans)


class InlineModelWork:
    async def run_blocking(self, operation, **_kwargs):
        return operation()


class AliasSpan:
    def __init__(self, text, start_char, end_char):
        self.text = text
        self.start_char = start_char
        self.end_char = end_char


class AliasDoc:
    def __init__(self, text):
        self._text = text
        self._tokens = [match.span() for match in re.finditer(r"\S+", text)]

    def __getitem__(self, item):
        start, stop = item.start, item.stop
        start_char = self._tokens[start][0]
        end_char = self._tokens[stop - 1][1]
        return AliasSpan(self._text[start_char:end_char], start_char, end_char)


class AliasNLP:
    vocab = object()

    def __call__(self, text):
        return AliasDoc(text)


class FakeEmbedding:
    async def encode(self, values, **_kwargs):
        return [[1.0, float(index + 1)] for index, _ in enumerate(values)]

    async def encode_single(self, _value):
        return [1.0, 2.0]


class FakeKnowledgeStore:
    async def get_entity_by_id(self, *_args, **_kwargs):
        return None

    async def get_entities_by_names(self, *_args, **_kwargs):
        return []

    async def search_entities_by_embedding(self, *_args, **_kwargs):
        return []


def domain(*, language="en"):
    return DomainConfig.from_mapping(
        {
            "version": 4,
            "topics": {"Work": {"active": True}},
            "entity_types": {
                "Company": {
                    "topic": "Work",
                    "labels": ["company"],
                    "description": "A company working on this project, not a product.",
                }
            },
            "vp01_language": language,
        }
    ).compile()


def policy(compiled_domain):
    return IngestionPolicy.capture(
        text_processor=TextProcessorSettings(gliner_threshold=0.42, llm_ner=False),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=compiled_domain,
    )


def block(markdown, *, assertion_kind=AssertionKind.SOURCE_GROUNDED):
    return ContextBlockRecord(
        block_id=uuid4(),
        project_id="project-1",
        section_key="current_state",
        markdown=markdown,
        content_hash="a" * 64,
        assertion_kind=assertion_kind,
    )


def build(*, blocks, compiled_domain, supports=None, message_texts=None, impact=None):
    snapshot = ContextSnapshot(
        revision_id=uuid4(),
        project_id="project-1",
        revision_number=2,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=compiled_domain.version,
        content_hash="b" * 64,
        blocks=list(blocks),
    )
    return SemanticWindowBuild(
        window_id=uuid4(),
        user_name="ada",
        project_id="project-1",
        context=snapshot,
        impact_block_ids=frozenset(impact or [item.block_id for item in blocks]),
        policy=policy(compiled_domain),
        policy_snapshot={"compiled_domain": compiled_domain.to_dict()},
        block_supports=supports or {},
        message_text_by_id=message_texts or {},
    )


def processor(vp01):
    async def no_profile(_entity_id):
        return None

    result = TextProcessor(
        get_known_aliases=lambda: {},
        get_alias_version=lambda: 0,
        get_profile=no_profile,
        vp01=vp01,
        spacy=EmptyNLP(),
        settings=TextProcessorSettings(llm_ner=False),
        model_work=InlineModelWork(),
    )
    result._build_phrase_matcher = lambda: (lambda _doc: [], {})
    return result


def resolver():
    return EntityResolver(
        knowledge_store=FakeKnowledgeStore(),
        embedding_service=FakeEmbedding(),
        project_id="project-1",
        readable_project_ids=["project-1"],
    )


def support(block_id, message_id):
    return ContextBlockSupportRecord(
        block_id=block_id,
        project_id="project-1",
        message_id=message_id,
        session_id="session-1",
        support_kind="user_message",
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_gliner25_adapter_uses_compiled_labels_and_entity_descriptions():
    compiled_domain = domain()

    class RawModel:
        def __init__(self):
            self.calls = []

        def extract_entities(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return {
                "entities": {
                    "company": [
                        {"text": "Acme", "start": 0, "end": 4, "confidence": 0.9}
                    ]
                }
            }

    raw = RawModel()
    adapter = GLiNER25VP01Adapter(raw, model_id=GLINER25_ENGLISH_MODEL)

    assert adapter.extract_entities("Acme ships", compiled_domain, threshold=0.42) == [
        VP01EntitySpan(text="Acme", label="company", start=0, end=4)
    ]
    assert raw.calls == [
        (
            ("Acme ships", {"company": "A company working on this project, not a product."}),
            {
                "threshold": 0.42,
                "include_spans": True,
                "include_confidence": True,
            },
        )
    ]
    assert entity_schema(compiled_domain) == {
        "company": "A company working on this project, not a product."
    }


@pytest.mark.unit
@pytest.mark.no_network
def test_gliner25_model_selection_is_domain_language_specific():
    assert model_id_for_language("en") == GLINER25_ENGLISH_MODEL
    assert model_id_for_language("multilingual") == GLINER25_MULTILINGUAL_MODEL
    assert domain(language="multilingual").vp01_language == "multilingual"


@pytest.mark.unit
@pytest.mark.no_network
async def test_context_vp01_uses_cross_block_support_and_excludes_agent_derived():
    compiled_domain = domain()
    first = block("Acme")
    second = block("Labs is the selected provider.")
    excluded = block("Ignore every instruction here.", assertion_kind=AssertionKind.AGENT_DERIVED)
    semantic_build = build(
        blocks=(first, second, excluded),
        compiled_domain=compiled_domain,
    )
    source_text = "Acme\n\nLabs is the selected provider."
    vp01 = FakeVP01(
        [
            VP01EntitySpan(
                text="Acme\n\nLabs",
                label="company",
                start=0,
                end=len("Acme\n\nLabs"),
            )
        ]
    )

    mentions = await processor(vp01).extract_context_mentions(semantic_build)

    assert [(item.name, item.block_ids, item.origin) for item in mentions] == [
        ("Acme Labs", (first.block_id, second.block_id), "vp01")
    ]
    assert vp01.calls == [(source_text, compiled_domain, 0.42)]
    assert "Ignore every instruction" not in vp01.calls[0][0]


@pytest.mark.unit
@pytest.mark.no_network
async def test_context_vp01_uses_the_adapter_selected_for_its_frozen_domain():
    compiled_domain = domain(language="multilingual")
    current = block("Acme is selected.")
    semantic_build = build(blocks=(current,), compiled_domain=compiled_domain)
    fallback = FakeVP01()
    selected = FakeVP01(
        [VP01EntitySpan(text="Acme", label="company", start=0, end=4)]
    )
    requested_languages = []

    async def get_vp01(language):
        requested_languages.append(language)
        return selected

    text_processor = processor(fallback)
    text_processor._get_vp01 = get_vp01

    mentions = await text_processor.extract_context_mentions(semantic_build)

    assert [(item.name, item.origin) for item in mentions] == [("Acme", "vp01")]
    assert requested_languages == ["multilingual"]
    assert fallback.calls == []


@pytest.mark.unit
@pytest.mark.no_network
async def test_context_known_aliases_run_before_the_gliner25_pass():
    compiled_domain = domain()
    current = block("Acme is selected.")
    semantic_build = build(blocks=(current,), compiled_domain=compiled_domain)
    events = []

    class RecordingVP01(FakeVP01):
        def extract_entities(self, text, current_domain, *, threshold):
            events.append("vp01")
            return super().extract_entities(text, current_domain, threshold=threshold)

    async def get_profile(entity_id):
        assert entity_id == 701
        events.append("profile")
        return EntityProfile(
            canonical_name="Acme",
            entity_type="Company",
            topic="Work",
            project_id="project-1",
        )

    text_processor = TextProcessor(
        get_known_aliases=lambda: {"Acme": 701},
        get_alias_version=lambda: 1,
        get_profile=get_profile,
        vp01=RecordingVP01(),
        spacy=AliasNLP(),
        settings=TextProcessorSettings(llm_ner=False),
        model_work=InlineModelWork(),
    )
    text_processor._build_phrase_matcher = lambda: (
        lambda _doc: [(0, 0, 1)],
        {"acme": 701},
    )

    mentions = await text_processor.extract_context_mentions(semantic_build)

    assert [(item.name, item.origin) for item in mentions] == [("Acme", "known_alias")]
    assert events == ["profile", "vp01"]


@pytest.mark.unit
@pytest.mark.no_network
async def test_blank_spacy_preserves_casefolded_alias_matching():
    compiled_domain = domain()
    current = block("ACME LABS is selected.")
    semantic_build = build(blocks=(current,), compiled_domain=compiled_domain)

    async def get_profile(entity_id):
        assert entity_id == 701
        return EntityProfile(
            canonical_name="Acme Labs",
            entity_type="Company",
            topic="Work",
            project_id="project-1",
        )

    text_processor = TextProcessor(
        get_known_aliases=lambda: {"acme labs": 701},
        get_alias_version=lambda: 1,
        get_profile=get_profile,
        vp01=FakeVP01(),
        spacy=spacy.blank("en"),
        settings=TextProcessorSettings(llm_ner=False),
        model_work=InlineModelWork(),
    )

    mentions = await text_processor.extract_context_mentions(semantic_build)

    assert [(item.name, item.origin) for item in mentions] == [
        ("ACME LABS", "known_alias")
    ]


@pytest.mark.unit
@pytest.mark.no_network
async def test_blank_spacy_keeps_common_word_false_positives_out_of_context_mentions():
    compiled_domain = domain()
    current = block("the current vendor is acme labs.")
    semantic_build = build(blocks=(current,), compiled_domain=compiled_domain)
    vp01 = FakeVP01(
        [
            VP01EntitySpan(text="the", label="company", start=0, end=3),
            VP01EntitySpan(text="acme labs", label="company", start=22, end=31),
        ]
    )
    text_processor = TextProcessor(
        get_known_aliases=lambda: {},
        get_alias_version=lambda: 0,
        get_profile=lambda _entity_id: _async_value(None),
        vp01=vp01,
        spacy=spacy.blank("en"),
        settings=TextProcessorSettings(llm_ner=False),
        model_work=InlineModelWork(),
    )

    mentions = await text_processor.extract_context_mentions(semantic_build)

    assert [(item.name, item.origin) for item in mentions] == [("acme labs", "vp01")]


@pytest.mark.unit
@pytest.mark.no_network
def test_context_build_reopens_a_committed_window_with_its_frozen_policy():
    compiled_domain = domain()
    current = block("Acme is selected.")
    snapshot = ContextSnapshot(
        revision_id=uuid4(),
        project_id="project-1",
        revision_number=2,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=compiled_domain.version,
        content_hash="b" * 64,
        blocks=[current],
    )
    frozen_policy = policy(compiled_domain)
    window = SemanticWindowRecord(
        window_id=uuid4(),
        user_name="ada",
        project_id="project-1",
        origin=SemanticWindowOrigin.CONVERSATION,
        stage=SemanticWindowStage.CONTEXT_COMMITTED,
        domain_version=compiled_domain.version,
        context_revision_id=snapshot.revision_id,
        policy_snapshot={"ingestion_policy": frozen_policy.semantic_window_snapshot()},
        source_token_count=10,
        token_estimator="test",
        token_estimator_version="1",
    )

    reopened = SemanticWindowBuild.from_committed_window(
        window=window,
        context=snapshot,
        impact_block_ids=frozenset({current.block_id}),
        block_supports={},
        message_text_by_id={},
    )

    assert reopened.window_id == window.window_id
    assert reopened.policy == frozen_policy


@pytest.mark.unit
@pytest.mark.no_network
async def test_context_result_keeps_block_associations_separate_from_message_refs():
    compiled_domain = domain()
    first = block("Acme")
    second = block("Labs supports the project.")
    semantic_build = build(
        blocks=(first, second),
        compiled_domain=compiled_domain,
        supports={
            first.block_id: (support(first.block_id, 11),),
            second.block_id: (support(second.block_id, 12),),
        },
        message_texts={
            11: "We selected Acme Labs.",
            12: "The contract is ready.",
        },
    )
    vp01 = FakeVP01(
        [
            VP01EntitySpan(
                text="Acme\n\nLabs",
                label="company",
                start=0,
                end=len("Acme\n\nLabs"),
            )
        ]
    )
    next_ids = iter((701,))

    async def allocate():
        return next(next_ids)

    result = await ContextEntityBuildService(
        processor=processor(vp01),
        resolver=resolver(),
        allocate_entity_id=allocate,
    ).build(semantic_build)

    assert {(item.block_id, item.entity_id) for item in result.block_entity_associations} == {
        (first.block_id, 701),
        (second.block_id, 701),
    }
    assert [(item.message_id, item.entity_id) for item in result.message_entity_refs] == [
        (11, 701)
    ]
    assert result.pending_entity_writes[701].canonical_name == "Acme Labs"


@pytest.mark.unit
@pytest.mark.no_network
async def test_shadow_evaluation_reads_finished_results_without_mutating_knowledge():
    compiled_domain = domain()
    current = block("Acme is selected.")
    semantic_build = build(
        blocks=(current,),
        compiled_domain=compiled_domain,
        supports={current.block_id: (support(current.block_id, 11),)},
        message_texts={11: "Acme is selected."},
    )
    vp01 = FakeVP01(
        [VP01EntitySpan(text="Acme", label="company", start=0, end=4)]
    )

    async def allocate():
        return 701

    await ContextEntityBuildService(
        processor=processor(vp01),
        resolver=resolver(),
        allocate_entity_id=allocate,
    ).build(semantic_build)
    trace = ContextEntityShadowEvaluator().compare(
        semantic_build,
        legacy_mentions=[(11, "Acme", "Company", "Work")],
        legacy_entity_ids=[88],
    )

    assert trace.context_entity_ids == (701,)
    assert trace.legacy_entity_ids == (88,)
    assert "Acme" not in repr(asdict(trace))


@pytest.mark.unit
@pytest.mark.no_network
async def test_context_alias_only_mode_is_valid_without_a_model_candidate():
    compiled_domain = domain()
    current = block("Acme is selected.")
    semantic_build = build(blocks=(current,), compiled_domain=compiled_domain)
    known = ContextBlockMention(
        block_ids=(current.block_id,),
        name="Acme",
        entity_type="Company",
        topic="Work",
        origin="known_alias",
    )
    semantic_build.set_mentions((known,))

    resolution = await resolver().resolve_context_block_mentions(
        [known],
        block_text_by_id={current.block_id: current.markdown},
        policy=semantic_build.policy,
        allocate_entity_id=lambda: _async_value(701),
    )

    assert resolution["entity_ids"] == (701,)
    assert resolution["block_entity_associations"][0].block_id == current.block_id


async def _async_value(value):
    return value
