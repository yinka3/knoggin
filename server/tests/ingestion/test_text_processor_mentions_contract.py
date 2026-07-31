import pytest
from pydantic import ValidationError

from common.conf.topics_config import TopicConfig
from common.exceptions import LLMProviderError
from common.schema.ingestion.contracts import ExtractionTrace
from common.schema.ingestion.extraction import EntityExtraction, EntityMention
from common.schema.settings import TextProcessorSettings, TopicSchema
from core.ingestion.batch import IngestionBatch
from core.ingestion.services.processor import TextProcessor
from core.knowledge.entity.profile import EntityProfile
from tests.fixtures.factories import make_topic_config

MESSAGES = [
    {
        "id": 1,
        "message": "Alice is working with Bob on the Knoggin project.",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "role": "user",
    },
    {
        "id": 2,
        "message": "Linear helps Alice track project work.",
        "timestamp": "2026-01-01T00:01:00+00:00",
        "role": "user",
    },
]


class FakeSpan:
    def __init__(self, text):
        self.text = text


class FakeDoc:
    def __init__(self, text):
        self.text = text

    def __getitem__(self, item):
        if isinstance(item, slice) and isinstance(item.start, str):
            return FakeSpan(item.start)
        return FakeSpan("")


class FakeNLP:
    vocab = object()

    def __call__(self, text):
        return FakeDoc(text)

    def make_doc(self, text):
        return FakeDoc(text)


class FakeMatcher:
    def __init__(self, matches_by_text):
        self.matches_by_text = matches_by_text

    def __call__(self, doc):
        return [
            ("KNOWN", span_text, None)
            for span_text in self.matches_by_text.get(doc.text, [])
        ]


class RecordingPhraseMatcher:
    instances = []

    def __init__(self, vocab, attr):
        self.vocab = vocab
        self.attr = attr
        self.patterns = []
        RecordingPhraseMatcher.instances.append(self)

    def add(self, label, patterns):
        self.patterns.extend(pattern.text for pattern in patterns)


class FakeLLM:
    extraction_model = "fake-ner-model"

    def __init__(self, response=None, *, raise_error=False):
        self.response = response if response is not None else EntityExtraction()
        self.raise_error = raise_error
        self.calls = []

    async def generate_structured(self, **kwargs):
        self.calls.append(kwargs)
        if self.raise_error:
            raise LLMProviderError("fake ner failure")
        return self.response


def make_topic_config_with_tools():
    return TopicConfig(
        {
            **make_topic_config().raw,
            "Tools": TopicSchema(
                active=True,
                labels=["tool"],
                aliases=["apps"],
            ),
        }
    )


def make_entity(
    name,
    *,
    msg_id="m1",
    typ="project",
    topic="General",
    confidence=0.95,
):
    return EntityMention(
        msg_id=msg_id,
        name=name,
        type=typ,
        topic=topic,
        confidence=confidence,
    )


def make_profile(canonical_name, *, typ="person", topic="Identity"):
    return EntityProfile(canonical_name=canonical_name, entity_type=typ, topic=topic)


def make_processor(
    *,
    known_aliases=None,
    profiles=None,
    known_matches=None,
    gliner_matches=None,
    llm_response=None,
    llm_raises=False,
    topic_config=None,
    llm_ner=False,
):
    known_aliases = known_aliases or {}
    profiles = profiles or {}
    known_matches = known_matches or {}
    gliner_matches = gliner_matches or {}
    llm = FakeLLM(llm_response, raise_error=llm_raises)
    alias_version = 0

    async def get_profile(entity_id):
        return profiles.get(entity_id)

    processor = TextProcessor(
        llm=llm,
        topic_config=topic_config or make_topic_config_with_tools(),
        get_known_aliases=lambda: known_aliases,
        get_alias_version=lambda: alias_version,
        get_profile=get_profile,
        gliner=object(),
        spacy=FakeNLP(),
        settings=TextProcessorSettings(llm_ner=llm_ner),
    )

    processor._build_phrase_matcher = lambda: (
        FakeMatcher(known_matches),
        known_aliases,
    )
    processor.run_gliner = lambda text: list(gliner_matches.get(text, []))
    return processor, llm


async def extract(processor, *, messages=None, trace=None, issues=None):
    batch = IngestionBatch.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        messages=MESSAGES if messages is None else messages,
        session_text="",
    )
    if trace is not None:
        batch.trace = trace
    if issues is not None:
        batch.issues = issues
    return await processor.extract_mentions(batch)


@pytest.mark.ingestion
@pytest.mark.no_network
def test_build_phrase_matcher_reuses_cache_until_alias_version_changes(monkeypatch):
    known_aliases = {" Bob ": 102}
    alias_version = 1

    async def get_profile(_entity_id):
        return None

    monkeypatch.setattr(
        "core.ingestion.services.processor.PhraseMatcher",
        RecordingPhraseMatcher,
    )
    RecordingPhraseMatcher.instances.clear()

    processor = TextProcessor(
        llm=FakeLLM(),
        topic_config=make_topic_config_with_tools(),
        get_known_aliases=lambda: known_aliases,
        get_alias_version=lambda: alias_version,
        get_profile=get_profile,
        gliner=object(),
        spacy=FakeNLP(),
        settings=TextProcessorSettings(),
    )

    first = processor._build_phrase_matcher()
    second = processor._build_phrase_matcher()

    assert first is second
    assert len(RecordingPhraseMatcher.instances) == 1
    assert RecordingPhraseMatcher.instances[0].patterns == ["bob"]

    known_aliases["bobby"] = 102
    alias_version = 2

    third = processor._build_phrase_matcher()

    assert third is not first
    assert len(RecordingPhraseMatcher.instances) == 2
    assert RecordingPhraseMatcher.instances[1].patterns == ["bob", "bobby"]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_empty_messages_returns_empty():
    processor, llm = make_processor()
    trace = ExtractionTrace()

    result = await extract(processor, messages=[], trace=trace, issues=[])

    assert result == []
    assert llm.calls == []
    assert trace == ExtractionTrace()


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_known_aliases_are_highest_priority():
    processor, _ = make_processor(
        known_aliases={"bob": 102},
        profiles={102: make_profile("Robert Chen")},
        known_matches={MESSAGES[0]["message"]: ["Bob"]},
    )
    trace = ExtractionTrace()

    result = await extract(processor, trace=trace, issues=[])

    assert (1, "Bob", "person", "Identity") in result
    assert trace.known_mentions == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_known_alias_dedupes_duplicate_matches():
    processor, _ = make_processor(
        known_aliases={"bob": 102},
        profiles={102: make_profile("Robert Chen")},
        known_matches={MESSAGES[0]["message"]: ["Bob", "Bob"]},
    )

    result = await extract(processor, trace=ExtractionTrace(), issues=[])

    assert result.count((1, "Bob", "person", "Identity")) == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_known_alias_missing_profile_records_issue_and_skips():
    processor, _ = make_processor(
        known_aliases={"bob": 102},
        known_matches={MESSAGES[0]["message"]: ["Bob"]},
    )
    issues = []

    result = await extract(processor, trace=ExtractionTrace(), issues=issues)

    assert result == []
    assert [issue.code for issue in issues] == ["known_alias_profile_missing"]
    assert issues[0].metadata == {"entity_id": 102, "msg_id": 1}


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_gliner_accepts_valid_labeled_mentions():
    processor, _ = make_processor(
        gliner_matches={MESSAGES[0]["message"]: [("Linear", "tool")]},
    )
    trace = ExtractionTrace()

    result = await extract(processor, trace=trace, issues=[])

    assert (1, "Linear", "tool", "Tools") in result
    assert trace.gliner_raw_mentions == 1
    assert trace.gliner_accepted_mentions == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_gliner_skips_label_without_active_topic():
    processor, _ = make_processor(
        gliner_matches={MESSAGES[0]["message"]: [("Knoggin", "project")]},
    )
    trace = ExtractionTrace()

    result = await extract(processor, trace=trace, issues=[])

    assert result == []
    assert trace.gliner_raw_mentions == 1
    assert trace.gliner_accepted_mentions == 0


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_gliner_skips_spans_covered_by_known_aliases():
    processor, _ = make_processor(
        known_aliases={"bob": 102},
        profiles={102: make_profile("Robert Chen")},
        known_matches={MESSAGES[0]["message"]: ["Bob"]},
        gliner_matches={MESSAGES[0]["message"]: [("Bob", "person")]},
    )
    trace = ExtractionTrace()

    result = await extract(processor, trace=trace, issues=[])

    assert result.count((1, "Bob", "person", "Identity")) == 1
    assert trace.gliner_raw_mentions == 1
    assert trace.gliner_accepted_mentions == 0


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_gliner_filters_invalid_generic_or_pronoun_mentions():
    processor, _ = make_processor(
        gliner_matches={MESSAGES[0]["message"]: [("he", "person"), ("the", "General")]},
    )
    trace = ExtractionTrace()

    result = await extract(processor, trace=trace, issues=[])

    assert result == []
    assert trace.gliner_raw_mentions == 2
    assert trace.gliner_accepted_mentions == 0


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_llm_disabled_returns_known_and_gliner_only():
    processor, llm = make_processor(
        known_aliases={"bob": 102},
        profiles={102: make_profile("Robert Chen")},
        known_matches={MESSAGES[0]["message"]: ["Bob"]},
        gliner_matches={MESSAGES[0]["message"]: [("Linear", "tool")]},
    )
    trace = ExtractionTrace()

    result = await extract(processor, trace=trace, issues=[])

    assert result == [
        (1, "Bob", "person", "Identity"),
        (1, "Linear", "tool", "Tools"),
    ]
    assert llm.calls == []
    assert trace.fallbacks == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_llm_failure_falls_back_and_records_issue():
    processor, _ = make_processor(
        gliner_matches={MESSAGES[0]["message"]: [("Linear", "tool")]},
        llm_raises=True,
        llm_ner=True,
    )
    trace = ExtractionTrace()
    issues = []

    result = await extract(processor, trace=trace, issues=issues)

    assert result == [(1, "Linear", "tool", "Tools")]
    assert trace.fallbacks == [
        {
            "stage": "ner",
            "fallback": "empty_mentions",
            "error_code": "llm_provider_error",
        }
    ]
    assert [issue.code for issue in issues] == ["llm_extraction_failed"]
    assert issues[0].metadata["error_code"] == "llm_provider_error"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_empty_llm_result_records_known_gliner_fallback():
    processor, _ = make_processor(
        gliner_matches={MESSAGES[0]["message"]: [("Linear", "tool")]},
        llm_response=EntityExtraction(mentions=[]),
        llm_ner=True,
    )
    trace = ExtractionTrace()
    issues = []

    result = await extract(processor, trace=trace, issues=issues)

    assert result == [(1, "Linear", "tool", "Tools")]
    assert trace.fallbacks == []
    assert issues == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_accepts_valid_llm_mentions():
    processor, _ = make_processor(
        llm_response=EntityExtraction(
            mentions=[make_entity("Linear", msg_id="m2", typ="tool", topic="Tools")]
        ),
        llm_ner=True,
    )
    trace = ExtractionTrace()

    result = await extract(processor, trace=trace, issues=[])

    assert (2, "Linear", "tool", "Tools") in result
    assert trace.llm_mentions_seen == 1
    assert trace.llm_mentions_accepted == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_resolves_local_llm_msg_id_to_real_message_id():
    messages = [
        {**MESSAGES[0], "id": 41},
        {**MESSAGES[1], "id": 99},
    ]
    processor, llm = make_processor(
        llm_response=EntityExtraction(
            mentions=[make_entity("Linear", msg_id="m2", typ="tool", topic="Tools")]
        ),
        llm_ner=True,
    )

    result = await extract(
        processor,
        messages=messages,
        trace=ExtractionTrace(),
        issues=[],
    )

    assert result == [(99, "Linear", "tool", "Tools")]
    assert "[MSG m1]" in llm.calls[0]["user"]
    assert "[MSG m2]" in llm.calls[0]["user"]
    assert "[MSG 41]" not in llm.calls[0]["user"]
    assert "[MSG 99]" not in llm.calls[0]["user"]


@pytest.mark.ingestion
@pytest.mark.no_network
def test_ner_result_requires_a_local_message_reference():
    with pytest.raises(ValidationError):
        EntityExtraction.model_validate(
            {
                "mentions": [
                    {
                        "msg_id": 1,
                        "name": "Linear",
                        "type": "tool",
                        "topic": "Tools",
                        "confidence": 0.95,
                    }
                ]
            }
        )


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_rejects_invalid_llm_msg_id():
    processor, _ = make_processor(
        llm_response=EntityExtraction(mentions=[make_entity("Linear", msg_id="m999")]),
        llm_ner=True,
    )
    trace = ExtractionTrace()
    issues = []

    result = await extract(processor, trace=trace, issues=issues)

    assert result == []
    assert trace.llm_mentions_seen == 1
    assert trace.llm_mentions_rejected == 1
    assert [issue.code for issue in issues] == ["invalid_msg_id"]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_rejects_low_confidence_llm_mentions():
    processor, _ = make_processor(
        llm_response=EntityExtraction(
            mentions=[make_entity("Linear", msg_id="m2", confidence=0.5)]
        ),
        llm_ner=True,
    )
    trace = ExtractionTrace()
    issues = []

    result = await extract(processor, trace=trace, issues=issues)

    assert result == []
    assert trace.llm_mentions_rejected == 1
    assert issues[0].code == "low_confidence"
    assert issues[0].severity == "info"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_rejects_duplicate_llm_mentions_already_covered():
    processor, _ = make_processor(
        known_aliases={"alice": 101},
        profiles={101: make_profile("Alice")},
        known_matches={MESSAGES[0]["message"]: ["Alice"]},
        llm_response=EntityExtraction(
            mentions=[make_entity("Alice", msg_id="m1", typ="person", topic="Identity")]
        ),
        llm_ner=True,
    )
    trace = ExtractionTrace()
    issues = []

    result = await extract(processor, trace=trace, issues=issues)

    assert result == [(1, "Alice", "person", "Identity")]
    assert trace.llm_mentions_rejected == 1
    assert [issue.code for issue in issues] == ["duplicate_mention"]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_does_not_expose_known_entity_ids_to_llm():
    processor, llm = make_processor(
        known_aliases={"alice": 101},
        profiles={101: make_profile("Alice")},
        known_matches={MESSAGES[0]["message"]: ["Alice"]},
        llm_response=EntityExtraction(),
        llm_ner=True,
    )

    await extract(processor, trace=ExtractionTrace(), issues=[])

    prompt = llm.calls[0]["user"]
    assert "entity_id=101" not in prompt
    assert '"Alice" — already known; do not return it' in prompt


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_rejects_invalid_llm_entity():
    processor, _ = make_processor(
        llm_response=EntityExtraction(
            mentions=[
                make_entity(
                    "Mystery",
                    msg_id="m1",
                    typ="concept",
                    topic="ImpossibleTopic",
                )
            ]
        ),
        llm_ner=True,
    )
    trace = ExtractionTrace()
    issues = []

    result = await extract(processor, trace=trace, issues=issues)

    assert result == []
    assert trace.llm_mentions_rejected == 1
    assert [issue.code for issue in issues] == ["invalid_entity"]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_mentions_uses_named_ner_prompt_with_user_name():
    processor, llm = make_processor(
        llm_response=EntityExtraction(),
        llm_ner=True,
    )

    await extract(processor, trace=ExtractionTrace(), issues=[])

    assert "VEGAPUNK-01" in llm.calls[0]["system"]
    assert "ada" in llm.calls[0]["system"]
    assert llm.calls[0]["temperature"] == 0.0
    assert llm.calls[0]["response_model"] is EntityExtraction
