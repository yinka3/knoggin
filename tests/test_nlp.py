"""
Tests for main/nlp_pipe.py.

This module validates the deterministic logic and LLM orchestration of the NLPPipeline.
It uses real spaCy (en_core_web_md) and GLiNER models (loaded once per session)
and mocks LLM interactions for orchestration testing.
"""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch

import spacy
from gliner import GLiNER

from main.nlp_pipe import NLPPipeline
from shared.config.topics_config import TopicConfig
from concurrent.futures import ThreadPoolExecutor
from main.processor import BatchProcessor
from shared.models.schema.dtypes import Fact
from datetime import datetime, timezone


# --- Session-Scoped Model Fixtures ---

@pytest.fixture(scope="session")
def spacy_model():
    return spacy.load("en_core_web_md")


@pytest.fixture(scope="session")
def gliner_model():
    return GLiNER.from_pretrained("urchade/gliner_medium-v2.1")


# --- Topic Configurations ---

TOPICS_CONFIG = {
    "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
    "Identity": {"active": True, "labels": ["person"], "hierarchy": {}, "aliases": []},
    "Work": {
        "active": True,
        "labels": ["company", "project", "role"],
        "hierarchy": {"company": ["project"]},
        "aliases": ["career", "job"],
    },
    "Education": {
        "active": True,
        "labels": ["university", "course", "professor"],
        "hierarchy": {"university": ["course"]},
        "aliases": ["school", "academic"],
    },
    "Health": {
        "active": True,
        "labels": ["doctor", "condition", "medication"],
        "hierarchy": {},
        "aliases": ["medical"],
    },
}

# Label "professor" appears in Education only.
# Label "person" appears in Identity only.
# If we add "person" to Work too, it becomes ambiguous — used in specific tests.

TOPICS_WITH_AMBIGUITY = {
    **TOPICS_CONFIG,
    "Social": {
        "active": True,
        "labels": ["person", "event"],
        "hierarchy": {},
        "aliases": [],
    },
}


# --- Pipeline and Processor Factories ---

def make_pipeline(
    spacy_model,
    gliner_model,
    topics_config=None,
    known_aliases=None,
    profiles=None,
    llm_response=None,
):
    topics_config = topics_config or TOPICS_CONFIG
    tc = TopicConfig(topics_config)

    llm = MagicMock()
    llm.call_llm = AsyncMock(return_value=llm_response)

    pipeline = NLPPipeline(
        llm=llm,
        topic_config=tc,
        get_known_aliases=lambda: known_aliases or {},
        get_profiles=lambda: profiles or {},
        gliner=gliner_model,
        spacy=spacy_model,
    )
    return pipeline, llm

def make_processor(llm_response=None):
    store = MagicMock()
    store.get_neighbor_ids.return_value = set()
    
    llm = MagicMock()
    llm.call_llm = AsyncMock(return_value=llm_response)

    resolver = MagicMock()
    nlp = MagicMock()
    redis = MagicMock()
    tc = TopicConfig({"General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []}})

    proc = BatchProcessor(
        session_id="test",
        redis_client=redis,
        llm=llm,
        ent_resolver=resolver,
        nlp_pipe=nlp,
        store=store,
        cpu_executor=ThreadPoolExecutor(max_workers=1),
        user_name="Yinka",
        topic_config=tc,
        get_next_ent_id=AsyncMock(return_value=99),
    )
    return proc, store


# --- Realistic Conversation Data ---

WORK_MESSAGES = [
    {"id": 1, "message": "I just started working at Palantir last month on the Foundry platform", "role": "user"},
    {"id": 2, "message": "My manager Priya Sharma has been really helpful with onboarding", "role": "user"},
    {"id": 3, "message": "We're using the Apollo project internally for client deployments", "role": "user"},
]

EDUCATION_MESSAGES = [
    {"id": 10, "message": "I studied computer science at Carnegie Mellon back in 2019", "role": "user"},
    {"id": 11, "message": "Professor Yejin Choi's NLP course completely changed how I think about language models", "role": "user"},
]

MIXED_MESSAGES = [
    {"id": 20, "message": "Had lunch with Marcus at the Anthropic office yesterday", "role": "user"},
    {"id": 21, "message": "He mentioned that Dr. Patel recommended a new medication for his migraines", "role": "user"},
    {"id": 22, "message": "We also talked about the Kubernetes migration at Datadog", "role": "user"},
]

EMPTY_MESSAGES = [
    {"id": 30, "message": "Yeah I think so too", "role": "user"},
    {"id": 31, "message": "Not much going on today honestly", "role": "user"},
]

KNOWN_ENTITY_MESSAGES = [
    {"id": 40, "message": "I caught up with Priya Sharma about the quarterly review", "role": "user"},
    {"id": 41, "message": "She said Palantir is restructuring the Foundry team next quarter", "role": "user"},
]


# --- Label to Topic Mapping Logic ---

class TestBuildLabelToTopics:
    """Tests the internal logic for mapping GLiNER labels to configured topics."""

    def test_labels_mapped_to_topics(self, spacy_model, gliner_model):
        pipeline, _ = make_pipeline(spacy_model, gliner_model)
        mapping = pipeline._label_to_topics

        assert mapping["company"] == ["Work"]
        assert mapping["professor"] == ["Education"]
        assert mapping["person"] == ["Identity"]

    def test_inactive_topic_excluded(self, spacy_model, gliner_model):
        config = {**TOPICS_CONFIG, "Cooking": {"active": False, "labels": ["recipe"], "hierarchy": {}, "aliases": []}}
        pipeline, _ = make_pipeline(spacy_model, gliner_model, topics_config=config)
        assert "recipe" not in pipeline._label_to_topics

    def test_ambiguous_label_maps_to_multiple(self, spacy_model, gliner_model):
        pipeline, _ = make_pipeline(spacy_model, gliner_model, topics_config=TOPICS_WITH_AMBIGUITY)
        topics = pipeline._label_to_topics["person"]
        assert "Identity" in topics
        assert "Social" in topics
        assert len(topics) == 2

    def test_empty_labels_not_in_map(self, spacy_model, gliner_model):
        pipeline, _ = make_pipeline(spacy_model, gliner_model)
        # General has no labels
        assert "general" not in pipeline._label_to_topics


# --- Topic Assignment Logic ---

class TestAssignTopic:
    """Tests how the pipeline assigns topics to labels, handling ambiguity and defaults."""

    def test_single_topic(self, spacy_model, gliner_model):
        pipeline, _ = make_pipeline(spacy_model, gliner_model)
        topic, ambiguous = pipeline._assign_topic("company")
        assert topic == "Work"
        assert ambiguous is False

    def test_ambiguous_label(self, spacy_model, gliner_model):
        pipeline, _ = make_pipeline(spacy_model, gliner_model, topics_config=TOPICS_WITH_AMBIGUITY)
        topic, ambiguous = pipeline._assign_topic("person")
        assert topic is None
        assert ambiguous is True

    def test_unknown_label_falls_to_general(self, spacy_model, gliner_model):
        pipeline, _ = make_pipeline(spacy_model, gliner_model)
        topic, ambiguous = pipeline._assign_topic("spaceship")
        assert topic == "General"
        assert ambiguous is False

    def test_empty_label_falls_to_general(self, spacy_model, gliner_model):
        pipeline, _ = make_pipeline(spacy_model, gliner_model)
        topic, ambiguous = pipeline._assign_topic("")
        assert topic == "General"
        assert ambiguous is False

    def test_no_general_returns_none(self, spacy_model, gliner_model):
        config = {k: {**v, "active": False} if k == "General" else v for k, v in TOPICS_CONFIG.items()}
        pipeline, _ = make_pipeline(spacy_model, gliner_model, topics_config=config)
        topic, ambiguous = pipeline._assign_topic("spaceship")
        assert topic is None
        assert ambiguous is False


# --- GLiNER Integration (Real Model) ---

class TestRunGliner:
    """Validates GLiNER extraction quality and filtering using the real model."""

    def test_extracts_named_entities(self, spacy_model, gliner_model):
        pipeline, _ = make_pipeline(spacy_model, gliner_model)
        results = pipeline.run_gliner("I just started working at Palantir on the Foundry platform")
        names = [span for span, _ in results]
        # Palantir should be extracted as a company
        assert any("Palantir" in n for n in names)

    def test_extracts_person(self, spacy_model, gliner_model):
        pipeline, _ = make_pipeline(spacy_model, gliner_model)
        results = pipeline.run_gliner("My manager Priya Sharma has been really helpful")
        names = [span for span, _ in results]
        assert any("Priya" in n for n in names)

    def test_filters_pronouns(self, spacy_model, gliner_model):
        pipeline, _ = make_pipeline(spacy_model, gliner_model)
        results = pipeline.run_gliner("He told me that they would handle it")
        names = [span.lower() for span, _ in results]
        for pronoun in ["he", "me", "they", "it"]:
            assert pronoun not in names

    def test_person_label_bypasses_generic_filter(self, spacy_model, gliner_model):
        """Common names labeled 'person' should not be filtered by is_generic_phrase."""
        pipeline, _ = make_pipeline(spacy_model, gliner_model)
        results = pipeline.run_gliner("I had coffee with Grace yesterday morning")
        labels = {span: label for span, label in results}
        # If GLiNER picks up Grace as person, it should survive
        if "Grace" in labels:
            assert labels["Grace"] == "person"

    def test_no_entities_in_casual_text(self, spacy_model, gliner_model):
        pipeline, _ = make_pipeline(spacy_model, gliner_model)
        results = pipeline.run_gliner("Yeah I think that sounds good, let me know")
        assert len(results) == 0

    def test_multiple_entities_same_sentence(self, spacy_model, gliner_model):
        pipeline, _ = make_pipeline(spacy_model, gliner_model)
        results = pipeline.run_gliner(
            "The Kubernetes migration at Datadog is being led by their SRE team"
        )
        names = [span for span, _ in results]
        assert len(names) >= 1  # At minimum Datadog

    def test_entity_with_title_prefix(self, spacy_model, gliner_model):
        """Entities like 'Dr. Patel' or 'Professor Chen' should survive with prefix."""
        pipeline, _ = make_pipeline(spacy_model, gliner_model)
        results = pipeline.run_gliner(
            "I had a consultation with Dr. Ramirez about the treatment plan last Thursday"
        )
        names = [span for span, _ in results]
        assert any("Ramirez" in n for n in names)

    def test_dense_entity_text(self, spacy_model, gliner_model):
        """Long text with many entities packed together."""
        pipeline, _ = make_pipeline(spacy_model, gliner_model)
        results = pipeline.run_gliner(
            "At the Bloomberg conference in Tokyo, Sarah Chen from Goldman Sachs "
            "presented alongside Raj Patel from Morgan Stanley and the keynote was "
            "delivered by Christine Lagarde from the European Central Bank"
        )
        names = [span for span, _ in results]
        # Should find at least 2-3 of the people/orgs
        assert len(names) >= 2

    def test_no_labels_returns_empty(self, spacy_model, gliner_model):
        """If topic config has no active labels, GLiNER has nothing to search for."""
        empty_config = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
        }
        pipeline, _ = make_pipeline(spacy_model, gliner_model, topics_config=empty_config)
        results = pipeline.run_gliner("Elon Musk just acquired Twitter for billions")
        assert results == []

    def test_lower_threshold_extracts_more(self, spacy_model, gliner_model):
        """Lower GLiNER threshold should surface more candidate entities."""
        text = "I had coffee with Grace at the new place on Market Street"

        pipeline_strict, _ = make_pipeline(spacy_model, gliner_model)
        pipeline_loose, _ = make_pipeline(spacy_model, gliner_model)
        pipeline_strict.gliner_threshold = 0.90
        pipeline_loose.gliner_threshold = 0.50

        strict_results = pipeline_strict.run_gliner(text)
        loose_results = pipeline_loose.run_gliner(text)

        assert len(loose_results) >= len(strict_results)

    def test_very_high_threshold_extracts_fewer(self, spacy_model, gliner_model):
        """Very high threshold should filter out low-confidence entities."""
        text = "Working at Palantir on the Foundry platform with Priya Sharma"

        pipeline, _ = make_pipeline(spacy_model, gliner_model)
        pipeline.gliner_threshold = 0.99
        results = pipeline.run_gliner(text)

        assert len(results) <= 3


# --- PhraseMatcher Integration (Real spaCy) ---

class TestPhraseMatcher:
    """Tests exact phrase matching against known entity aliases using spaCy."""

    def test_matches_known_aliases(self, spacy_model, gliner_model):
        known = {"priya sharma": 2, "palantir": 3}
        pipeline, _ = make_pipeline(spacy_model, gliner_model, known_aliases=known)
        matcher, aliases = pipeline._build_phrase_matcher()

        doc = pipeline._nlp("I talked to Priya Sharma about Palantir yesterday")
        matches = [(doc[start:end].text, aliases.get(doc[start:end].text.lower())) for _, start, end in matcher(doc)]

        matched_names = [name.lower() for name, _ in matches]
        assert "priya sharma" in matched_names
        assert "palantir" in matched_names

    def test_case_insensitive_matching(self, spacy_model, gliner_model):
        known = {"anthropic": 5}
        pipeline, _ = make_pipeline(spacy_model, gliner_model, known_aliases=known)
        matcher, aliases = pipeline._build_phrase_matcher()

        doc = pipeline._nlp("Working at ANTHROPIC has been great")
        matches = [doc[start:end].text for _, start, end in matcher(doc)]
        assert any("ANTHROPIC" in m for m in matches)

    def test_empty_aliases(self, spacy_model, gliner_model):
        pipeline, _ = make_pipeline(spacy_model, gliner_model, known_aliases={})
        matcher, aliases = pipeline._build_phrase_matcher()

        doc = pipeline._nlp("Just some random text about nothing")
        matches = list(matcher(doc))
        assert len(matches) == 0

    def test_multi_word_alias_partial_no_match(self, spacy_model, gliner_model):
        """'Priya' alone should NOT match the alias 'priya sharma'."""
        known = {"priya sharma": 2}
        pipeline, _ = make_pipeline(spacy_model, gliner_model, known_aliases=known)
        matcher, aliases = pipeline._build_phrase_matcher()

        doc = pipeline._nlp("I talked to Priya about the project yesterday")
        matches = [doc[start:end].text for _, start, end in matcher(doc)]
        assert len(matches) == 0

    def test_alias_not_substring_matched_in_longer_word(self, spacy_model, gliner_model):
        """Alias 'art' should NOT match inside 'Arthur' — PhraseMatcher uses token boundaries."""
        known = {"art": 5}
        pipeline, _ = make_pipeline(spacy_model, gliner_model, known_aliases=known)
        matcher, aliases = pipeline._build_phrase_matcher()

        doc = pipeline._nlp("Arthur went to the gallery to see the new exhibit")
        matched_texts = [doc[start:end].text.lower() for _, start, end in matcher(doc)]
        assert "arthur" not in matched_texts

    def test_multiple_aliases_same_entity(self, spacy_model, gliner_model):
        """Multiple aliases for the same entity should all match independently."""
        known = {"palantir": 3, "palantir technologies": 3}
        pipeline, _ = make_pipeline(spacy_model, gliner_model, known_aliases=known)
        matcher, aliases = pipeline._build_phrase_matcher()

        doc = pipeline._nlp("Palantir Technologies announced earnings and Palantir stock jumped")
        match_ids = [aliases.get(doc[start:end].text.lower()) for _, start, end in matcher(doc)]
        assert all(eid == 3 for eid in match_ids if eid)
        assert len(match_ids) >= 2


# --- Extract Mentions Orchestration ---

class TestExtractMentionsMocked:
    """Tests the full extraction pipeline with mocked LLM calls."""

    @pytest.mark.asyncio
    async def test_known_entities_prioritized(self, spacy_model, gliner_model):
        """Known entities from PhraseMatcher should appear in output and
        prevent duplicate extraction by GLiNER for the same spans."""
        known = {"priya sharma": 2, "palantir": 3}
        profiles = {
            2: {"canonical_name": "Priya Sharma", "type": "person", "topic": "Identity"},
            3: {"canonical_name": "Palantir", "type": "company", "topic": "Work"},
        }

        llm_response = "<entities>\n</entities>"
        pipeline, llm = make_pipeline(
            spacy_model, gliner_model,
            known_aliases=known, profiles=profiles,
            llm_response=llm_response,
        )

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", KNOWN_ENTITY_MESSAGES, "test-session")

        names = [name for _, name, _, _ in results]
        assert "Priya Sharma" in names or "priya sharma" in [n.lower() for n in names]
        assert "Palantir" in names or "palantir" in [n.lower() for n in names]

    @pytest.mark.asyncio
    async def test_gliner_entities_in_output(self, spacy_model, gliner_model):
        """GLiNER-detected entities should appear when no known aliases cover them."""
        llm_response = "<entities>\n</entities>"
        pipeline, _ = make_pipeline(spacy_model, gliner_model, llm_response=llm_response)

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", WORK_MESSAGES, "test-session")

        names_lower = [name.lower() for _, name, _, _ in results]
        # GLiNER should find at least Palantir or Priya from the work messages
        assert any("palantir" in n for n in names_lower) or any("priya" in n for n in names_lower)

    @pytest.mark.asyncio
    async def test_vp01_catches_missed_entities(self, spacy_model, gliner_model):
        """VP-01 LLM should discover entities that PhraseMatcher and GLiNER missed."""
        llm_response = """
        <entities>
        10 | Carnegie Mellon | university | Education | 0.95
        11 | Yejin Choi | professor | Education | 0.92
        </entities>
        """
        pipeline, _ = make_pipeline(spacy_model, gliner_model, llm_response=llm_response)

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", EDUCATION_MESSAGES, "test-session")

        names_lower = [name.lower() for _, name, _, _ in results]
        assert "carnegie mellon" in names_lower
        assert "yejin choi" in names_lower

    @pytest.mark.asyncio
    async def test_vp01_validates_entities(self, spacy_model, gliner_model):
        """VP-01 entities that fail validate_entity should be filtered out."""
        llm_response = """
        <entities>
        1 | Palantir | company | Work | 0.95
        1 | he | person | Identity | 0.9
        1 | 42 | thing | General | 0.85
        </entities>
        """
        pipeline, _ = make_pipeline(spacy_model, gliner_model, llm_response=llm_response)

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", WORK_MESSAGES[:1], "test-session")

        names_lower = [name.lower() for _, name, _, _ in results]
        assert "he" not in names_lower
        assert "42" not in names_lower

    @pytest.mark.asyncio
    async def test_empty_messages_returns_empty(self, spacy_model, gliner_model):
        pipeline, _ = make_pipeline(spacy_model, gliner_model, llm_response=None)

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", [], "test-session")

        assert results == []

    @pytest.mark.asyncio
    async def test_casual_messages_minimal_extraction(self, spacy_model, gliner_model):
        """Messages with no real entities should produce few or no extractions."""
        llm_response = "<entities>\n</entities>"
        pipeline, _ = make_pipeline(spacy_model, gliner_model, llm_response=llm_response)

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", EMPTY_MESSAGES, "test-session")

        assert len(results) <= 1  # might pick up nothing, or at most one false positive

    @pytest.mark.asyncio
    async def test_known_covers_gliner_dedup(self, spacy_model, gliner_model):
        """If PhraseMatcher already matched 'Palantir', GLiNER shouldn't duplicate it."""
        known = {"palantir": 3}
        profiles = {3: {"canonical_name": "Palantir", "type": "company", "topic": "Work"}}
        llm_response = "<entities>\n</entities>"

        pipeline, _ = make_pipeline(
            spacy_model, gliner_model,
            known_aliases=known, profiles=profiles,
            llm_response=llm_response,
        )

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", WORK_MESSAGES[:1], "test-session")

        palantir_mentions = [r for r in results if "palantir" in r[1].lower()]
        # Should appear once (from known), not twice
        assert len(palantir_mentions) == 1

    @pytest.mark.asyncio
    async def test_mixed_conversation_extracts_across_topics(self, spacy_model, gliner_model):
        """A conversation spanning Work + Health should extract entities from both."""
        llm_response = """
        <entities>
        20 | Marcus | person | Identity | 0.9
        20 | Anthropic | company | Work | 0.95
        21 | Dr. Patel | doctor | Health | 0.9
        22 | Datadog | company | Work | 0.92
        </entities>
        """
        pipeline, _ = make_pipeline(spacy_model, gliner_model, llm_response=llm_response)

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", MIXED_MESSAGES, "test-session")

        names_lower = [name.lower() for _, name, _, _ in results]
        topics = [topic for _, _, _, topic in results]

        assert "anthropic" in names_lower or "datadog" in names_lower
        assert "dr. patel" in names_lower
        assert "Work" in topics or "Health" in topics

    @pytest.mark.asyncio
    async def test_llm_returns_none(self, spacy_model, gliner_model):
        """If LLM returns None (timeout/error), pipeline should still return
        PhraseMatcher + GLiNER results without crashing."""
        pipeline, _ = make_pipeline(spacy_model, gliner_model, llm_response=None)

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", WORK_MESSAGES, "test-session")

        # Should still have GLiNER results, just no VP-01 additions
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_vp01_below_confidence_filtered(self, spacy_model, gliner_model):
        """VP-01 entities below min_confidence (0.8) should be dropped."""
        llm_response = """
        <entities>
        1 | Palantir | company | Work | 0.95
        2 | Maybe Corp | company | Work | 0.5
        </entities>
        """
        pipeline, _ = make_pipeline(spacy_model, gliner_model, llm_response=llm_response)

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", WORK_MESSAGES[:2], "test-session")

        names_lower = [name.lower() for _, name, _, _ in results]
        assert "maybe corp" not in names_lower

    @pytest.mark.asyncio
    async def test_user_name_not_extracted(self, spacy_model, gliner_model):
        """The user's own name should not appear as an entity — VP-01 prompt
        instructs this, and if GLiNER catches it, it should be treated as known."""
        messages = [{"id": 50, "message": "I'm Yinka and I work at Anthropic", "role": "user"}]
        llm_response = "<entities>\n50 | Anthropic | company | Work | 0.95\n</entities>"

        pipeline, _ = make_pipeline(spacy_model, gliner_model, llm_response=llm_response)

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", messages, "test-session")

        names_lower = [name.lower() for _, name, _, _ in results]
        # VP-01 prompt says don't extract user name. If it leaks, test catches it.
        assert "yinka" not in names_lower

    @pytest.mark.asyncio
    async def test_vp01_recovers_gliner_filtered_entity(self, spacy_model, gliner_model):
        """If GLiNER filters a valid entity (e.g. common name without person label),
        VP-01 should recover it and the pipeline should include it in output."""
        messages = [
            {"id": 1, "message": (
                "I went hiking with Grace last weekend at Mount Tamalpais, "
                "she's training for a ultramarathon and wanted to test her endurance "
                "on some real elevation gain before the race next month"
            ), "role": "user"},
        ]
        # VP-01 recovers Grace as a person
        llm_response = """
        <entities>
        1 | Grace | person | Identity | 0.9
        1 | Mount Tamalpais | location | General | 0.88
        </entities>
        """
        pipeline, _ = make_pipeline(spacy_model, gliner_model, llm_response=llm_response)

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", messages, "test-session")

        names_lower = [name.lower() for _, name, _, _ in results]
        assert "grace" in names_lower

    @pytest.mark.asyncio
    async def test_ambiguous_topic_sent_to_vp01(self, spacy_model, gliner_model):
        """When a label maps to multiple topics, the entity should be sent to VP-01
        as ambiguous for resolution. VP-01 picks the correct topic."""
        messages = [
            {"id": 1, "message": (
                "Marcus invited me to the charity gala next Friday, he's organizing "
                "the whole event with his team at the community center downtown"
            ), "role": "user"},
        ]
        # "person" is ambiguous in TOPICS_WITH_AMBIGUITY (Identity + Social)
        # VP-01 should resolve Marcus to the correct topic
        llm_response = """
        <entities>
        1 | Marcus | person | Identity | 0.92
        </entities>
        """
        pipeline, _ = make_pipeline(
            spacy_model, gliner_model,
            topics_config=TOPICS_WITH_AMBIGUITY,
            llm_response=llm_response,
        )

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", messages, "test-session")

        marcus = [r for r in results if "marcus" in r[1].lower()]
        assert len(marcus) >= 1

    @pytest.mark.asyncio
    async def test_same_entity_different_references(self, spacy_model, gliner_model):
        """Same person referenced as 'Dr. Okonkwo', 'Okonkwo', and 'she' across messages.
        Pipeline should extract the named references, not pronouns."""
        messages = [
            {"id": 1, "message": (
                "Had my appointment with Dr. Okonkwo this morning, she spent about "
                "thirty minutes reviewing my bloodwork results and going over the options"
            ), "role": "user"},
            {"id": 2, "message": (
                "Okonkwo said the levels are mostly fine but wants to recheck in a month, "
                "she's also referring me to an endocrinologist just to be safe"
            ), "role": "user"},
        ]
        llm_response = """
        <entities>
        1 | Dr. Okonkwo | doctor | Health | 0.95
        </entities>
        """
        pipeline, _ = make_pipeline(spacy_model, gliner_model, llm_response=llm_response)

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", messages, "test-session")

        names_lower = [name.lower() for _, name, _, _ in results]
        # Should have Okonkwo references, never "she"
        assert any("okonkwo" in n for n in names_lower)
        assert "she" not in names_lower

    @pytest.mark.asyncio
    async def test_known_entity_resolved_across_messages(self, spacy_model, gliner_model):
        """A known entity mentioned in multiple messages should appear for each message."""
        known = {"palantir": 3}
        profiles = {3: {"canonical_name": "Palantir", "type": "company", "topic": "Work"}}
        messages = [
            {"id": 1, "message": (
                "The onboarding process at Palantir has been pretty smooth so far, "
                "they gave me a buddy to help navigate the internal tools and codebase"
            ), "role": "user"},
            {"id": 2, "message": (
                "Palantir's engineering blog had a great post about their deployment pipeline, "
                "I'm trying to understand how Foundry handles multi-tenant isolation"
            ), "role": "user"},
            {"id": 3, "message": (
                "Totally unrelated but I need to book flights for the holidays, "
                "thinking about visiting my parents in Lagos for two weeks"
            ), "role": "user"},
        ]
        llm_response = "<entities>\n</entities>"
        pipeline, _ = make_pipeline(
            spacy_model, gliner_model,
            known_aliases=known, profiles=profiles,
            llm_response=llm_response,
        )

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", messages, "test-session")

        palantir_msg_ids = [msg_id for msg_id, name, _, _ in results if "palantir" in name.lower()]
        # Should appear for msg 1 and 2, not msg 3
        assert 1 in palantir_msg_ids
        assert 2 in palantir_msg_ids
        assert 3 not in palantir_msg_ids

    @pytest.mark.asyncio
    async def test_known_entity_covers_gliner_substring(self, spacy_model, gliner_model):
        """Known entity 'Alice Johnson' should prevent GLiNER from also extracting 
        'Alice' as a separate entity — the is_covered check handles this."""
        known = {"alice johnson": 1, "alice": 1}
        profiles = {1: {"canonical_name": "Alice Johnson", "type": "person", "topic": "Identity"}}
        messages = [
            {"id": 1, "message": (
                "Alice Johnson and I worked on the proposal together last night, "
                "Alice handled the financial projections while I did the technical section"
            ), "role": "user"},
        ]
        llm_response = "<entities>\n</entities>"
        pipeline, _ = make_pipeline(
            spacy_model, gliner_model,
            known_aliases=known, profiles=profiles,
            llm_response=llm_response,
        )

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", messages, "test-session")

        # All mentions should resolve to entity 1, no duplicate "Alice" as separate entity
        alice_results = [r for r in results if "alice" in r[1].lower()]
        entity_types = set(r[2] for r in alice_results)
        # All should be "person" from the known profile, not a separate extraction
        assert entity_types == {"person"}

    @pytest.mark.asyncio
    async def test_large_batch_stress(self, spacy_model, gliner_model):
        """Eight messages with diverse entities — pipeline should handle without error."""
        messages = [
            {"id": 1, "message": (
                "Started the morning with a standup call with the Figma design team, "
                "Lucia walked us through the new component library she's been building"
            ), "role": "user"},
            {"id": 2, "message": (
                "Then I had a one-on-one with my manager Priya about the Q3 roadmap, "
                "she wants me to take ownership of the search infrastructure overhaul"
            ), "role": "user"},
            {"id": 3, "message": (
                "Lunch with Kenji at the ramen place near the office, he was telling me "
                "about some drama at Notion between the product and engineering teams"
            ), "role": "user"},
            {"id": 4, "message": (
                "Spent the afternoon debugging a particularly nasty race condition in the "
                "Redis pub/sub layer that only shows up under high concurrency"
            ), "role": "user"},
            {"id": 5, "message": (
                "Had a call with the Stripe integration team about webhook reliability, "
                "their engineer Amanda walked me through their retry architecture"
            ), "role": "user"},
            {"id": 6, "message": (
                "Dr. Hassan from the company wellness program did a presentation on "
                "managing stress during crunch periods which was actually pretty useful"
            ), "role": "user"},
            {"id": 7, "message": (
                "After work I went to the climbing gym with Marcus, he's been training "
                "for a competition at Brooklyn Boulders next month"
            ), "role": "user"},
            {"id": 8, "message": (
                "Before bed I spent an hour working through a LeetCode problem that "
                "Chen Wei recommended, something about dynamic programming on trees"
            ), "role": "user"},
        ]
        llm_response = """
        <entities>
        1 | Lucia | person | Identity | 0.9
        2 | Priya | person | Identity | 0.9
        3 | Kenji | person | Identity | 0.9
        3 | Notion | company | Work | 0.92
        5 | Amanda | person | Identity | 0.88
        5 | Stripe | company | Work | 0.95
        6 | Dr. Hassan | person | Identity | 0.9
        7 | Marcus | person | Identity | 0.9
        7 | Brooklyn Boulders | location | General | 0.85
        8 | Chen Wei | person | Identity | 0.9
        </entities>
        """
        pipeline, _ = make_pipeline(spacy_model, gliner_model, llm_response=llm_response)

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", messages, "test-session")

        names_lower = [name.lower() for _, name, _, _ in results]
        # Should extract at least 5+ distinct entities from this dense batch
        unique_names = set(names_lower)
        assert len(unique_names) >= 5, f"Only extracted {len(unique_names)} unique entities from 8 messages"

        # Spot check key entities
        assert any("priya" in n for n in names_lower)
        assert any("stripe" in n for n in names_lower)
        assert any("marcus" in n for n in names_lower)

    @pytest.mark.asyncio
    async def test_refresh_topic_mappings(self, spacy_model, gliner_model):
        """After updating topic config, refreshing mappings should reflect changes."""
        pipeline, _ = make_pipeline(spacy_model, gliner_model)

        assert "recipe" not in pipeline._label_to_topics

        # Simulate adding a new topic at runtime
        pipeline.topic_config.update({
            **TOPICS_CONFIG,
            "Cooking": {"active": True, "labels": ["recipe", "ingredient"], "hierarchy": {}, "aliases": []},
        })
        pipeline.refresh_topic_mappings()

        assert "recipe" in pipeline._label_to_topics
        assert pipeline._label_to_topics["recipe"] == ["Cooking"]

    @pytest.mark.asyncio
    async def test_assistant_messages_processed(self, spacy_model, gliner_model):
        """Assistant messages should also be scanned for entities."""
        messages = [
            {"id": 1, "message": "Do you know anything about Palantir?", "role": "user"},
            {"id": 2, "message": "Palantir is a data analytics company founded by Peter Thiel.", "role": "assistant"},
        ]
        llm_response = """
        <entities>
        2 | Peter Thiel | person | Identity | 0.92
        </entities>
        """
        pipeline, _ = make_pipeline(spacy_model, gliner_model, llm_response=llm_response)

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", messages, "test-session")

        names_lower = [name.lower() for _, name, _, _ in results]
        # Should extract entities from both user and assistant messages
        assert any("palantir" in n for n in names_lower)

    @pytest.mark.asyncio
    async def test_mixed_roles_no_crash(self, spacy_model, gliner_model):
        """Batch with alternating user/assistant roles should process without error."""
        messages = [
            {"id": 1, "message": "I started working at Anthropic last week", "role": "user"},
            {"id": 2, "message": "That's great! Anthropic does important AI safety work.", "role": "assistant"},
            {"id": 3, "message": "Yeah, my manager Dario has been really welcoming", "role": "user"},
            {"id": 4, "message": "Dario Amodei is the CEO. What team are you on?", "role": "assistant"},
        ]
        llm_response = """
        <entities>
        1 | Anthropic | company | Work | 0.95
        3 | Dario | person | Identity | 0.9
        </entities>
        """
        pipeline, _ = make_pipeline(spacy_model, gliner_model, llm_response=llm_response)

        with patch("main.nlp_pipe.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", messages, "test-session")

        assert isinstance(results, list)
        names_lower = [name.lower() for _, name, _, _ in results]
        assert any("anthropic" in n for n in names_lower)
