"""
Live LLM tests for VP-01 extraction quality.

Requires OPENROUTER_API_KEY.
"""

import os
import pytest
from unittest.mock import patch, AsyncMock
from dotenv import load_dotenv

import spacy
from gliner import GLiNER

from src.core.nlp import NLPPipeline
from src.common.config.topics_config import TopicConfig
from src.common.services.llm_service import LLMService


load_dotenv()

pytestmark = pytest.mark.skipif(
    not os.environ.get("OPENROUTER_API_KEY"),
    reason="OPENROUTER_API_KEY not set",
)


# --- Fixtures ---

@pytest.fixture(scope="session")
def spacy_model():
    return spacy.load("en_core_web_md")


@pytest.fixture(scope="session")
def gliner_model():
    return GLiNER.from_pretrained("urchade/gliner_medium-v2.1")


@pytest.fixture(scope="session")
def llm_service():
    return LLMService(
        api_key=os.environ["OPENROUTER_API_KEY"],
        extraction_model="google/gemini-2.5-flash",
    )


# --- Topic Config ---

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
    "Hobbies": {
        "active": True,
        "labels": ["game", "sport", "instrument"],
        "hierarchy": {},
        "aliases": ["interests", "recreation"],
    },
}


def make_live_pipeline(spacy_model, gliner_model, llm_service, known_aliases=None, profiles=None):
    tc = TopicConfig(TOPICS_CONFIG)
    return NLPPipeline(
        llm=llm_service,
        topic_config=tc,
        get_known_aliases=lambda: known_aliases or {},
        get_profiles=lambda: profiles or {},
        gliner=gliner_model,
        spacy=spacy_model,
    )


# --- Conversation Scenarios ---

STARTUP_FOUNDER_MSGS = [
    {"id": 1, "message": (
        "So me and Tariq Hassan have been grinding on Synapse for about eight months now, "
        "it started as a side project but once we got into YC we realized we needed to go all in. "
        "The core product is basically a real-time data pipeline for fintech companies, "
        "think of it like a managed Kafka but with built-in compliance checks."
    ), "role": "user"},
    {"id": 2, "message": (
        "We just closed a seed round last week actually, Sequoia led it which was kind of surreal. "
        "The plan is to use most of the money on hiring — we need at least three senior engineers "
        "for the platform team and someone to own the developer relations side of things. "
        "Tariq has been handling most of the technical architecture himself and it's not sustainable."
    ), "role": "user"},
    {"id": 3, "message": (
        "Our advisor Dr. Mehta has been incredibly helpful through all of this, "
        "she's the one who connected us with a partnership lead at Stripe. "
        "Apparently Stripe has an internal tool that does something similar to what we're building "
        "but they might want to integrate with us instead of maintaining it themselves. "
        "We have a call with their team next Thursday to scope it out."
    ), "role": "user"},
]

GRAD_STUDENT_MSGS = [
    {"id": 10, "message": (
        "I'm in my third year at Stanford right now, working on my thesis under Professor Emily Zhang. "
        "The focus is on reinforcement learning from human feedback, specifically trying to improve "
        "alignment techniques for smaller language models. We're building on some of the ideas from "
        "the InstructGPT paper but taking a different approach to the reward modeling step."
    ), "role": "user"},
    {"id": 11, "message": (
        "Emily has been pushing me to publish before the NeurIPS deadline which is coming up fast. "
        "The experiments are mostly done but the writing is killing me, I've rewritten the intro "
        "like four times already. My labmate Chen Wei has been helping with the ablation studies "
        "which saves me a ton of time honestly."
    ), "role": "user"},
    {"id": 12, "message": (
        "I'm also exploring doing a summer internship at DeepMind if the visa situation works out, "
        "they have a team in London working on multi-agent RL that overlaps with my research. "
        "The alternative would be staying at Stanford and TAing for the intro ML course again, "
        "which pays the bills but doesn't move the thesis forward at all."
    ), "role": "user"},
]

CASUAL_WITH_ENTITIES_MSGS = [
    {"id": 20, "message": (
        "Had a really good weekend actually, went to see Oppenheimer at the AMC near Union Square "
        "with my friend Kenji on Saturday. Neither of us had seen it yet and the IMAX version was "
        "completely sold out so we just watched the regular showing. Still incredible though, "
        "Cillian Murphy absolutely killed it."
    ), "role": "user"},
    {"id": 21, "message": (
        "After the movie we walked over to Ichiran for ramen because Kenji has been obsessed with "
        "that place lately. Over dinner he told me about his new job at Notion — apparently they're "
        "building out a whole AI features team and he got brought on as one of the founding engineers. "
        "Sounds like a really good opportunity for him, he's been wanting to work on LLM stuff."
    ), "role": "user"},
]

AGENT_ASSISTANT_MSGS = [
    {"id": 30, "message": (
        "Hey can you remind me what Lucia said about the Figma migration last time we talked about it? "
        "I remember she had some concerns about the timeline but I can't remember the specifics, "
        "and I have a planning meeting in an hour where it's probably going to come up."
    ), "role": "user"},
    {"id": 31, "message": (
        "Lucia mentioned that the Figma migration would likely be complete by end of Q2, but she flagged "
        "two risks — the design token export is more complex than originally scoped, and the Vercel team "
        "hasn't confirmed their deployment window yet. She suggested having a sync with both teams before "
        "committing to the Q2 target publicly. She also mentioned reaching out to Jake on the DevOps side "
        "to make sure the CI pipeline can handle the new asset format."
    ), "role": "assistant"},
]

HEALTH_MSGS = [
    {"id": 40, "message": (
        "Had my follow-up with Dr. Okonkwo yesterday and she's referring me to a neurologist at Mount Sinai "
        "because the headaches have been getting worse over the past month. She thinks it might be tension-related "
        "but wants to rule out anything more serious. The appointment isn't until mid-April though which is "
        "kind of frustrating given how frequently they've been happening."
    ), "role": "user"},
    {"id": 41, "message": (
        "In the meantime she prescribed Sumatriptan for when the migraines hit, said to take it early "
        "as soon as I notice the aura starting. She also told me to keep a headache diary tracking when they "
        "happen, what I ate, how much sleep I got the night before, that kind of thing. "
        "I'm supposed to go back in three weeks so she can review the diary and adjust the treatment plan."
    ), "role": "user"},
]

HOBBY_MSGS = [
    {"id": 50, "message": (
        "I've been working on Chopin's Ballade No. 1 in G minor for about two months now and honestly "
        "it might be the hardest piece I've ever attempted. The technical demands are insane, especially "
        "the coda section where everything just explodes into these massive fortissimo octave runs. "
        "I can play through most of it at like 70% tempo but getting it up to performance speed feels impossible."
    ), "role": "user"},
    {"id": 51, "message": (
        "My piano teacher Dmitri keeps telling me I need to slow down and work the coda section hands-separately "
        "before trying to put it together. He studied at the Moscow Conservatory so he has this very disciplined "
        "Russian school approach to practice. He's probably right but it's so tempting to just barrel through it. "
        "He also suggested I listen to Zimerman's recording for phrasing ideas which I've been doing on repeat."
    ), "role": "user"},
]

UBIQUITY_FILTER_MSGS = [
    {"id": 60, "message": (
        "Had a meeting on Zoom this morning about the iPhone rollout plan for the new enterprise app, "
        "nothing too exciting just the usual status update stuff. Then I spent most of the afternoon "
        "debugging a React Native issue that only shows up on Android for some reason."
    ), "role": "user"},
    {"id": 61, "message": (
        "Grabbed a coffee at Starbucks before heading to the WeWork because the office coffee machine "
        "has been broken all week. Ran into my old coworker Daniela there which was a nice surprise, "
        "haven't seen her since she left to join Plaid last year."
    ), "role": "user"},
    {"id": 62, "message": (
        "Actually funny story, I applied to work at Starbucks corporate a couple years ago for a product "
        "manager role on their mobile ordering team. Went through like four rounds of interviews and didn't "
        "get it, but in retrospect it worked out because that's what led me to the startup world. "
        "Daniela said Plaid is hiring PMs too if I'm ever looking."
    ), "role": "user"},
]


# --- Extraction Quality Tests ---

class TestLiveExtractionQuality:

    @pytest.mark.asyncio
    async def test_startup_entities(self, spacy_model, gliner_model, llm_service):
        """Should extract: Synapse, Tariq Hassan, Sequoia, Dr. Mehta, Stripe."""
        pipeline = make_live_pipeline(spacy_model, gliner_model, llm_service)

        with patch("src.core.nlp.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", STARTUP_FOUNDER_MSGS, "live-test")

        names_lower = [name.lower() for _, name, _, _ in results]
        print(f"\n[startup] Extracted: {names_lower}")

        assert any("tariq" in n for n in names_lower), "Missed cofounder Tariq Hassan"
        assert any("sequoia" in n for n in names_lower), "Missed investor Sequoia"
        assert any("synapse" in n for n in names_lower), "Missed project Synapse"
        assert any("stripe" in n for n in names_lower), "Missed company Stripe"

    @pytest.mark.asyncio
    async def test_grad_student_entities(self, spacy_model, gliner_model, llm_service):
        """Should extract: Stanford, Emily Zhang, Chen Wei, DeepMind."""
        pipeline = make_live_pipeline(spacy_model, gliner_model, llm_service)

        with patch("src.core.nlp.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", GRAD_STUDENT_MSGS, "live-test")

        names_lower = [name.lower() for _, name, _, _ in results]
        print(f"\n[grad] Extracted: {names_lower}")

        assert any("stanford" in n for n in names_lower), "Missed university Stanford"
        assert any("emily" in n or "zhang" in n for n in names_lower), "Missed professor Emily Zhang"
        assert any("chen" in n for n in names_lower), "Missed labmate Chen Wei"
        assert any("deepmind" in n for n in names_lower), "Missed company DeepMind"

    @pytest.mark.asyncio
    async def test_casual_entities(self, spacy_model, gliner_model, llm_service):
        """Should extract: Kenji, Notion."""
        pipeline = make_live_pipeline(spacy_model, gliner_model, llm_service)

        with patch("src.core.nlp.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", CASUAL_WITH_ENTITIES_MSGS, "live-test")

        names_lower = [name.lower() for _, name, _, _ in results]
        print(f"\n[casual] Extracted: {names_lower}")

        assert any("kenji" in n for n in names_lower), "Missed friend Kenji"
        assert any("notion" in n for n in names_lower), "Missed company Notion"

    @pytest.mark.asyncio
    async def test_assistant_turn_extraction(self, spacy_model, gliner_model, llm_service):
        """Should extract from assistant messages: Lucia, Figma, Vercel, Jake."""
        pipeline = make_live_pipeline(spacy_model, gliner_model, llm_service)

        with patch("src.core.nlp.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", AGENT_ASSISTANT_MSGS, "live-test")

        names_lower = [name.lower() for _, name, _, _ in results]
        print(f"\n[assistant] Extracted: {names_lower}")

        assert any("lucia" in n for n in names_lower), "Missed person Lucia"
        assert any("figma" in n for n in names_lower), "Missed company/project Figma"
        assert any("vercel" in n for n in names_lower), "Missed company Vercel"

    @pytest.mark.asyncio
    async def test_health_entities(self, spacy_model, gliner_model, llm_service):
        """Should extract: Dr. Okonkwo, Mount Sinai, Sumatriptan."""
        pipeline = make_live_pipeline(spacy_model, gliner_model, llm_service)

        with patch("src.core.nlp.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", HEALTH_MSGS, "live-test")

        names_lower = [name.lower() for _, name, _, _ in results]
        print(f"\n[health] Extracted: {names_lower}")

        assert any("okonkwo" in n for n in names_lower), "Missed doctor Dr. Okonkwo"
        assert any("mount sinai" in n for n in names_lower), "Missed hospital Mount Sinai"

    @pytest.mark.asyncio
    async def test_hobby_entities(self, spacy_model, gliner_model, llm_service):
        """Should extract: Dmitri, Chopin."""
        pipeline = make_live_pipeline(spacy_model, gliner_model, llm_service)

        with patch("src.core.nlp.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", HOBBY_MSGS, "live-test")

        names_lower = [name.lower() for _, name, _, _ in results]
        print(f"\n[hobby] Extracted: {names_lower}")

        assert any("dmitri" in n for n in names_lower), "Missed teacher Dmitri"
        assert any("chopin" in n for n in names_lower), "Missed composer Chopin"

    @pytest.mark.asyncio
    async def test_known_entity_priority(self, spacy_model, gliner_model, llm_service):
        """Pre-registered entities should appear; VP-01 should still discover new ones."""
        known = {"tariq hassan": 10, "tariq": 10, "synapse": 11}
        profiles = {
            10: {"canonical_name": "Tariq Hassan", "type": "person", "topic": "Identity"},
            11: {"canonical_name": "Synapse", "type": "project", "topic": "Work"},
        }
        pipeline = make_live_pipeline(spacy_model, gliner_model, llm_service,
                                      known_aliases=known, profiles=profiles)

        with patch("src.core.nlp.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", STARTUP_FOUNDER_MSGS, "live-test")

        names_lower = [name.lower() for _, name, _, _ in results]
        print(f"\n[known-priority] Extracted: {names_lower}")

        assert any("tariq" in n for n in names_lower)
        assert any("synapse" in n for n in names_lower)
        assert any("sequoia" in n or "stripe" in n for n in names_lower), \
            "VP-01 should still discover entities beyond the known set"

    @pytest.mark.asyncio
    async def test_topic_assignment_accuracy(self, spacy_model, gliner_model, llm_service):
        """Entities should be assigned to correct topics."""
        pipeline = make_live_pipeline(spacy_model, gliner_model, llm_service)

        with patch("src.core.nlp.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", HEALTH_MSGS, "live-test")

        topics_by_name = {}
        for _, name, _, topic in results:
            topics_by_name[name.lower()] = topic
        print(f"\n[topics] Assignments: {topics_by_name}")

        for name, topic in topics_by_name.items():
            if "okonkwo" in name:
                assert topic == "Health", f"Dr. Okonkwo should be Health, got {topic}"
            if "mount sinai" in name:
                assert topic == "Health", f"Mount Sinai should be Health, got {topic}"

    @pytest.mark.asyncio
    async def test_ubiquity_filter(self, spacy_model, gliner_model, llm_service):
        """Daniela and Plaid should be extracted; casual brand mentions may be filtered."""
        pipeline = make_live_pipeline(spacy_model, gliner_model, llm_service)

        with patch("src.core.nlp.emit", new_callable=AsyncMock):
            results = await pipeline.extract_mentions("Yinka", UBIQUITY_FILTER_MSGS, "live-test")

        names_lower = [name.lower() for _, name, _, _ in results]
        print(f"\n[ubiquity] Extracted: {names_lower}")

        assert any("daniela" in n for n in names_lower), "Missed person Daniela"
        assert any("plaid" in n for n in names_lower), "Missed company Plaid"

        # Starbucks with employment context may survive; Zoom/iPhone as tools ideally filtered
        if "starbucks" in names_lower:
            pass  # caught the employment relationship
        if "zoom" in names_lower or "iphone" in names_lower:
            print("  WARNING: Ubiquity filter missed casual brand mention")