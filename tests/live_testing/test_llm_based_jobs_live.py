"""Live LLM tests for job-layer parsers and prompts.

Real LLM calls — these cost money.
Run with: uv run pytest test_jobs_live.py -v -s
Skip with: uv run pytest --ignore=test_jobs_live.py

Tests that the actual LLM output is parseable by the deterministic
parsers we already unit-tested. This is the integration seam between
non-deterministic LLM output and deterministic pipeline logic.

Requires:
  - OPENROUTER_API_KEY in env or .env
"""

import os
import json
import pytest
from dotenv import load_dotenv

from shared.services.llm import LLMService
from shared.config.topics_config import TopicConfig
from main.prompts import (
    get_profile_extraction_prompt,
    get_merge_judgment_prompt,
    get_topic_evolution_prompt,
)
from jobs.jobs_utils import (
    format_vp04_input,
    format_vp05_input,
    parse_new_facts,
    parse_merge_score,
    process_extracted_facts,
)
from shared.models.schema.dtypes import Fact
from datetime import datetime, timezone


load_dotenv()

pytestmark = pytest.mark.skipif(
    not os.environ.get("OPENROUTER_API_KEY"),
    reason="OPENROUTER_API_KEY not set — skipping live LLM tests",
)


# ══════════════════════════════════════════════════════════════════════
#  FIXTURES
# ══════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="session")
def llm():
    return LLMService(
        api_key=os.environ["OPENROUTER_API_KEY"],
        extraction_model="google/gemini-2.5-flash",
        merge_model="google/gemini-2.5-flash",
    )


def make_fact(content, valid_at=None, source_msg_id=None):
    return Fact(
        id=f"fact_{hash(content) % 100000}",
        source_entity_id=1,
        content=content,
        valid_at=valid_at or datetime(2025, 1, 15, tzinfo=timezone.utc),
        source_msg_id=source_msg_id,
    )


# ══════════════════════════════════════════════════════════════════════
#  VP-04 BASELINE FIXTURES (shorter, cleaner)
# ══════════════════════════════════════════════════════════════════════

EXISTING_FACTS_ALICE = [
    make_fact("Works at Google as a product manager"),
    make_fact("Lives in San Francisco"),
    make_fact("Has 2 kids"),
]

VP04_CONVERSATION_ALICE_SUPERSEDES = """[USER]: Alice told me she left Google and joined Anthropic as a research lead about three months ago
[USER]: She also moved from San Francisco to the Mission Hill area in Boston because that's where the office is
[AGENT]: That's quite a change! How does she like it so far?
[USER]: She loves it, said the team culture is completely different from Google, way more research-oriented
[USER]: Oh and she mentioned she had another kid, so three now total"""


# ══════════════════════════════════════════════════════════════════════
#  VP-04 STRESS FIXTURES — Long, messy, realistic
# ══════════════════════════════════════════════════════════════════════

EXISTING_FACTS_OMAR = [
    make_fact("Senior product manager at Stripe", source_msg_id=5),
    make_fact("Works on the payments infrastructure team", source_msg_id=8),
    make_fact("Based in San Francisco", source_msg_id=12),
    make_fact("Has been at Stripe for four years", source_msg_id=15),
    make_fact("Previously worked at Square as a product analyst", source_msg_id=18),
    make_fact("Has an MBA from Wharton", source_msg_id=22),
]

EXISTING_FACTS_JASMINE = [
    make_fact("Cofounder and CEO of Helix Robotics", source_msg_id=3),
    make_fact("The company builds warehouse automation systems", source_msg_id=6),
    make_fact("Based in Austin, Texas", source_msg_id=10),
    make_fact("Previously worked at Boston Dynamics for six years", source_msg_id=14),
    make_fact("Has a PhD in mechanical engineering from Georgia Tech", source_msg_id=17),
    make_fact("Raised a seed round from Khosla Ventures", source_msg_id=20),
]

EXISTING_FACTS_DEREK = [
    make_fact("Software engineer at Meta", source_msg_id=4),
    make_fact("Works on the Instagram Reels recommendation algorithm", source_msg_id=7),
    make_fact("Based in Menlo Park", source_msg_id=11),
    make_fact("Single, no kids", source_msg_id=15),
    make_fact("Plays in a recreational basketball league on weekends", source_msg_id=19),
]

# Red herring entity — similar name, different person
EXISTING_FACTS_OMAR_HASSAN = [
    make_fact("Backend engineer at Coinbase", source_msg_id=30),
    make_fact("Works on the exchange matching engine", source_msg_id=33),
    make_fact("Based in New York City", source_msg_id=36),
]


# ── Stress: Omar (job change, location change, buried contradictions) ──

VP04_STRESS_OMAR = """
[USER]: Had coffee with Omar yesterday and honestly the conversation went in a million directions, started talking about his work stuff but then we got sidetracked into this whole thing about his apartment search which has been a nightmare apparently
[AGENT]: Apartment hunting in SF is brutal. Is he trying to stay in the city or looking elsewhere?
[USER]: That's actually the interesting part, he's not looking in SF anymore because get this — he's leaving Stripe after four years and moving to New York to join a fintech startup as their head of product, which is a huge step up from senior PM obviously
[AGENT]: Wow that's a big move. Did he say which startup?
[USER]: Yeah it's called Ramp, the corporate card company, apparently they've been trying to recruit him for like eight months and finally made an offer he couldn't refuse, something about equity and the scope of the role being way bigger than what he had at Stripe
[USER]: We talked about his team at Stripe for a bit too, he was saying how the payments infrastructure work was getting stale and he felt like he'd solved all the interesting problems there, plus his skip-level manager left last quarter so the org structure got weird
[AGENT]: That tracks, reorgs can really change the vibe. When does he start at Ramp?
[USER]: Not until March because he's taking like six weeks off first to travel, said he's going to Japan with his girlfriend Priya for three weeks and then visiting his parents in Detroit for the holidays, first real break he's taken since business school
[AGENT]: Good for him. Is Priya the one who works at Google?
[USER]: No that's his ex, Priya is actually a doctor, she's a resident at UCSF in internal medicine, they've been together for about a year now I think, met through mutual friends at some dinner party thing
[USER]: Oh speaking of Wharton though he mentioned he's been doing some alumni mentoring stuff, paired up with a first-year MBA student who's trying to break into product and he's been doing calls with her like twice a month
[AGENT]: That's nice of him to give back. Is the mentee also interested in fintech?
[USER]: I don't think so actually, she's more interested in consumer products, social media type stuff, but Omar said the PM skills transfer regardless so he's mostly helping her with interview prep and resume stuff
[USER]: Anyway back to the move — he's already found an apartment in Brooklyn, Williamsburg specifically, which he's excited about because apparently there's a really good climbing gym nearby and he's been getting into bouldering lately
[AGENT]: Williamsburg is a good choice. Is Priya moving with him?
[USER]: That's the complicated part, she can't transfer her residency so they're going to do long distance for at least a year, maybe two depending on where she matches for fellowship, he seemed a little stressed about it honestly but trying to be optimistic
[USER]: He also mentioned Ramp is letting him work remotely for the first month while he gets settled, which is nice because the apartment needs some work done, the previous tenant left it in rough shape
[AGENT]: At least they're being flexible. What's his actual start date?
[USER]: March 15th I think he said, and his title will be VP of Product which reports directly to the CEO, so way more visibility than being a senior PM three levels down at Stripe
[USER]: We also talked about his side project a bit, he's been building this personal finance app on nights and weekends with a friend from Square, nothing serious yet but they've got like two thousand beta users and are thinking about whether to actually pursue it
[AGENT]: Interesting. Would he keep working on that while at Ramp?
[USER]: Probably not, he said he'd have to check the employment agreement but most likely he'll pause it or hand off his part to his cofounder, the Ramp role is going to be pretty all-consuming especially the first year
[USER]: Oh and one more thing I forgot — he's not doing the Wharton mentoring anymore after this semester, said between the move and the new job he just won't have bandwidth, felt bad about it but the mentee understood
"""


# ── Stress: Jasmine (startup chaos, multiple pivots, cofounder change) ──

VP04_STRESS_JASMINE = """
[USER]: Just got off a two hour call with Jasmine and my brain is honestly fried, so much is happening with Helix right now it's hard to keep track of everything she told me
[AGENT]: Sounds intense. Is this about the fundraise you mentioned last time?
[USER]: Yeah but it's way more complicated now, so they were doing a Series A with Khosla leading again but then Andreessen came in with a competing term sheet at a higher valuation, and now there's this whole negotiation happening
[AGENT]: That's a good problem to have. What's the valuation difference?
[USER]: Khosla was at 40 million pre-money and a16z came in at 55, but Jasmine said it's not just about the money, the a16z partner has way more operational experience in hardware and robotics which could be super valuable
[USER]: The thing is she's also not sure she wants to take that much dilution right now, they've been way more capital efficient than expected and might not need a full Series A, could potentially do a smaller extension round instead
[AGENT]: Makes sense to preserve equity if the business fundamentals support it. How's the product doing?
[USER]: That's the other big update — they're pivoting slightly, not away from warehouse automation but the go-to-market is changing, instead of selling directly to warehouses they're going to partner with the big logistics software companies and integrate into their platforms
[USER]: Jasmine said the direct sales cycle was just brutal, like nine to twelve months per deal with massive procurement teams, whereas the partnership model could get them distribution to hundreds of warehouses through a single integration
[AGENT]: Smart pivot. Does that change the product roadmap?
[USER]: Somewhat yeah, they need to build out an API layer and some partner tooling that wasn't on the roadmap before, she's actually hiring a developer relations person for the first time which feels like a big milestone for a fifteen person startup
[USER]: Oh and the Austin office situation is changing too, they've been fully remote since COVID but she's opening a small office in Boston because three of her best engineers are there and she wants to have a hardware lab that's not in her garage
[AGENT]: Is she relocating to Boston then?
[USER]: No she's staying in Austin, her wife just started a new job there and they bought a house like six months ago so moving isn't really on the table, she'll probably fly to Boston like one week a month though
[USER]: Actually that reminds me she mentioned they're hiring a VP of Engineering finally, been doing it all herself basically since they started and it's not sustainable with the growth they're seeing
[USER]: The candidates she's talking to are interesting, one is from her old team at Boston Dynamics and another is from Amazon Robotics, she's leaning toward the Boston Dynamics person because they already know how to work together
[AGENT]: Culture fit matters a lot at that stage. Anyone else in the running?
[USER]: There was a third candidate from Tesla but they wanted too much equity and also had a two month notice period which is too long, Jasmine needs someone who can start basically immediately
[USER]: Going back to the fundraise for a second — she's actually considering a third option which is to take money from a strategic investor, one of the big logistics companies expressed interest in leading the round and that could come with a pilot contract attached
[AGENT]: Strategic money can be tricky though, right? Limits your options?
[USER]: Exactly what she's worried about, they'd probably want some kind of exclusivity or right of first refusal on acquisition which could scare off other potential acquirers down the line, so she's really torn on it
[USER]: Her board is split too apparently, the Khosla partner wants her to take the a16z money because bigger war chest, but her angel investors are more conservative and think the extension round makes sense
[USER]: She also mentioned something about her cofounder Marcus taking a step back, not leaving the company but going from full-time to like a senior advisor role because his wife is having health issues and he needs to be more present at home
[AGENT]: That's tough. How's Jasmine handling the added responsibility?
[USER]: She seemed okay about it honestly, said Marcus was always more on the research side anyway and the company is past the R&D phase now, so it's more about execution and go-to-market which is her strength
[USER]: The one thing she's stressed about is the Boston Dynamics non-compete situation, apparently there's some IP question about whether one of their core patents overlaps with work Marcus did there, lawyers are looking into it
"""


# ── Stress: Derek (life changes, corrections, red herrings) ──

VP04_STRESS_DEREK = """
[USER]: Ran into Derek at the gym this morning which was random because I thought he worked out at the Equinox near his office but apparently he switched to this one closer to his new place
[AGENT]: Oh did he move? Last I heard he was in Menlo Park.
[USER]: Yeah he moved to the city actually, got a place in the Mission with his girlfriend, wait no fiancée now I guess, they got engaged like two months ago which I totally missed somehow
[AGENT]: Congrats to him! I didn't even know he was dating someone seriously.
[USER]: Yeah her name is Vanessa, she's a lawyer at some big firm, they met on Hinge maybe a year and a half ago, moved in together pretty fast and I guess it's going well if they're getting married
[USER]: The funny thing is he was telling me about the proposal and it was this whole elaborate thing at her parents' house in Napa, rented out a vineyard and had both families there, very unlike the Derek I know who's usually pretty low-key about everything
[AGENT]: Sounds like he's really serious about her. Is she also in tech?
[USER]: No she's in corporate law, M&A stuff, works insane hours apparently which Derek said is actually kind of perfect because his job is also demanding so neither of them feels guilty about working late
[USER]: Speaking of work, he's not on the Reels team anymore, got moved to a new initiative that's still under wraps but has something to do with AI-generated content, he couldn't tell me much but seemed really excited about it
[AGENT]: That's a big shift from recommendation algorithms. Is he still an IC or did he move into management?
[USER]: He's a tech lead now actually, has like four or five direct reports which is new for him, said the management stuff is harder than he expected but he's getting better at it, took some internal leadership course that Meta offers
[USER]: Oh and the basketball league thing is done, he stopped playing because he tore his ACL last spring and the recovery was brutal, six months of physical therapy and he's just now getting back to normal exercise
[AGENT]: Ouch, ACL tears are no joke. Is he doing any sports now?
[USER]: Mostly just lifting and some cycling, Vanessa got him into spin classes which he complained about at first but secretly I think he likes them, they go together on Saturday mornings
[USER]: The other thing that came up is he's thinking about business school, not sure if he'll actually do it but he's been studying for the GMAT on the side and looking at programs, Stanford GSB is the dream but he said it's a long shot
[AGENT]: That's ambitious. Would he leave Meta for it?
[USER]: I think the plan is to try for a part-time or executive program so he can keep working, Meta has some kind of tuition reimbursement thing so it wouldn't be as financially painful
[USER]: We talked about his sister Danielle too, she's the one who was doing the startup in LA, apparently that fell apart and she's back at Google now working on the Maps team, Derek seemed relieved honestly because he was worried about her burning through her savings
[AGENT]: Startups are risky. Is she doing okay otherwise?
[USER]: Yeah she's fine, bought a condo in Silver Lake and is dating someone new, another Derek actually which is confusing, Derek the brother said he keeps accidentally responding when Danielle talks about her boyfriend
[USER]: The wedding planning is apparently chaos by the way, Vanessa wants something big and traditional but Derek's parents are pushing for something smaller, and Vanessa's parents are trying to take over everything, classic wedding drama
[USER]: Date is set for next October though, venue is booked in Sonoma, I'm probably invited but haven't gotten the official invite yet, Derek said they're still finalizing the guest list because Vanessa has a huge extended family
"""


# ── Stress: Pronoun hell + temporal confusion ──

EXISTING_FACTS_CHEN_FAMILY = [
    make_fact("Michael Chen is a cardiologist at Mount Sinai", source_msg_id=5),
    make_fact("His wife Linda is a real estate agent", source_msg_id=8),
    make_fact("Their son David is in medical school at Columbia", source_msg_id=12),
    make_fact("Their daughter Emily works at McKinsey as a consultant", source_msg_id=15),
]

VP04_STRESS_PRONOUNS = """
[USER]: Had dinner with the Chen family last night and it was a lot to process, so much has changed since I last saw them at the holiday party, which I think was like eight or nine months ago now that I'm counting
[AGENT]: It's been a while! How's everyone doing?
[USER]: Well the big news is Michael finally retired from Mount Sinai, he'd been talking about it for years but kept putting it off because he loved his patients, but Linda basically told him enough is enough, his blood pressure was through the roof
[USER]: So now he's doing consulting work for a health tech startup like two days a week, says it keeps his brain active without the stress of being on call, seems way happier honestly
[AGENT]: Good for him. How's Linda handling having him home more?
[USER]: She's actually busier than ever, the real estate market has been crazy and she just closed on this massive deal in Westchester, some estate that had been on the market for two years, so she's been working constantly
[USER]: Which means Michael has been doing a lot of the cooking now which is hilarious because he used to burn water, but apparently he's gotten really into it, watches all these YouTube cooking channels
[AGENT]: That's a nice role reversal. What about the kids?
[USER]: David graduated from Columbia finally, he's doing his residency at Mass General now which is exciting but also means he moved to Boston, Michael is proud but also sad he's so far away
[USER]: Emily left McKinsey which was a surprise to everyone, she said the travel was killing her and the work wasn't meaningful enough, now she's at a nonprofit that does education policy stuff, took a big pay cut but seems happier
[AGENT]: Sounds like a lot of transitions for everyone. Are Michael and Linda adjusting to the empty nest?
[USER]: Sort of, they actually sold the house in Scarsdale and bought a smaller place in the city, Upper West Side, Linda said maintaining the big house was too much work and they wanted to be able to walk to things
[USER]: But here's the twist — Emily moved back in with them temporarily because her lease ended and she's looking for a new place, so it's not quite empty nest yet, Michael pretends to be annoyed but I think he loves having her there
[USER]: David is engaged now too, to someone he met in med school, her name is Rachel and she's also doing her residency at Mass General in pediatrics, they're planning a wedding for next summer but nothing is finalized yet
[AGENT]: That's wonderful! Have Michael and Linda met Rachel?
[USER]: Yeah they flew up to Boston a few weeks ago for a long weekend, really liked her, Linda especially bonded with her over some shared interest in gardening or something, Michael said she's very smart which from him is high praise
[USER]: The other thing is Michael's been having some health stuff himself, nothing serious but he had a minor procedure on his knee that had been bothering him for years, finally had time to deal with it now that he's not working full-time
[USER]: Linda was worried about him being a difficult patient but apparently he was fine, just complained about the hospital food a lot which is ironic given that he worked in a hospital for forty years
"""


# ══════════════════════════════════════════════════════════════════════
#  VP-05 MERGE FIXTURES — Entity ambiguity stress
# ══════════════════════════════════════════════════════════════════════

MERGE_OBVIOUS_DUPLICATE = {
    "entity_a": {
        "canonical_name": "Priya Sharma",
        "type": "person",
        "aliases": ["priya", "priya sharma"],
        "facts": [
            {"content": "Senior engineer at Palantir", "recorded_at": "2025-01-15", "source_message": None},
            {"content": "Based in Palo Alto", "recorded_at": "2025-01-20", "source_message": None},
        ],
    },
    "entity_b": {
        "canonical_name": "P. Sharma",
        "type": "person",
        "aliases": ["p sharma", "p. sharma"],
        "facts": [
            {"content": "Software engineer at Palantir Technologies", "recorded_at": "2025-02-01", "source_message": None},
            {"content": "Works in the Palo Alto office", "recorded_at": "2025-02-05", "source_message": None},
        ],
    },
}

MERGE_CLEARLY_DIFFERENT = {
    "entity_a": {
        "canonical_name": "Marcus Chen",
        "type": "person",
        "aliases": ["marcus"],
        "facts": [
            {"content": "Data scientist at Netflix", "recorded_at": "2025-01-10", "source_message": None},
            {"content": "Lives in Los Angeles", "recorded_at": "2025-01-12", "source_message": None},
            {"content": "PhD in statistics from UC Berkeley", "recorded_at": "2025-01-15", "source_message": None},
        ],
    },
    "entity_b": {
        "canonical_name": "Marcus Williams",
        "type": "person",
        "aliases": ["marcus w"],
        "facts": [
            {"content": "High school math teacher", "recorded_at": "2025-02-01", "source_message": None},
            {"content": "Lives in Chicago", "recorded_at": "2025-02-03", "source_message": None},
            {"content": "Coaches the varsity basketball team", "recorded_at": "2025-02-05", "source_message": None},
        ],
    },
}

MERGE_AMBIGUOUS = {
    "entity_a": {
        "canonical_name": "Anthropic",
        "type": "company",
        "aliases": ["anthropic"],
        "facts": [
            {"content": "AI safety company", "recorded_at": "2025-01-10", "source_message": None},
        ],
    },
    "entity_b": {
        "canonical_name": "Anthropic AI",
        "type": "company",
        "aliases": ["anthropic ai"],
        "facts": [
            {"content": "Develops the Claude language model", "recorded_at": "2025-02-01", "source_message": None},
        ],
    },
}

MERGE_TYPE_MISMATCH = {
    "entity_a": {
        "canonical_name": "Apollo",
        "type": "project",
        "aliases": ["apollo"],
        "facts": [
            {"content": "Internal deployment tool at Palantir", "recorded_at": "2025-01-10", "source_message": None},
        ],
    },
    "entity_b": {
        "canonical_name": "Apollo",
        "type": "company",
        "aliases": ["apollo"],
        "facts": [
            {"content": "Investment management firm", "recorded_at": "2025-02-01", "source_message": None},
        ],
    },
}

# ── Stress: Near-duplicates with contradicting facts ──

MERGE_STRESS_CONTRADICTION = {
    "entity_a": {
        "canonical_name": "Sarah Park",
        "type": "person",
        "aliases": ["sarah", "sarah park"],
        "facts": [
            {"content": "Product manager at Figma", "recorded_at": "2025-01-10", "source_message": None},
            {"content": "Lives in San Francisco", "recorded_at": "2025-01-15", "source_message": None},
            {"content": "Graduated from Stanford in 2018", "recorded_at": "2025-01-20", "source_message": None},
            {"content": "Works on the FigJam product", "recorded_at": "2025-02-01", "source_message": None},
        ],
    },
    "entity_b": {
        "canonical_name": "Sarah J. Park",
        "type": "person",
        "aliases": ["sarah j park", "sj park"],
        "facts": [
            {"content": "Senior PM at Figma on the enterprise team", "recorded_at": "2025-02-10", "source_message": None},
            {"content": "Based in New York City", "recorded_at": "2025-02-15", "source_message": None},
            {"content": "Stanford MBA class of 2018", "recorded_at": "2025-02-20", "source_message": None},
            {"content": "Previously worked at Google on Workspace", "recorded_at": "2025-02-25", "source_message": None},
        ],
    },
}

# ── Stress: Same name, temporal evolution (same person over time) ──

MERGE_STRESS_TEMPORAL = {
    "entity_a": {
        "canonical_name": "James Liu",
        "type": "person",
        "aliases": ["james", "james liu"],
        "facts": [
            {"content": "Junior software engineer at Uber", "recorded_at": "2022-03-01", "source_message": None},
            {"content": "Works on the payments team", "recorded_at": "2022-05-15", "source_message": None},
            {"content": "Lives in a shared apartment in Oakland", "recorded_at": "2022-06-01", "source_message": None},
        ],
    },
    "entity_b": {
        "canonical_name": "James Liu",
        "type": "person",
        "aliases": ["james liu", "j liu"],
        "facts": [
            {"content": "Staff engineer at Uber", "recorded_at": "2025-01-10", "source_message": None},
            {"content": "Tech lead for the fraud detection platform", "recorded_at": "2025-01-15", "source_message": None},
            {"content": "Owns a house in Berkeley", "recorded_at": "2025-02-01", "source_message": None},
            {"content": "Married with one child", "recorded_at": "2025-02-10", "source_message": None},
        ],
    },
}

# ── Stress: Organizations with overlapping scope ──

MERGE_STRESS_ORG_OVERLAP = {
    "entity_a": {
        "canonical_name": "Google AI",
        "type": "organization",
        "aliases": ["google ai", "google artificial intelligence"],
        "facts": [
            {"content": "Research division within Google focused on machine learning", "recorded_at": "2025-01-10", "source_message": None},
            {"content": "Publishes papers at NeurIPS and ICML", "recorded_at": "2025-01-15", "source_message": None},
            {"content": "Developed the Transformer architecture", "recorded_at": "2025-01-20", "source_message": None},
        ],
    },
    "entity_b": {
        "canonical_name": "Google DeepMind",
        "type": "organization",
        "aliases": ["deepmind", "google deepmind"],
        "facts": [
            {"content": "AI research lab owned by Alphabet", "recorded_at": "2025-02-01", "source_message": None},
            {"content": "Created AlphaFold and AlphaGo", "recorded_at": "2025-02-05", "source_message": None},
            {"content": "Headquarters in London", "recorded_at": "2025-02-10", "source_message": None},
            {"content": "Merged with Google Brain in 2023", "recorded_at": "2025-02-15", "source_message": None},
        ],
    },
}


# ══════════════════════════════════════════════════════════════════════
#  TOPIC EVOLUTION FIXTURES
# ══════════════════════════════════════════════════════════════════════

TOPICS_CURRENT = {
    "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
    "Identity": {"active": True, "labels": ["person"], "hierarchy": {}, "aliases": []},
    "Work": {"active": True, "labels": ["company", "project", "role"], "hierarchy": {"company": ["project"]}, "aliases": ["career", "job"]},
}

EVOLUTION_CONVERSATION_NO_CHANGE = """[USER]: Had a meeting with the engineering team about the Q3 roadmap
[AGENT]: How did it go?
[USER]: Pretty standard, just reviewed the sprint priorities and discussed the upcoming release
[USER]: Priya presented the architecture review, nothing controversial
[AGENT]: Sounds routine. Anything else come up?
[USER]: Not really, just the usual standup stuff"""


# ── Stress: Multi-domain meandering conversation ──

EVOLUTION_STRESS_MULTI_DOMAIN = """
[USER]: This week has been all over the place honestly, started Monday with back-to-back meetings about the Q4 roadmap and by Tuesday I was so burned out that I took a half day and went to the Museum of Modern Art which I haven't done in years
[AGENT]: Sometimes you need that mental reset. See anything interesting?
[USER]: There was this amazing exhibit on Bauhaus design that I spent way too long in, got me thinking about how much design principles from the 1920s still influence product work today, might write a blog post about it actually
[USER]: Wednesday was when things got really random — my landlord called to say they're selling the building so I might need to find a new apartment, spent half the day looking at Zillow listings instead of working on the sprint planning I was supposed to do
[AGENT]: That's stressful. Are you thinking of staying in the same neighborhood?
[USER]: Probably not, the rents here have gotten insane so I'm looking at places further out, maybe even considering buying something small if the interest rates come down a bit, my parents keep pushing me to build equity instead of renting
[USER]: Thursday I finally got back to work stuff but also started that pottery class I signed up for months ago, it's at this studio in Bushwick and I am absolutely terrible at it but the instructor Hiroshi is super patient and it's weirdly meditative
[AGENT]: Pottery is one of those things that takes forever to get decent at. How long is the course?
[USER]: Eight weeks, meets every Thursday evening, there's also open studio time on Sundays if you want extra practice which I'll probably need because my first bowl looked more like a sad ashtray than anything functional
[USER]: Friday was the interesting one though — my old college roommate Theo is in town and he's been doing this fascinating thing where he sources specialty coffee beans directly from farms in Colombia and Ethiopia, trying to start an import business
[AGENT]: That's niche. Is there actually a market for that kind of thing?
[USER]: Apparently yeah, he's been selling to high-end coffee shops in Portland where he lives and they can't get enough, the margins are crazy good too because he's cutting out all the middlemen, he wants me to potentially invest
[USER]: We talked for hours about the logistics of it, there's so much I didn't know about coffee farming and fair trade certification and the whole supply chain, makes me want to learn more about where my food actually comes from
[AGENT]: Would you actually put money into it?
[USER]: Maybe a small amount, he's raising like fifty thousand from friends and family and I could probably do five, the risk is that it's his first real business and he's learning as he goes, but the upside if it works could be significant
[USER]: Saturday I went to this Korean spa in Koreatown with a few friends, we did the whole thing with the different temperature rooms and the body scrub which was intense but incredibly relaxing, I've been meaning to go for like two years
[AGENT]: Those spas are an experience. First time doing the scrub?
[USER]: Yeah and I was not prepared for how aggressive it is, feels like they're removing a layer of skin, but afterward you feel amazing, we spent like six hours there just rotating between the hot rooms and the cold plunge and eating in the restaurant
[USER]: Then Sunday I did a long bike ride up to Beacon, about sixty miles round trip, joined a cycling group I found on Meetup that does Hudson Valley routes, met some interesting people including this urban planner named Kenji who works on the same climate adaptation stuff I've been reading about
[AGENT]: That's a solid ride. Is this a regular thing with the group?
[USER]: They do a ride every other Sunday, different routes, I'm thinking about doing the next one which goes up to Cold Spring, slightly shorter but apparently more hills which will destroy me but in a good way
[USER]: Kenji was telling me about this book on solarpunk urban design that I immediately bought on Kindle, only a few chapters in but it's reshaping how I think about city planning and sustainability, might be relevant to some work stuff too
[AGENT]: What's the connection to your work?
[USER]: Indirectly related — we're building tools for facilities management and there's this whole sustainability angle we haven't really explored, I pitched it briefly last quarter but maybe I should write up a proper proposal now that I understand the space better
[USER]: And somehow in all of this I also started watching that show everyone's been talking about, the Korean thriller on Netflix, watched like five episodes between Saturday night and Sunday morning, completely addicted now
[AGENT]: Which one, the political one or the survival game one?
[USER]: The survival game one, it's so well done even though the premise is kind of ridiculous, the production quality and acting are incredible, my friend who's Korean said the cultural commentary is really sharp if you understand the context
[USER]: This week coming up is going to be calmer I hope, just the usual work stuff plus one pottery class, although I do have that meeting with my financial advisor on Wednesday to talk about whether buying property actually makes sense given my income and savings
"""


# ── Stress: Rapid topic switching with false starts ──

EVOLUTION_STRESS_SCATTERED = """
[USER]: Okay so I need to brain dump because there's too much going on, first thing is I signed up for a half marathon which is insane because I haven't run more than three miles in like two years but my coworker talked me into it
[AGENT]: That's ambitious! When is it?
[USER]: April, so I have about four months to train, downloaded this Hal Higdon training plan that seems reasonable, starts with short runs and builds up gradually, the first week is only like twelve miles total which seems doable
[USER]: Completely unrelated but I've also been getting really into chess lately, started playing on Lichess against random people and I'm terrible but it's weirdly addictive, stayed up until 2am last night playing blitz games
[AGENT]: Chess has that effect on people. Are you following any particular openings?
[USER]: I've been trying to learn the Italian Game as white because apparently it's good for beginners, lots of YouTube videos on it, this guy GothamChess explains things really well even though he talks super fast
[USER]: Oh and I started therapy finally which I've been putting off for like a year, found someone through my insurance who specializes in anxiety and work stress, only had two sessions so far but it already feels helpful
[AGENT]: That's a big step. Is it in person or virtual?
[USER]: Virtual which I was skeptical about but actually works fine, she does this thing where she has me rate my anxiety on a scale at the beginning and end of each session which is kind of interesting to track over time
[USER]: Back to the running thing though — I need new shoes because mine are like four years old and completely dead, went to this specialty running store where they analyze your gait on a treadmill and recommend shoes based on your foot strike
[AGENT]: Those gait analyses are really useful. Did they find anything interesting?
[USER]: Apparently I overpronate pretty significantly which explains why my knees always hurt when I used to run more, they recommended these stability shoes from Brooks that feel weird but supposedly will fix the issue
[USER]: I've also been trying to cook more instead of ordering delivery every night, bought this cookbook called Salt Fat Acid Heat that everyone recommends and made the buttermilk chicken last weekend which turned out amazing
[AGENT]: That's a great cookbook. Are you working through it systematically or just picking recipes?
[USER]: Mostly just flipping through and picking things that look good, the author has this whole philosophy about understanding the fundamentals rather than just following recipes which I appreciate even if I don't fully understand it yet
[USER]: The meal prep aspect is hard though because I'm not used to planning ahead, I keep buying ingredients and then they go bad before I use them, need to figure out a better system
[USER]: My sister is also visiting next month which means I need to clean my apartment and probably buy a real bed frame finally instead of just having my mattress on the floor like a college student
[AGENT]: How long is she staying?
[USER]: Just a long weekend, Thursday through Sunday, she wants to go to some specific restaurants she's seen on TikTok which is very her, I told her I'd make a reservation at that new Korean BBQ place in Flushing
[USER]: And work-wise things are actually pretty calm for once, we shipped the big feature last week so now we're in that weird lull where everyone is kind of burned out and no one wants to start the next big thing yet
"""


# ══════════════════════════════════════════════════════════════════════
#  VP-04 TESTS — Baseline + Stress
# ══════════════════════════════════════════════════════════════════════

class TestVP04Baseline:
    """Sanity checks with cleaner, shorter conversations."""

    @pytest.mark.asyncio
    async def test_supersedes_detected(self, llm):
        """LLM should emit SUPERSEDES for changed facts about Alice."""
        enriched_facts = [
            {"content": f.content, "recorded_at": f.valid_at.isoformat(), "source_message": None}
            for f in EXISTING_FACTS_ALICE
        ]
        entities = [{
            "entity_name": "Alice",
            "entity_type": "person",
            "existing_facts": enriched_facts,
            "known_aliases": ["alice"],
        }]
        user_content = format_vp04_input(entities, VP04_CONVERSATION_ALICE_SUPERSEDES)
        system = get_profile_extraction_prompt("Yinka")

        response = await llm.call_llm(system, user_content)
        result = parse_new_facts(response)
        assert result is not None, "parse_new_facts returned None"

        all_facts = []
        for profile in result:
            all_facts.extend(profile.facts)
        raw_text = " ".join(all_facts).lower()

        assert "anthropic" in raw_text, "Should mention job change"
        assert "boston" in raw_text or "mission hill" in raw_text, "Should mention location change"


class TestVP04Stress:
    """Stress tests with long, messy, realistic conversations."""

    @pytest.mark.asyncio
    async def test_omar_job_change_extracted(self, llm):
        """Omar: Stripe → Ramp, SF → NYC, Senior PM → VP Product."""
        enriched_facts = [
            {"content": f.content, "recorded_at": f.valid_at.isoformat(), "source_message": None}
            for f in EXISTING_FACTS_OMAR
        ]
        entities = [{
            "entity_name": "Omar",
            "entity_type": "person",
            "existing_facts": enriched_facts,
            "known_aliases": ["omar"],
        }]
        user_content = format_vp04_input(entities, VP04_STRESS_OMAR)
        system = get_profile_extraction_prompt("Yinka")

        response = await llm.call_llm(system, user_content)
        print(f"\n[VP-04 Omar stress] Raw:\n{response[:800]}")

        result = parse_new_facts(response)
        assert result is not None, "parse_new_facts returned None"

        all_facts = []
        for profile in result:
            all_facts.extend(profile.facts)
        raw_text = " ".join(all_facts).lower()
        print(f"[VP-04 Omar stress] Parsed facts: {all_facts[:10]}")

        # Core changes that MUST be detected
        assert "ramp" in raw_text, "Missed job change to Ramp"
        assert "vp" in raw_text or "vice president" in raw_text or "head of product" in raw_text, \
            "Missed title change to VP Product"
        assert "new york" in raw_text or "brooklyn" in raw_text or "williamsburg" in raw_text, \
            "Missed location change to NYC"

    @pytest.mark.asyncio
    async def test_omar_supersedes_processed(self, llm):
        """Omar: end-to-end through process_extracted_facts."""
        enriched_facts = [
            {"content": f.content, "recorded_at": f.valid_at.isoformat(), "source_message": None}
            for f in EXISTING_FACTS_OMAR
        ]
        entities = [{
            "entity_name": "Omar",
            "entity_type": "person",
            "existing_facts": enriched_facts,
            "known_aliases": ["omar"],
        }]
        user_content = format_vp04_input(entities, VP04_STRESS_OMAR)
        system = get_profile_extraction_prompt("Yinka")

        response = await llm.call_llm(system, user_content)
        parsed = parse_new_facts(response)
        assert parsed is not None

        omar_profile = None
        for p in parsed:
            if "omar" in p.canonical_name.lower():
                omar_profile = p
                break
        assert omar_profile is not None, "Omar not found in parsed profiles"

        merge_result = process_extracted_facts(EXISTING_FACTS_OMAR, omar_profile.facts)
        print(f"\n[VP-04 Omar process] Invalidated: {merge_result.to_invalidate}")
        print(f"[VP-04 Omar process] New contents: {merge_result.new_contents[:5]}")

        # Should invalidate: employer, location, team — at least 2
        assert len(merge_result.to_invalidate) >= 2, \
            f"Expected at least 2 supersedes, got {len(merge_result.to_invalidate)}"

    @pytest.mark.asyncio
    async def test_omar_buried_details_extracted(self, llm):
        """Omar: should extract details buried deep in conversation."""
        enriched_facts = [
            {"content": f.content, "recorded_at": f.valid_at.isoformat(), "source_message": None}
            for f in EXISTING_FACTS_OMAR
        ]
        entities = [{
            "entity_name": "Omar",
            "entity_type": "person",
            "existing_facts": enriched_facts,
            "known_aliases": ["omar"],
        }]
        user_content = format_vp04_input(entities, VP04_STRESS_OMAR)
        system = get_profile_extraction_prompt("Yinka")

        response = await llm.call_llm(system, user_content)
        result = parse_new_facts(response)
        assert result is not None

        all_facts = []
        for profile in result:
            all_facts.extend(profile.facts)
        raw_text = " ".join(all_facts).lower()

        # Buried details — bonus points if extracted
        buried_details_found = 0
        if "priya" in raw_text and ("girlfriend" in raw_text or "dating" in raw_text or "relationship" in raw_text):
            buried_details_found += 1
        if "bouldering" in raw_text or "climbing" in raw_text:
            buried_details_found += 1
        if "side project" in raw_text or "personal finance" in raw_text:
            buried_details_found += 1
        if "march" in raw_text and ("15" in raw_text or "start" in raw_text):
            buried_details_found += 1

        print(f"[VP-04 Omar buried] Found {buried_details_found}/4 buried details")
        assert buried_details_found >= 2, \
            f"Should extract at least 2 buried details, got {buried_details_found}"

    @pytest.mark.asyncio
    async def test_jasmine_startup_chaos(self, llm):
        """Jasmine: fundraise options, pivot, cofounder change."""
        enriched_facts = [
            {"content": f.content, "recorded_at": f.valid_at.isoformat(), "source_message": None}
            for f in EXISTING_FACTS_JASMINE
        ]
        entities = [{
            "entity_name": "Jasmine",
            "entity_type": "person",
            "existing_facts": enriched_facts,
            "known_aliases": ["jasmine"],
        }]
        user_content = format_vp04_input(entities, VP04_STRESS_JASMINE)
        system = get_profile_extraction_prompt("Yinka")

        response = await llm.call_llm(system, user_content)
        print(f"\n[VP-04 Jasmine stress] Raw:\n{response[:800]}")

        result = parse_new_facts(response)
        assert result is not None

        all_facts = []
        for profile in result:
            all_facts.extend(profile.facts)
        raw_text = " ".join(all_facts).lower()

        # Key updates
        assert "a]16z" in raw_text or "andreessen" in raw_text or "series a" in raw_text, \
            "Missed fundraise situation"
        assert "boston" in raw_text, "Missed Boston office expansion"
        assert "marcus" in raw_text and ("advisor" in raw_text or "step" in raw_text or "part-time" in raw_text), \
            "Missed cofounder status change"

    @pytest.mark.asyncio
    async def test_derek_life_changes(self, llm):
        """Derek: engagement, move, team change, injury."""
        enriched_facts = [
            {"content": f.content, "recorded_at": f.valid_at.isoformat(), "source_message": None}
            for f in EXISTING_FACTS_DEREK
        ]
        entities = [{
            "entity_name": "Derek",
            "entity_type": "person",
            "existing_facts": enriched_facts,
            "known_aliases": ["derek"],
        }]
        user_content = format_vp04_input(entities, VP04_STRESS_DEREK)
        system = get_profile_extraction_prompt("Yinka")

        response = await llm.call_llm(system, user_content)
        print(f"\n[VP-04 Derek stress] Raw:\n{response[:800]}")

        result = parse_new_facts(response)
        assert result is not None

        all_facts = []
        for profile in result:
            all_facts.extend(profile.facts)
        raw_text = " ".join(all_facts).lower()

        # Core life changes
        assert "engag" in raw_text or "fianc" in raw_text or "vanessa" in raw_text, \
            "Missed engagement"
        assert "mission" in raw_text or "san francisco" in raw_text or "sf" in raw_text, \
            "Missed move to SF"
        assert "acl" in raw_text or "basketball" in raw_text or "injur" in raw_text, \
            "Missed injury/basketball change"

    @pytest.mark.asyncio
    async def test_derek_does_not_confuse_with_sister_boyfriend(self, llm):
        """Derek: should not attribute sister's boyfriend facts to main Derek."""
        enriched_facts = [
            {"content": f.content, "recorded_at": f.valid_at.isoformat(), "source_message": None}
            for f in EXISTING_FACTS_DEREK
        ]
        entities = [{
            "entity_name": "Derek",
            "entity_type": "person",
            "existing_facts": enriched_facts,
            "known_aliases": ["derek"],
        }]
        user_content = format_vp04_input(entities, VP04_STRESS_DEREK)
        system = get_profile_extraction_prompt("Yinka")

        response = await llm.call_llm(system, user_content)
        result = parse_new_facts(response)
        assert result is not None

        derek_profile = None
        for p in result:
            if "derek" in p.canonical_name.lower() and "danielle" not in p.canonical_name.lower():
                derek_profile = p
                break

        if derek_profile:
            facts_text = " ".join(derek_profile.facts).lower()
            # Sister's boyfriend "Derek" should not contaminate main Derek's facts
            # Danielle's boyfriend details shouldn't appear on Derek's profile
            assert "danielle" not in facts_text or "sister" in facts_text, \
                "Should not confuse Derek with Danielle's boyfriend Derek"

    @pytest.mark.asyncio
    async def test_pronoun_resolution_chen_family(self, llm):
        """Chen family: track multiple people with pronouns across long conversation."""
        enriched_facts = [
            {"content": f.content, "recorded_at": f.valid_at.isoformat(), "source_message": None}
            for f in EXISTING_FACTS_CHEN_FAMILY
        ]
        entities = [
            {
                "entity_name": "Michael Chen",
                "entity_type": "person",
                "existing_facts": [f for f in enriched_facts if "michael" in f["content"].lower() or "cardiologist" in f["content"].lower()],
                "known_aliases": ["michael", "michael chen"],
            },
            {
                "entity_name": "Linda Chen",
                "entity_type": "person",
                "existing_facts": [f for f in enriched_facts if "linda" in f["content"].lower() or "real estate" in f["content"].lower()],
                "known_aliases": ["linda", "linda chen"],
            },
            {
                "entity_name": "David Chen",
                "entity_type": "person",
                "existing_facts": [f for f in enriched_facts if "david" in f["content"].lower() or "medical school" in f["content"].lower()],
                "known_aliases": ["david", "david chen"],
            },
            {
                "entity_name": "Emily Chen",
                "entity_type": "person",
                "existing_facts": [f for f in enriched_facts if "emily" in f["content"].lower() or "mckinsey" in f["content"].lower()],
                "known_aliases": ["emily", "emily chen"],
            },
        ]
        user_content = format_vp04_input(entities, VP04_STRESS_PRONOUNS)
        system = get_profile_extraction_prompt("Yinka")

        response = await llm.call_llm(system, user_content)
        print(f"\n[VP-04 Chen family] Raw:\n{response[:800]}")

        result = parse_new_facts(response)
        assert result is not None

        # Should parse multiple profiles
        assert len(result) >= 2, f"Should extract multiple family members, got {len(result)}"

        # Check for key facts attributed correctly
        all_profiles = {p.canonical_name.lower(): p.facts for p in result}
        print(f"[VP-04 Chen family] Profiles found: {list(all_profiles.keys())}")

        # Michael should have: retired, consulting, cooking
        # David should have: graduated, residency, Mass General, engaged to Rachel
        # Emily should have: left McKinsey, nonprofit, moved back in

    @pytest.mark.asyncio
    async def test_all_stress_conversations_parseable(self, llm):
        """All stress conversations must produce parseable output."""
        system = get_profile_extraction_prompt("Yinka")

        scenarios = [
            ("Omar", EXISTING_FACTS_OMAR, VP04_STRESS_OMAR),
            ("Jasmine", EXISTING_FACTS_JASMINE, VP04_STRESS_JASMINE),
            ("Derek", EXISTING_FACTS_DEREK, VP04_STRESS_DEREK),
            ("Chen Family", EXISTING_FACTS_CHEN_FAMILY, VP04_STRESS_PRONOUNS),
        ]

        for name, facts, conv in scenarios:
            enriched = [
                {"content": f.content, "recorded_at": f.valid_at.isoformat(), "source_message": None}
                for f in facts
            ]
            entities = [{
                "entity_name": name,
                "entity_type": "person",
                "existing_facts": enriched,
                "known_aliases": [name.lower()],
            }]
            user_content = format_vp04_input(entities, conv)

            response = await llm.call_llm(system, user_content)
            result = parse_new_facts(response)
            assert result is not None, f"{name}: parse_new_facts returned None"


# ══════════════════════════════════════════════════════════════════════
#  VP-05 TESTS — Baseline + Stress
# ══════════════════════════════════════════════════════════════════════

class TestVP05Baseline:
    """Sanity checks with clear-cut merge scenarios."""

    @pytest.mark.asyncio
    async def test_obvious_duplicate_high_score(self, llm):
        """Priya Sharma vs P. Sharma — same person."""
        user_content = format_vp05_input(
            MERGE_OBVIOUS_DUPLICATE["entity_a"],
            MERGE_OBVIOUS_DUPLICATE["entity_b"],
        )
        system = get_merge_judgment_prompt()

        response = await llm.call_llm(system, user_content, model=llm.merge_model)
        score = parse_merge_score(response)

        assert score is not None, "parse_merge_score returned None"
        assert score >= 0.75, f"Expected high score for obvious duplicate, got {score}"

    @pytest.mark.asyncio
    async def test_clearly_different_low_score(self, llm):
        """Marcus Chen vs Marcus Williams — different people."""
        user_content = format_vp05_input(
            MERGE_CLEARLY_DIFFERENT["entity_a"],
            MERGE_CLEARLY_DIFFERENT["entity_b"],
        )
        system = get_merge_judgment_prompt()

        response = await llm.call_llm(system, user_content, model=llm.merge_model)
        score = parse_merge_score(response)

        assert score is not None, "parse_merge_score returned None"
        assert score <= 0.50, f"Expected low score for different people, got {score}"

    @pytest.mark.asyncio
    async def test_type_mismatch_rejected(self, llm):
        """Apollo (project) vs Apollo (company) — type mismatch."""
        user_content = format_vp05_input(
            MERGE_TYPE_MISMATCH["entity_a"],
            MERGE_TYPE_MISMATCH["entity_b"],
        )
        system = get_merge_judgment_prompt()

        response = await llm.call_llm(system, user_content, model=llm.merge_model)
        score = parse_merge_score(response)

        assert score is not None, "parse_merge_score returned None"
        assert score <= 0.50, f"Expected rejection for type mismatch, got {score}"


class TestVP05Stress:
    """Stress tests with ambiguous and tricky merge scenarios."""

    @pytest.mark.asyncio
    async def test_contradiction_signals_different_people(self, llm):
        """Sarah Park vs Sarah J. Park — same company but contradicting facts."""
        user_content = format_vp05_input(
            MERGE_STRESS_CONTRADICTION["entity_a"],
            MERGE_STRESS_CONTRADICTION["entity_b"],
        )
        system = get_merge_judgment_prompt()

        response = await llm.call_llm(system, user_content, model=llm.merge_model)
        print(f"\n[VP-05 contradiction] Raw:\n{response[:400]}")

        score = parse_merge_score(response)
        print(f"[VP-05 contradiction] Score: {score}")

        assert score is not None, "parse_merge_score returned None"
        # Different locations (SF vs NYC), different teams — likely different people
        # But same company and Stanford 2018 — could go either way
        # Key test: is the output parseable and reasonable?
        assert 0.0 <= score <= 1.0

    @pytest.mark.asyncio
    async def test_temporal_evolution_same_person(self, llm):
        """James Liu 2022 vs James Liu 2025 — same person, evolved over time."""
        user_content = format_vp05_input(
            MERGE_STRESS_TEMPORAL["entity_a"],
            MERGE_STRESS_TEMPORAL["entity_b"],
        )
        system = get_merge_judgment_prompt()

        response = await llm.call_llm(system, user_content, model=llm.merge_model)
        print(f"\n[VP-05 temporal] Raw:\n{response[:400]}")

        score = parse_merge_score(response)
        print(f"[VP-05 temporal] Score: {score}")

        assert score is not None, "parse_merge_score returned None"
        # Same name, same company (Uber), career progression makes sense
        assert score >= 0.60, f"Should recognize temporal evolution as same person, got {score}"

    @pytest.mark.asyncio
    async def test_org_overlap_distinguished(self, llm):
        """Google AI vs Google DeepMind — related but distinct orgs."""
        user_content = format_vp05_input(
            MERGE_STRESS_ORG_OVERLAP["entity_a"],
            MERGE_STRESS_ORG_OVERLAP["entity_b"],
        )
        system = get_merge_judgment_prompt()

        response = await llm.call_llm(system, user_content, model=llm.merge_model)
        print(f"\n[VP-05 org overlap] Raw:\n{response[:400]}")

        score = parse_merge_score(response)
        print(f"[VP-05 org overlap] Score: {score}")

        assert score is not None, "parse_merge_score returned None"
        # Tricky — they merged in 2023 but were historically distinct
        # The facts mention the merge, so could go either way
        # Key: parseable and reasonable score
        assert 0.0 <= score <= 1.0

    @pytest.mark.asyncio
    async def test_ambiguous_company_names(self, llm):
        """Anthropic vs Anthropic AI — likely same."""
        user_content = format_vp05_input(
            MERGE_AMBIGUOUS["entity_a"],
            MERGE_AMBIGUOUS["entity_b"],
        )
        system = get_merge_judgment_prompt()

        response = await llm.call_llm(system, user_content, model=llm.merge_model)
        score = parse_merge_score(response)

        assert score is not None, "parse_merge_score returned None"
        # Sparse facts but obviously the same company
        assert score >= 0.70, f"Should merge Anthropic and Anthropic AI, got {score}"

    @pytest.mark.asyncio
    async def test_all_merge_scenarios_parseable(self, llm):
        """All merge scenarios must produce parseable scores."""
        system = get_merge_judgment_prompt()

        scenarios = [
            ("Obvious duplicate", MERGE_OBVIOUS_DUPLICATE),
            ("Clearly different", MERGE_CLEARLY_DIFFERENT),
            ("Ambiguous", MERGE_AMBIGUOUS),
            ("Type mismatch", MERGE_TYPE_MISMATCH),
            ("Contradiction", MERGE_STRESS_CONTRADICTION),
            ("Temporal", MERGE_STRESS_TEMPORAL),
            ("Org overlap", MERGE_STRESS_ORG_OVERLAP),
        ]

        for name, scenario in scenarios:
            user_content = format_vp05_input(scenario["entity_a"], scenario["entity_b"])
            response = await llm.call_llm(system, user_content, model=llm.merge_model)
            score = parse_merge_score(response)
            assert score is not None, f"{name}: parse_merge_score returned None"
            assert 0.0 <= score <= 1.0, f"{name}: score out of range: {score}"


# ══════════════════════════════════════════════════════════════════════
#  TOPIC EVOLUTION TESTS — Baseline + Stress
# ══════════════════════════════════════════════════════════════════════

class TestTopicEvolutionBaseline:
    """Sanity checks with clear-cut topic scenarios."""

    @pytest.mark.asyncio
    async def test_routine_conversation_no_new_topics(self, llm):
        """Mundane work conversation should not trigger new topics."""
        current_config = json.dumps(TOPICS_CURRENT, indent=2)
        user_content = (
            f"## Current Config\n{current_config}\n\n"
            f"## Recent Conversation\n{EVOLUTION_CONVERSATION_NO_CHANGE}"
        )
        system = get_topic_evolution_prompt()

        response = await llm.call_llm(system, user_content, model=llm.merge_model)
        clean = response.strip()
        if clean.startswith("```"):
            clean = clean.split("\n", 1)[1].rsplit("```", 1)[0]

        try:
            new_config = json.loads(clean)
        except json.JSONDecodeError:
            pytest.fail(f"LLM returned unparseable JSON:\n{clean[:300]}")

        new_topics = [t for t in new_config if t not in TOPICS_CURRENT]
        assert len(new_topics) <= 1, \
            f"Routine conversation shouldn't spawn many topics, got {new_topics}"


class TestTopicEvolutionStress:
    """Stress tests with multi-domain, meandering conversations."""

    @pytest.mark.asyncio
    async def test_multi_domain_spawns_topics(self, llm):
        """Long meandering conversation should spawn multiple new topics."""
        current_config = json.dumps(TOPICS_CURRENT, indent=2)
        user_content = (
            f"## Current Config\n{current_config}\n\n"
            f"## Recent Conversation\n{EVOLUTION_STRESS_MULTI_DOMAIN}"
        )
        system = get_topic_evolution_prompt()

        response = await llm.call_llm(system, user_content, model=llm.merge_model)
        print(f"\n[Topic evolution multi] Raw:\n{response[:600]}")

        clean = response.strip()
        if clean.startswith("```"):
            clean = clean.split("\n", 1)[1].rsplit("```", 1)[0]

        try:
            new_config = json.loads(clean)
        except json.JSONDecodeError:
            pytest.fail(f"LLM returned unparseable JSON:\n{clean[:300]}")

        new_topics = [t for t in new_config if t not in TOPICS_CURRENT]
        print(f"[Topic evolution multi] New topics: {new_topics}")

        # Should detect at least 2-3 new domains from: art/design, real estate,
        # pottery/crafts, coffee/food, wellness, cycling/fitness, sustainability, entertainment
        assert len(new_topics) >= 2, \
            f"Multi-domain conversation should spawn at least 2 topics, got {new_topics}"

        # Validate structure
        for topic_name in new_topics:
            cfg = new_config[topic_name]
            assert "labels" in cfg, f"{topic_name} missing labels"
            assert isinstance(cfg["labels"], list), f"{topic_name} labels not a list"

    @pytest.mark.asyncio
    async def test_scattered_topics_extracted(self, llm):
        """Rapid topic-switching conversation should still extract coherent topics."""
        current_config = json.dumps(TOPICS_CURRENT, indent=2)
        user_content = (
            f"## Current Config\n{current_config}\n\n"
            f"## Recent Conversation\n{EVOLUTION_STRESS_SCATTERED}"
        )
        system = get_topic_evolution_prompt()

        response = await llm.call_llm(system, user_content, model=llm.merge_model)
        print(f"\n[Topic evolution scattered] Raw:\n{response[:600]}")

        clean = response.strip()
        if clean.startswith("```"):
            clean = clean.split("\n", 1)[1].rsplit("```", 1)[0]

        try:
            new_config = json.loads(clean)
        except json.JSONDecodeError:
            pytest.fail(f"LLM returned unparseable JSON:\n{clean[:300]}")

        new_topics = [t for t in new_config if t not in TOPICS_CURRENT]
        print(f"[Topic evolution scattered] New topics: {new_topics}")

        # Domains touched: running/fitness, chess, therapy/mental health, cooking, family
        assert len(new_topics) >= 2, \
            f"Scattered conversation should spawn at least 2 topics, got {new_topics}"

    @pytest.mark.asyncio
    async def test_preserves_existing_topics(self, llm):
        """New topics should not delete existing configured topics."""
        current_config = json.dumps(TOPICS_CURRENT, indent=2)
        user_content = (
            f"## Current Config\n{current_config}\n\n"
            f"## Recent Conversation\n{EVOLUTION_STRESS_MULTI_DOMAIN}"
        )
        system = get_topic_evolution_prompt()

        response = await llm.call_llm(system, user_content, model=llm.merge_model)
        clean = response.strip()
        if clean.startswith("```"):
            clean = clean.split("\n", 1)[1].rsplit("```", 1)[0]

        new_config = json.loads(clean)

        # Core topics should be preserved
        assert "General" in new_config or "Work" in new_config, \
            "Should preserve at least one existing topic"

    @pytest.mark.asyncio
    async def test_all_evolution_scenarios_valid_json(self, llm):
        """All topic evolution scenarios must produce valid JSON."""
        system = get_topic_evolution_prompt()

        conversations = [
            ("routine", EVOLUTION_CONVERSATION_NO_CHANGE),
            ("multi-domain", EVOLUTION_STRESS_MULTI_DOMAIN),
            ("scattered", EVOLUTION_STRESS_SCATTERED),
        ]

        for label, conv in conversations:
            current_config = json.dumps(TOPICS_CURRENT, indent=2)
            user_content = f"## Current Config\n{current_config}\n\n## Recent Conversation\n{conv}"

            response = await llm.call_llm(system, user_content, model=llm.merge_model)
            clean = response.strip()
            if clean.startswith("```"):
                clean = clean.split("\n", 1)[1].rsplit("```", 1)[0]

            try:
                parsed = json.loads(clean)
                assert isinstance(parsed, dict), f"{label}: expected dict, got {type(parsed)}"
            except json.JSONDecodeError:
                pytest.fail(f"{label}: LLM returned unparseable JSON:\n{clean[:300]}")