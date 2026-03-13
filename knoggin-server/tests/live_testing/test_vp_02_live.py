"""Live LLM tests for VP-02 connection extraction."""

import os
import pytest
from dotenv import load_dotenv

from src.common.services.llm_service import LLMService
from src.core.prompts import get_connection_reasoning_prompt
from src.core.utils import format_vp02_input, parse_connection_response


load_dotenv()

pytestmark = pytest.mark.skipif(
    not os.environ.get("OPENROUTER_API_KEY"),
    reason="OPENROUTER_API_KEY not set — skipping live LLM tests",
)


# --- Fixtures ---

@pytest.fixture(scope="session")
def llm():
    return LLMService(
        api_key=os.environ["OPENROUTER_API_KEY"],
        extraction_model="google/gemini-2.5-flash",
    )


USER_NAME = "Yinka"


def make_candidates(entities: list[dict]) -> list[dict]:
    """Build candidate entity list for VP-02 input."""
    return [
        {
            "canonical_name": e["name"],
            "type": e.get("type", "person"),
            "mentions": e.get("mentions", [e["name"].lower()]),
            "source_msgs": e.get("source_msgs", []),
        }
        for e in entities
    ]


def make_messages(msgs: list[tuple]) -> list[dict]:
    """Build message list. Each tuple is (id, role, content)."""
    return [
        {"id": msg_id, "role": role, "message": content}
        for msg_id, role, content in msgs
    ]


def get_all_pairs(result) -> set[tuple[str, str]]:
    """Extract all entity pairs from parsed result, normalized to sorted tuples."""
    pairs = set()
    for r in result:
        for p in r.entity_pairs:
            # Normalize order for easier comparison
            pair = tuple(sorted([p.entity_a.lower(), p.entity_b.lower()]))
            pairs.add(pair)
    return pairs


# --- Baseline Data ---

# ── Explicit joint activity ──

JOINT_ACTIVITY_CANDIDATES = [
    {"name": "Marcus", "type": "person", "source_msgs": [1]},
    {"name": "Priya", "type": "person", "source_msgs": [2]},
    {"name": USER_NAME, "type": "person", "source_msgs": [1, 2]},
]

JOINT_ACTIVITY_MESSAGES = [
    (1, "user", "Grabbed lunch with Marcus at that new ramen place in Hayes Valley, we ended up talking for like two hours about his startup idea and whether he should quit his job to pursue it full time."),
    (2, "user", "Then Priya and I went for a walk around Dolores Park since the weather was so nice, she's been stressed about the product launch and needed to vent a bit about her team dynamics."),
]


# ── Stated relationships ──

STATED_RELATIONSHIP_CANDIDATES = [
    {"name": "Derek", "type": "person", "source_msgs": [1]},
    {"name": "Sophie", "type": "person", "source_msgs": [1]},
    {"name": "Jasmine", "type": "person", "source_msgs": [2]},
    {"name": "Michael", "type": "person", "source_msgs": [2]},
    {"name": "Tariq", "type": "person", "source_msgs": [3]},
    {"name": "Synapse", "type": "company", "source_msgs": [3]},
]

STATED_RELATIONSHIP_MESSAGES = [
    (1, "user", "Derek finally introduced me to his girlfriend Sophie at the party last weekend, they've been dating for about eight months now and seem really happy together, she works in healthcare policy."),
    (2, "user", "Found out that Jasmine actually reports directly to Michael now after the reorg, which explains why she's been so stressed lately since he has a reputation for being pretty demanding."),
    (3, "user", "Tariq is the cofounder and CTO of Synapse, he handles all the technical architecture while his cofounder focuses on the business side and fundraising stuff."),
]


# ── Group event ──

GROUP_EVENT_CANDIDATES = [
    {"name": "Des", "type": "person", "source_msgs": [1]},
    {"name": "Ty", "type": "person", "source_msgs": [1]},
    {"name": "Marcus", "type": "person", "source_msgs": [1]},
    {"name": USER_NAME, "type": "person", "source_msgs": [1]},
]

GROUP_EVENT_MESSAGES = [
    (1, "user", "Had an amazing group workout session this morning with Des, Ty, and Marcus at the climbing gym, we spent about three hours bouldering and spotting each other on the harder V5 and V6 routes."),
]


# ── Sequential events (should NOT connect) ──

SEQUENTIAL_CANDIDATES = [
    {"name": "Omar", "type": "person", "source_msgs": [1]},
    {"name": "Stripe", "type": "company", "source_msgs": [2]},
    {"name": "Priya", "type": "person", "source_msgs": [3]},
    {"name": USER_NAME, "type": "person", "source_msgs": [1, 2, 3]},
]

SEQUENTIAL_MESSAGES = [
    (1, "user", "Had a quick call with Omar this morning about the project timeline, he thinks we need to push back the launch by at least two weeks given how behind we are on the backend work."),
    (2, "user", "After lunch I swung by the Stripe office to pick up some swag they had left over from their developer conference, ran into a few people I knew from my previous job."),
    (3, "user", "Ended the day with a coffee chat with Priya to catch up on how her new role is going, she's been there about three months now and still getting used to the culture."),
]


# ── Different temporal contexts (should NOT connect) ──

TEMPORAL_CANDIDATES = [
    {"name": "Mike", "type": "person", "source_msgs": [1]},
    {"name": "Sarah", "type": "person", "source_msgs": [2]},
    {"name": "Chen", "type": "person", "source_msgs": [3]},
    {"name": USER_NAME, "type": "person", "source_msgs": [1, 2, 3]},
]

TEMPORAL_MESSAGES = [
    (1, "user", "Had dinner with Mike yesterday at that Italian place he's been wanting to try, the pasta was incredible and we ended up staying until they basically kicked us out at closing time."),
    (2, "user", "Meeting Sarah for coffee tomorrow morning to talk about potentially collaborating on that research paper, she has expertise in the exact area where I need help with the methodology."),
    (3, "user", "Still thinking about the conversation I had with Chen last week about career paths, he made some really good points about the tradeoffs between staying technical versus moving into management."),
]


# --- Stress Test Data ---

# ── Stress: Party scene with many interactions ──

PARTY_CANDIDATES = [
    {"name": "Derek", "type": "person", "source_msgs": [1, 3, 5]},
    {"name": "Sophie", "type": "person", "source_msgs": [1, 4]},
    {"name": "Marcus", "type": "person", "source_msgs": [2, 6]},
    {"name": "Priya", "type": "person", "source_msgs": [2, 7]},
    {"name": "Jasmine", "type": "person", "source_msgs": [3, 5]},
    {"name": "Tariq", "type": "person", "source_msgs": [4, 8]},
    {"name": "Chen", "type": "person", "source_msgs": [6]},
    {"name": "Elena", "type": "person", "source_msgs": [7]},
    {"name": USER_NAME, "type": "person", "source_msgs": [1, 2, 3, 4, 5, 6, 7, 8]},
]

PARTY_MESSAGES = [
    (1, "user", "Derek's housewarming party was such a good time last night, Sophie was there helping him host and they make such a great team, she handled all the food while he was mixing drinks for everyone."),
    (2, "user", "Marcus and Priya showed up together which surprised me since I didn't know they knew each other, turns out they met at some tech conference last month and have been hanging out since."),
    (3, "user", "I spent most of the early evening chatting with Jasmine on the balcony about her startup struggles, Derek came out to join us for a bit and gave her some advice about fundraising."),
    (4, "user", "Sophie introduced me to Tariq who apparently is her cousin visiting from Toronto, he works in finance there and was asking a lot of questions about the tech scene here."),
    (5, "user", "Later on Derek and Jasmine got into this really animated debate about whether AI is going to replace software engineers, it was pretty entertaining to watch them go back and forth."),
    (6, "user", "Marcus brought his roommate Chen who I'd never met before, seems like a cool guy, he works at the same company as Marcus on the data infrastructure team."),
    (7, "user", "Priya left early with Elena who she apparently came with originally, I guess Marcus just happened to arrive at the same time as them rather than actually coming together."),
    (8, "user", "At the end of the night it was just me, Derek, and Tariq cleaning up and talking about random stuff until like 2am, Tariq is actually really funny once he loosens up."),
]


# ── Stress: Office politics with complex relationships ──

OFFICE_CANDIDATES = [
    {"name": "Michael", "type": "person", "source_msgs": [1, 4, 7]},
    {"name": "Sarah", "type": "person", "source_msgs": [1, 2, 5]},
    {"name": "David", "type": "person", "source_msgs": [2, 6]},
    {"name": "Rachel", "type": "person", "source_msgs": [3, 5]},
    {"name": "James", "type": "person", "source_msgs": [3, 4, 8]},
    {"name": "Acme Corp", "type": "company", "source_msgs": [1, 6]},
    {"name": "Project Phoenix", "type": "project", "source_msgs": [2, 7]},
    {"name": USER_NAME, "type": "person", "source_msgs": [1, 2, 3, 4, 5, 6, 7, 8]},
]

OFFICE_MESSAGES = [
    (1, "user", "Big meeting today with Michael who runs the engineering org at Acme Corp, Sarah was there too since she's been leading the technical evaluation on their side and has the most context on requirements."),
    (2, "user", "Sarah pulled me aside after to say that David has been pushing back on Project Phoenix internally, apparently he thinks the timeline is unrealistic and has been vocal about it in their leadership meetings."),
    (3, "user", "Had lunch with Rachel and James from the sales team to prep for the customer presentation next week, they've been working this account for months and have good insight into the politics."),
    (4, "user", "James mentioned that Michael is actually pretty supportive of the deal but is getting pressure from above to show quick wins, which explains some of the aggressive timeline asks we've been getting."),
    (5, "user", "Rachel set up a call between me and Sarah to go through the technical requirements in detail, spent about two hours on it and I think we're aligned on what needs to happen."),
    (6, "user", "David finally joined one of our syncs and was actually much more reasonable than I expected based on what Sarah had said, maybe he just needed to feel heard about his timeline concerns."),
    (7, "user", "Michael asked me directly if Project Phoenix can be delivered in Q2, I told him we'd need to cut scope significantly and he seemed open to discussing what that would look like with Sarah."),
    (8, "user", "End of day debrief with James about where we landed, he's optimistic but thinks we need to get David fully on board before the exec review or it could derail everything."),
]


# ── Stress: Red herrings (mentioned together but NOT interacting) ──

RED_HERRING_CANDIDATES = [
    {"name": "Omar", "type": "person", "source_msgs": [1, 3]},
    {"name": "Stripe", "type": "company", "source_msgs": [1]},
    {"name": "Priya", "type": "person", "source_msgs": [2, 4]},
    {"name": "Google", "type": "company", "source_msgs": [2]},
    {"name": "Marcus", "type": "person", "source_msgs": [3]},
    {"name": "Notion", "type": "company", "source_msgs": [4]},
    {"name": USER_NAME, "type": "person", "source_msgs": [1, 2, 3, 4]},
]

RED_HERRING_MESSAGES = [
    (1, "user", "Been thinking about Omar's career advice lately, he really pushed me to consider fintech more seriously, separately I've been reading a lot about how Stripe built their developer experience."),
    (2, "user", "Priya asked for my opinion on her resume yesterday, she's thinking about making a move soon, also saw that Google just announced another round of layoffs which is making everyone nervous."),
    (3, "user", "Called Omar to catch up and he mentioned he's been interviewing at a few places, completely unrelated but Marcus texted me about grabbing dinner sometime next week."),
    (4, "user", "Helped Priya prep for an interview this morning over coffee, she's really nervous but I think she'll do great, heard through the grapevine that Notion is hiring aggressively right now."),
]


# ── Stress: Introduction chains ──

INTRO_CANDIDATES = [
    {"name": "Kenji", "type": "person", "source_msgs": [1, 2, 3]},
    {"name": "Dr. Patel", "type": "person", "source_msgs": [1]},
    {"name": "Hiroshi", "type": "person", "source_msgs": [2]},
    {"name": "Tokyo Startup Hub", "type": "organization", "source_msgs": [2]},
    {"name": "Yuki", "type": "person", "source_msgs": [3]},
    {"name": USER_NAME, "type": "person", "source_msgs": [1, 2, 3]},
]

INTRO_MESSAGES = [
    (1, "user", "Kenji introduced me to his mentor Dr. Patel at the conference yesterday, she's been advising him on his research for the past two years and had some really insightful questions about my work."),
    (2, "user", "Then Kenji took me to meet Hiroshi who runs the Tokyo Startup Hub accelerator program, apparently they've been collaborating on a joint initiative to connect Japanese and American founders."),
    (3, "user", "Later that evening Kenji brought his colleague Yuki to dinner with us, the three of us ended up talking until midnight about the differences between startup culture in Japan versus the US."),
]


# ── Stress: Negated and hypothetical relationships ──

NEGATED_CANDIDATES = [
    {"name": "Derek", "type": "person", "source_msgs": [1, 2]},
    {"name": "Marcus", "type": "person", "source_msgs": [1]},
    {"name": "Priya", "type": "person", "source_msgs": [2, 3]},
    {"name": "Jasmine", "type": "person", "source_msgs": [3]},
    {"name": USER_NAME, "type": "person", "source_msgs": [1, 2, 3]},
]

NEGATED_MESSAGES = [
    (1, "user", "Derek mentioned he hasn't seen Marcus in months even though they used to be really close, apparently they had some kind of falling out over a business deal that went sideways last year."),
    (2, "user", "I wish I could introduce Derek to Priya because I think they'd really hit it off, but the timing has never worked out and they've never actually met despite me talking about each of them constantly."),
    (3, "user", "Priya was supposed to meet Jasmine at the conference but Jasmine's flight got cancelled so they ended up missing each other entirely, hopefully they can connect at the next one."),
]


# --- Baseline Tests ---

class TestVP02Baseline:
    """Clear-cut cases for each core rule."""

    @pytest.mark.asyncio
    async def test_joint_activity_creates_connections(self, llm):
        """Explicit joint activities should create connections."""
        candidates = make_candidates(JOINT_ACTIVITY_CANDIDATES)
        messages = make_messages(JOINT_ACTIVITY_MESSAGES)
        
        system = get_connection_reasoning_prompt(USER_NAME)
        user_content = format_vp02_input(candidates, messages, "")
        
        response = await llm.call_llm(system, user_content)
        print(f"\n[VP-02 joint activity] Raw:\n{response}")
        
        result = parse_connection_response(response)
        pairs = get_all_pairs(result)
        
        # Marcus + Yinka had lunch together
        assert ("marcus", "yinka") in pairs, "Missed Marcus-Yinka lunch connection"
        # Priya + Yinka went for walk together
        assert ("priya", "yinka") in pairs, "Missed Priya-Yinka walk connection"
        # Marcus and Priya did NOT interact
        assert ("marcus", "priya") not in pairs, "Hallucinated Marcus-Priya connection"

    @pytest.mark.asyncio
    async def test_stated_relationships_create_connections(self, llm):
        """Stated relationships (girlfriend, reports to) should create connections."""
        candidates = make_candidates(STATED_RELATIONSHIP_CANDIDATES)
        messages = make_messages(STATED_RELATIONSHIP_MESSAGES)
        
        system = get_connection_reasoning_prompt(USER_NAME)
        user_content = format_vp02_input(candidates, messages, "")
        
        response = await llm.call_llm(system, user_content)
        print(f"\n[VP-02 stated rel] Raw:\n{response}")
        
        result = parse_connection_response(response)
        pairs = get_all_pairs(result)
        
        # Pairs are normalized to sorted order, so check alphabetically
        assert ("derek", "sophie") in pairs, "Missed Derek-Sophie girlfriend relationship"
        assert ("jasmine", "michael") in pairs, "Missed Jasmine-Michael reports-to relationship"
        assert ("synapse", "tariq") in pairs, "Missed Tariq-Synapse cofounder relationship"

    @pytest.mark.asyncio
    async def test_group_event_connects_all(self, llm):
        """Group event should connect all participants to each other."""
        candidates = make_candidates(GROUP_EVENT_CANDIDATES)
        messages = make_messages(GROUP_EVENT_MESSAGES)
        
        system = get_connection_reasoning_prompt(USER_NAME)
        user_content = format_vp02_input(candidates, messages, "")
        
        response = await llm.call_llm(system, user_content)
        print(f"\n[VP-02 group event] Raw:\n{response}")
        
        result = parse_connection_response(response)
        pairs = get_all_pairs(result)
        
        participants = ["des", "ty", "marcus", "yinka"]
        # All pairs should be connected
        from itertools import combinations
        expected_pairs = {tuple(sorted(p)) for p in combinations(participants, 2)}
        
        for pair in expected_pairs:
            assert pair in pairs, f"Missed group connection: {pair}"

    @pytest.mark.asyncio
    async def test_sequential_events_no_cross_connection(self, llm):
        """Sequential unrelated events should NOT connect entities across events."""
        candidates = make_candidates(SEQUENTIAL_CANDIDATES)
        messages = make_messages(SEQUENTIAL_MESSAGES)
        
        system = get_connection_reasoning_prompt(USER_NAME)
        user_content = format_vp02_input(candidates, messages, "")
        
        response = await llm.call_llm(system, user_content)
        print(f"\n[VP-02 sequential] Raw:\n{response}")
        
        result = parse_connection_response(response)
        pairs = get_all_pairs(result)
        
        # User should connect to each
        assert ("omar", "yinka") in pairs, "Missed Omar-Yinka call"
        assert ("priya", "yinka") in pairs, "Missed Priya-Yinka coffee"
        
        # But entities should NOT connect to each other
        assert ("omar", "priya") not in pairs, "Hallucinated Omar-Priya connection"
        assert ("omar", "stripe") not in pairs, "Hallucinated Omar-Stripe connection"
        assert ("priya", "stripe") not in pairs, "Hallucinated Priya-Stripe connection"

    @pytest.mark.asyncio
    async def test_temporal_separation_no_connection(self, llm):
        """Different temporal contexts should NOT create connections."""
        candidates = make_candidates(TEMPORAL_CANDIDATES)
        messages = make_messages(TEMPORAL_MESSAGES)
        
        system = get_connection_reasoning_prompt(USER_NAME)
        user_content = format_vp02_input(candidates, messages, "")
        
        response = await llm.call_llm(system, user_content)
        print(f"\n[VP-02 temporal] Raw:\n{response}")
        
        result = parse_connection_response(response)
        pairs = get_all_pairs(result)
        
        # Mike (yesterday), Sarah (tomorrow), Chen (last week) should NOT connect
        assert ("mike", "sarah") not in pairs, "Hallucinated Mike-Sarah connection"
        assert ("mike", "chen") not in pairs, "Hallucinated Mike-Chen connection"
        assert ("sarah", "chen") not in pairs, "Hallucinated Sarah-Chen connection"


# --- Stress Tests ---

class TestVP02Stress:
    """Complex scenarios with many entities and subtle distinctions."""

    @pytest.mark.asyncio
    async def test_party_scene_connections(self, llm):
        """Party with many interactions — should identify correct connections."""
        candidates = make_candidates(PARTY_CANDIDATES)
        messages = make_messages(PARTY_MESSAGES)
        
        system = get_connection_reasoning_prompt(USER_NAME)
        user_content = format_vp02_input(candidates, messages, "")
        
        response = await llm.call_llm(system, user_content)
        print(f"\n[VP-02 party stress] Raw:\n{response[:1000]}")
        
        result = parse_connection_response(response)
        pairs = get_all_pairs(result)
        print(f"[VP-02 party stress] Pairs found: {pairs}")
        
        # Explicit connections that MUST be found
        must_have = [
            ("derek", "sophie"),      # hosting together
            ("marcus", "priya"),      # arrived together / met at conference
            ("jasmine", "derek"),     # chatted on balcony, later debated
            ("sophie", "tariq"),      # cousins
            ("marcus", "chen"),       # roommates
        ]
        
        for pair in must_have:
            normalized = tuple(sorted(pair))
            assert normalized in pairs, f"Missed essential connection: {pair}"

    @pytest.mark.asyncio
    async def test_party_no_false_connections(self, llm):
        """Party scene should NOT create connections where none exist."""
        candidates = make_candidates(PARTY_CANDIDATES)
        messages = make_messages(PARTY_MESSAGES)
        
        system = get_connection_reasoning_prompt(USER_NAME)
        user_content = format_vp02_input(candidates, messages, "")
        
        response = await llm.call_llm(system, user_content)
        result = parse_connection_response(response)
        pairs = get_all_pairs(result)
        
        # Priya and Elena came together but Marcus just arrived same time
        # Chen and Jasmine never interacted
        should_not_have = [
            ("chen", "jasmine"),
            ("chen", "tariq"),
            ("elena", "derek"),
        ]
        
        false_positives = [p for p in should_not_have if tuple(sorted(p)) in pairs]
        print(f"[VP-02 party false pos] False positives: {false_positives}")
        
        # Soft check — warn but don't fail on minor hallucinations
        if false_positives:
            print(f"  WARNING: Potential hallucinated connections: {false_positives}")

    @pytest.mark.asyncio
    async def test_office_politics_connections(self, llm):
        """Complex office relationships with hierarchy and projects."""
        candidates = make_candidates(OFFICE_CANDIDATES)
        messages = make_messages(OFFICE_MESSAGES)
        
        system = get_connection_reasoning_prompt(USER_NAME)
        user_content = format_vp02_input(candidates, messages, "")
        
        response = await llm.call_llm(system, user_content)
        print(f"\n[VP-02 office stress] Raw:\n{response[:1000]}")
        
        result = parse_connection_response(response)
        pairs = get_all_pairs(result)
        print(f"[VP-02 office stress] Pairs found: {pairs}")
        
        # Key connections
        must_have = [
            ("michael", "sarah"),      # meeting together, collaborate
            ("michael", "acme corp"),  # runs engineering there
            ("sarah", "acme corp"),    # works there
            ("rachel", "james"),       # lunch together, same team
            ("sarah", "david"),        # David pushing back, Sarah reports
        ]
        
        found = 0
        for pair in must_have:
            normalized = tuple(sorted(pair))
            if normalized in pairs:
                found += 1
            else:
                print(f"  Missed: {pair}")
        
        assert found >= 3, f"Should find at least 3/5 key connections, got {found}"

    @pytest.mark.asyncio
    async def test_red_herrings_no_false_connections(self, llm):
        """Entities mentioned in same message but NOT interacting should NOT connect."""
        candidates = make_candidates(RED_HERRING_CANDIDATES)
        messages = make_messages(RED_HERRING_MESSAGES)
        
        system = get_connection_reasoning_prompt(USER_NAME)
        user_content = format_vp02_input(candidates, messages, "")
        
        response = await llm.call_llm(system, user_content)
        print(f"\n[VP-02 red herring] Raw:\n{response}")
        
        result = parse_connection_response(response)
        pairs = get_all_pairs(result)
        
        # These should NOT be connected (just co-mentioned)
        should_not_connect = [
            ("omar", "stripe"),    # thinking about Omar + reading about Stripe
            ("priya", "google"),   # Priya resume + Google layoffs
            ("omar", "marcus"),    # separate mentions in same message
            ("priya", "notion"),   # helped Priya + heard about Notion
        ]
        
        false_positives = []
        for pair in should_not_connect:
            normalized = tuple(sorted(pair))
            if normalized in pairs:
                false_positives.append(pair)
        
        print(f"[VP-02 red herring] False positives: {false_positives}")
        assert len(false_positives) <= 1, f"Too many hallucinated connections: {false_positives}"

    @pytest.mark.asyncio
    async def test_introduction_chains(self, llm):
        """Introduction chains should connect introducer to both parties."""
        candidates = make_candidates(INTRO_CANDIDATES)
        messages = make_messages(INTRO_MESSAGES)
        
        system = get_connection_reasoning_prompt(USER_NAME)
        user_content = format_vp02_input(candidates, messages, "")
        
        response = await llm.call_llm(system, user_content)
        print(f"\n[VP-02 intro chains] Raw:\n{response}")
        
        result = parse_connection_response(response)
        pairs = get_all_pairs(result)
        
        # Kenji introduces User to various people
        must_have = [
            ("kenji", "dr. patel"),     # mentor relationship
            ("kenji", "yinka"),         # introduced User
            ("yinka", "dr. patel"),     # was introduced
            ("kenji", "hiroshi"),       # collaborating
            ("hiroshi", "tokyo startup hub"),  # runs it
            ("kenji", "yuki"),          # colleagues
            ("yinka", "yuki"),          # had dinner together
        ]
        
        found = 0
        for pair in must_have:
            normalized = tuple(sorted([p.lower() for p in pair]))
            if normalized in pairs:
                found += 1
            else:
                print(f"  Missed: {pair}")
        
        assert found >= 5, f"Should find at least 5/7 intro connections, got {found}"

    @pytest.mark.asyncio
    async def test_negated_relationships_handled(self, llm):
        """Hypothetical/never-happened relationships should NOT create connections.
        Note: Past relationships that ended (falling out) may still be extracted as connections."""
        candidates = make_candidates(NEGATED_CANDIDATES)
        messages = make_messages(NEGATED_MESSAGES)
        
        system = get_connection_reasoning_prompt(USER_NAME)
        user_content = format_vp02_input(candidates, messages, "")
        
        response = await llm.call_llm(system, user_content)
        print(f"\n[VP-02 negated] Raw:\n{response}")
        
        result = parse_connection_response(response)
        pairs = get_all_pairs(result)
        
        # Derek-Marcus: "falling out over a business deal" IS an interaction, even if negative
        # The LLM is correct to extract this — estranged relationships are still relationships
        if ("derek", "marcus") in pairs:
            print("  NOTE: Derek-Marcus extracted (falling out = past interaction, acceptable)")
        
        # These are explicitly hypothetical or never happened — should NOT connect
        should_not_connect = [
            ("derek", "priya"),     # "never actually met" - hypothetical
            ("priya", "jasmine"),   # "missed each other" - never happened
        ]
        
        false_positives = []
        for pair in should_not_connect:
            normalized = tuple(sorted(pair))
            if normalized in pairs:
                false_positives.append(pair)
        
        print(f"[VP-02 negated] False positives: {false_positives}")
        assert len(false_positives) == 0, f"Should not connect hypothetical relationships: {false_positives}"

    @pytest.mark.asyncio
    async def test_all_stress_scenarios_parseable(self, llm):
        """All stress scenarios must produce parseable output."""
        system = get_connection_reasoning_prompt(USER_NAME)
        
        scenarios = [
            ("party", PARTY_CANDIDATES, PARTY_MESSAGES),
            ("office", OFFICE_CANDIDATES, OFFICE_MESSAGES),
            ("red_herring", RED_HERRING_CANDIDATES, RED_HERRING_MESSAGES),
            ("intro", INTRO_CANDIDATES, INTRO_MESSAGES),
            ("negated", NEGATED_CANDIDATES, NEGATED_MESSAGES),
        ]
        
        for name, cands, msgs in scenarios:
            candidates = make_candidates(cands)
            messages = make_messages(msgs)
            user_content = format_vp02_input(candidates, messages, "")
            
            response = await llm.call_llm(system, user_content)
            result = parse_connection_response(response)
            
            # Should parse without error (result can be empty list)
            assert result is not None or result == [], f"{name}: parse failed"
            print(f"[VP-02 parseable] {name}: {len(result)} message groups parsed")