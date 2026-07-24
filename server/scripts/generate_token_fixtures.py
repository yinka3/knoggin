#!/usr/bin/env python3
"""Generate representative ingestion LLM input fixtures.

Fixtures are JSON objects with the same high-level shape used by
LLMService.generate_structured(system=..., user=...).
"""

# ruff: noqa: E402

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_ROOT = REPO_ROOT / "src" / "common" / "templates"

USER_NAME = "Yinka"
FIXTURE_ROOT = REPO_ROOT / "codebase-analysis-docs" / "token-fixtures"

LABEL_BLOCK = """\
General:
  labels: person, organization, project, product, place, tool, concept
Work:
  labels: project, task, teammate, client, vendor, milestone
Personal:
  labels: person, appointment, preference, habit, health
Technical:
  labels: service, database, api, webhook, document, source_event
"""

CASUAL_MESSAGES = [
    "Remember Maya owns the design review for Friday.",
    (
        "I moved the dentist appointment to Thursday morning because the "
        "original Friday slot conflicts with Maya's design review."
    ),
    (
        "Use Notion for project notes and Asana for roadmap tracking. I do not "
        "want project decisions split across random chat threads anymore."
    ),
    (
        "Jordan is the new contact for the vendor renewal, and he said the "
        "contract paperwork should land before the end of the week."
    ),
    (
        "The family trip is now planned for Lake Tahoe in August. Keep the cabin "
        "reservation, packing list, and Theo's birthday gift reminders together."
    ),
    (
        "I prefer short weekly summaries for personal reminders, but for work "
        "projects I want enough detail to remember who owns what."
    ),
    "Add Theo to the birthday gift list.",
    (
        "The plumber from ClearFlow is coming next Tuesday between 9 and 11, and "
        "I need to remember to move the storage boxes away from the utility sink."
    ),
]

DAILY_MESSAGES = [
    (
        "I think source_events should be the durable record, not document chunks. "
        "The chunk is useful for extraction and citations, but it should not be "
        "the whole provenance model."
    ),
    (
        "Document chunks are extraction units, but the original file is the "
        "source. If the user asks where a fact came from, the answer should be "
        "something like Contract.pdf, page 4, chunk 12."
    ),
    (
        "Actually, do not add source ranking yet. Corrections should be "
        "conversational. If Stripe and a note disagree, I can just tell Knoggin "
        "what the active memory should be."
    ),
    (
        "Keep source types to message, doc, and webhook for now. Webhook is "
        "broad enough to cover provider callbacks, polling jobs, game moves, "
        "and automation events."
    ),
    (
        "Webhook automation should create its own session so I can inspect the "
        "process later."
    ),
    (
        "For PDFs, I want page and chunk provenance available even if the first "
        "UI only shows the file name."
    ),
    (
        "The profile job should consolidate less often but with richer context "
        "for daily users."
    ),
    (
        "Relationship extraction matters when I talk through project ownership "
        "and dependencies."
    ),
    (
        "Maya owns the design review, Jordan handles vendor renewal, and Priya "
        "is tracking the API migration."
    ),
    (
        "If Knoggin sees contradictory facts, I will correct the memory in chat "
        "instead of relying on source ranking."
    ),
    (
        "The token analysis should count real system and user prompts, not "
        "guessed sizes. I do not want us estimating token counts from vibes when "
        "we can render fixtures and run tiktoken over the actual input strings."
    ),
    (
        "For ingestion, outputs are out of scope until the input side is measured. "
        "The first layer should only look at what Knoggin sends into the model: "
        "system prompts, formatted messages, retrieved facts, and candidate context."
    ),
    (
        "I want casual, daily, and heavy user cases represented separately because "
        "a casual reminder user, a daily thinking partner user, and a webhook-heavy "
        "automation user have very different input shapes."
    ),
    (
        "People talk differently: some are terse, some ramble, and some work in "
        "bursts. The fixture data needs to include that instead of pretending "
        "every message is a five-word command."
    ),
    (
        "The markdown should explain that frequency and semantic density are "
        "different axes."
    ),
    (
        "For source integration, sessions are the process boundary across chat, "
        "docs, and webhooks."
    ),
]

HEAVY_MESSAGES = [
    "Drop source ranking.",
    (
        "Use source_events as the durable input record. I want that to be the "
        "thing that says Knoggin observed this piece of input in this project "
        "and session."
    ),
    (
        "Webhook should be broad. It can mean a Stripe event, a GitHub issue "
        "change, a polling job normalization, a CRM update, or even a game move."
    ),
    (
        "Make sessions the process boundary. If someone wants automation through "
        "a webhook, that can be a separate session, and if they want to interact "
        "with Knoggin directly they can open another session."
    ),
    (
        "Keep only message, doc, webhook as the top-level source types. Anything "
        "more specific can live in event_type or metadata until there is a real "
        "reason to promote it."
    ),
    (
        "Documents need file and chunk provenance. I want the document to remain "
        "the durable uploaded artifact, while chunks are the extraction and "
        "citation units."
    ),
    (
        "Chunks are extraction units. They can carry page, paragraph, character "
        "offset, and chunk index metadata, but facts should still point back "
        "through a source event."
    ),
    (
        "Facts are active memory, not raw observations. That distinction matters "
        "because the system can observe something without deciding that it is "
        "current truth."
    ),
    (
        "Audit explains changes. If a correction invalidates a bad fact and "
        "creates a new one, I want to know what source event and user action "
        "caused that change."
    ),
    (
        "Corrections happen conversationally. I am intentionally not asking for "
        "a source authority ranking engine right now."
    ),
    (
        "Do not overbuild conflict ranking. We can keep contradiction handling "
        "simple and use manual or conversational correction when memory is wrong."
    ),
    (
        "Token counts must come from fixtures. If we need to model input shapes, "
        "we should generate sample system and user strings, then count them "
        "with tiktoken."
    ),
    (
        "Profile refinement should have more breathing room for heavier users. "
        "That means larger background windows and richer existing fact context, "
        "not necessarily huge interactive batches."
    ),
    (
        "Interactive chat should stay responsive, even if automation and document "
        "ingestion use larger background batches."
    ),
    (
        "Automation can batch bigger because it is not usually waiting on a human "
        "watching the response in real time."
    ),
    (
        "Webhooks need idempotency because providers retry, polling sees the same "
        "external object more than once, and duplicate extraction would inflate "
        "memory."
    ),
    (
        "PDFs need page metadata. DOCX needs paragraph metadata when possible. "
        "Both need file-level provenance so citations can explain the source."
    ),
    (
        "The source model should stay small. I do not want every integration to "
        "turn into a new top-level source type unless it changes the core system."
    ),
    (
        "The graph should not trust semantic writes blindly. Extraction can "
        "suggest observations, but Python and Postgres should enforce scope, "
        "valid IDs, provenance, and audit."
    ),
    (
        "Here is the synthesis: Knoggin should record every observed input as a "
        "scoped source event, then let extraction produce candidate observations. "
        "Messages, documents, and webhooks can all fit this model. For documents, "
        "the durable file and chunk records preserve provenance. For webhooks, "
        "provider metadata and idempotency protect the pipeline. Active facts "
        "remain separate from raw observations, and user corrections should "
        "invalidate or replace facts through the audit layer."
    ),
    (
        "For token analysis, the heavy user is not just higher volume. They "
        "revise terminology, send bursts of related messages, and expect the "
        "system to track the newest direction."
    ),
    (
        "A single long design message can be heavier than twenty tiny webhook "
        "events because it carries more semantic branches and more project "
        "relationships."
    ),
    (
        "The analysis needs to separate source frequency, average message length, "
        "semantic density, memory density, and correction rate."
    ),
]


def repeat_to_size(items: list[str], size: int) -> list[str]:
    if size <= len(items):
        return items[:size]
    output: list[str] = []
    index = 0
    while len(output) < size:
        output.append(items[index % len(items)])
        index += 1
    return output


def message_rows(messages: list[str], start_id: int = 1) -> list[dict[str, Any]]:
    return [
        {
            "id": start_id + index,
            "role": "user",
            "role_label": "USER",
            "message": message,
        }
        for index, message in enumerate(messages)
    ]


def gliner_rows(messages: list[dict[str, Any]]) -> list[tuple[int, str, str]]:
    candidates = [
        ("Maya", "person"),
        ("Notion", "tool"),
        ("Asana", "tool"),
        ("source_events", "concept"),
        ("webhook", "api"),
        ("PDF", "document"),
        ("Knoggin", "product"),
        ("Jordan", "person"),
        ("Priya", "person"),
    ]
    rows = []
    for msg in messages:
        text = msg["message"]
        for span, label in candidates:
            if span.lower() in text.lower():
                rows.append((msg["id"], span, label))
    return rows


def ambiguous_rows(
    messages: list[dict[str, Any]],
) -> list[tuple[int, str, str, list[str]]]:
    rows = []
    for msg in messages:
        text = msg["message"].lower()
        if "source_events" in text:
            rows.append(
                (msg["id"], "source_events", "concept", ["Technical", "General"])
            )
        if "notion" in text:
            rows.append((msg["id"], "Notion", "tool", ["Work", "General"]))
    return rows


def known_entities(case: str) -> list[tuple[str, int]]:
    base = [("Yinka", 1), ("Knoggin", 2)]
    if case == "casual":
        return base + [("Maya", 101), ("Notion", 102)]
    if case == "daily":
        return base + [
            ("source_events", 201),
            ("Document Source Integration", 202),
            ("Maya", 203),
            ("Jordan", 204),
        ]
    return base + [
        ("source_events", 301),
        ("webhook", 302),
        ("document chunks", 303),
        ("Fact Audit Layer", 304),
        ("Knoggin", 305),
    ]


def entity_extraction_fixture(
    case: str,
    messages: list[dict[str, Any]],
) -> dict[str, str]:
    return {
        "system": load_named_prompt("extract_entities"),
        "user": format_vp01_input(
            messages,
            known_entities(case),
            gliner_rows(messages),
            ambiguous_rows(messages),
            LABEL_BLOCK,
        ),
    }


def relationship_candidates(size: int) -> list[dict[str, Any]]:
    names = [
        ("Maya", "person", ["Maya"]),
        ("Jordan", "person", ["Jordan"]),
        ("Priya", "person", ["Priya"]),
        ("source_events", "concept", ["source events", "source_events"]),
        ("webhook", "api", ["webhook", "webhooks"]),
        ("Document Source Integration", "project", ["document source integration"]),
        ("Notion", "tool", ["Notion"]),
        ("Asana", "tool", ["Asana"]),
        ("Fact Audit Layer", "project", ["fact audit layer"]),
        ("Knoggin", "product", ["Knoggin"]),
    ]
    output = []
    for index in range(size):
        name, typ, aliases = names[index % len(names)]
        output.append(
            {
                "canonical_name": name if index < len(names) else f"{name} {index}",
                "type": typ,
                "mentions": aliases,
                "source_msgs": [1 + (index % 8)],
            }
        )
    return output


def relationship_fixture(
    messages: list[dict[str, Any]],
    candidate_count: int,
) -> dict[str, str]:
    session_context = (
        "[MSG 900] [USER]: Earlier discussion established that sessions are "
        "process boundaries and source events are observed inputs."
    )
    return {
        "system": load_named_prompt("extract_relationships"),
        "user": format_vp02_input(
            relationship_candidates(candidate_count),
            messages,
            session_context,
        ),
    }


def relevance_fixture(pair_count: int) -> dict[str, str]:
    facts = [
        "source_events are durable observed inputs",
        "document chunks are extraction units",
        "webhook ingestion uses idempotency keys",
        "facts are active memory",
        "sessions are process boundaries",
    ]
    messages = repeat_to_size(DAILY_MESSAGES + HEAVY_MESSAGES, pair_count)
    lines = []
    for index, message in enumerate(messages, 1):
        selected_facts = ", ".join(
            facts[(index + offset) % len(facts)] for offset in range(3)
        )
        lines.append(f'{index}. Message: "{message}" | Facts: {selected_facts}')
    return {
        "system": load_named_prompt("judge_relevance"),
        "user": (
            "For each index, determine if the message relates to the "
            "entity's facts.\n\n"
            + "\n".join(lines)
        ),
    }


def fact_rows(entity_name: str, count: int) -> list[dict[str, str]]:
    templates = [
        "{entity} owns the design review.",
        "{entity} is involved in source integration.",
        "{entity} uses webhook automation.",
        "{entity} prefers conversational correction.",
        "{entity} tracks document provenance.",
        "{entity} works on profile refinement.",
        "{entity} is connected to the token analysis.",
        "{entity} needs richer background context.",
    ]
    rows = []
    for index in range(count):
        content = templates[index % len(templates)].format(entity=entity_name)
        rows.append(
            {
                "content": content,
                "recorded_at": f"2026-06-{1 + (index % 28):02d}T12:00:00Z",
                "source_message": f"[MSG {100 + index}] {content}",
            }
        )
    return rows


def profile_entities(entity_count: int, facts_per_entity: int) -> list[dict[str, Any]]:
    names = [
        ("Maya", "person"),
        ("Jordan", "person"),
        ("Priya", "person"),
        ("source_events", "concept"),
        ("webhook", "api"),
        ("Document Source Integration", "project"),
        ("Fact Audit Layer", "project"),
        ("Notion", "tool"),
    ]
    entities = []
    for index in range(entity_count):
        name, typ = names[index % len(names)]
        canonical = name if index < len(names) else f"{name} {index}"
        entities.append(
            {
                "entity_name": canonical,
                "entity_type": typ,
                "known_aliases": [canonical.lower(), canonical],
                "existing_facts": fact_rows(canonical, facts_per_entity),
            }
        )
    return entities


def conversation_window(messages: list[str], start_id: int = 1) -> str:
    return "\n".join(
        f"[MSG {start_id + index}] [USER]: {message}"
        for index, message in enumerate(messages)
    )


def profile_fixture(
    entity_count: int,
    facts_per_entity: int,
    window_messages: list[str],
) -> dict[str, str]:
    return {
        "system": load_named_prompt("extract_facts"),
        "user": format_vp04_input(
            profile_entities(entity_count, facts_per_entity),
            conversation_window(window_messages),
        ),
    }


def user_profile_fixture(
    facts_per_entity: int,
    window_messages: list[str],
) -> dict[str, str]:
    return {
        "system": load_named_prompt("extract_facts"),
        "user": format_vp04_input(
            [
                {
                    "entity_name": USER_NAME,
                    "entity_type": "person",
                    "known_aliases": [USER_NAME],
                    "existing_facts": fact_rows(USER_NAME, facts_per_entity),
                }
            ],
            conversation_window(window_messages),
        ),
    }


def contradiction_fixture(pair_count: int) -> dict[str, str]:
    old_facts = [
        "Knoggin treats messages as the only ingestion source.",
        "Document chunks are the durable source.",
        "Source ranking should decide contradictions automatically.",
        "Profile refinement should run after every message.",
    ]
    new_facts = [
        "Knoggin supports message, doc, and webhook source types.",
        "Documents are durable files and chunks are extraction units.",
        "Users correct contradictory memory conversationally.",
        "Profile refinement should consolidate in larger background passes.",
    ]
    lines = [
        "## Facts to evaluate for contradictions:",
        (
            "Return one judgment for every numbered pair below, using exactly "
            "these indexes."
        ),
    ]
    for index in range(pair_count):
        old = old_facts[index % len(old_facts)]
        new = new_facts[index % len(new_facts)]
        lines.append(f'{index + 1}. FACT_A: "{old}" | FACT_B: "{new}"')
    return {
        "system": load_named_prompt("judge_contradiction"),
        "user": "\n".join(lines),
    }


CASE_CONFIGS = {
    "casual": {
        "messages": CASUAL_MESSAGES,
        "entity_messages": 8,
        "relationship_entities": 12,
        "relevance_pairs": 8,
        "profile_entities": 8,
        "profile_facts": 8,
        "profile_window": 30,
        "user_profile_facts": 20,
        "user_profile_window": 30,
        "contradiction_pairs": 4,
    },
    "daily": {
        "messages": DAILY_MESSAGES,
        "entity_messages": 16,
        "relationship_entities": 40,
        "relevance_pairs": 32,
        "profile_entities": 16,
        "profile_facts": 100,
        "profile_window": 80,
        "user_profile_facts": 100,
        "user_profile_window": 45,
        "contradiction_pairs": 16,
    },
    "heavy": {
        "messages": HEAVY_MESSAGES,
        "entity_messages": 40,
        "relationship_entities": 100,
        "relevance_pairs": 80,
        "profile_entities": 40,
        "profile_facts": 200,
        "profile_window": 160,
        "user_profile_facts": 150,
        "user_profile_window": 45,
        "contradiction_pairs": 48,
    },
}


def load_prompt_section(file_name: str, section: str) -> str:
    path = TEMPLATE_ROOT / file_name
    current_section = None
    current_lines: list[str] = []

    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            if current_section == section:
                return "\n".join(current_lines).strip()
            current_section = line[3:].strip()
            current_lines = []
        elif current_section is not None:
            current_lines.append(line)

    if current_section == section:
        return "\n".join(current_lines).strip()
    raise ValueError(f"Prompt section {section!r} not found in {file_name}")


def render_user_prompt(template: str) -> str:
    return template.replace("{user_name}", USER_NAME)


def load_named_prompt(prompt_name: str) -> str:
    sections = {
        "extract_entities": ("prompts/extraction.md", "Extract Entities"),
        "extract_relationships": ("prompts/extraction.md", "Extract Relationships"),
        "extract_facts": ("prompts/refinement.md", "Extract Facts"),
        "judge_contradiction": ("prompts/refinement.md", "Judge Contradiction"),
        "judge_relevance": ("prompts/refinement.md", "Judge Relevance"),
    }
    file_name, section = sections[prompt_name]
    prompt = load_prompt_section(file_name, section)
    if "{user_name}" in prompt:
        return render_user_prompt(prompt)
    return prompt


def format_vp01_input(
    messages: list[dict[str, Any]],
    known_ents: list[tuple[str, int]],
    gliner_ents: list[tuple[int, str, str]],
    ambiguous: list[tuple[int, str, str, list[str]]],
    label_block: str,
) -> str:
    lines = ["## Label Schema\n", label_block, "\n## Messages\n"]
    valid_msg_ids = [msg["id"] for msg in messages]
    lines.append(f"Valid msg_id values: {valid_msg_ids}")
    for msg in messages:
        label = msg.get("role_label") or "USER"
        content = msg.get("message") or msg.get("content") or ""
        lines.append(f'[MSG {msg["id"]}] [{label}]: "{content}"')

    lines.append("\n## Known Entities (from graph - do not override)\n")
    if known_ents:
        for span_text, eid in known_ents:
            lines.append(f'- "{span_text}" -> entity_id={eid}')
    else:
        lines.append("(none)")

    lines.append("\n## GLiNER Extractions (can override if wrong)\n")
    if gliner_ents:
        for msg_id, span, label in gliner_ents:
            lines.append(f'- MSG {msg_id}: "{span}" -> {label}')
    else:
        lines.append("(none)")

    if ambiguous:
        lines.append("\n## Ambiguous (Task 1: assign topic)")
        for msg_id, span_text, label, topics in ambiguous:
            lines.append(
                f'- MSG {msg_id}: "{span_text}" ({label}) -> choose from: {topics}'
            )

    lines.append("\n## Discovery (Task 2: find missed entities)")
    lines.append(
        "Scan messages above for proper nouns not listed in Known Entities or "
        "GLiNER extractions."
    )
    lines.append("Include the MSG id where you found each entity.")
    lines.append("Only return msg_id values from the Valid msg_id list above.")
    return "\n".join(lines)


def format_vp02_input(
    candidates: list[dict[str, Any]],
    messages: list[dict[str, Any]],
    session_context: str,
) -> str:
    lines = ["## Candidate Entities"]
    canonical_names = [
        c.get("canonical_name") for c in candidates if c.get("canonical_name")
    ]
    lines.append(f"Valid canonical entity names: {canonical_names}")
    for c in candidates:
        msg_ids = c.get("source_msgs", [])
        source = f" (from MSG {', '.join(str(m) for m in msg_ids)})" if msg_ids else ""
        lines.append(f"{c['canonical_name']} [{c['type']}]{source}")
        if c.get("mentions"):
            lines.append(f"  Mentions: {', '.join(c['mentions'])}")

    lines.append("\n## Messages")
    valid_msg_ids = [msg["id"] for msg in messages]
    lines.append(f"Valid msg_id values: {valid_msg_ids}")
    for msg in messages:
        label = msg.get("role_label") or "USER"
        content = msg.get("message") or msg.get("content") or msg.get("text") or ""
        lines.append(f'[MSG {msg["id"]}] [{label}]: "{content}"')

    lines.append("\n## Output Constraints")
    lines.append("Use only Valid canonical entity names for entity_a and entity_b.")
    lines.append(
        "Use only Valid canonical entity names for user_connections.entity_name."
    )
    lines.append("Use only Valid msg_id values from the Messages section.")
    lines.append(
        f'Do not put "{USER_NAME}" in entity_a or entity_b; use '
        "user_connections instead."
    )
    lines.append("Do not use Session Context as evidence for a connection.")
    lines.append("\n## Session Context (for pronoun resolution only)")
    lines.append(session_context or "(none)")
    return "\n".join(lines)


def format_entity_block(ent: dict[str, Any]) -> list[str]:
    name = ent.get("canonical_name", ent.get("entity_name", "Unknown"))
    etype = ent.get("type", ent.get("entity_type", "Unknown"))
    output = [f"### {name} [{etype}]"]
    aliases = ent.get("aliases", ent.get("known_aliases", []))
    output.append(f"Aliases: {', '.join(aliases)}" if aliases else "Aliases: (none)")
    facts = ent.get("facts", ent.get("existing_facts", []))
    if facts:
        output.append("Facts:")
        for fact in facts:
            source = fact.get("source_message")
            source_info = f', source: "{source}"' if source else ""
            output.append(
                f"  - {fact.get('content', '')} "
                f"(recorded: {fact.get('recorded_at', 'unknown')}{source_info})"
            )
    return output


def format_vp04_input(entities: list[dict[str, Any]], conversation_text: str) -> str:
    lines = ["## Entities"]
    entity_names = [
        ent.get("canonical_name", ent.get("entity_name", "Unknown"))
        for ent in entities
    ]
    lines.append(f"Valid canonical_name values: {entity_names}")
    for ent in entities:
        lines.extend(format_entity_block(ent))
        lines.append("")
    lines.append("## Prior Conversation For Context")
    lines.append(conversation_text)
    lines.append("")
    lines.append("## Output Constraints")
    lines.append("Use only Valid canonical_name values.")
    lines.append(
        "Use source_msg_id only when the fact is grounded in a [MSG_<id>] or "
        "[MSG <id>] line above."
    )
    lines.append("Use exact existing fact text for supersedes or invalidates.")
    return "\n".join(lines)


def write_fixture(case: str, name: str, payload: dict[str, str]) -> None:
    target = FIXTURE_ROOT / case / f"{name}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def generate_case(case: str, config: dict[str, Any]) -> None:
    messages = repeat_to_size(config["messages"], config["entity_messages"])
    rows = message_rows(messages)
    window = repeat_to_size(config["messages"], config["profile_window"])
    user_window = repeat_to_size(config["messages"], config["user_profile_window"])

    write_fixture(case, "entity_extraction", entity_extraction_fixture(case, rows))
    write_fixture(
        case,
        "relationship_extraction",
        relationship_fixture(
            rows[: min(len(rows), 12)],
            config["relationship_entities"],
        ),
    )
    write_fixture(case, "fact_relevance", relevance_fixture(config["relevance_pairs"]))
    write_fixture(
        case,
        "entity_profile_extraction",
        profile_fixture(config["profile_entities"], config["profile_facts"], window),
    )
    write_fixture(
        case,
        "user_profile_extraction",
        user_profile_fixture(config["user_profile_facts"], user_window),
    )
    write_fixture(
        case,
        "contradiction_judgment",
        contradiction_fixture(config["contradiction_pairs"]),
    )


def main() -> int:
    for case, config in CASE_CONFIGS.items():
        generate_case(case, config)
    print(f"Wrote token fixtures to {FIXTURE_ROOT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
