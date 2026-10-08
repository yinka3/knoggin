"""Create reviewable synthetic readiness packets; never approve labels."""

import json
from pathlib import Path

ROOT = Path(__file__).parent
NAMES = [
    ("Lena", "Ortiz", "Person"), ("Owen", "Reed", "Person"),
    ("Nadia", "Shah", "Person"), ("Evan", "Brooks", "Person"),
    ("Tessa", "Wu", "Person"), ("Iris", "Cole", "Person"),
    ("Felix", "Grant", "Person"), ("Zara", "Bell", "Person"),
    ("Aster", "Systems", "Company"), ("Birch", "Dynamics", "Company"),
    ("Cypress", "Labs", "Company"), ("Dune", "Networks", "Company"),
    ("Echo", "Analytics", "Company"), ("Flint", "Software", "Company"),
    ("Grove", "Platform", "Project"), ("Haven", "Migration", "Project"),
    ("Indigo", "Release", "Project"), ("Jade", "Upgrade", "Project"),
    ("Kestrel", "Store", "Database"), ("Lotus", "Engine", "Database"),
]


def write_packet(name, cases):
    packet = {
        "version": 1,
        "review_status": "pending_human_review",
        "evaluation_role": "synthetic_stress",
        "purpose": "New synthetic stress cases for frozen JEV policies. Template variants are correlated and do not replace representative project evidence.",
        "review_instructions": "Review every label and reason before marking reviewed. Do not tune policy after observing this packet's model results.",
        "cases": cases,
    }
    (ROOT / name).write_text(json.dumps(packet, indent=2) + "\n", encoding="utf-8")


def build_identity():
    cases = []
    templates = [
        "{alias}, formally named {name}, is the subject of this record.",
        "The record expands {alias} as {name}.",
        "Here {alias} refers specifically to {name}, rather than {other}.",
        "We use {alias} as the short name for {name} in this report.",
        "The item described as {alias} has the full name {name}.",
        "This occurrence of {alias} identifies {name}; {other} is a separate identity.",
        "The report identifies {alias} ({name}) as the subject.",
    ]
    for index, (alias, suffix, kind) in enumerate(NAMES):
        name, other = f"{alias} {suffix}", f"{alias} Alternative"
        first = 1000 + index * 10
        candidates = [
            {"entity_id": first, "canonical_name": name, "aliases": [alias], "type": kind},
            {"entity_id": first + 1, "canonical_name": other, "aliases": [alias], "type": kind},
        ]
        for variant, template in enumerate(templates):
            cases.append({
                "id": f"explicit_{index}_{variant}", "family": f"explicit_{variant}",
                "mention": alias, "context": template.format(alias=alias, name=name, other=other),
                "candidates": candidates if variant % 2 == 0 else list(reversed(candidates)),
                "proposed_choice": first,
                "reason": "The context explicitly links the short mention to the selected canonical identity.",
            })
        for variant, context in enumerate((f"{alias} was mentioned yesterday.", f"A future update about {alias} is expected.")):
            cases.append({
                "id": f"ambiguous_{index}_{variant}", "family": "shared_alias_ambiguous",
                "mention": alias, "context": context, "candidates": candidates,
                "proposed_choice": "insufficient_evidence",
                "reason": "Both identities share the alias and the context does not distinguish them.",
            })
        if index < 5:
            cases.append({
                "id": f"discovery_{index}", "family": "missing_alias",
                "mention": f"UnlistedHandle{index}",
                "context": f"UnlistedHandle{index} is the new alias for {name}.",
                "candidates": [], "stored_identity": candidates[0] | {"aliases": []},
                "proposed_choice": "candidate_recall_failure",
                "reason": "The stored identity has no lexical alias matching the supplied handle.",
            })
        else:
            cases.append({
                "id": f"different_{index}", "family": "explicit_different_identity",
                "mention": alias,
                "context": f"Here {alias} is a new identity called {alias} Distinct, unrelated to {name} or {other}.",
                "candidates": candidates, "proposed_choice": "none_of_these",
                "reason": "The context explicitly states that neither stored identity is the subject.",
            })
    return cases


def build_extraction():
    cases = []
    for index, (alias, suffix, kind) in enumerate(NAMES):
        name = f"{alias} {suffix}"
        action = {
            "Person": "is the engineer responsible for the review",
            "Company": "is the company supplying the service",
            "Project": "is the project delivering the migration",
            "Database": "is the database storing the application records",
        }[kind]
        for known in (False, True):
            cases.append({
                "id": f"positive_{index}_{int(known)}", "family": f"{'known' if known else 'unknown'}_{kind}",
                "candidate": name, "context": f"{name} {action}.",
                "proposed_type": kind if known else None, "expected_type": kind,
                "reason": "The sentence explicitly establishes the configured entity type.",
            })
        cases.append({
            "id": f"ambiguous_{index}", "family": "ambiguous_type",
            "candidate": alias, "context": f"{alias} appears in the report.",
            "proposed_type": None, "expected_type": None,
            "expected_choice": "insufficient_evidence",
            "reason": "The generic occurrence does not establish any one configured type.",
        })
    return cases


if __name__ == "__main__":
    write_packet("jev_identity_readiness_cases.json", build_identity())
    write_packet("jev_extraction_readiness_cases.json", build_extraction())
