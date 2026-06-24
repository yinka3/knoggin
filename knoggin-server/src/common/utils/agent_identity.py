import re
from typing import Dict, Mapping


EDITABLE_BRAIN_SECTIONS = (
    "Behavioral Directives",
    "Project Context",
    "User Preferences & Lessons Learned",
)
SELF_CONCEPTION_SECTION = "Self-Conception"
REQUIRED_BRAIN_SECTIONS = (SELF_CONCEPTION_SECTION, *EDITABLE_BRAIN_SECTIONS)
MAX_BRAIN_SECTION_CHARS = 4000
MAX_BRAIN_CHARS = 12000

PERSONA_FIELDS = (
    ("attention_bias", "Attention Bias"),
    ("reasoning_style", "Reasoning Style"),
    ("social_temperament", "Social Temperament"),
    ("communication_signature", "Communication Signature"),
    ("productive_flaw", "Productive Flaw"),
)
MAX_PERSONA_FIELD_CHARS = 600


def render_persona_profile(profile: Mapping[str, str]) -> str:
    """Validate and render the settings-facing persona profile as Markdown."""
    sections = []
    for key, title in PERSONA_FIELDS:
        value = str(profile.get(key, "")).strip()
        if not value:
            raise ValueError(f"Persona field is required: {key}")
        if len(value) > MAX_PERSONA_FIELD_CHARS:
            raise ValueError(
                f"Persona field '{key}' exceeds {MAX_PERSONA_FIELD_CHARS} characters"
            )
        if re.search(r"(?m)^##?\s+", value):
            raise ValueError(f"Persona field '{key}' cannot contain headings")
        sections.append(f"## {title}\n{value}")
    return "\n\n".join(sections)


def parse_persona_profile(persona: str) -> Dict[str, str]:
    """Parse a structured persona for settings; preserve legacy text safely."""
    text = (persona or "").strip()
    parsed: Dict[str, str] = {}
    for index, (key, title) in enumerate(PERSONA_FIELDS):
        match = re.search(rf"(?m)^## {re.escape(title)}\s*$", text)
        if not match:
            continue
        later_matches = [
            re.search(rf"(?m)^## {re.escape(next_title)}\s*$", text[match.end() :])
            for _, next_title in PERSONA_FIELDS[index + 1 :]
        ]
        starts = [item.start() for item in later_matches if item]
        end = match.end() + min(starts) if starts else len(text)
        parsed[key] = text[match.end() : end].strip()

    if len(parsed) == len(PERSONA_FIELDS):
        return parsed

    return {
        "attention_bias": text or "Notices relevant details in the current task.",
        "reasoning_style": "Uses evidence and explicit reasoning.",
        "social_temperament": "Helpful and collaborative.",
        "communication_signature": "Communicates clearly and directly.",
        "productive_flaw": "May spend extra time pursuing interesting details.",
    }


def normalize_agent_brain(markdown: str, persona: str = "") -> str:
    """Return a sectioned Brain while preserving already-structured Markdown."""
    body = (markdown or "").strip()
    body = re.sub(
        r"(?m)^# Core Identity\s*$",
        f"# {SELF_CONCEPTION_SECTION}",
        body,
        count=1,
    )
    headings = set(re.findall(r"(?m)^#\s+(.+?)\s*$", body))
    if all(section in headings for section in REQUIRED_BRAIN_SECTIONS):
        return body

    directives = body or "- [Empty]"
    return (
        f"# {SELF_CONCEPTION_SECTION}\n"
        "You are a personal intelligence agent responsible for helping the user "
        "understand, organize, and act on accumulated knowledge.\n\n"
        f"# Behavioral Directives\n{directives}\n\n"
        "# Project Context\n- [Empty]\n\n"
        "# User Preferences & Lessons Learned\n- [Empty]"
    )


def replace_brain_section(markdown: str, section: str, content: str) -> str:
    if section not in EDITABLE_BRAIN_SECTIONS:
        allowed = ", ".join(EDITABLE_BRAIN_SECTIONS)
        raise ValueError(f"Section is not agent-editable. Allowed sections: {allowed}")

    clean_content = content.strip()
    if not clean_content:
        raise ValueError("Brain section content cannot be empty")
    if len(clean_content) > MAX_BRAIN_SECTION_CHARS:
        raise ValueError(
            f"Brain section exceeds {MAX_BRAIN_SECTION_CHARS} characters"
        )
    if re.search(r"(?m)^#\s+", clean_content):
        raise ValueError("Brain section content cannot create top-level sections")

    heading = f"# {section}"
    heading_match = re.search(rf"(?m)^{re.escape(heading)}\s*$", markdown)
    if not heading_match:
        raise ValueError(f"Brain is missing required section: {section}")

    remainder = markdown[heading_match.end() :]
    next_heading = re.search(r"(?m)^#\s+", remainder)
    section_end = (
        heading_match.end() + next_heading.start()
        if next_heading
        else len(markdown)
    )
    updated = (
        markdown[: heading_match.end()]
        + "\n"
        + clean_content
        + "\n\n"
        + markdown[section_end:].lstrip()
    ).rstrip()
    if len(updated) > MAX_BRAIN_CHARS:
        raise ValueError(f"Brain exceeds {MAX_BRAIN_CHARS} characters")
    return updated
