import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Mapping

import yaml


_TEMPLATE_DIR = Path(__file__).parent.parent / "templates"
_SECTION_HEADING = re.compile(r"^## (.+?)\s*$")
_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


@dataclass(frozen=True)
class PromptDefinition:
    file_name: str
    section: str
    placeholders: frozenset[str] = frozenset()


PIPELINE_PROMPTS: Dict[str, PromptDefinition] = {
    "extract_entities": PromptDefinition(
        "prompts/extraction.md",
        "Extract Entities",
        frozenset({"user_name"}),
    ),
    "extract_relationships": PromptDefinition(
        "prompts/extraction.md",
        "Extract Relationships",
        frozenset({"user_name"}),
    ),
    "extract_facts": PromptDefinition(
        "prompts/refinement.md",
        "Extract Facts",
        frozenset({"user_name"}),
    ),
    "judge_contradiction": PromptDefinition(
        "prompts/refinement.md",
        "Judge Contradiction",
    ),
    "judge_relevance": PromptDefinition(
        "prompts/refinement.md",
        "Judge Relevance",
    ),
    "judge_merge": PromptDefinition(
        "prompts/merge.md",
        "Judge Merge",
    ),
}

_prompt_cache: Dict[Path, tuple[int, Dict[str, str]]] = {}
_SUSPICIOUS_ENCODING_MARKERS = ("\ufffd", "\x00", "Ã", "Â", "â€", "â†")


def _template_path(file_name: str) -> Path:
    path = (_TEMPLATE_DIR / file_name).resolve()
    template_root = _TEMPLATE_DIR.resolve()
    if path != template_root and template_root not in path.parents:
        raise ValueError(f"Prompt path escapes the template directory: {file_name}")
    return path


def _parse_markdown_sections(file_name: str) -> Dict[str, str]:
    file_path = _template_path(file_name)
    if not file_path.is_file():
        raise FileNotFoundError(f"Prompt file not found: {file_path}")

    modified_ns = file_path.stat().st_mtime_ns
    cached = _prompt_cache.get(file_path)
    if cached and cached[0] == modified_ns:
        return cached[1]

    content = file_path.read_text(encoding="utf-8")
    bad_marker = next(
        (marker for marker in _SUSPICIOUS_ENCODING_MARKERS if marker in content),
        None,
    )
    if bad_marker is not None:
        raise ValueError(
            f"Prompt file appears to contain encoding corruption: {file_name}"
        )
    sections: Dict[str, str] = {}
    current_section = None
    current_content = []

    for line in content.splitlines():
        match = _SECTION_HEADING.match(line)
        if match:
            if current_section is not None:
                body = "\n".join(current_content).strip()
                if not body:
                    raise ValueError(
                        f"Prompt section '{current_section}' is empty in {file_name}"
                    )
                sections[current_section] = body

            current_section = match.group(1).strip()
            if not current_section:
                raise ValueError(f"Prompt file contains an empty section name: {file_name}")
            if current_section in sections:
                raise ValueError(
                    f"Duplicate prompt section '{current_section}' in {file_name}"
                )
            current_content = []
        elif current_section is not None:
            current_content.append(line)

    if current_section is not None:
        body = "\n".join(current_content).strip()
        if not body:
            raise ValueError(
                f"Prompt section '{current_section}' is empty in {file_name}"
            )
        sections[current_section] = body

    if not sections:
        raise ValueError(f"No '## ' prompt sections found in {file_name}")

    _prompt_cache[file_path] = (modified_ns, sections)
    return sections


def load_pipeline_prompt_template(file_name: str, section: str) -> str:
    """Load one named Markdown section without rendering its placeholders."""
    sections = _parse_markdown_sections(file_name)
    prompt = sections.get(section)
    if prompt is None:
        available = ", ".join(sorted(sections))
        raise ValueError(
            f"Section '{section}' not found in {file_name}. Available: {available}"
        )
    return prompt


def render_prompt_text(
    prompt_template: str,
    values: Mapping[str, object],
    *,
    required: Iterable[str] | None = None,
    prompt_name: str = "prompt",
) -> str:
    """Render a prompt and reject missing, unexpected, or unresolved variables."""
    placeholders = set(_PLACEHOLDER.findall(prompt_template))
    required_set = set(required) if required is not None else placeholders
    provided = set(values)

    missing = required_set - provided
    if missing:
        raise ValueError(
            f"{prompt_name} is missing prompt values: {', '.join(sorted(missing))}"
        )

    unexpected = provided - placeholders
    if unexpected:
        raise ValueError(
            f"{prompt_name} received unused prompt values: "
            f"{', '.join(sorted(unexpected))}"
        )

    rendered = prompt_template
    for key, value in values.items():
        rendered = rendered.replace(f"{{{key}}}", str(value))

    unresolved = set(_PLACEHOLDER.findall(rendered))
    if unresolved:
        raise ValueError(
            f"{prompt_name} has unresolved placeholders: "
            f"{', '.join(sorted(unresolved))}"
        )
    return rendered


def validate_prompt_template(
    prompt_template: str,
    *,
    required: Iterable[str],
    prompt_name: str,
) -> None:
    """Validate a configurable prompt without rendering it."""
    actual = set(_PLACEHOLDER.findall(prompt_template))
    expected = set(required)
    if actual != expected:
        raise ValueError(
            f"{prompt_name} placeholder contract mismatch: "
            f"expected {sorted(expected)}, found {sorted(actual)}"
        )


def load_pipeline_prompt(file_name: str, section: str, **kwargs) -> str:
    """Load and strictly render one Markdown prompt section."""
    template = load_pipeline_prompt_template(file_name, section)
    return render_prompt_text(
        template,
        kwargs,
        prompt_name=f"{file_name}#{section}",
    )


def load_named_prompt(prompt_name: str, **kwargs) -> str:
    """Load a manifest-declared prompt with its required variables."""
    definition = PIPELINE_PROMPTS.get(prompt_name)
    if definition is None:
        raise ValueError(f"Unknown pipeline prompt: {prompt_name}")
    template = load_pipeline_prompt_template(
        definition.file_name,
        definition.section,
    )
    return render_prompt_text(
        template,
        kwargs,
        required=definition.placeholders,
        prompt_name=prompt_name,
    )


def validate_prompt_library() -> None:
    """Validate required files, sections, and placeholder contracts."""
    sections_by_file: Dict[str, Dict[str, str]] = {}
    for name, definition in PIPELINE_PROMPTS.items():
        sections = sections_by_file.setdefault(
            definition.file_name,
            _parse_markdown_sections(definition.file_name),
        )
        if definition.section not in sections:
            raise ValueError(
                f"Required prompt section '{definition.section}' is missing "
                f"from {definition.file_name}"
            )
        actual = set(_PLACEHOLDER.findall(sections[definition.section]))
        if actual != set(definition.placeholders):
            raise ValueError(
                f"Prompt '{name}' placeholder contract mismatch: "
                f"expected {sorted(definition.placeholders)}, found {sorted(actual)}"
            )


def load_agent_file(agent_id: str) -> tuple[dict, str]:
    """Load the packaged default identity seed."""
    if agent_id != "AGENT_IDENTITY":
        raise FileNotFoundError(
            "Packaged agent files are seeds only; runtime agents live in Postgres"
        )

    file_path = _template_path("AGENT_IDENTITY.md")
    content = file_path.read_text(encoding="utf-8")
    if content.startswith("---"):
        parts = content.split("---", 2)
        if len(parts) >= 3:
            frontmatter = yaml.safe_load(parts[1]) or {}
            if not isinstance(frontmatter, dict):
                raise ValueError("AGENT_IDENTITY frontmatter must be a mapping")
            return frontmatter, parts[2].strip()
    return {}, content.strip()


def load_agent_config(agent_id: str = "AGENT_IDENTITY") -> "AgentConfig":
    """Load the packaged default agent used to seed Postgres."""
    from common.schema.agent_contracts import AgentConfig

    frontmatter, body = load_agent_file(agent_id)
    return AgentConfig(
        id=agent_id,
        name=frontmatter.get("name", "knoggin_server"),
        persona=frontmatter.get(
            "persona",
            {
                "attention_bias": "Notices relevant details in the current task.",
                "reasoning_style": "Uses evidence and explicit reasoning.",
                "social_temperament": "Helpful and collaborative.",
                "communication_signature": "Communicates clearly and directly.",
                "productive_flaw": (
                    "May spend extra time pursuing interesting details."
                ),
            },
        ),
        model=frontmatter.get("model"),
        temperature=frontmatter.get("temperature", 0.7),
        instructions=body,
        enabled_tools=frontmatter.get("enabled_tools"),
        is_default=frontmatter.get("is_default", False),
    )
