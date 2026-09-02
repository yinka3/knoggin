"""One-way rendering of a run notebook for the agent prompt.

The renderer consumes a copied notebook snapshot.  It never parses rendered
text back into state and never mutates the canonical notebook records.
"""

from __future__ import annotations

import json
from copy import deepcopy
from typing import Any, Callable

from jinja2 import Environment, StrictUndefined

from core.agent.notebook import RunNotebook

_IDENTIFIER_KEYS = {
    "id",
    "entity_id",
    "relationship_id",
    "episode_id",
    "message_id",
    "document_id",
    "project_id",
    "session_id",
    "user_id",
    "agent_id",
    "run_id",
}

_REFERENCE_PREFIXES = {
    "entities": "E",
    "relationships": "R",
    "episodes": "EP",
    "paths": "P",
    "messages": "M",
    "documents": "D",
    "entity": "E",
    "relationship": "R",
    "episode": "EP",
    "path": "P",
    "message": "M",
    "document": "D",
    "web_discoveries": "W",
    "web_reads": "WR",
    "action": "A",
}

NOTEBOOK_TEMPLATE = """RUN NOTEBOOK
{% if summary.text %}Summary: {{ summary.text }}{% if summary.references %} ({{ summary.references|join(', ') }}){% endif %}
{% endif %}{% if entity_pages %}
Entity pages:
{% for page in entity_pages %}- {{ page.reference }}{% if page.name %} {{ page.name }}{% endif %}
  relationships: {{ page.relationships|join(', ') if page.relationships else 'none' }}
  episodes: {{ page.episodes|join(', ') if page.episodes else 'none' }}
  evidence: {{ page.evidence|join(', ') if page.evidence else 'none' }}
{% endfor %}{% endif %}{% if entities %}
Entities:
{% for item in entities %}- {{ item.reference }}{% if item.name %} {{ item.name }}{% endif %}{% if item.details %} — {{ item.details }}{% endif %}
{% endfor %}{% endif %}{% if relationships %}
Relationships:
{% for item in relationships %}- {{ item.reference }}{% if item.endpoints %} {{ item.endpoints }}{% endif %}{% if item.label %}: {{ item.label }}{% endif %}{% if item.evidence %} (evidence: {{ item.evidence|join(', ') }}){% endif %}
{% endfor %}{% endif %}{% if episodes %}
Episodes:
{% for item in episodes %}- {{ item.reference }}{% if item.summary %}: {{ item.summary }}{% endif %}{% if item.evidence %} (evidence: {{ item.evidence|join(', ') }}){% endif %}
{% endfor %}{% endif %}{% if paths %}
Paths:
{% for item in paths %}- {{ item.reference }}{% if item.description %}: {{ item.description }}{% endif %}{% if item.evidence %} (evidence: {{ item.evidence|join(', ') }}){% endif %}
{% endfor %}{% endif %}{% if messages %}
Messages:
{% for item in messages %}- {{ item.reference }}{% if item.content %}: {{ item.content }}{% endif %}{% endfor %}
{% endif %}{% if documents %}
Documents:
{% for item in documents %}- {{ item.reference }}{% if item.name %} {{ item.name }}{% endif %}{% if item.content %}: {{ item.content }}{% endif %}
{% endfor %}{% endif %}{% if web_discoveries %}
Web discoveries:
{% for item in web_discoveries %}- {{ item.reference }}{% if item.title %} {{ item.title }}{% endif %}{% if item.url %}: {{ item.url }}{% endif %}{% if item.snippet %} — {{ item.snippet }}{% endif %}
{% endfor %}{% endif %}{% if web_reads %}
Web reads:
{% for item in web_reads %}- {{ item.reference }}{% if item.title %} {{ item.title }}{% endif %}{% if item.url %}: {{ item.url }}{% endif %}{% if item.content %} — {{ item.content }}{% endif %}
{% endfor %}{% endif %}{% if actions %}
Actions:
{% for item in actions %}- {{ item.reference }} {{ item.tool }}{% if item.result %}: {{ item.result }}{% endif %}
{% endfor %}{% endif %}{% if possible_next_steps %}
Possible next steps:
{% for item in possible_next_steps %}- [{{ item.audience }}] {{ item.tool }}{% if item.arguments %} {{ item.arguments }}{% endif %}{% if item.reason %} — {{ item.reason }}{% elif item.when %} — {{ item.when }}{% endif %}
{% endfor %}{% endif %}"""


def notebook_environment() -> Environment:
    """Create the strict, one-way environment used for notebook prompts."""

    return Environment(
        autoescape=False,
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )


class _ReferenceLocalizer:
    def __init__(self, snapshot: dict[str, Any]) -> None:
        self._handles: dict[str, str] = {}
        counters: dict[str, int] = {}
        for section, records in self._record_sections(snapshot):
            prefix = _REFERENCE_PREFIXES[section]
            for reference in records:
                counters[prefix] = counters.get(prefix, 0) + 1
                self._handles[reference] = f"{prefix}{counters[prefix]}"

        for reference in snapshot.get("entity_pages", {}):
            self._handles.setdefault(reference, self._new_handle("E", counters))

        for reference in snapshot.get("actions", {}):
            self._handles.setdefault(reference, self._new_handle("A", counters))

    @staticmethod
    def _record_sections(snapshot: dict[str, Any]):
        knowledge = snapshot.get("knowledge", {})
        evidence = snapshot.get("evidence", {})
        yield from (
            (section, knowledge.get(section, {}))
            for section in ("entities", "relationships", "episodes", "paths")
        )
        yield "messages", evidence.get("messages", {})
        yield "documents", evidence.get("documents", {})
        web = evidence.get("web", {})
        yield "web_discoveries", web.get("discoveries", {})
        yield "web_reads", web.get("reads", {})

    @staticmethod
    def _new_handle(prefix: str, counters: dict[str, int]) -> str:
        counters[prefix] = counters.get(prefix, 0) + 1
        return f"{prefix}{counters[prefix]}"

    def reference(self, value: object) -> str:
        if isinstance(value, str) and value in self._handles:
            return self._handles[value]
        if isinstance(value, str) and ":" in value:
            prefix, _, _ = value.partition(":")
            display_prefix = _REFERENCE_PREFIXES.get(prefix)
            if display_prefix:
                return display_prefix
        return str(value)

    def identifier(self, key: str, value: object) -> object:
        if key == "entity_id":
            return self.reference(f"entity:{value}")
        if key == "relationship_id":
            return self.reference(f"relationship:{value}")
        if key == "episode_id":
            return self.reference(f"episode:{value}")
        if key == "document_id":
            return self.reference(f"document:{value}:0")
        return value


def _safe_text(value: object, *, limit: int = 320) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text if len(text) <= limit else f"{text[: limit - 1]}…"


def _localize_arguments(value: Any, localizer: _ReferenceLocalizer, key: str = ""):
    if isinstance(value, dict):
        return {
            child_key: _localize_arguments(child, localizer, child_key)
            for child_key, child in value.items()
        }
    if isinstance(value, list):
        if key.endswith("_refs"):
            return [localizer.reference(item) for item in value]
        return [_localize_arguments(item, localizer, key) for item in value]
    if key == "entity_id" and value is not None:
        return localizer.reference(f"entity:{value}")
    if key == "relationship_id" and value is not None:
        return localizer.reference(f"relationship:{value}")
    if key == "episode_id" and value is not None:
        return localizer.reference(f"episode:{value}")
    if key == "document_id" and value is not None:
        return localizer.reference(f"document:{value}:0")
    return value


def _public_details(
    record: dict[str, Any], localizer: _ReferenceLocalizer
) -> str:
    details = []
    for key, value in record.items():
        if (
            key in _IDENTIFIER_KEYS
            or key.endswith("_id")
            or key.endswith("_ids")
            or key.endswith("_refs")
            or key == "evidence"
        ):
            continue
        if isinstance(value, (str, int, float, bool)) and value not in ("", None):
            details.append(f"{key}={_safe_text(value, limit=100)}")
    return ", ".join(details[:6])


def _record_list(
    records: dict[str, dict[str, Any]],
    localizer: _ReferenceLocalizer,
    *,
    section: str,
) -> list[dict[str, Any]]:
    values = []
    for reference, record in records.items():
        item = deepcopy(record)
        item["reference"] = localizer.reference(reference)
        item["details"] = _public_details(item, localizer)
        item["evidence"] = [
            localizer.reference(ref)
            for ref in item.get("evidence_refs", [])
            if isinstance(ref, str)
        ]
        if section == "entities":
            item["name"] = _safe_text(
                item.get("canonical_name") or item.get("name") or ""
            )
        elif section == "relationships":
            source = item.get("source_entity_id", item.get("source"))
            target = item.get("target_entity_id", item.get("target"))
            source_ref = localizer.reference(f"entity:{source}") if source is not None else "?"
            target_ref = localizer.reference(f"entity:{target}") if target is not None else "?"
            item["endpoints"] = f"{source_ref} -> {target_ref}"
            item["label"] = _safe_text(
                item.get("observed_relationship_label")
                or item.get("relationship_type")
                or item.get("label")
                or ""
            )
        elif section == "episodes":
            item["summary"] = _safe_text(item.get("summary") or "")
        elif section == "paths":
            item["description"] = _safe_text(
                item.get("description")
                or item.get("path")
                or item.get("step")
                or ""
            )
        elif section in {"messages", "documents"}:
            item["content"] = _safe_text(item.get("message") or item.get("content") or "")
            item["name"] = _safe_text(
                item.get("document_name") or item.get("original_name") or ""
            )
        elif section in {"web_discoveries", "web_reads"}:
            item["title"] = _safe_text(item.get("title") or "")
            item["url"] = _safe_text(item.get("url") or "")
            item["snippet"] = _safe_text(item.get("snippet") or "")
            item["content"] = _safe_text(item.get("content") or "")
        values.append(item)
    return values


def _render_context(notebook: RunNotebook) -> dict[str, Any]:
    snapshot = notebook.as_dict()
    localizer = _ReferenceLocalizer(snapshot)
    knowledge = snapshot["knowledge"]
    evidence = snapshot["evidence"]
    web = evidence["web"]

    entity_pages = []
    for reference, page in snapshot["entity_pages"].items():
        entity = knowledge["entities"].get(reference, {})
        entity_pages.append(
            {
                "reference": localizer.reference(reference),
                "name": _safe_text(entity.get("canonical_name") or entity.get("name") or ""),
                "relationships": [localizer.reference(ref) for ref in page["relationship_refs"]],
                "episodes": [localizer.reference(ref) for ref in page["episode_refs"]],
                "evidence": [localizer.reference(ref) for ref in page["evidence_refs"]],
            }
        )

    actions = []
    for reference, action in snapshot["actions"].items():
        actions.append(
            {
                "reference": localizer.reference(reference),
                "tool": _safe_text(action.get("tool")),
                "result": _safe_text(json.dumps(action.get("result"), default=str)),
            }
        )

    possible_next_steps = []
    for hint in snapshot["possible_next_steps"]:
        item = deepcopy(hint)
        arguments = _localize_arguments(item.get("arguments", {}), localizer)
        item["arguments"] = _safe_text(json.dumps(arguments, default=str))
        item.setdefault("reason", "")
        item.setdefault("when", "")
        possible_next_steps.append(item)

    return {
        "summary": {
            "text": _safe_text(snapshot["summary"].get("text") or ""),
            "references": [localizer.reference(ref) for ref in snapshot["summary"].get("references", [])],
        },
        "entity_pages": entity_pages,
        "entities": _record_list(knowledge["entities"], localizer, section="entities"),
        "relationships": _record_list(knowledge["relationships"], localizer, section="relationships"),
        "episodes": _record_list(knowledge["episodes"], localizer, section="episodes"),
        "paths": _record_list(knowledge["paths"], localizer, section="paths"),
        "messages": _record_list(evidence["messages"], localizer, section="messages"),
        "documents": _record_list(evidence["documents"], localizer, section="documents"),
        "web_discoveries": _record_list(web["discoveries"], localizer, section="web_discoveries"),
        "web_reads": _record_list(web["reads"], localizer, section="web_reads"),
        "actions": actions,
        "possible_next_steps": possible_next_steps,
    }


def render_notebook(
    notebook: RunNotebook,
    *,
    template: str = NOTEBOOK_TEMPLATE,
    environment_factory: Callable[[], Environment] = notebook_environment,
) -> str:
    """Render a bounded model-facing notebook view without changing state."""

    if not isinstance(notebook, RunNotebook):
        raise TypeError("render_notebook expects a RunNotebook")
    environment = environment_factory()
    return environment.from_string(template).render(**_render_context(notebook)).strip()


__all__ = [
    "NOTEBOOK_TEMPLATE",
    "notebook_environment",
    "render_notebook",
]
