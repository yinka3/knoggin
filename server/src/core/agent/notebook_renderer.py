"""One-way rendering of a run notebook for the agent prompt.

The renderer consumes a copied notebook snapshot.  It never parses rendered
text back into state and never mutates the canonical notebook records.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from typing import Any, Callable

from jinja2 import Environment, StrictUndefined

from common.utils.local_references import register_short_uuid_references
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
  evidence: {{ page.evidence|join(', ') if page.evidence else 'none' }}{{ '\n' }}
{% endfor %}{% endif %}{% if entities %}
Entities:
{% for item in entities %}- {{ item.reference }}{% if item.name %} {{ item.name }}{% endif %}{% if item.details %} — {{ item.details }}{% endif %}{{ '\n' }}
{% endfor %}{% endif %}{% if relationships %}
Relationships:
{% for item in relationships %}- {{ item.reference }}{% if item.endpoints %} {{ item.endpoints }}{% endif %}{% if item.label %}: {{ item.label }}{% endif %}{% if item.evidence %} (evidence: {{ item.evidence|join(', ') }}){% endif %}{{ '\n' }}
  qualification: observed evidence, not a current-state claim
{% if item.context %}  context: {{ item.context }}
{% endif %}{{ '\n' }}
{% endfor %}{% endif %}{% if episodes %}
Episodes:
{% for item in episodes %}- {{ item.reference }}{% if item.summary %}: {{ item.summary }}{% endif %}
{% if item.chronology %}  chronology: {{ item.chronology }}
{% endif %}{% if item.developments %}  developments: {{ item.developments|join('; ') }}
{% endif %}{% if item.updates %}  updates: {{ item.updates|join('; ') }}
{% endif %}{% if item.unresolved %}  unresolved: {{ item.unresolved|join('; ') }}
{% endif %}{% if item.evidence %}  evidence in notebook: {{ item.evidence|join(', ') }}
{% endif %}{% if item.support %}  historical support:
{% for source in item.support %}  - {{ source.label }}{% if source.url %}: {{ source.url }}{% endif %}{% if source.excerpt %} — {{ source.excerpt }}{% endif %}{{ '\n' }}
{% endfor %}{% endif %}{{ '\n' }}
{% endfor %}{% endif %}{% if paths %}
Paths:
{% for item in paths %}- {{ item.reference }}{% if item.description %}: {{ item.description }}{% endif %}{% if item.evidence %} (evidence: {{ item.evidence|join(', ') }}){% endif %}{{ '\n' }}
{% endfor %}{% endif %}{% if messages %}
Messages:
{% for item in messages %}- {{ item.reference }}{% if item.content %}: {{ item.content }}{% endif %}{{ '\n' }}{% endfor %}
{% endif %}{% if documents %}
Documents:
{% for item in documents %}- {{ item.reference }}{% if item.name %} {{ item.name }}{% endif %}{% if item.content %}: {{ item.content }}{% endif %}
{{ '\n' }}{% endfor %}{% endif %}{% if web_discoveries %}
Web discoveries (not read):
{% for item in web_discoveries %}- {{ item.reference }}{% if item.title %} {{ item.title }}{% endif %}{% if item.url %}: {{ item.url }}{% endif %}{% if item.snippet %} — discovery snippet: {{ item.snippet }}{% endif %}{{ '\n' }}
{% endfor %}{% endif %}{% if web_reads %}
Web reads:
{% for item in web_reads %}- {{ item.reference }}{% if item.title %} {{ item.title }}{% endif %}{% if item.url %}: {{ item.url }}{% endif %}{% if item.content %} — read passage: {{ item.content }}{% endif %}{{ '\n' }}
{% endfor %}{% endif %}{% if actions %}
Actions:
{% for item in actions %}- {{ item.reference }} {{ item.tool }}{% if item.result %}: {{ item.result }}{% endif %}{{ '\n' }}
{% endfor %}{% endif %}{% if possible_next_steps %}
Possible next steps:
{% for item in possible_next_steps %}- [{{ item.audience }}] {{ item.tool }}{% if item.arguments %} {{ item.arguments }}{% endif %}{% if item.reason %} — {{ item.reason }}{% elif item.when %} — {{ item.when }}{% endif %}{{ '\n' }}
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
    def __init__(
        self,
        snapshot: dict[str, Any],
        *,
        local_uuid_references: Mapping[str, str] | None = None,
    ) -> None:
        self._handles: dict[str, str] = {}
        counters: dict[str, int] = {}
        episode_handles = self._episode_handles(snapshot, local_uuid_references)
        for section, records in self._record_sections(snapshot):
            prefix = _REFERENCE_PREFIXES[section]
            for reference in records:
                if section == "episodes":
                    episode_id = reference.removeprefix("episode:")
                    self._handles[reference] = episode_handles[episode_id]
                else:
                    counters[prefix] = counters.get(prefix, 0) + 1
                    self._handles[reference] = f"{prefix}{counters[prefix]}"

        for reference in snapshot.get("entity_pages", {}):
            if reference not in self._handles:
                self._handles[reference] = self._new_handle("E", counters)

        for reference in snapshot.get("actions", {}):
            if reference not in self._handles:
                self._handles[reference] = self._new_handle("A", counters)

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
    def _episode_handles(
        snapshot: dict[str, Any],
        local_uuid_references: Mapping[str, str] | None,
    ) -> dict[str, str]:
        episode_ids = [
            reference.removeprefix("episode:")
            for reference in snapshot.get("knowledge", {}).get("episodes", {})
        ]
        fallback = register_short_uuid_references(episode_ids, "ep", {})
        known_handles = {
            str(actual_id): str(handle)
            for handle, actual_id in (local_uuid_references or {}).items()
            if isinstance(handle, str) and handle.startswith("ep_")
        }
        return {
            episode_id: known_handles.get(episode_id, fallback[episode_id])
            for episode_id in episode_ids
        }

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

    def known_reference(self, value: str) -> str | None:
        return self._handles.get(value)


def _safe_text(value: object, *, limit: int = 320) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text if len(text) <= limit else f"{text[: limit - 1]}…"


def _bounded_text_items(
    value: object,
    *,
    max_items: int = 3,
    item_limit: int = 160,
) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [
        _safe_text(item, limit=item_limit)
        for item in value
        if isinstance(item, str) and item.strip()
    ][:max_items]


def _episode_chronology(item: dict[str, Any]) -> str:
    first = _safe_text(item.get("first_message_at"), limit=80)
    last = _safe_text(item.get("last_message_at"), limit=80)
    if first and last:
        return first if first == last else f"{first} to {last}"
    return first or last


def _historical_episode_support(item: dict[str, Any]) -> list[dict[str, str]]:
    sources = item.get("sources_consulted", [])
    if not isinstance(sources, list):
        return []
    support = []
    for source in sources:
        if not isinstance(source, dict):
            continue
        kind = _safe_text(source.get("source_kind") or "source", limit=64).replace(
            "_", " "
        )
        status = _safe_text(source.get("source_status") or "unknown", limit=64).replace(
            "_", " "
        )
        support.append(
            {
                "label": f"{kind} ({status})",
                "url": _safe_text(source.get("canonical_url") or "", limit=200),
                "excerpt": _safe_text(source.get("excerpt") or "", limit=160),
            }
        )
        if len(support) == 3:
            break
    return support


def _entity_display_reference(
    localizer: _ReferenceLocalizer,
    identifier: object,
) -> str:
    if identifier is None:
        return "?"
    reference = f"entity:{identifier}"
    return localizer.known_reference(reference) or _safe_text(identifier, limit=100)


def _path_description(item: dict[str, Any], localizer: _ReferenceLocalizer) -> str:
    source = item.get("entity_a", item.get("source_entity_id", item.get("source")))
    target = item.get("entity_b", item.get("target_entity_id", item.get("target")))
    if source is not None and target is not None:
        return f"{_entity_display_reference(localizer, source)} -> {_entity_display_reference(localizer, target)}"
    return _safe_text(
        item.get("description") or item.get("path") or item.get("step") or ""
    )


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
        return value
    if key == "relationship_id" and value is not None:
        return localizer.reference(f"relationship:{value}")
    if key == "episode_id" and value is not None:
        return localizer.reference(f"episode:{value}")
    if key == "document_id" and value is not None:
        return localizer.reference(f"document:{value}:0")
    return value


def _public_details(record: dict[str, Any], localizer: _ReferenceLocalizer) -> str:
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
            source_ref = _entity_display_reference(localizer, source)
            target_ref = _entity_display_reference(localizer, target)
            item["endpoints"] = f"{source_ref} -> {target_ref}"
            item["label"] = _safe_text(
                item.get("observed_relationship_label")
                or item.get("relationship_type")
                or item.get("label")
                or ""
            )
            item["context"] = _safe_text(item.get("context") or "")
        elif section == "episodes":
            item["summary"] = _safe_text(item.get("summary") or "")
            item["chronology"] = _episode_chronology(item)
            item["developments"] = _bounded_text_items(item.get("new_developments"))
            item["updates"] = _bounded_text_items(item.get("updates"))
            item["unresolved"] = _bounded_text_items(item.get("unresolved"))
            item["support"] = _historical_episode_support(item)
        elif section == "paths":
            item["description"] = _path_description(item, localizer)
        elif section in {"messages", "documents"}:
            content = item.get("message") or item.get("content") or ""
            if not content and isinstance(item.get("context"), list):
                content = " ".join(
                    str(context.get("content", ""))
                    for context in item["context"]
                    if isinstance(context, dict) and context.get("content")
                )
            item["content"] = _safe_text(content)
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


def _render_context(
    notebook: RunNotebook,
    *,
    local_uuid_references: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    snapshot = notebook.as_dict()
    localizer = _ReferenceLocalizer(
        snapshot,
        local_uuid_references=local_uuid_references,
    )
    knowledge = snapshot["knowledge"]
    evidence = snapshot["evidence"]
    web = evidence["web"]

    entity_pages = []
    for reference, page in snapshot["entity_pages"].items():
        entity = knowledge["entities"].get(reference, {})
        entity_pages.append(
            {
                "reference": localizer.reference(reference),
                "name": _safe_text(
                    entity.get("canonical_name") or entity.get("name") or ""
                ),
                "relationships": [
                    localizer.reference(ref) for ref in page["relationship_refs"]
                ],
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
            "references": [
                localizer.reference(ref)
                for ref in snapshot["summary"].get("references", [])
            ],
        },
        "entity_pages": entity_pages,
        "entities": _record_list(knowledge["entities"], localizer, section="entities"),
        "relationships": _record_list(
            knowledge["relationships"], localizer, section="relationships"
        ),
        "episodes": _record_list(knowledge["episodes"], localizer, section="episodes"),
        "paths": _record_list(knowledge["paths"], localizer, section="paths"),
        "messages": _record_list(evidence["messages"], localizer, section="messages"),
        "documents": _record_list(
            evidence["documents"], localizer, section="documents"
        ),
        "web_discoveries": _record_list(
            web["discoveries"], localizer, section="web_discoveries"
        ),
        "web_reads": _record_list(web["reads"], localizer, section="web_reads"),
        "actions": actions,
        "possible_next_steps": possible_next_steps,
    }


def render_notebook(
    notebook: RunNotebook,
    *,
    local_uuid_references: Mapping[str, str] | None = None,
    template: str = NOTEBOOK_TEMPLATE,
    environment_factory: Callable[[], Environment] = notebook_environment,
) -> str:
    """Render a bounded model-facing notebook view without changing state."""

    if not isinstance(notebook, RunNotebook):
        raise TypeError("render_notebook expects a RunNotebook")
    environment = environment_factory()
    return (
        environment.from_string(template)
        .render(
            **_render_context(
                notebook,
                local_uuid_references=local_uuid_references,
            )
        )
        .strip()
    )


__all__ = [
    "NOTEBOOK_TEMPLATE",
    "notebook_environment",
    "render_notebook",
]
