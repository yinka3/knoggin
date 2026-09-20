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
    "activities": "ACT",
    "episodes": "EP",
    "paths": "P",
    "messages": "M",
    "documents": "D",
    "observation_supports": "O",
    "entity": "E",
    "relationship": "R",
    "activity": "ACT",
    "episode": "EP",
    "path": "P",
    "message": "M",
    "document": "D",
    "observation_support": "O",
    "web_discoveries": "W",
    "web_reads": "WR",
    "action": "A",
}

_SHORT_TEXT_LIMIT = 320
_PASSAGE_TEXT_LIMIT = 1_200

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
{% endfor %}{% endif %}{% if activities %}
Activities:
{% for item in activities %}- {{ item.reference }}{% if item.entity %} {{ item.entity }}{% endif %}{% if item.time %} at {{ item.time }}{% endif %}{% if item.evidence %} (evidence: {{ item.evidence|join(', ') }}){% endif %}{{ '\n' }}
{% endfor %}{% endif %}{% if episodes %}
Episodes:
{% for item in episodes %}- {{ item.reference }}{% if item.summary %}: {{ item.summary }}{% endif %}
{% if item.chronology %}  chronology: {{ item.chronology }}
{% endif %}{% if item.developments %}  developments: {{ item.developments|join('; ') }}
{% endif %}{% if item.updates %}  updates: {{ item.updates|join('; ') }}
{% endif %}{% if item.unresolved %}  unresolved: {{ item.unresolved|join('; ') }}
{% endif %}{% if item.evidence %}  evidence in notebook: {{ item.evidence|join(', ') }}
{% endif %}{% if item.support %}  historical support:
{% for source in item.support %}  - {{ source.label }}{% if source.locator %} [{{ source.locator }}]{% endif %}{% if source.url %}: {{ source.url }}{% endif %}{% if source.excerpt %} — {{ source.excerpt }}{% endif %}{{ '\n' }}
{% endfor %}{% endif %}{{ '\n' }}
{% endfor %}{% endif %}{% if paths %}
Paths:
{% for item in paths %}- {{ item.reference }}{% if item.description %}: {{ item.description }}{% endif %}{% if item.evidence %} (evidence: {{ item.evidence|join(', ') }}){% endif %}{% if item.observation_supports %} (support: {{ item.observation_supports|join(', ') }}){% endif %}{{ '\n' }}
{% endfor %}{% endif %}{% if messages %}
Messages:
{% for item in messages %}- {{ item.reference }}{% if item.content %}: {{ item.content }}{% endif %}{{ '\n' }}{% endfor %}
{% endif %}{% if documents %}
Documents:
{% for item in documents %}- {{ item.reference }}{% if item.name %} {{ item.name }}{% endif %}{% if item.content %}: {{ item.content }}{% endif %}{% if item.handle %} [document: {{ item.handle }}]{% endif %}{% if item.locator %} [{{ item.locator }}]{% endif %}
{% if item.continuation %}  continuation: {{ item.continuation }}
{% endif %}
{{ '\n' }}{% endfor %}{% endif %}{% if observation_supports %}Observation support (expanded on demand):
{% for item in observation_supports %}- {{ item.reference }} observation {{ item.observation_id }}{% if item.status %} ({{ item.status }}){% endif %}
{% if item.context_blocks %}  context blocks: {{ item.context_blocks|join('; ') }}
{% endif %}{% if item.sources %}  source excerpts:
{% for source in item.sources %}  - {{ source.label }}{% if source.locator %} [{{ source.locator }}]{% endif %}{% if source.excerpt %}: {{ source.excerpt }}{% endif %}{{ '\n' }}
{% endfor %}{% endif %}{% endfor %}{% endif %}{% if web_discoveries %}
Web discoveries (not read):
{% for item in web_discoveries %}- {{ item.reference }}{% if item.title %} {{ item.title }}{% endif %}{% if item.url %}: {{ item.url }}{% endif %}{% if item.snippet %} — discovery snippet: {{ item.snippet }}{% endif %}{{ '\n' }}
{% endfor %}{% endif %}{% if web_reads %}
Web reads:
{% for item in web_reads %}- {{ item.reference }}{% if item.title %} {{ item.title }}{% endif %}{% if item.url %}: {{ item.url }}{% endif %}{% if item.content %} — read passage: {{ item.content }}{% endif %}{% if item.locator %} [{{ item.locator }}]{% endif %}
{% if item.continuation %}  continuation: {{ item.continuation }}
{% endif %}{{ '\n' }}
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
            for section in (
                "entities",
                "relationships",
                "activities",
                "episodes",
                "paths",
            )
        )
        yield "messages", evidence.get("messages", {})
        yield "documents", evidence.get("documents", {})
        yield "observation_supports", evidence.get("observation_supports", {})
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


def _safe_text(value: object, *, limit: int = _SHORT_TEXT_LIMIT) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text if len(text) <= limit else f"{text[: limit - 1]}…"


def _passage_text(value: object) -> tuple[str, bool]:
    """Render a useful but bounded evidence passage and report display clipping."""

    if value is None:
        return "", False
    text = str(value).strip()
    return _safe_text(text, limit=_PASSAGE_TEXT_LIMIT), len(text) > _PASSAGE_TEXT_LIMIT


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
                "locator": _format_locator(source.get("locator")),
                "url": _safe_text(source.get("canonical_url") or "", limit=200),
                "excerpt": _safe_text(source.get("excerpt") or "", limit=160),
            }
        )
        if len(support) == 3:
            break
    return support


def _format_locator(value: object) -> str:
    if not isinstance(value, dict):
        return ""
    kind = value.get("kind")
    if kind in {"text_lines", "code_lines"}:
        start = value.get("start_line")
        end = value.get("end_line")
        if isinstance(start, int) and isinstance(end, int):
            return f"lines {start}-{end}"
    if kind == "csv_rows":
        start = value.get("start_row")
        end = value.get("end_row")
        if isinstance(start, int) and isinstance(end, int):
            return f"rows {start}-{end}"
    if kind == "docx_paragraphs":
        start = value.get("start_paragraph")
        end = value.get("end_paragraph")
        if isinstance(start, int) and isinstance(end, int):
            return f"paragraphs {start}-{end}"
    if kind == "pdf_page" and isinstance(value.get("page"), int):
        return f"page {value['page']}"
    if kind == "search_result" and isinstance(value.get("rank"), int):
        return f"search result {value['rank']}"
    return _safe_text(kind or "", limit=48).replace("_", " ")


def _result_locator(item: Mapping[str, Any]) -> str:
    """Return the compact source position already available in a tool result."""

    locator = item.get("locator")
    if not isinstance(locator, Mapping):
        source_context = item.get("source_context")
        if isinstance(source_context, Mapping):
            locator = source_context.get("locator")
    if isinstance(locator, Mapping):
        return _format_locator(dict(locator))

    start_line = item.get("start_line")
    end_line = item.get("end_line")
    if type(start_line) is int and type(end_line) is int:
        return f"lines {start_line}-{end_line}"
    page_number = item.get("page_number")
    if type(page_number) is int:
        return f"page {page_number}"
    return ""


def _document_handle(item: Mapping[str, Any]) -> str:
    """Expose only the model-safe document handle accepted by read tools."""

    document_id = item.get("document_id")
    return document_id if isinstance(document_id, str) and document_id.startswith("doc_") else ""


def _source_continuation(
    item: Mapping[str, Any],
    *,
    display_clipped: bool,
) -> str:
    """Tell the model when a bounded view should be expanded through a tool."""

    next_start_line = item.get("next_start_line")
    if type(next_start_line) is not int:
        end_line = item.get("end_line")
        total_lines = item.get("total_lines")
        if (
            item.get("truncated") is True
            and type(end_line) is int
            and type(total_lines) is int
            and end_line < total_lines
        ):
            next_start_line = end_line + 1
    if type(next_start_line) is int:
        return (
            f"more source text is available from line {next_start_line}; reread a "
            "narrower range or use a targeted query."
        )
    if item.get("has_more") is True or item.get("truncated") is True:
        return "more source text is available; reread a narrower range or use a targeted query."
    if display_clipped:
        return "the displayed passage is clipped; reread a narrower range or use a targeted query."
    return ""


def _observation_support(item: dict[str, Any]) -> dict[str, Any]:
    subject = item.get("subject")
    observation_id = (
        _safe_text(subject.get("identifier"), limit=32)
        if isinstance(subject, dict)
        else ""
    )
    nodes = item.get("nodes")
    if not isinstance(nodes, list):
        nodes = []
    status = ""
    context_blocks: list[str] = []
    sources: list[dict[str, str]] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        pointer = node.get("pointer")
        kind = pointer.get("kind") if isinstance(pointer, dict) else None
        if kind == "relationship_observation" and not status:
            status = _safe_text(node.get("status"), limit=32)
        elif kind == "context_block":
            excerpt = _safe_text(node.get("excerpt"), limit=160)
            if excerpt:
                context_blocks.append(excerpt)
        elif kind == "source_reference":
            label = _safe_text(node.get("source_kind") or "source", limit=64).replace(
                "_", " "
            )
            sources.append(
                {
                    "label": label,
                    "locator": _format_locator(node.get("locator")),
                    "excerpt": _safe_text(node.get("excerpt"), limit=160),
                }
            )
    return {
        "observation_id": observation_id,
        "status": status,
        "context_blocks": context_blocks[:2],
        "sources": sources[:3],
        "expanded": bool(nodes),
    }


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
        elif section == "activities":
            entity_id = item.get("entity_id")
            entity_name = _safe_text(item.get("entity") or "")
            entity_ref = _entity_display_reference(localizer, entity_id)
            item["entity"] = (
                f"{entity_ref} {entity_name}".strip()
                if entity_id is not None
                else entity_name
            )
            item["time"] = _safe_text(item.get("time") or "", limit=80)
        elif section == "episodes":
            item["summary"] = _safe_text(item.get("summary") or "")
            item["chronology"] = _episode_chronology(item)
            item["developments"] = _bounded_text_items(item.get("new_developments"))
            item["updates"] = _bounded_text_items(item.get("updates"))
            item["unresolved"] = _bounded_text_items(item.get("unresolved"))
            item["support"] = _historical_episode_support(item)
        elif section == "paths":
            item["description"] = _path_description(item, localizer)
            item["observation_supports"] = [
                localizer.reference(reference)
                for reference in item.get("observation_refs", [])
                if isinstance(reference, str)
            ]
        elif section == "observation_supports":
            item.update(_observation_support(item))
        elif section in {"messages", "documents"}:
            content = item.get("message") or item.get("content") or ""
            if not content and isinstance(item.get("context"), list):
                content = " ".join(
                    str(context.get("content", ""))
                    for context in item["context"]
                    if isinstance(context, dict) and context.get("content")
                )
            item["content"], display_clipped = _passage_text(content)
            item["name"] = _safe_text(
                item.get("document_name") or item.get("original_name") or ""
            )
            if section == "documents":
                item["handle"] = _document_handle(item)
                item["locator"] = _result_locator(item)
                item["continuation"] = _source_continuation(
                    item,
                    display_clipped=display_clipped,
                )
        elif section in {"web_discoveries", "web_reads"}:
            item["title"] = _safe_text(item.get("title") or "")
            item["url"] = _safe_text(item.get("url") or "")
            item["snippet"] = _safe_text(item.get("snippet") or "")
            if section == "web_reads":
                item["content"], display_clipped = _passage_text(
                    item.get("content") or ""
                )
                item["locator"] = _result_locator(item)
                item["continuation"] = _source_continuation(
                    item,
                    display_clipped=display_clipped,
                )
            else:
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
    observation_supports = [
        item
        for item in _record_list(
            evidence["observation_supports"],
            localizer,
            section="observation_supports",
        )
        if item["expanded"]
    ]

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
        "activities": _record_list(
            knowledge["activities"], localizer, section="activities"
        ),
        "episodes": _record_list(knowledge["episodes"], localizer, section="episodes"),
        "paths": _record_list(knowledge["paths"], localizer, section="paths"),
        "messages": _record_list(evidence["messages"], localizer, section="messages"),
        "documents": _record_list(
            evidence["documents"], localizer, section="documents"
        ),
        "observation_supports": observation_supports,
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
