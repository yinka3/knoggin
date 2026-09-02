"""Canonical, run-local working memory for one agent execution.

The notebook is deliberately an Agent-layer concern.  Knowledge and document
services return their normal backend contracts; this module normalizes those
contracts into one reference-preserving state before anything is localized for
the model.  It is not persisted and it does not own execution policy.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, MutableSequence
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Literal
from urllib.parse import urlsplit

NotebookAudience = Literal["system", "agent"]


_KNOWLEDGE_SECTIONS = ("entities", "relationships", "episodes", "paths")
_EVIDENCE_SECTIONS = ("messages", "documents", "web_discoveries", "web_reads")
_ALL_SECTIONS = _KNOWLEDGE_SECTIONS + _EVIDENCE_SECTIONS


@dataclass(frozen=True, slots=True)
class NotebookApplyResult:
    """Summary of one canonical tool result admitted to the notebook."""

    changed: bool
    references: tuple[str, ...] = ()


@dataclass(slots=True)
class NotebookSummary:
    """One replacement summary plus references that remain addressable."""

    text: str | None = None
    references: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "text": self.text,
            "references": list(self.references),
        }


class _SectionView(MutableSequence[dict[str, Any]]):
    """List-shaped access to one normalized notebook section.

    The view exists only as a migration-friendly AgentRun surface.  Records
    remain keyed by canonical references in ``RunNotebook``; list operations
    update that one source of truth rather than creating a second accumulator.
    """

    def __init__(self, notebook: "RunNotebook", section: str):
        self._notebook = notebook
        self._section = section

    def __len__(self) -> int:
        return len(self._notebook._orders[self._section])

    def __getitem__(self, index):
        values = self._notebook._section_values(self._section)
        return values[index]

    def __setitem__(self, index, value) -> None:
        values = self._notebook._section_values(self._section)
        if isinstance(index, slice):
            replacement = list(value)
            values[index] = replacement
            self._notebook._replace_section(self._section, values)
            return
        values[index] = value
        self._notebook._replace_section(self._section, values)

    def __delitem__(self, index) -> None:
        values = self._notebook._section_values(self._section)
        del values[index]
        self._notebook._replace_section(self._section, values)

    def insert(self, index: int, value: dict[str, Any]) -> None:
        values = self._notebook._section_values(self._section)
        values.insert(index, value)
        self._notebook._replace_section(self._section, values)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self._notebook._section_values(self._section))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _SectionView):
            other = other._notebook._section_values(other._section)
        if isinstance(other, (list, tuple)):
            return self._notebook._section_values(self._section) == list(other)
        return NotImplemented

    def __repr__(self) -> str:
        return repr(self._notebook._section_values(self._section))

    def sort(self, *args, **kwargs) -> None:
        values = self._notebook._section_values(self._section)
        values.sort(*args, **kwargs)
        self._notebook._replace_section(self._section, values)


class RunNotebook:
    """Normalized working memory for a single AgentRun.

    Scope, budgets, permissions, lifecycle, and tool authorization remain on
    ``AgentRun``.  This object owns only accumulated knowledge/evidence and
    model-facing guidance.  The initial generation intentionally has no
    rollover policy; semantic generations are Batch 12 work.
    """

    def __init__(self, *, limits: Any | None = None, generation: int = 1) -> None:
        if not isinstance(generation, int) or generation < 1:
            raise ValueError("notebook generation must be a positive integer")
        self.generation = generation
        self._limits = limits
        self._records: dict[str, dict[str, dict[str, Any]]] = {
            section: {} for section in _ALL_SECTIONS
        }
        self._orders: dict[str, list[str]] = {section: [] for section in _ALL_SECTIONS}
        self._entity_pages: dict[str, dict[str, Any]] = {}
        self._actions: dict[str, dict[str, Any]] = {}
        self.possible_next_steps: list[dict[str, Any]] = []
        self.summary = NotebookSummary()
        self._last_applied_references: tuple[str, ...] = ()

    @property
    def entities(self) -> _SectionView:
        return _SectionView(self, "entities")

    @property
    def relationships(self) -> _SectionView:
        return _SectionView(self, "relationships")

    @property
    def episodes(self) -> _SectionView:
        return _SectionView(self, "episodes")

    @property
    def paths(self) -> _SectionView:
        return _SectionView(self, "paths")

    @property
    def messages(self) -> _SectionView:
        return _SectionView(self, "messages")

    @property
    def documents(self) -> _SectionView:
        return _SectionView(self, "documents")

    @property
    def web_discoveries(self) -> _SectionView:
        return _SectionView(self, "web_discoveries")

    @property
    def web_reads(self) -> _SectionView:
        return _SectionView(self, "web_reads")

    @property
    def entity_pages(self) -> dict[str, dict[str, Any]]:
        return self._entity_pages

    @property
    def actions(self) -> list[dict[str, Any]]:
        return list(self._actions.values())

    @property
    def last_applied_references(self) -> tuple[str, ...]:
        return self._last_applied_references

    @property
    def evidence_summary(self) -> str | None:
        """Compatibility view for the pre-notebook summary field."""

        return self.summary.text

    @evidence_summary.setter
    def evidence_summary(self, value: str | None) -> None:
        self.summary.text = value

    def _section_values(self, section: str) -> list[dict[str, Any]]:
        return [self._records[section][key] for key in self._orders[section]]

    def section_reference(self, section: str, item: dict[str, Any]) -> str:
        """Return the canonical reference used for one section record."""

        return self._reference_for_section(section, item)

    def is_last_applied(self, section: str, item: dict[str, Any]) -> bool:
        return self._reference_for_section(section, item) in self._last_applied_references

    def _replace_section(self, section: str, values: list[dict[str, Any]]) -> None:
        records: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        for value in values:
            if not isinstance(value, dict):
                continue
            reference = self._reference_for_section(section, value)
            records[reference] = deepcopy(value)
            if reference not in order:
                order.append(reference)
        self._records[section] = records
        self._orders[section] = order

    def _limit(self, section: str) -> int | None:
        if self._limits is None:
            return None
        names = {
            "entities": "max_accumulated_profiles",
            "relationships": "max_accumulated_graph",
            "episodes": "max_accumulated_episodes",
            "paths": "max_accumulated_paths",
            "messages": "max_accumulated_messages",
            "documents": "max_accumulated_messages",
            "web_discoveries": "max_accumulated_sources",
            "web_reads": "max_accumulated_sources",
        }
        value = getattr(self._limits, names.get(section, ""), None)
        return value if isinstance(value, int) and value > 0 else None

    def _reference_for_section(self, section: str, item: dict[str, Any]) -> str:
        if section == "entities":
            identifier = item.get("entity_id", item.get("id"))
            return f"entity:{identifier}" if identifier is not None else self._hash_ref(section, item)
        if section == "relationships":
            identifier = item.get("relationship_id")
            if identifier is not None:
                return f"relationship:{identifier}"
            return self._hash_ref(
                section,
                {
                    key: item.get(key)
                    for key in (
                        "project_id",
                        "source_entity_id",
                        "target_entity_id",
                        "source",
                        "target",
                        "relationship_type",
                        "observed_relationship_label",
                    )
                },
            )
        if section == "episodes":
            identifier = item.get("episode_id", item.get("id"))
            return f"episode:{identifier}" if identifier is not None else self._hash_ref(section, item)
        if section == "paths":
            identifier = item.get("path_id")
            if identifier is not None:
                return f"path:{identifier}"
            return self._hash_ref(
                section,
                {
                    key: item.get(key)
                    for key in (
                        "project_id",
                        "entity_a_id",
                        "entity_b_id",
                        "entity_a",
                        "entity_b",
                        "relationship_id",
                        "step",
                    )
                },
            )
        if section == "messages":
            identifier = item.get("id", item.get("message_id"))
            scope = (item.get("project_id"), item.get("session_id"))
            if identifier is not None:
                return f"message:{scope[0] or ''}:{scope[1] or ''}:{identifier}"
            return self._hash_ref(section, item)
        if section == "documents":
            document_id = item.get("document_id", item.get("id", "document"))
            chunk = item.get("chunk_index", 0)
            return f"document:{document_id}:{chunk}"
        if section in {"web_discoveries", "web_reads"}:
            return self._hash_ref(section, self._source_identity(item))
        return self._hash_ref(section, item)

    @staticmethod
    def _hash_ref(section: str, item: object) -> str:
        payload = json.dumps(item, sort_keys=True, default=str, separators=(",", ":"))
        digest = hashlib.sha256(payload.encode()).hexdigest()[:24]
        return f"{section}:{digest}"

    @staticmethod
    def _source_identity(item: dict[str, Any]) -> dict[str, Any]:
        return {
            key: item.get(key)
            for key in (
                "source_kind",
                "url",
                "content_hash",
                "page_number",
                "start_line",
                "end_line",
            )
        }

    def _upsert(
        self,
        section: str,
        item: dict[str, Any],
        *,
        reference: str | None = None,
    ) -> str:
        value = deepcopy(item)
        ref = reference or self._reference_for_section(section, value)
        existing = self._records[section].get(ref)
        if existing is not None:
            merged = dict(existing)
            merged.update(value)
            value = merged
        self._records[section][ref] = value
        if ref not in self._orders[section]:
            self._orders[section].append(ref)
        limit = self._limit(section)
        if limit is not None and len(self._orders[section]) > limit:
            overflow = self._orders[section][:-limit]
            self._orders[section] = self._orders[section][-limit:]
            for old_ref in overflow:
                self._records[section].pop(old_ref, None)
        return ref

    def _add_message(self, item: dict[str, Any]) -> str:
        message = deepcopy(item)
        if message.get("id") is None and message.get("message_id") is not None:
            message["id"] = message["message_id"]
        return self._upsert("messages", message)

    def _add_document(self, item: dict[str, Any]) -> str:
        return self._upsert("documents", item)

    def _add_entity_page(self, entity_ref: str) -> dict[str, Any]:
        return self._entity_pages.setdefault(
            entity_ref,
            {
                "entity_ref": entity_ref,
                "relationship_refs": [],
                "episode_refs": [],
                "evidence_refs": [],
            },
        )

    def _link_entity(self, identifier: Any) -> str | None:
        if identifier is None:
            return None
        return f"entity:{identifier}"

    def _add_entity(self, item: dict[str, Any]) -> str:
        value = deepcopy(item)
        value.pop("top_connections", None)
        ref = self._upsert("entities", value)
        self._add_entity_page(ref)
        return ref

    def _add_relationship(self, item: dict[str, Any]) -> str:
        value = deepcopy(item)
        evidence = value.pop("evidence", [])
        evidence_refs = list(value.pop("evidence_refs", []))
        for message in evidence if isinstance(evidence, list) else []:
            if isinstance(message, dict):
                evidence_refs.append(self._add_message(message))
        if evidence_refs:
            value["evidence_refs"] = list(dict.fromkeys(evidence_refs))
        ref = self._upsert("relationships", value)
        for identifier in (
            value.get("source_entity_id"),
            value.get("target_entity_id"),
        ):
            entity_ref = self._link_entity(identifier)
            if entity_ref:
                page = self._add_entity_page(entity_ref)
                if ref not in page["relationship_refs"]:
                    page["relationship_refs"].append(ref)
                for evidence_ref in value.get("evidence_refs", []):
                    if evidence_ref not in page["evidence_refs"]:
                        page["evidence_refs"].append(evidence_ref)
        return ref

    def _add_episode(self, item: dict[str, Any], *, group: dict[str, Any] | None = None) -> str:
        value = deepcopy(item)
        if group:
            for key in ("query", "entity_name", "entity_id", "similarity"):
                if key in group and key not in value:
                    value[key] = group[key]
        evidence = value.pop("evidence", [])
        evidence_refs = list(value.pop("evidence_refs", []))
        for message in evidence if isinstance(evidence, list) else []:
            if not isinstance(message, dict):
                continue
            message = dict(message)
            if message.get("id") is None and message.get("message_id") is not None:
                message["id"] = message["message_id"]
            evidence_refs.append(self._add_message(message))
        if evidence_refs:
            value["evidence_refs"] = list(dict.fromkeys(evidence_refs))
        ref = self._upsert("episodes", value)
        for entity in value.get("entities", []) if isinstance(value.get("entities"), list) else []:
            if not isinstance(entity, dict):
                continue
            entity_ref = self._link_entity(entity.get("entity_id", entity.get("id")))
            if entity_ref:
                page = self._add_entity_page(entity_ref)
                if ref not in page["episode_refs"]:
                    page["episode_refs"].append(ref)
        return ref

    def _add_path(self, item: dict[str, Any]) -> str:
        value = deepcopy(item)
        evidence = value.pop("evidence", [])
        evidence_refs = list(value.pop("evidence_refs", []))
        for message in evidence if isinstance(evidence, list) else []:
            if isinstance(message, dict):
                evidence_refs.append(self._add_message(message))
        if evidence_refs:
            value["evidence_refs"] = list(dict.fromkeys(evidence_refs))
        return self._upsert("paths", value)

    @staticmethod
    def _valid_url(value: object) -> bool:
        if not isinstance(value, str) or not value.strip():
            return False
        parsed = urlsplit(value.strip())
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)

    def _add_source(self, section: str, item: dict[str, Any]) -> str | None:
        if not self._valid_url(item.get("url")):
            return None
        return self._upsert(section, item)

    def _record_action(self, tool_name: str, data: Any) -> str:
        payload = {"tool": tool_name, "data": data}
        ref = self._hash_ref("action", payload)
        self._actions.setdefault(
            ref,
            {
                "action_ref": ref,
                "tool": tool_name,
                "result": deepcopy(data),
            },
        )
        return ref

    def _add_system_hint(
        self,
        tool: str,
        arguments: dict[str, Any],
        when: str,
        references: list[str] | tuple[str, ...] = (),
    ) -> None:
        hint = {
            "audience": "system",
            "tool": tool,
            "arguments": deepcopy(arguments),
            "when": when,
            "references": list(references),
        }
        if hint not in self.possible_next_steps:
            self.possible_next_steps.append(hint)

    def record_agent_hint(
        self,
        tool: str,
        arguments: dict[str, Any],
        reason: str,
    ) -> dict[str, Any]:
        """Record a short operational hint, never hidden chain-of-thought."""

        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("agent hint reason must be non-blank")
        reason = " ".join(reason.split())
        if len(reason) > 240:
            raise ValueError("agent hint reason must be at most 240 characters")
        hint = {
            "audience": "agent",
            "tool": str(tool),
            "arguments": deepcopy(arguments),
            "reason": reason,
        }
        self.possible_next_steps = [
            item
            for item in self.possible_next_steps
            if not (item.get("audience") == "agent" and item.get("tool") == tool)
        ]
        self.possible_next_steps.append(hint)
        return hint

    def set_summary(self, text: str | None, references: list[str] | tuple[str, ...] = ()) -> None:
        if text is not None and (not isinstance(text, str) or not text.strip()):
            raise ValueError("notebook summary must be non-blank when provided")
        self.summary = NotebookSummary(
            text=" ".join(text.split()) if text is not None else None,
            references=list(dict.fromkeys(str(ref) for ref in references)),
        )

    @staticmethod
    def _model_message(item: dict[str, Any]) -> dict[str, Any]:
        """Expose one canonical message in the formatter's compact shape."""

        value = deepcopy(item)
        if isinstance(value.get("context"), list):
            return value
        content = value.get("message", value.get("content", ""))
        value.setdefault("message", content)
        value.setdefault("score", 0.5)
        value["context"] = [
            {
                "role": value.get("role", "assistant"),
                "timestamp": value.get("timestamp", ""),
                "content": content,
                "is_hit": True,
            }
        ]
        return value

    def _messages_for_refs(self, references: object) -> list[dict[str, Any]]:
        if not isinstance(references, list):
            return []
        values = []
        for reference in references:
            if isinstance(reference, str) and reference in self._records["messages"]:
                values.append(self._model_message(self._records["messages"][reference]))
        return values

    def model_view(self) -> dict[str, Any]:
        """Build a bounded, formatter-friendly view without changing state."""

        relationships = []
        for item in self._section_values("relationships"):
            value = deepcopy(item)
            value["evidence"] = self._messages_for_refs(value.get("evidence_refs"))
            relationships.append(value)

        paths = []
        for item in self._section_values("paths"):
            value = deepcopy(item)
            value["evidence"] = self._messages_for_refs(value.get("evidence_refs"))
            paths.append(value)

        episodes = []
        for item in self._section_values("episodes"):
            value = deepcopy(item)
            value["evidence"] = self._messages_for_refs(value.get("evidence_refs"))
            episodes.append(
                {
                    "resolution": value.pop("resolution", "unknown"),
                    "results": [value],
                }
            )

        documents = self._section_values("documents")
        document_messages = []
        for document in documents:
            content = document.get("content", "")
            document_messages.append(
                {
                    "id": (
                        f"document:{document.get('document_id', 'document')}:"
                        f"{document.get('chunk_index', 0)}"
                    ),
                    "document_id": document.get("document_id", "document"),
                    "chunk_index": document.get("chunk_index", 0),
                    "content": content,
                    "message": content,
                    "role": "document",
                    "score": document.get("score", 0.5),
                    "source_type": "document",
                    "source": document.get("document_name", "uploaded document"),
                    "context": [
                        {
                            "role": "document",
                            "timestamp": document.get("document_name", "uploaded document"),
                            "content": content,
                            "is_hit": True,
                        }
                    ],
                }
            )

        return {
            "profiles": [deepcopy(item) for item in self._section_values("entities")],
            "entity_pages": deepcopy(self._entity_pages),
            "graph": relationships,
            "paths": paths,
            "episodes": episodes,
            "messages": [
                self._model_message(item) for item in self._section_values("messages")
            ]
            + document_messages,
            "sources": [
                deepcopy(item) for item in self._section_values("web_discoveries")
            ]
            + [deepcopy(item) for item in self._section_values("web_reads")],
            "documents": [deepcopy(item) for item in documents],
            "actions": self.actions,
            "possible_next_steps": deepcopy(self.possible_next_steps),
            "summary": self.summary.as_dict(),
        }

    def apply(self, tool_name: str, result: dict[str, Any]) -> NotebookApplyResult:
        """Normalize one untouched backend result into canonical notebook state."""

        if not isinstance(result, dict) or result.get("error"):
            self._last_applied_references = ()
            return NotebookApplyResult(False)
        data = result.get("data")
        if data is None or data == [] or data == {}:
            self._last_applied_references = ()
            return NotebookApplyResult(False)

        references: list[str] = []
        if tool_name == "search_entity":
            for item in data if isinstance(data, list) else []:
                if isinstance(item, dict):
                    references.append(self._add_entity(item))
            for ref in dict.fromkeys(references):
                entity_id = ref.removeprefix("entity:")
                self._add_system_hint(
                    "get_connections",
                    {"entity_id": int(entity_id) if entity_id.isdigit() else entity_id},
                    "when more relationship detail is needed",
                    [ref],
                )
                self._add_system_hint(
                    "episode_check",
                    {"entity_id": int(entity_id) if entity_id.isdigit() else entity_id},
                    "when history or developments are needed",
                    [ref],
                )
        elif tool_name == "search_messages":
            for item in data if isinstance(data, list) else []:
                if isinstance(item, dict):
                    references.append(self._add_message(item))
        elif tool_name in {"get_connections", "get_recent_activity"}:
            for item in data if isinstance(data, list) else []:
                if isinstance(item, dict) and (
                    {"source", "target"}.issubset(item)
                    or {
                        "source_entity_id",
                        "target_entity_id",
                    }.issubset(item)
                ):
                    references.append(self._add_relationship(item))
        elif tool_name == "find_path":
            for item in data if isinstance(data, list) else []:
                if isinstance(item, dict):
                    references.append(self._add_path(item))
        elif tool_name in {"episode_check", "read_recent_episodes"}:
            groups = data.get("results", []) if isinstance(data, dict) else []
            for group in groups if isinstance(groups, list) else []:
                if not isinstance(group, dict):
                    continue
                episodes = group.get("episodes", [])
                if isinstance(episodes, list) and episodes:
                    for item in episodes:
                        if isinstance(item, dict):
                            references.append(self._add_episode(item, group=group))
                elif group.get("id") is not None or group.get("message") is not None:
                    references.append(self._add_message(group))
            if isinstance(data, list):
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    if (
                        item.get("episode_id") is not None
                        or item.get("summary") is not None
                        or (
                            item.get("id") is not None
                            and not any(
                                key in item
                                for key in (
                                    "message",
                                    "content",
                                    "role",
                                    "context",
                                )
                            )
                        )
                    ):
                        references.append(self._add_episode(item))
                    elif item.get("id") is not None or item.get("message") is not None:
                        references.append(self._add_message(item))
        elif tool_name == "read_episode":
            for item in data if isinstance(data, list) else []:
                if isinstance(item, dict):
                    references.append(self._add_message(item))
        elif tool_name in {
            "list_documents",
            "get_document_manifest",
            "get_document_info",
            "search_documents",
            "read_document",
        }:
            for item in data if isinstance(data, list) else []:
                if isinstance(item, dict) and item.get("document_id") is not None:
                    references.append(self._add_document(item))
        elif tool_name in {"web_search", "news_search"}:
            section = "web_discoveries"
            for item in data if isinstance(data, list) else []:
                if isinstance(item, dict):
                    item = dict(item)
                    item.setdefault(
                        "source_kind",
                        "news_search_result"
                        if tool_name == "news_search"
                        else "web_search_result",
                    )
                    ref = self._add_source(section, item)
                    if ref:
                        references.append(ref)
                        self._add_system_hint(
                            "read_web_page",
                            {"url": item.get("url")},
                            "when the source is promising and exact evidence is needed",
                            [ref],
                        )
        elif tool_name == "read_web_page":
            for item in data if isinstance(data, list) else []:
                if isinstance(item, dict):
                    item = dict(item)
                    item.setdefault("source_kind", "web_page")
                    ref = self._add_source("web_reads", item)
                    if ref:
                        references.append(ref)
        elif tool_name == "load_topic_context":
            for topic_data in data.values() if isinstance(data, dict) else []:
                if not isinstance(topic_data, dict):
                    continue
                for item in topic_data.get("messages", []):
                    if isinstance(item, dict):
                        topic_message = dict(item)
                        topic_message.setdefault("score", 1.0)
                        topic_message.setdefault(
                            "context",
                            [
                                {
                                    "role": topic_message.get("role", "assistant"),
                                    "timestamp": topic_message.get("timestamp", ""),
                                    "content": topic_message.get("message", ""),
                                    "is_hit": True,
                                }
                            ],
                        )
                        references.append(self._add_message(topic_message))
        else:
            references.append(self._record_action(tool_name, data))

        self._last_applied_references = tuple(dict.fromkeys(references))
        return NotebookApplyResult(bool(references), self._last_applied_references)

    def references_for_result(
        self, tool_name: str, result: dict[str, Any]
    ) -> tuple[str, ...]:
        """Preview the references admitted by a result without mutating state."""

        preview = deepcopy(self)
        return preview.apply(tool_name, result).references

    def has_any(self) -> bool:
        return bool(
            any(self._orders[section] for section in _ALL_SECTIONS)
            or self._actions
            or self.summary.text
        )

    def fingerprint(self) -> str:
        return json.dumps(self.as_dict(), sort_keys=True, default=str, separators=(",", ":"))

    def as_dict(self) -> dict[str, Any]:
        return {
            "generation": self.generation,
            "knowledge": {
                section: {
                    reference: deepcopy(self._records[section][reference])
                    for reference in self._orders[section]
                }
                for section in _KNOWLEDGE_SECTIONS
            },
            "evidence": {
                "messages": {
                    reference: deepcopy(self._records["messages"][reference])
                    for reference in self._orders["messages"]
                },
                "documents": {
                    reference: deepcopy(self._records["documents"][reference])
                    for reference in self._orders["documents"]
                },
                "web": {
                    "discoveries": {
                        reference: deepcopy(self._records["web_discoveries"][reference])
                        for reference in self._orders["web_discoveries"]
                    },
                    "reads": {
                        reference: deepcopy(self._records["web_reads"][reference])
                        for reference in self._orders["web_reads"]
                    },
                },
            },
            "entity_pages": deepcopy(self._entity_pages),
            "actions": deepcopy(self._actions),
            "possible_next_steps": deepcopy(self.possible_next_steps),
            "summary": self.summary.as_dict(),
        }

    def clear(self) -> None:
        for section in _ALL_SECTIONS:
            self._records[section].clear()
            self._orders[section].clear()
        self._entity_pages.clear()
        self._actions.clear()
        self.possible_next_steps.clear()
        self.summary = NotebookSummary()
        self._last_applied_references = ()
