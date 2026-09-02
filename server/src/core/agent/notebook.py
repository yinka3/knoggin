"""Canonical, run-local working memory for one agent execution.

The notebook is deliberately an Agent-layer concern.  Knowledge and document
services return their normal backend contracts; this module normalizes those
contracts into one reference-preserving state before anything is localized for
the model.  It is not persisted and it does not own execution policy.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator, MutableSequence
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Literal
from urllib.parse import urlsplit

NotebookAudience = Literal["system", "agent"]


_KNOWLEDGE_SECTIONS = ("entities", "relationships", "episodes", "paths")
_EVIDENCE_SECTIONS = ("messages", "documents", "web_discoveries", "web_reads")
_ALL_SECTIONS = _KNOWLEDGE_SECTIONS + _EVIDENCE_SECTIONS
_ACTION_TOOLS = frozenset(
    {
        "edit_brain",
        "restore_brain_section",
        "create_file",
        "update_file",
        "append_file",
        "move_file",
        "delete_file",
        "report_relationship_conflict",
        "propose_entity_merge",
    }
)


@dataclass(frozen=True, slots=True)
class NotebookApplyResult:
    """Summary of one canonical tool result admitted to the notebook."""

    changed: bool
    references: tuple[str, ...] = ()
    accepted: bool = True
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class NotebookCapacity:
    """Independent semantic and rendered-size limits for one run notebook."""

    max_entities: int = 20
    max_relationships: int = 40
    max_episodes: int = 8
    max_paths: int = 8
    max_messages: int = 30
    max_documents: int = 30
    max_web_discoveries: int = 12
    max_web_reads: int = 12
    max_actions: int = 12
    max_next_steps: int = 12
    max_summary_chars: int = 4000
    max_render_tokens: int = 10000

    @classmethod
    def from_limits(cls, limits: Any | None) -> "NotebookCapacity":
        if limits is None:
            return cls()
        return cls(
            max_entities=_positive_limit(limits, "max_accumulated_profiles", 20),
            max_relationships=_positive_limit(limits, "max_accumulated_graph", 40),
            max_episodes=_positive_limit(limits, "max_accumulated_episodes", 8),
            max_paths=_positive_limit(limits, "max_accumulated_paths", 8),
            max_messages=_positive_limit(limits, "max_accumulated_messages", 30),
            max_documents=_positive_limit(limits, "max_accumulated_messages", 30),
            max_web_discoveries=_positive_limit(limits, "max_accumulated_sources", 12),
            max_web_reads=_positive_limit(limits, "max_accumulated_sources", 12),
        )


@dataclass(frozen=True, slots=True)
class NotebookRolloverResult:
    """Summary of a generation rollover."""

    generation: int
    retained_references: tuple[str, ...]
    summary_references: tuple[str, ...]


def _positive_limit(owner: Any, name: str, default: int) -> int:
    value = getattr(owner, name, default)
    return value if isinstance(value, int) and value > 0 else default


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

    def __init__(
        self,
        *,
        limits: Any | None = None,
        capacity: NotebookCapacity | None = None,
        generation: int = 1,
        token_counter: Callable[[str], int] | None = None,
    ) -> None:
        if not isinstance(generation, int) or generation < 1:
            raise ValueError("notebook generation must be a positive integer")
        self.generation = generation
        self.capacity = capacity or NotebookCapacity.from_limits(limits)
        self._token_counter = token_counter
        self._records: dict[str, dict[str, dict[str, Any]]] = {
            section: {} for section in _ALL_SECTIONS
        }
        self._orders: dict[str, list[str]] = {section: [] for section in _ALL_SECTIONS}
        self._entity_pages: dict[str, dict[str, Any]] = {}
        self._actions: dict[str, dict[str, Any]] = {}
        self.possible_next_steps: list[dict[str, Any]] = []
        self.summary = NotebookSummary()
        self._last_applied_references: tuple[str, ...] = ()
        self._contribution_history: list[tuple[str, tuple[str, ...]]] = []
        self._last_apply_result = NotebookApplyResult(False)

    def __deepcopy__(self, memo: dict[int, Any]) -> "RunNotebook":
        """Copy state without cloning the live model tokenizer or service."""

        clone = type(self)(
            capacity=self.capacity,
            generation=self.generation,
            token_counter=self._token_counter,
        )
        memo[id(self)] = clone
        clone._records = deepcopy(self._records, memo)
        clone._orders = deepcopy(self._orders, memo)
        clone._entity_pages = deepcopy(self._entity_pages, memo)
        clone._actions = deepcopy(self._actions, memo)
        clone.possible_next_steps = deepcopy(self.possible_next_steps, memo)
        clone.summary = deepcopy(self.summary, memo)
        clone._last_applied_references = self._last_applied_references
        clone._contribution_history = deepcopy(self._contribution_history, memo)
        clone._last_apply_result = self._last_apply_result
        return clone

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
    def last_apply_result(self) -> NotebookApplyResult:
        return self._last_apply_result

    def set_token_counter(self, token_counter: Callable[[str], int] | None) -> None:
        """Install the active model tokenizer without making it notebook state."""

        if token_counter is not None and not callable(token_counter):
            raise TypeError("token_counter must be callable")
        self._token_counter = token_counter

    def _counts(self) -> dict[str, int]:
        return {
            section: len(self._orders[section]) for section in _ALL_SECTIONS
        } | {
            "actions": len(self._actions),
            "possible_next_steps": len(self.possible_next_steps),
        }

    def _render_token_count(self) -> int:
        rendered = self.render()
        if self._token_counter is not None:
            return max(0, int(self._token_counter(rendered)))
        return len(rendered.split())

    def _fits_capacity(self) -> bool:
        counts = self._counts()
        section_limits = {
            "entities": self.capacity.max_entities,
            "relationships": self.capacity.max_relationships,
            "episodes": self.capacity.max_episodes,
            "paths": self.capacity.max_paths,
            "messages": self.capacity.max_messages,
            "documents": self.capacity.max_documents,
            "web_discoveries": self.capacity.max_web_discoveries,
            "web_reads": self.capacity.max_web_reads,
            "actions": self.capacity.max_actions,
            "possible_next_steps": self.capacity.max_next_steps,
        }
        return (
            all(counts[key] <= limit for key, limit in section_limits.items())
            and len(self.summary.text or "") <= self.capacity.max_summary_chars
            and self._render_token_count() <= self.capacity.max_render_tokens
        )

    def capacity_report(self) -> dict[str, Any]:
        counts = self._counts()
        limits = {
            "entities": self.capacity.max_entities,
            "relationships": self.capacity.max_relationships,
            "episodes": self.capacity.max_episodes,
            "paths": self.capacity.max_paths,
            "messages": self.capacity.max_messages,
            "documents": self.capacity.max_documents,
            "web_discoveries": self.capacity.max_web_discoveries,
            "web_reads": self.capacity.max_web_reads,
            "actions": self.capacity.max_actions,
            "possible_next_steps": self.capacity.max_next_steps,
            "summary_chars": self.capacity.max_summary_chars,
            "render_tokens": self.capacity.max_render_tokens,
        }
        token_count = self._render_token_count()
        pressured = any(
            counts[key] >= max(1, int(limit * 0.8))
            for key, limit in limits.items()
            if key in counts
        ) or len(self.summary.text or "") >= int(self.capacity.max_summary_chars * 0.8) or token_count >= int(self.capacity.max_render_tokens * 0.8)
        status = "FULL" if not self._fits_capacity() else "PRESSURED" if pressured else "OPEN"
        return {
            "status": status,
            "generation": self.generation,
            "counts": counts,
            "limits": limits,
            "render_tokens": token_count,
        }

    def capacity_state(self) -> str:
        return str(self.capacity_report()["status"])

    @property
    def evidence_summary(self) -> str | None:
        """Compatibility view for the pre-notebook summary field."""

        return self.summary.text

    @evidence_summary.setter
    def evidence_summary(self, value: str | None) -> None:
        self.set_summary(value, self.summary.references)

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
        previous_records = self._records[section]
        previous_order = self._orders[section]
        self._records[section] = records
        self._orders[section] = order
        if not self._fits_capacity():
            self._records[section] = previous_records
            self._orders[section] = previous_order
            raise ValueError(f"notebook {section} section exceeds capacity")

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
            for key, incoming in value.items():
                if (
                    section == "documents"
                    and key == "content"
                    and isinstance(merged.get(key), str)
                    and merged[key].strip()
                    and isinstance(incoming, str)
                    and incoming.strip()
                ):
                    # A repeated chunk identity is the same evidence, not a
                    # reason to replace the first bounded passage with a
                    # differently-ranked duplicate.
                    continue
                if (
                    section == "web_discoveries"
                    and key in {"title", "snippet", "provider", "query", "rank"}
                    and merged.get(key) not in (None, "")
                ):
                    continue
                if (
                    key == "score"
                    and isinstance(merged.get(key), (int, float))
                    and isinstance(incoming, (int, float))
                ):
                    merged[key] = max(merged[key], incoming)
                else:
                    merged[key] = incoming
            value = merged
        self._records[section][ref] = value
        if ref not in self._orders[section]:
            self._orders[section].append(ref)
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

    def _add_episode(
        self,
        item: dict[str, Any],
        *,
        group: dict[str, Any] | None = None,
    ) -> str:
        value = deepcopy(item)
        if group:
            for key in (
                "query",
                "entity_name",
                "entity_id",
                "similarity",
                "resolution",
            ):
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
        previous_steps = self.possible_next_steps
        self.possible_next_steps = [
            item
            for item in self.possible_next_steps
            if not (item.get("audience") == "agent" and item.get("tool") == tool)
        ]
        self.possible_next_steps.append(hint)
        if not self._fits_capacity():
            self.possible_next_steps = previous_steps
            raise ValueError("notebook next-step guidance exceeds capacity")
        return hint

    def set_summary(self, text: str | None, references: list[str] | tuple[str, ...] = ()) -> None:
        if text is not None and (not isinstance(text, str) or not text.strip()):
            raise ValueError("notebook summary must be non-blank when provided")
        previous_summary = self.summary
        self.summary = NotebookSummary(
            text=" ".join(text.split()) if text is not None else None,
            references=list(dict.fromkeys(str(ref) for ref in references)),
        )
        if not self._fits_capacity():
            self.summary = previous_summary
            raise ValueError("notebook summary exceeds capacity")

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

    def _apply_unchecked(self, tool_name: str, result: dict[str, Any]) -> NotebookApplyResult:
        """Normalize one result without evaluating capacity.

        Callers should use :meth:`apply`; this unchecked form exists so an
        update can be evaluated on an isolated candidate and adopted
        atomically.
        """

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
            resolution = data.get("resolution") if isinstance(data, dict) else None
            for group in groups if isinstance(groups, list) else []:
                if not isinstance(group, dict):
                    continue
                if resolution is not None and "resolution" not in group:
                    group = {**group, "resolution": resolution}
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
        elif tool_name in _ACTION_TOOLS or (
            isinstance(data, dict)
            and any(key in data for key in ("success", "status", "action"))
        ):
            references.append(self._record_action(tool_name, data))

        self._last_applied_references = tuple(dict.fromkeys(references))
        if self._last_applied_references:
            self._contribution_history.append(
                (str(tool_name), self._last_applied_references)
            )
            self._contribution_history = self._contribution_history[-8:]
        return NotebookApplyResult(bool(references), self._last_applied_references)

    def _adopt_from(self, candidate: "RunNotebook") -> None:
        """Adopt candidate state while retaining this notebook's policy hooks."""

        self.generation = candidate.generation
        self._records = candidate._records
        self._orders = candidate._orders
        self._entity_pages = candidate._entity_pages
        self._actions = candidate._actions
        self.possible_next_steps = candidate.possible_next_steps
        self.summary = candidate.summary
        self._last_applied_references = candidate._last_applied_references
        self._contribution_history = candidate._contribution_history
        self._last_apply_result = candidate._last_apply_result

    def apply(self, tool_name: str, result: dict[str, Any]) -> NotebookApplyResult:
        """Apply one result atomically, rolling over once when it does not fit."""

        if not isinstance(result, dict) or result.get("error"):
            self._last_applied_references = ()
            self._last_apply_result = NotebookApplyResult(False)
            return self._last_apply_result
        data = result.get("data")
        if data is None or data == [] or data == {}:
            self._last_applied_references = ()
            self._last_apply_result = NotebookApplyResult(False)
            return self._last_apply_result

        before = self.fingerprint()
        candidate = deepcopy(self)
        applied = candidate._apply_unchecked(tool_name, result)
        if candidate._fits_capacity():
            applied = NotebookApplyResult(
                before != candidate.fingerprint(), applied.references
            )
            candidate._last_apply_result = applied
            self._adopt_from(candidate)
            return applied

        rolled = deepcopy(self)
        try:
            rollover = rolled.rollover()
        except ValueError:
            self._last_applied_references = ()
            self._last_apply_result = NotebookApplyResult(
                False,
                (),
                False,
                "capacity",
            )
            return self._last_apply_result
        retried = rolled._apply_unchecked(tool_name, result)
        if retried.references and rolled._fits_capacity():
            applied = NotebookApplyResult(
                before != rolled.fingerprint(),
                retried.references,
                True,
                f"rolled_over:{rollover.generation}",
            )
            rolled._last_apply_result = applied
            self._adopt_from(rolled)
            return applied

        self._last_applied_references = ()
        self._last_apply_result = NotebookApplyResult(
            False,
            (),
            False,
            "capacity",
        )
        return self._last_apply_result

    @staticmethod
    def _ref_section(reference: str) -> str | None:
        if not isinstance(reference, str):
            return None
        prefix = reference.split(":", 1)[0]
        prefix = {
            "entity": "entities",
            "relationship": "relationships",
            "episode": "episodes",
            "path": "paths",
            "message": "messages",
            "document": "documents",
            "web_discovery": "web_discoveries",
            "web_read": "web_reads",
        }.get(prefix, prefix)
        if prefix in _ALL_SECTIONS or prefix == "action":
            return prefix
        return None

    def _known_reference(self, reference: str) -> bool:
        section = self._ref_section(reference)
        return bool(
            section == "action"
            and reference in self._actions
            or section in _ALL_SECTIONS
            and reference in self._records[section]
            or reference in self._entity_pages
        )

    def _dependency_references(self, section: str, item: dict[str, Any]) -> set[str]:
        dependencies: set[str] = set()
        for reference in item.get("evidence_refs", []):
            if isinstance(reference, str):
                dependencies.add(reference)
        if section == "relationships":
            identifiers = (
                item.get("source_entity_id"),
                item.get("target_entity_id"),
            )
        elif section == "paths":
            identifiers = (
                item.get("entity_a_id"),
                item.get("entity_b_id"),
            )
        elif section == "episodes":
            identifiers = tuple(
                entity.get("entity_id", entity.get("id"))
                for entity in item.get("entities", [])
                if isinstance(entity, dict)
            )
        else:
            identifiers = ()
        dependencies.update(
            f"entity:{identifier}" for identifier in identifiers if identifier is not None
        )
        return dependencies

    def _retain_references(
        self,
        active_references: list[str] | tuple[str, ...] | None,
        recent_contributions: int,
    ) -> set[str]:
        roots: list[str] = [
            ref for ref in (active_references or ()) if self._known_reference(ref)
        ]
        roots.extend(
            ref
            for ref in self._last_applied_references
            if self._known_reference(ref)
        )
        for _, references in self._contribution_history[-recent_contributions:]:
            roots.extend(references)
        roots.extend(ref for ref in self.summary.references if self._known_reference(ref))

        retained = {ref for ref in roots if self._known_reference(ref)}
        for hint in self.possible_next_steps:
            if hint.get("audience") == "system" and any(
                isinstance(ref, str) and ref in retained
                for ref in hint.get("references", [])
            ):
                retained.update(
                    ref for ref in hint.get("references", []) if isinstance(ref, str)
                )
        changed = True
        while changed:
            changed = False
            for section in _ALL_SECTIONS:
                for reference in tuple(retained):
                    if self._ref_section(reference) != section:
                        continue
                    item = self._records[section].get(reference)
                    if not item:
                        continue
                    for dependency in self._dependency_references(section, item):
                        if self._known_reference(dependency) and dependency not in retained:
                            retained.add(dependency)
                            changed = True
        return retained

    def _bounded_retained_references(self, retained: set[str]) -> set[str]:
        section_limits = {
            "entities": self.capacity.max_entities,
            "relationships": self.capacity.max_relationships,
            "episodes": self.capacity.max_episodes,
            "paths": self.capacity.max_paths,
            "messages": self.capacity.max_messages,
            "documents": self.capacity.max_documents,
            "web_discoveries": self.capacity.max_web_discoveries,
            "web_reads": self.capacity.max_web_reads,
        }
        bounded: set[str] = set()
        for section, limit in section_limits.items():
            ordered = [ref for ref in self._orders[section] if ref in retained]
            bounded.update(ordered[-limit:])
        page_entities = [
            ref for ref in self._entity_pages if ref in retained
        ]
        bounded.update(page_entities[-self.capacity.max_entities :])
        bounded.update(
            ref
            for ref in list(retained)
            if self._ref_section(ref) == "action"
        )
        actions = [ref for ref in self._actions if ref in bounded]
        bounded.difference_update(
            ref for ref in bounded if self._ref_section(ref) == "action"
        )
        bounded.update(actions[-self.capacity.max_actions :])
        return bounded

    def rollover(
        self,
        summary: str | None = None,
        *,
        active_references: list[str] | tuple[str, ...] | None = None,
        recent_contributions: int = 3,
    ) -> NotebookRolloverResult:
        """Start a bounded generation while retaining addressable context.

        The retained neighborhood is selected from explicitly active refs,
        recent tool contributions, and system hints, then expanded with the
        evidence and entity dependencies those objects require.
        """

        if recent_contributions < 1:
            raise ValueError("recent_contributions must be positive")
        retained = self._bounded_retained_references(
            self._retain_references(active_references, recent_contributions)
        )
        prior_entity_page_refs = list(self._entity_pages)
        for section in _ALL_SECTIONS:
            self._records[section] = {
                reference: self._records[section][reference]
                for reference in self._orders[section]
                if reference in retained
            }
            self._orders[section] = list(self._records[section])

        self._actions = {
            reference: action
            for reference, action in self._actions.items()
            if reference in retained
        }
        self._entity_pages = {}
        retained_entity_refs = [
            ref
            for ref in prior_entity_page_refs
            if ref in retained or ref in self._orders["entities"]
        ]
        for reference in retained_entity_refs[-self.capacity.max_entities :]:
            self._entity_pages[reference] = {
                "entity_ref": reference,
                "relationship_refs": [],
                "episode_refs": [],
                "evidence_refs": [],
            }
        for section in ("relationships", "episodes"):
            for reference in self._orders[section]:
                item = self._records[section][reference]
                for entity_ref in self._dependency_references(section, item):
                    if entity_ref.startswith("entity:") and entity_ref in self._entity_pages:
                        page = self._entity_pages[entity_ref]
                        key = "relationship_refs" if section == "relationships" else "episode_refs"
                        page[key].append(reference)
                for evidence_ref in item.get("evidence_refs", []):
                    if isinstance(evidence_ref, str) and evidence_ref in self._records["messages"]:
                        for page in self._entity_pages.values():
                            if reference in page["relationship_refs"] + page["episode_refs"]:
                                if evidence_ref not in page["evidence_refs"]:
                                    page["evidence_refs"].append(evidence_ref)

        self.possible_next_steps = [
            hint
            for hint in self.possible_next_steps
            if hint.get("audience") == "agent"
            or any(
                isinstance(ref, str) and ref in retained
                for ref in hint.get("references", [])
            )
        ][: self.capacity.max_next_steps]
        summary_references = tuple(
            ref for ref in self.summary.references if ref in retained
        )
        if summary is not None and not summary_references:
            summary_references = tuple(
                ref
                for section in _ALL_SECTIONS
                for ref in self._orders[section]
            )[: self.capacity.max_next_steps]
        if summary is None:
            summary = (
                "Previous generation retained "
                f"{len(self._orders['entities'])} entities, "
                f"{len(self._orders['relationships'])} relationships, "
                f"{len(self._orders['episodes'])} episodes, and "
                f"{sum(len(self._orders[name]) for name in _EVIDENCE_SECTIONS)} evidence objects."
            )
            summary_references = tuple(
                ref
                for section in _ALL_SECTIONS
                for ref in self._orders[section]
            )[: self.capacity.max_next_steps]
        self.generation += 1
        normalized_summary = " ".join(summary.split()) if summary else None
        if normalized_summary and len(normalized_summary) > self.capacity.max_summary_chars:
            normalized_summary = normalized_summary[: self.capacity.max_summary_chars - 1] + "…"
        self.set_summary(normalized_summary, summary_references)
        self._contribution_history = [("rollover", tuple(retained))]
        self._last_applied_references = ()
        self._last_apply_result = NotebookApplyResult(False)
        return NotebookRolloverResult(
            self.generation,
            tuple(
                ref
                for section in _ALL_SECTIONS
                for ref in self._orders[section]
            ),
            tuple(self.summary.references),
        )

    def references_for_result(
        self,
        tool_name: str,
        result: dict[str, Any],
        *,
        local_references: dict[str, str] | None = None,
    ) -> tuple[str, ...]:
        """Preview the references admitted by a result without mutating state."""

        preview = deepcopy(self)
        candidate = deepcopy(result)
        if local_references:
            candidate = self._restore_local_references(candidate, local_references)
        return preview.apply(tool_name, candidate).references

    @staticmethod
    def _restore_local_references(value: Any, local_references: dict[str, str]):
        if isinstance(value, str):
            return local_references.get(value, value)
        if isinstance(value, list):
            return [
                RunNotebook._restore_local_references(item, local_references)
                for item in value
            ]
        if isinstance(value, dict):
            return {
                key: RunNotebook._restore_local_references(item, local_references)
                for key, item in value.items()
            }
        return value

    def render(self) -> str:
        """Render this notebook for a model-facing prompt view."""

        from core.agent.notebook_renderer import render_notebook

        return render_notebook(self)

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
        self._contribution_history.clear()
        self._last_apply_result = NotebookApplyResult(False)
