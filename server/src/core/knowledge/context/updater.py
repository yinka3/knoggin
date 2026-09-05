"""LLM-backed, provenance-validated project Context reconciliation."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Protocol, TypeVar
from uuid import UUID

from common.conf.domain_config import CompiledDomain
from common.schema.context import (
    AssertionKind,
    ContextAdd,
    ContextBlockRecord,
    ContextDelete,
    ContextEditBase,
    ContextReplace,
    ContextSnapshot,
    ContextSupportKind,
    LLMContextUpdate,
)
from common.schema.episode.models import Episode
from core.knowledge.context.models import ContextBlockSupport, ContextMaterialization
from core.knowledge.context.prompts import get_context_update_prompt
from core.knowledge.context.render import (
    apply_context_edits,
    render_context_model_input,
)

ResponseT = TypeVar("ResponseT")


class StructuredGenerator(Protocol):
    """The one typed model operation needed by the Context updater."""

    async def generate_structured(
        self,
        *,
        response_model: type[ResponseT],
        system: str,
        user: str,
        temperature: float = 1.0,
    ) -> ResponseT: ...


@dataclass(frozen=True, slots=True)
class ContextUpdateResult:
    """One validated Context update before the caller persists it."""

    materialization: ContextMaterialization | None
    edit_summary: str
    operation_count: int


@dataclass(frozen=True, slots=True)
class _MessageEvidence:
    message_id: int
    session_id: str
    role: str
    timestamp_ms: int | None


@dataclass(frozen=True, slots=True)
class _SourceEvidence:
    source_ref_id: UUID
    message: _MessageEvidence


@dataclass(frozen=True, slots=True)
class _ResolvedEvidence:
    messages: tuple[_MessageEvidence, ...] = ()
    sources: tuple[_SourceEvidence, ...] = ()


@dataclass(slots=True)
class ContextUpdateBuild:
    """One server-owned local-reference catalog and operation validator."""

    project_id: str
    domain: CompiledDomain
    snapshot: ContextSnapshot | None
    messages: list[dict[str, Any]]
    assistant_source_refs: list[dict[str, Any]]
    episodes: list[Episode]
    _message_by_handle: dict[str, _MessageEvidence] = field(
        init=False, default_factory=dict
    )
    _source_by_handle: dict[str, _SourceEvidence] = field(
        init=False, default_factory=dict
    )
    _episode_messages_by_handle: dict[str, tuple[_MessageEvidence, ...]] = field(
        init=False, default_factory=dict
    )
    _context_block_by_handle: dict[str, ContextBlockRecord] = field(
        init=False, default_factory=dict
    )

    def __post_init__(self) -> None:
        if not isinstance(self.project_id, str) or not self.project_id.strip():
            raise ValueError("Context update requires a project_id")
        if not isinstance(self.domain, CompiledDomain):
            raise TypeError("Context update requires a compiled domain")
        self.messages = [dict(message) for message in self.messages]
        self.assistant_source_refs = [dict(reference) for reference in self.assistant_source_refs]
        self._prepare_context_handles()
        self._prepare_catalog()

    def _prepare_context_handles(self) -> None:
        if self.snapshot is None:
            return
        section_order = {
            section.key: index for index, section in enumerate(self.domain.context_sections)
        }
        ordered = sorted(
            enumerate(self.snapshot.blocks),
            key=lambda item: (section_order.get(item[1].section_key, len(section_order)), item[0]),
        )
        self._context_block_by_handle = {
            f"C{index}": block for index, (_, block) in enumerate(ordered, start=1)
        }

    def _prepare_catalog(self) -> None:
        seen_messages: set[tuple[int, str]] = set()
        for index, raw in enumerate(self.messages, start=1):
            try:
                message = _MessageEvidence(
                    message_id=int(raw["message_id"]),
                    session_id=str(raw["session_id"]).strip(),
                    role=str(raw["role"]),
                    timestamp_ms=(
                        None if raw.get("timestamp_ms") is None else int(raw["timestamp_ms"])
                    ),
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("Context window contains malformed message evidence") from exc
            if message.message_id <= 0 or not message.session_id or message.role not in {"user", "assistant"}:
                raise ValueError("Context window contains invalid message evidence")
            key = (message.message_id, message.session_id)
            if key in seen_messages:
                raise ValueError("Context window repeats message evidence")
            seen_messages.add(key)
            self._message_by_handle[f"M{index}"] = message

        message_handles = {
            (item.message_id, item.session_id): handle
            for handle, item in self._message_by_handle.items()
        }
        seen_sources: set[UUID] = set()
        for index, raw in enumerate(self.assistant_source_refs, start=1):
            try:
                source_id = UUID(str(raw["source_ref_id"]))
                key = (int(raw["message_id"]), str(raw["session_id"]).strip())
            except (KeyError, TypeError, ValueError, AttributeError) as exc:
                raise ValueError("Context window contains malformed assistant source evidence") from exc
            parent = self._message_by_handle.get(message_handles.get(key, ""))
            if parent is None or parent.role != "assistant" or source_id in seen_sources:
                raise ValueError("Context source evidence is outside its frozen assistant message")
            seen_sources.add(source_id)
            self._source_by_handle[f"S{index}"] = _SourceEvidence(source_id, parent)

        by_pair = {
            (message.message_id, message.session_id): message
            for message in self._message_by_handle.values()
        }
        seen_episodes: set[str] = set()
        for index, episode in enumerate(self.episodes, start=1):
            if episode.project_id != self.project_id or episode.episode_id in seen_episodes:
                raise ValueError("Context Episode aid is outside its project or repeated")
            seen_episodes.add(episode.episode_id)
            current_messages = tuple(
                by_pair[(item.message_id, item.session_id)]
                for item in episode.messages
                if (item.message_id, item.session_id) in by_pair
            )
            self._episode_messages_by_handle[f"E{index}"] = current_messages

    def evidence_brief(self) -> str:
        """Render complete source catalogs without exposing durable identifiers."""

        lines = ["CURRENT CONTEXT:", self._current_context_text(), "", "FROZEN MESSAGES:"]
        for handle, message in self._message_by_handle.items():
            raw = self.messages[int(handle[1:]) - 1]
            source_time = "unknown" if message.timestamp_ms is None else str(message.timestamp_ms)
            content = str(raw.get("content") or "").strip()
            lines.extend(
                (
                    f"[{handle}] role={message.role} session={message.session_id} source_time_ms={source_time}",
                    "<message>",
                    content,
                    "</message>",
                )
            )
        lines.extend(("", "ASSISTANT SOURCES:"))
        if not self._source_by_handle:
            lines.append("(none)")
        for handle, source in self._source_by_handle.items():
            raw = self.assistant_source_refs[int(handle[1:]) - 1]
            parent_handle = next(
                local for local, message in self._message_by_handle.items() if message == source.message
            )
            source_kind = str(raw.get("source_kind") or "source")
            locator = str(raw.get("locator") or "")
            excerpt = str(raw.get("excerpt") or "").strip()
            lines.extend(
                (
                    f"[{handle}] owner={parent_handle} kind={source_kind} locator={locator}",
                    "<source_excerpt>",
                    excerpt,
                    "</source_excerpt>",
                )
            )
        lines.extend(("", "EPISODE INTERPRETATION AIDS:"))
        if not self._episode_messages_by_handle:
            lines.append("(none)")
        for handle, messages in self._episode_messages_by_handle.items():
            episode = self.episodes[int(handle[1:]) - 1]
            current_handles = [
                local
                for local, message in self._message_by_handle.items()
                if message in messages
            ]
            lines.append(
                f"[{handle}] current_window_messages={','.join(current_handles) or '(none)'}"
            )
            lines.append(episode.summary)
        return "\n".join(lines)

    def apply(self, output: LLMContextUpdate) -> ContextUpdateResult:
        """Resolve local evidence, derive block times, and materialize safely."""

        if not isinstance(output, LLMContextUpdate):
            raise TypeError("Context updater requires LLMContextUpdate")
        resolved_operations: list[ContextEditBase] = []
        evidence_by_operation: list[_ResolvedEvidence] = []
        for operation in output.operations:
            evidence = self._resolve_evidence(operation)
            self._reject_stale_target(operation, evidence)
            resolved_operations.append(self._with_derived_source_time(operation, evidence))
            evidence_by_operation.append(evidence)

        materialization = apply_context_edits(
            self.snapshot,
            resolved_operations,
            self.domain,
            project_id=self.project_id,
        )
        if materialization is None:
            return ContextUpdateResult(
                materialization=None,
                edit_summary=output.edit_summary or "No Context changes",
                operation_count=len(output.operations),
            )
        if len(materialization.operation_new_block_ids) != len(resolved_operations):
            raise RuntimeError("Context edit application did not retain operation mapping")
        supports: list[ContextBlockSupport] = []
        for operation, evidence, block_id in zip(
            resolved_operations,
            evidence_by_operation,
            materialization.operation_new_block_ids,
            strict=True,
        ):
            if block_id is None:
                continue
            supports.extend(self._supports_for_operation(block_id, operation, evidence))
        materialization = replace(materialization, supports=tuple(supports))
        return ContextUpdateResult(
            materialization=materialization,
            edit_summary=output.edit_summary or "Updated project Context",
            operation_count=len(output.operations),
        )

    def _current_context_text(self) -> str:
        if self.snapshot is None:
            return "(no prior Context revision)"
        return render_context_model_input(self.snapshot, self.domain).rstrip()

    def _resolve_evidence(self, operation: ContextEditBase) -> _ResolvedEvidence:
        messages: list[_MessageEvidence] = []
        sources: list[_SourceEvidence] = []
        for reference in operation.evidence:
            handle = reference.handle
            if handle in self._message_by_handle:
                messages.append(self._message_by_handle[handle])
            elif handle in self._source_by_handle:
                sources.append(self._source_by_handle[handle])
            elif handle in self._episode_messages_by_handle:
                expanded = self._episode_messages_by_handle[handle]
                if not expanded:
                    raise ValueError("Context Episode aid has no current-window evidence")
                messages.extend(expanded)
            else:
                raise ValueError(f"Context operation references unknown evidence {handle}")
        resolved = _ResolvedEvidence(
            messages=tuple(dict.fromkeys(messages)),
            sources=tuple(dict.fromkeys(sources)),
        )
        if not resolved.messages and not resolved.sources:
            raise ValueError("Context operation has no resolvable evidence")
        if isinstance(operation, ContextDelete):
            return resolved
        if operation.assertion_kind is AssertionKind.HUMAN_ASSERTED:
            raise ValueError("the Context model cannot create human_asserted blocks")
        if operation.assertion_kind is AssertionKind.USER_ASSERTED:
            if not any(message.role == "user" for message in resolved.messages):
                raise ValueError("user_asserted Context requires a current user message")
        elif operation.assertion_kind is AssertionKind.SOURCE_GROUNDED:
            if not resolved.sources:
                raise ValueError("source_grounded Context requires an assistant source reference")
        elif operation.assertion_kind is not AssertionKind.AGENT_DERIVED:
            raise ValueError("Context assertion kind is unsupported")
        return resolved

    @staticmethod
    def _evidence_source_time(evidence: _ResolvedEvidence) -> int | None:
        source_times = [
            message.timestamp_ms
            for message in (*evidence.messages, *(source.message for source in evidence.sources))
            if message.timestamp_ms is not None
        ]
        return max(source_times) if source_times else None

    def _reject_stale_target(
        self,
        operation: ContextEditBase,
        evidence: _ResolvedEvidence,
    ) -> None:
        if not isinstance(operation, (ContextReplace, ContextDelete)):
            return
        target = self._context_block_by_handle.get(operation.target.handle)
        if target is None or target.source_time_ms is None:
            return
        evidence_time = self._evidence_source_time(evidence)
        if evidence_time is None or evidence_time < target.source_time_ms:
            raise ValueError("older or untimed evidence cannot replace or delete newer Context")

    @staticmethod
    def _with_derived_source_time(
        operation: ContextEditBase,
        evidence: _ResolvedEvidence,
    ) -> ContextEditBase:
        if isinstance(operation, ContextDelete):
            return operation
        payload = operation.model_dump()
        payload["source_time_ms"] = ContextUpdateBuild._evidence_source_time(evidence)
        if isinstance(operation, ContextAdd):
            return ContextAdd.model_validate(payload)
        if isinstance(operation, ContextReplace):
            return ContextReplace.model_validate(payload)
        raise TypeError("Context operation type is unsupported")

    @staticmethod
    def _supports_for_operation(
        block_id: UUID,
        operation: ContextEditBase,
        evidence: _ResolvedEvidence,
    ) -> list[ContextBlockSupport]:
        if isinstance(operation, ContextDelete):
            return []
        selected: list[ContextBlockSupport] = []
        if operation.assertion_kind is AssertionKind.USER_ASSERTED:
            messages = [message for message in evidence.messages if message.role == "user"]
        elif operation.assertion_kind is AssertionKind.SOURCE_GROUNDED:
            messages = []
        else:
            messages = list(evidence.messages)
        for message in messages:
            selected.append(
                ContextBlockSupport(
                    block_id=block_id,
                    message_id=message.message_id,
                    session_id=message.session_id,
                    support_kind=(
                        ContextSupportKind.USER_MESSAGE
                        if message.role == "user"
                        else ContextSupportKind.ASSISTANT_MESSAGE
                    ),
                )
            )
        if operation.assertion_kind in {
            AssertionKind.SOURCE_GROUNDED,
            AssertionKind.AGENT_DERIVED,
        }:
            for source in evidence.sources:
                selected.append(
                    ContextBlockSupport(
                        block_id=block_id,
                        message_id=source.message.message_id,
                        session_id=source.message.session_id,
                        support_kind=ContextSupportKind.ASSISTANT_SOURCE,
                        source_ref_id=source.source_ref_id,
                    )
                )
        return list(dict.fromkeys(selected))


class ContextUpdater:
    """Call the Context LLM once, then validate before any durable write."""

    def __init__(self, *, llm: StructuredGenerator | None) -> None:
        self._llm = llm

    async def update(
        self,
        *,
        user_name: str,
        project_id: str,
        domain: CompiledDomain,
        snapshot: ContextSnapshot | None,
        messages: list[dict[str, Any]],
        assistant_source_refs: list[dict[str, Any]],
        episodes: list[Episode],
    ) -> ContextUpdateResult:
        if self._llm is None:
            raise RuntimeError("ContextUpdater requires an LLM")
        build = ContextUpdateBuild(
            project_id=project_id,
            domain=domain,
            snapshot=snapshot,
            messages=messages,
            assistant_source_refs=assistant_source_refs,
            episodes=episodes,
        )
        output = await self._llm.generate_structured(
            response_model=LLMContextUpdate,
            system=get_context_update_prompt(user_name),
            user=build.evidence_brief(),
            temperature=0.0,
        )
        return build.apply(output)
