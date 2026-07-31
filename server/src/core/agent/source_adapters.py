"""Narrow source adapters for explicit pasted text in an incoming turn."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Mapping, Sequence

from loguru import logger

from common.schema.source_reference import SourceReferenceCandidate

if TYPE_CHECKING:
    from core.agent.run import AgentRun
    from core.agent.types import ToolCall


@dataclass(frozen=True)
class PastedTextSpan:
    """A zero-based, end-exclusive span in the canonical user message."""

    start_char: int
    end_char: int


_FENCED_BLOCK_RE = re.compile(
    r"(?ms)^```[^\r\n]*\r?\n(?P<excerpt>.+?)\r?\n```[ \t]*(?=$|\r?\n)"
)
_PASTED_TEXT_TAG_RE = re.compile(
    r"(?is)<pasted_text>(?P<excerpt>.*?)</pasted_text>"
)


def build_pasted_text_candidates(
    *,
    project_id: str,
    session_id: str,
    source_message_id: int,
    message_content: str,
    agent_run_id: str,
    spans: Sequence[PastedTextSpan | Mapping[str, int]] | None = None,
) -> list[SourceReferenceCandidate]:
    """Create candidates for structured spans or clearly delimited text blocks."""
    if not isinstance(source_message_id, int) or isinstance(source_message_id, bool):
        raise ValueError("source_message_id must be a positive integer")
    if source_message_id < 1:
        raise ValueError("source_message_id must be a positive integer")
    if not isinstance(message_content, str):
        raise ValueError("message_content must be text")

    resolved_spans = (
        _validate_structured_spans(message_content, spans)
        if spans is not None
        else _find_delimited_pasted_text_spans(message_content)
    )
    return [
        SourceReferenceCandidate(
            project_id=project_id,
            session_id=session_id,
            source_kind="user_pasted_text",
            source_message_id=source_message_id,
            content_hash=_sha256_excerpt(
                message_content[span.start_char : span.end_char]
            ),
            locator={
                "kind": "character_span",
                "start_char": span.start_char,
                "end_char": span.end_char,
            },
            excerpt=message_content[span.start_char : span.end_char],
            metadata={"pasted_text": True},
            encounter_kind="user_pasted_text",
            agent_run_id=agent_run_id,
            result_position=index,
        )
        for index, span in enumerate(resolved_spans)
    ]


def _validate_structured_spans(
    message_content: str,
    spans: Sequence[PastedTextSpan | Mapping[str, int]],
) -> list[PastedTextSpan]:
    resolved = []
    for raw_span in spans:
        if isinstance(raw_span, PastedTextSpan):
            span = raw_span
        elif isinstance(raw_span, Mapping):
            span = PastedTextSpan(
                start_char=raw_span.get("start_char"),
                end_char=raw_span.get("end_char"),
            )
        else:
            raise ValueError("pasted_text_spans entries must be objects")
        _validate_span(message_content, span)
        resolved.append(span)
    return _unique_nonblank_spans(message_content, resolved)


def _find_delimited_pasted_text_spans(message_content: str) -> list[PastedTextSpan]:
    spans = []
    for pattern in (_FENCED_BLOCK_RE, _PASTED_TEXT_TAG_RE):
        for match in pattern.finditer(message_content):
            spans.append(
                PastedTextSpan(
                    start_char=match.start("excerpt"),
                    end_char=match.end("excerpt"),
                )
            )
    return _unique_nonblank_spans(message_content, spans)


def _unique_nonblank_spans(
    message_content: str,
    spans: Iterable[PastedTextSpan],
) -> list[PastedTextSpan]:
    unique = {}
    for span in spans:
        _validate_span(message_content, span)
        if message_content[span.start_char : span.end_char].strip():
            unique[(span.start_char, span.end_char)] = span
    return [unique[key] for key in sorted(unique)]


def _validate_span(message_content: str, span: PastedTextSpan) -> None:
    if (
        not isinstance(span.start_char, int)
        or isinstance(span.start_char, bool)
        or not isinstance(span.end_char, int)
        or isinstance(span.end_char, bool)
        or span.start_char < 0
        or span.end_char <= span.start_char
        or span.end_char > len(message_content)
    ):
        raise ValueError("pasted_text_spans must be valid message character spans")


def _sha256_excerpt(excerpt: str) -> str:
    return hashlib.sha256(excerpt.encode("utf-8")).hexdigest()


_TOOL_SOURCE_ENCOUNTERS = {
    "search_documents": "document_search",
    "read_document": "document_read",
    "web_search": "web_search",
    "news_search": "news_search",
}


def capture_tool_source_candidates(
    ctx: AgentRun,
    call: ToolCall,
    result: Mapping,
) -> list[SourceReferenceCandidate]:
    """Validate source contexts from one successful tool call for this run.

    The raw backend result is intentionally consumed here, before model-facing
    result localization removes ``source_context``. Invalid or incomplete tool
    items are not sources and must not turn an otherwise successful tool call
    into an agent failure.
    """

    encounter_kind = _TOOL_SOURCE_ENCOUNTERS.get(call.name)
    if encounter_kind is None or not call.call_id:
        return []

    data = result.get("data")
    if not isinstance(data, list):
        return []

    candidates = []
    for result_position, item in enumerate(data):
        if not isinstance(item, Mapping):
            continue
        source_context = item.get("source_context")
        if not isinstance(source_context, Mapping):
            continue
        try:
            candidate = SourceReferenceCandidate.model_validate(
                {
                    **source_context,
                    "project_id": ctx.scope.project_id,
                    "session_id": ctx.scope.session_id,
                    "encounter_kind": encounter_kind,
                    "agent_run_id": ctx.run_id,
                    "tool_call_id": call.call_id,
                    "result_position": result_position,
                }
            )
        except ValueError as exc:
            logger.warning(
                "Ignoring invalid source context from {} result {}: {}",
                call.name,
                result_position,
                exc,
            )
            continue
        candidates.append(candidate)
    return candidates
