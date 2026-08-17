"""Detection and validation of pasted-text spans in user messages."""

from __future__ import annotations

import hashlib
import re
from typing import Iterable, Mapping, Sequence

from common.schema.source.locators import PastedTextLocator
from common.schema.source.references import SourceReferenceCandidate

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
    spans: Sequence[PastedTextLocator | Mapping[str, int]] | None = None,
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
            locator=span,
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
    spans: Sequence[PastedTextLocator | Mapping[str, int]],
) -> list[PastedTextLocator]:
    resolved = []
    for raw_span in spans:
        if isinstance(raw_span, PastedTextLocator):
            span = raw_span
        elif isinstance(raw_span, Mapping):
            span = PastedTextLocator.model_validate(raw_span)
        else:
            raise ValueError("pasted_text_spans entries must be objects")
        _validate_span(message_content, span)
        resolved.append(span)
    return _unique_nonblank_spans(message_content, resolved)


def _find_delimited_pasted_text_spans(
    message_content: str,
) -> list[PastedTextLocator]:
    spans = []
    for pattern in (_FENCED_BLOCK_RE, _PASTED_TEXT_TAG_RE):
        for match in pattern.finditer(message_content):
            spans.append(
                PastedTextLocator(
                    start_char=match.start("excerpt"),
                    end_char=match.end("excerpt"),
                )
            )
    return _unique_nonblank_spans(message_content, spans)


def _unique_nonblank_spans(
    message_content: str,
    spans: Iterable[PastedTextLocator],
) -> list[PastedTextLocator]:
    unique = {}
    for span in spans:
        _validate_span(message_content, span)
        if message_content[span.start_char : span.end_char].strip():
            unique[(span.start_char, span.end_char)] = span
    return [unique[key] for key in sorted(unique)]


def _validate_span(message_content: str, span: PastedTextLocator) -> None:
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
