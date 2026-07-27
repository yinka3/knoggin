"""Parsing for an explicit, one-document request selector."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


class DocumentSelectionError(ValueError):
    """A user-facing document selector could not be used safely."""


@dataclass(frozen=True)
class DocumentPathCommand:
    """A validated path command and the request with its token removed."""

    relative_path: str
    remaining_query: str


# Quotes allow spaces in a path; bare paths stop before punctuation so the
# punctuation remains part of the natural-language request.  The boundary
# excludes URL syntax such as ``https://example.com``.
_PATH_COMMAND_RE = re.compile(
    r"""
    (?<![A-Za-z0-9_:/])
    (?:
        [\"'](?P<quoted_path>/[^\"'\r\n]+)[\"']
        |
        (?P<bare_path>/[^\s\"'`,;:!?()]+)
    )
    """,
    re.VERBOSE,
)
_UNTERMINATED_QUOTED_PATH_RE = re.compile(
    r"(?<![A-Za-z0-9_:/])[\"']/(?=[^\"'\r\n]*(?:\r?\n|$))"
)
_BARE_SLASH_RE = re.compile(r"(?<![A-Za-z0-9_:/])/(?=$|\s|[.,;:!?)]")


def parse_document_path_command(
    user_query: str,
) -> Optional[DocumentPathCommand]:
    """Extract exactly one `/relative/path` selector from a request."""
    if not isinstance(user_query, str):
        raise DocumentSelectionError("Document requests must be text")
    if _UNTERMINATED_QUOTED_PATH_RE.search(user_query):
        raise DocumentSelectionError("Document path quotes must be closed")
    if _BARE_SLASH_RE.search(user_query):
        raise DocumentSelectionError("Document path cannot be empty")

    matches = list(_PATH_COMMAND_RE.finditer(user_query))
    if not matches:
        return None
    if len(matches) > 1:
        raise DocumentSelectionError(
            "Only one document path can be selected per request"
        )

    match = matches[0]
    path = match.group("quoted_path") or match.group("bare_path")
    remaining_query = _clean_remaining_query(
        user_query[: match.start()] + user_query[match.end() :]
    )
    return DocumentPathCommand(
        relative_path=_normalize_relative_path(path),
        remaining_query=remaining_query,
    )


def _normalize_relative_path(path: str) -> str:
    """Validate the selector against the stored normalized path contract."""
    if not path.startswith("/"):
        raise DocumentSelectionError("Document paths must start with '/'")
    relative_path = path[1:]
    if (
        not relative_path
        or "\\" in relative_path
        or "*" in relative_path
        or "?" in relative_path
    ):
        raise DocumentSelectionError("Document path is malformed")
    if any(part in {"", ".", ".."} for part in relative_path.split("/")):
        raise DocumentSelectionError(
            "Document paths cannot contain empty or parent-directory segments"
        )
    return relative_path


def _clean_remaining_query(query: str) -> str:
    """Remove selector-only whitespace without dropping surrounding punctuation."""
    query = re.sub(r"[ \t]{2,}", " ", query).strip()
    return re.sub(r"[ \t]+([,.;:!?])", r"\1", query)
