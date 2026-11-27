"""Split text into lines the way editors, git and tree-sitter count them.

``str.splitlines`` also breaks on form feed, vertical tab, ``\\x1c``-``\\x1e``,
``\\x85``, ``\\u2028``, ``\\u2029`` and a lone ``\\r``. A file containing any of
those would get chunk line numbers that disagree with tree-sitter rows, with
every editor, and between chunking and reading. Here only ``\\n`` ends a line;
a ``\\r\\n`` pair counts once.
"""

from __future__ import annotations


def split_lines(text: str, *, keepends: bool = False) -> list[str]:
    """Like ``text.splitlines(keepends)``, but breaking on ``\\n`` only."""

    if not text:
        return []
    parts = text.split("\n")
    trailing_newline = parts[-1] == ""
    if trailing_newline:
        parts.pop()
    if keepends:
        return [
            part + "\n" if index < len(parts) - 1 or trailing_newline else part
            for index, part in enumerate(parts)
        ]
    return [part[:-1] if part.endswith("\r") else part for part in parts]
