#!/usr/bin/env python3
"""Count tiktoken tokens for prompt/input fixtures.

This intentionally counts only visible text provided in fixture files. It does
not estimate provider-specific hidden chat envelope tokens, tool schema tokens,
or model-specific tokenizer differences outside tiktoken.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import tiktoken

TEXT_SUFFIXES = {
    ".txt",
    ".md",
    ".json",
    ".jsonl",
    ".yaml",
    ".yml",
    ".prompt",
}


@dataclass
class TokenCount:
    path: str
    section: str
    chars: int
    tokens: int


def build_encoding(model: str | None, encoding_name: str) -> tiktoken.Encoding:
    if model:
        try:
            return tiktoken.encoding_for_model(model)
        except KeyError:
            pass
    return tiktoken.get_encoding(encoding_name)


def count_text(
    encoding: tiktoken.Encoding,
    path: str,
    section: str,
    text: str,
) -> TokenCount:
    return TokenCount(
        path=path,
        section=section,
        chars=len(text),
        tokens=len(encoding.encode(text)),
    )


def iter_paths(paths: list[str], recursive: bool):
    for raw_path in paths:
        path = Path(raw_path)
        if path.is_dir():
            iterator = path.rglob("*") if recursive else path.iterdir()
            for child in iterator:
                if child.is_file() and child.suffix.lower() in TEXT_SUFFIXES:
                    yield child
        else:
            yield path


def content_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, dict) and "text" in item:
                parts.append(content_to_text(item["text"]))
            else:
                parts.append(content_to_text(item))
        return "\n".join(part for part in parts if part)
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def count_json_payload(
    encoding: tiktoken.Encoding,
    path: str,
    payload: Any,
) -> list[TokenCount]:
    counts: list[TokenCount] = []

    if isinstance(payload, dict) and isinstance(payload.get("messages"), list):
        total_text_parts: list[str] = []
        for index, message in enumerate(payload["messages"]):
            if not isinstance(message, dict):
                continue
            role = str(message.get("role", f"message_{index}"))
            text = content_to_text(message.get("content", ""))
            counts.append(
                count_text(encoding, path, f"messages[{index}].{role}", text)
            )
            total_text_parts.append(text)
        counts.append(
            count_text(
                encoding,
                path,
                "messages.total_visible_text",
                "\n".join(total_text_parts),
            )
        )
        return counts

    if isinstance(payload, dict) and ("system" in payload or "user" in payload):
        system = content_to_text(payload.get("system", ""))
        user = content_to_text(payload.get("user", ""))
        counts.append(count_text(encoding, path, "system", system))
        counts.append(count_text(encoding, path, "user", user))
        counts.append(
            count_text(encoding, path, "total_visible_text", f"{system}\n{user}")
        )
        return counts

    if isinstance(payload, list):
        for index, item in enumerate(payload):
            text = content_to_text(item)
            counts.append(count_text(encoding, path, f"item[{index}]", text))
        counts.append(
            count_text(
                encoding,
                path,
                "total_visible_text",
                "\n".join(content_to_text(item) for item in payload),
            )
        )
        return counts

    text = content_to_text(payload)
    counts.append(count_text(encoding, path, "json_text", text))
    return counts


def count_file(encoding: tiktoken.Encoding, path: Path) -> list[TokenCount]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        try:
            return count_json_payload(encoding, str(path), json.loads(text))
        except json.JSONDecodeError:
            pass

    if path.suffix.lower() == ".jsonl":
        counts: list[TokenCount] = []
        total_parts: list[str] = []
        for line_number, line in enumerate(text.splitlines(), 1):
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
                line_text = content_to_text(payload)
            except json.JSONDecodeError:
                line_text = line
            counts.append(
                count_text(encoding, str(path), f"line[{line_number}]", line_text)
            )
            total_parts.append(line_text)
        counts.append(
            count_text(
                encoding,
                str(path),
                "total_visible_text",
                "\n".join(total_parts),
            )
        )
        return counts

    return [count_text(encoding, str(path), "text", text)]


def print_table(counts: list[TokenCount]) -> None:
    print("path\tsection\tchars\ttokens")
    for row in counts:
        print(f"{row.path}\t{row.section}\t{row.chars}\t{row.tokens}")
    total_rows = rows_for_summary_total(counts)
    total_chars = sum(row.chars for row in total_rows)
    total_tokens = sum(row.tokens for row in total_rows)
    print(f"TOTAL\tall\t{total_chars}\t{total_tokens}")


def rows_for_summary_total(counts: list[TokenCount]) -> list[TokenCount]:
    by_path: dict[str, list[TokenCount]] = {}
    for row in counts:
        by_path.setdefault(row.path, []).append(row)

    summary_rows: list[TokenCount] = []
    for rows in by_path.values():
        total_visible = [
            row
            for row in rows
            if row.section.endswith("total_visible_text")
            or row.section == "total_visible_text"
        ]
        summary_rows.extend(total_visible or rows)
    return summary_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Count tiktoken tokens in prompt/input fixture files.",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="Files or directories to count. Reads stdin when omitted.",
    )
    parser.add_argument(
        "--model",
        help="Optional tiktoken model name. Falls back to --encoding if unknown.",
    )
    parser.add_argument(
        "--encoding",
        default="o200k_base",
        help="Fallback tiktoken encoding name. Default: o200k_base.",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recurse into directories.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of a tab-separated table.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    encoding = build_encoding(args.model, args.encoding)

    if not args.paths:
        text = sys.stdin.read()
        counts = [count_text(encoding, "<stdin>", "text", text)]
    else:
        counts = []
        for path in iter_paths(args.paths, args.recursive):
            counts.extend(count_file(encoding, path))

    if args.json:
        print(json.dumps([asdict(row) for row in counts], indent=2))
    else:
        print_table(counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
