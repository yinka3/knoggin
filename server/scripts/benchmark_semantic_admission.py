"""Measure exact-prefix semantic admission with the runtime tokenizer.

Run from the server directory: python scripts/benchmark_semantic_admission.py
This uses the real selector and cl100k_base tokenizer, with in-memory rows so
database latency does not hide the tokenizer's repeated-prefix cost.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from time import perf_counter

import tiktoken

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common.conf.domain_config import DomainConfig  # noqa: E402
from common.schema.settings import IngestionSettings  # noqa: E402
from core.ingestion.semantic_window_admission import (
    SemanticWindowAdmission,  # noqa: E402
)


class Rows:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    async def get_unclaimed_project_semantic_exchange_rows(self, **_kwargs):
        return self.rows


def exchange_rows(count: int) -> list[dict]:
    return [
        {
            "user_message_id": index,
            "session_id": "session-1",
            "user_content": f"Discussion item {index}: " + "project memory detail " * 12,
            "user_timestamp_ms": index,
            "user_lifecycle_state": "sealed",
            "user_exchange_state": "closed",
            "user_exchange_outcome": "user_only",
            "exchange_closed_at_ms": index,
            "assistant_message_id": None,
            "assistant_content": None,
            "assistant_timestamp_ms": None,
            "assistant_lifecycle_state": None,
            "already_claimed": False,
        }
        for index in range(1, count + 1)
    ]


async def main() -> None:
    tokenizer = tiktoken.get_encoding("cl100k_base")
    domain = DomainConfig(version=1, topics=(), entity_types=()).compile()
    print("exchanges,seconds,token_calls,characters_tokenized,source_tokens")
    for size in (64, 128, 256, 512):
        calls = 0
        characters = 0

        def count_tokens(value: str) -> int:
            nonlocal calls, characters
            calls += 1
            characters += len(value)
            return len(tokenizer.encode(value))

        admission = SemanticWindowAdmission(
            Rows(exchange_rows(size)),
            IngestionSettings(semantic_window_tokens=128_000),
            token_counter=count_tokens,
            now_ms=lambda: 1_000_000,
        )
        start = perf_counter()
        selected = await admission.select(
            user_name="ada", project_id="project-1", domain=domain,
            force_flush=True,
        )
        elapsed = perf_counter() - start
        assert selected is not None
        assert len(selected.messages) == size
        print(
            f"{size},{elapsed:.3f},{calls},{characters},"
            f"{selected.window.source_token_count}"
        )


if __name__ == "__main__":
    asyncio.run(main())
