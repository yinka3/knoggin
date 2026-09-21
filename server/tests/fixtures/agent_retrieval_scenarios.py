"""Deterministic scenarios for measuring Agent-facing message retrieval."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MessageRetrievalScenario:
    name: str
    query: str
    lexical_hits: tuple[tuple[int, float, str], ...]
    semantic_hits: tuple[tuple[int, float, str], ...]
    expected_message_ids: tuple[int, ...]


MESSAGE_RETRIEVAL_SCENARIOS = (
    MessageRetrievalScenario(
        name="lexical_only",
        query="violet launch phrase",
        lexical_hits=((11, 0.9, "session-1"),),
        semantic_hits=(),
        expected_message_ids=(11,),
    ),
    MessageRetrievalScenario(
        name="semantic_paraphrase",
        query="what color identifies deployment",
        lexical_hits=(),
        semantic_hits=((12, 0.86, "session-1"),),
        expected_message_ids=(12,),
    ),
    MessageRetrievalScenario(
        name="overlapping_channels",
        query="durable project memory",
        lexical_hits=((13, 0.8, "session-1"),),
        semantic_hits=((13, 0.92, "session-1"), (14, 0.75, "session-2")),
        expected_message_ids=(13, 14),
    ),
    MessageRetrievalScenario(
        name="irrelevant",
        query="unrelated request",
        lexical_hits=(),
        semantic_hits=(),
        expected_message_ids=(),
    ),
)
