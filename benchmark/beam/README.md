# BEAM-1M

This benchmark uses the 1M split: 35 independent conversation histories, each
with 20 probing questions across 10 capability categories. Build memory once
per history and configuration, then answer that history's questions without
writing the questions or answers back to memory.

- [native](native/README.md): Knoggin's LongMemEval-M model roles and answering agent.
- [omnimemeval](omnimemeval/README.md): OmniMemEval-style retrieval, answer, and scoring rules.

Both configurations use the same dialogue and questions, isolated projects,
chronological session import, and the same BEAM nugget scorer. Only `chat`
messages enter memory. Keep `probing_questions`, gold answers, rubrics, profile,
plan, narrative, and other dataset metadata on the evaluation side.

The downloaded dataset and generated runs should stay out of Git. The source
split is `Mohammadta/BEAM`, `1M`; OmniMemEval's loader writes
`data/beam/beam_1m.json`. The inspected OmniMemEval revision was
`7c79d46e31dd85f3da2509b430bba9406656dd28`.

Ingestion limits and cadence are still open. A two-history pilot for each
configuration must establish safe window sizing, oversized-exchange behavior,
Episode coverage, and completed settlement before the full run. See
`docs/KNOGGIN_BEAM_1M_CONFIGURATION.md` for the broader run plan.
