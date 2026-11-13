# OmniMemEval-style configuration

Use the `1M` BEAM split and `gpt-4.1-mini-2025-04-14` for Knoggin extraction,
Episode generation, Context merge/maintenance, and answer generation. Use
`gpt-4o-mini-2024-07-18` as the judge. Record the provider-routed model IDs
actually sent by Knoggin.

Build one isolated memory per history. Map OmniMemEval's `add()` to ordered
session import, `search()` to a Knoggin memory retrieval with top-K 20, and
`delete()` to cleanup of that isolated project. Define and record how Knoggin
results are rendered into the retrieved context; this affects comparability.

For each question, retrieve once and make one temperature-0 answer call using
OmniMemEval's `BEAM_ANSWER_PROMPT` from `scripts/utils/prompts.py`. Do not run
Knoggin's tool-using answering agent. Score with OmniMemEval's BEAM rubric-item
judge and its separate event-ordering calculation. Report overall and all 10
category scores for the 700 answers, with failed/skipped items shown separately.

The upstream reference is `MemTensor/OmniMemEval` revision
`7c79d46e31dd85f3da2509b430bba9406656dd28` (`scripts/run_beam_eval.sh`,
`scripts/beam/`, and `scripts/utils/prompts.py`). This is an OmniMemEval-style
Knoggin run until adapter behavior and model routing have been verified. Match
the native configuration's ingestion policy where feasible; record deviations.
