# MEME results

As of November 12, 2026. These are results on all 100 scenarios of MEME's
filler32k set (50 personal, 50 software), run the way the paper's main table
was run.

## Headline

Knoggin scored **0.31 overall** (the mean of the six task scores). For
comparison, the paper's main table: MD-flat 0.42, text-embedding 0.33, Mem0
0.28, BM25 0.25, Karpathy 0.10, Graphiti 0.03.

Knoggin's Deletion score (0.35) is above every system in the paper. Aggregation
and Temporal are where it falls behind.

## How it was run

- **Models:** `openai/gpt-4.1-mini` for every role: extraction and Episodes,
  the per-window Context update, Context maintenance, entity merging, and
  answering. This matches the paper's main table.
- **Memory:** each scenario was imported into its own project, one session at a
  time at its own date, waiting for memory to settle after each session.
- **Asking:** at each checkpoint (before and after the facts change), memory was
  read once: Project Context, plus one hybrid Episode search with the source
  messages of the Episodes it found (raw message search as a fallback). That
  text went to the answer model.
- **Answer prompt:** MEME's own, verbatim (one user message, temperature 0, 500
  tokens).
- **Grading:** MEME's `judge.py` with `openai/gpt-4o` through OpenRouter.
  Cascade, Absence and Deletion count only "real" passes, as the paper does.
- **Features:** Episodes, Project Context and entity extraction were on.
  Conflict discovery, duplicate review, communities and chat compaction were
  off. Topics came from `domain.json` (Identity, Personal, Work, Interests,
  General).
- **Archive:** the harness and the MEME dataset were deleted after these runs
  and were never committed. The dataset and `judge.py` come from the public
  MEME repository.

## Results

| | Scenarios | Before | ER | Agg | Tr | Del | Cas | Abs | Overall |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **All** | 100 | 96% | 0.97 | 0.13 | 0.40 | 0.35 | 0.03 | 0.01 | **0.31** |
| Personal | 50 | 96% | 0.98 | 0.14 | 0.20 | 0.44 | 0.01 | 0.00 | 0.30 |
| Software | 50 | 96% | 0.96 | 0.12 | 0.60 | 0.26 | 0.04 | 0.01 | 0.33 |
| Batch n50 | 50 | 95% | 0.96 | 0.16 | 0.50 | 0.32 | 0.05 | 0.02 | 0.33 |
| Batch b50 | 50 | 97% | 0.98 | 0.10 | 0.30 | 0.38 | 0.01 | 0.00 | 0.30 |
| *MD-flat (paper)* | 100 | | 0.94 | 0.45 | 0.77 | 0.25 | 0.06 | 0.05 | *0.42* |

- "Before" is the share of before-change questions answered correctly (394
  questions, ER excluded). The judge doesn't score them; it's a sanity check
  that memory was built.
- Graded after-change questions: 100 each for ER, Agg, Tr and Del, 164 Cascade
  and 130 Absence.

## Caveats

- **The code changed between batches.** n50 ran on November 11 and b50 on
  November 12, with forget-feature work in between (uncommitted, so both
  manifests show the same `git_sha`). 17 scenarios ran on both versions during
  testing and scored 0.32 overall on each, so the batches are treated as
  comparable.
- **Cascade and Absence are near zero.** Knoggin usually had the new value but
  hedged or kept the old one alongside it, which the judge fails. gpt-4.1-mini
  also tended to add a new Context block beside the old fact instead of
  replacing it, and to save chat topics as facts about the user.
- **Some Absence labels look wrong** (pl_009, pl_041, pl_044, pl_047, sw_013,
  sw_016, sw_020, sw_026, sw_040, sw_048). They were scored as given.
- **Stronger internal models do much better.** In earlier testing on 64
  scenarios, with Haiku 5.5 / Sonnet 5.5 building memory and MEME's answer
  prompt, Knoggin scored 0.56 (Haiku answering) and 0.65 (GPT-6 Luna
  answering). That memory predates the Deletion fix, and those run files were
  deleted.
- **Cost:** about $15.50 per 50-scenario batch, including grading.
