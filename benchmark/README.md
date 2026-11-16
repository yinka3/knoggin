# Benchmarks

A short summary of Knoggin's three benchmark results, as of November 15, 2026.
Each benchmark's own notes have the setup, caveats and per-category scores.

| Benchmark | What was run | Knoggin | For comparison | Est. cost |
|---|---|---|---|---:|
| [MEME](meme/meme-results.md) | All 100 filler32k scenarios, `gpt-4.1-mini` for every role | **0.31** overall | Paper: MD-flat 0.42, text-embedding 0.33, Mem0 0.28 | ~$31 |
| [LongMemEval-M](longmemeval/longmemeval-results.md) | 100-question subset, 4 answer runs | **85.0%** Haiku, **84.5%** Luna, **83.2%** Qwen (4-run averages) | Not a full-benchmark score | ~$230–330 |
| [BEAM-1M](beam/beam-results.md) | All 35 histories (700 questions) | **0.675** Haiku, **0.688** Qwen | Hindsight 0.739, Honcho 0.631, paper's best 0.336 | ~$80 |
| [BEAM-500K](beam/beam-results.md) | All 35 histories; Haiku on 1–15 only | **0.702** Qwen, **0.719** Haiku (1–15) | Hindsight 0.711, Honcho ~0.65, paper's best 0.359 | ~$61 |

Comparison scores are mostly self-reported and use different judges and
models. Treat them as a rough range, not a ranking.

## Costs

All costs are estimates for the reported runs only. They leave out
experiments, development runs and reruns used to measure noise.

- **MEME:** the notes recorded about $15.50 per 50-scenario batch, grading
  included. Two batches make about $31.
- **LongMemEval-M:**
  - **Answering, about $24.** Logged answer tokens show about $2.20 (Haiku),
    $1.50 (Luna) and $2.30 (Qwen) per 100 questions, over 4 runs.
  - **Grading, about $3.** That's roughly 1,200 `gpt-4o` calls.
  - **Memory building, about $200–300.** It was never recorded, so this is the
    roughest number here. It assumes about 100 builds of about 1.5M tokens each
    (LongMemEval-M's published size), priced at BEAM's ingest rate of about
    $1.30–2.10 per million source tokens.
- **BEAM:**
  - **Memory building, about $47 (1M) and $37 (500K).** That's about $1.35 and
    $1.05 per history. It's Knoggin's own spending counter scaled up about
    1.4×, because on 500K the counter came in under the OpenRouter balance
    drop.
  - **Answering, about $19 (1M) and $14 (500K).** Priced from logged tokens:
    Haiku about $0.22 and Qwen about $0.30 per history.
  - **Grading, about $14 (1M) and $10 (500K).** That's about $0.20 per history
    per answer model, with `gpt-4.1-mini`.

Prices are OpenRouter's, per million input/output tokens:

| Model | Input | Output |
|---|---:|---:|
| Haiku 5.5 | $0.10 | $0.50 |
| GPT-6 Luna | $0.10 | $0.50 |
| Qwen 3.8 27B | $0.15 | $1.875 |
| Sonnet 5.5 | $2 | $10 |
| `gpt-4.1-mini` | $0.40 | $1.60 |
| `gpt-4o` | $2.50 | $10 |

Cached-token discounts were ignored, so the answering costs may run a little
high.

## Common setup

- **Memory models:** LongMemEval and BEAM built memory with Haiku 5.5
  (extraction and Episodes) and Sonnet 5.5 (merging and Project Context). MEME
  used `gpt-4.1-mini` for everything, to match its paper.
- **Answering:** LongMemEval and BEAM used the full Knoggin agent with the
  committed system prompt. Free-text search over raw messages was off. MEME
  read memory once and answered with MEME's own prompt.
- **Grading:** each benchmark's own grader and judge model.
- **Archives:** the harnesses, datasets and benchmark databases have been
  removed. Each folder keeps result files only, so none of these runs can be
  repeated without rebuilding them.
