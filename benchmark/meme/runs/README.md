# MEME result files

See [the results summary](../meme-results.md).

- `filler32k-n50/`, `filler32k-b50/`: the two 50-scenario batches. Together
  they cover all 100 filler32k scenarios with no overlap.
  - `eval_results/`: graded results from `judge.py`, one file per scenario.
  - `agent_outputs/`: the answers in the judge's input format.
  - `w1/`–`w5/`: per-worker raw output: `answers.jsonl`, `retrievals.jsonl`
    (the exact memory each answer was given), `checkpoints.jsonl`, Context
    `snapshots/`, the run `config/` and `manifest.json`.
  - `logs/`: worker logs and, for b50, the judge log.

This folder is an archive. The harness, datasets and build databases are gone,
so nothing here can be re-run, and memory can't be inspected beyond the
snapshots and retrievals saved here.
