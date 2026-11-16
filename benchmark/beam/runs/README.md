# BEAM result files

See [the results summary](../beam-results.md).

- `1m/<history>/` and `500k/<history>/` hold one folder for each of the 35
  histories, with:
  - `manifest.json`: settings, dataset hash and code commit.
  - `ingestion.jsonl`: one row per BEAM session, with windows, Episode
    coverage, time and Knoggin's spending counter.
  - `answers.jsonl`: every answer with its question, retrieval trace and token
    usage.
  - `scores-haiku.jsonl` and `scores-qwen.jsonl`: per-question judge results,
    including rubric items, and event-ordering F1 and tau.
    `500k/16`–`35` have Qwen only.
- `experiments/` holds the re-answered category only, as `answers-<history>.jsonl`
  plus `scores.jsonl`:
  - `tlimits-haiku`: higher tool limits on 1M temporal. The limits are in
    `agent_limits.json`.
  - `eofmt-haiku`, `eofmt-qwen`: the event-ordering format rule, 1M histories
    6–15.
  - `500k-abstrule-qwen`: the abstention rule.
  - `500k-norule1-qwen` to `500k-norule3-qwen`: the same questions with the
    committed prompt, used to measure run-to-run noise.
  - `history-focus-h1`: the `history_focus` persona on 1M history 1, with its
    persona in `persona_manifest.json`.

This folder is an archive. The harness, datasets, judge-call logs and build
databases are gone, so nothing here can be re-run without rebuilding them.
