# LongMemEval result files

See [the results summary](../longmemeval-results.md).

- `final100/`: run 1 (`older50/`, combined with `../m-new50/graded-all50/`),
  runs 2–4 (`round-1/` to `round-3/`), and the regrading check (`regrade-*`).
- `m-new50/`: the newer 50 graded results and their answer traces.
- `m-eight/`, `m-twelve/`, `m-new32/`: answer traces for the older 50.
- `persona-pref/`, `persona100/`: the persona change test.
- `hard13-qmax/`: a side test of Qwen 3.8 Max on 13 hard questions.
- `ready-independent/`: answers from the "I'm ready" investigation; not benchmark scores.
- `*-questions.json`: question IDs, types, prompts and expected answers. No
  chat histories.

This folder is an archive. The harness, datasets and build databases are gone,
so nothing here can be re-run without rebuilding them.
