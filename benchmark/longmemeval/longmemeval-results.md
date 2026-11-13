# LongMemEval-M results

As of November 9, 2026. These are results on a 100-question subset of
LongMemEval-M, not a score for the full benchmark.

## Headline

On the newer 50 questions, Knoggin scored **82% with Haiku 5.5, 82% with
GPT-6 Luna, and 86% with Qwen 3.8 27B**. Across four runs of all 100
questions, every model averaged about 83–85%.

The older 50 questions were used heavily during development. The newer 50 were
used less, but not zero: their answers informed the system prompt rewrite and
the "state the answer plainly" change before the graded run. Treat neither half
as a clean held-out score.

## How it was run

- **Memory:** each question's chat history was imported into its own project.
- **Asking:** the question was asked in a fresh session, dated to the question's
  date. Answers reused the built memory and were never rebuilt between runs.
- **Retrieval:** the agent could use Project Context, search Episodes, search the
  entity graph, and read the source messages of an Episode it had found. Free-text
  search over all raw messages was off. The agent could make several tool calls
  before answering.
- **Grading:** LongMemEval's own evaluator, with `gpt-4o-2024-08-06` through
  OpenRouter.
- **Question mix:** 26 temporal reasoning, 24 multi-session, 15 knowledge update,
  12 single-session user, 10 single-session assistant, 8 preference and 5 abstention.

## Ingestion settings

All four builds (eight, twelve, new32 and new50) used the same settings:

| Setting | Value | What it does |
|---|---|---|
| Extraction model | `anthropic/claude-haiku-5.5` | Pulls entities and facts out of messages |
| Merge model | `anthropic/claude-sonnet-5.5` | Decides whether two entities are the same one and should be merged |
| Maintenance model | `anthropic/claude-sonnet-5.5` | Keeps Project Context up to date |
| Sessions per settle | 10 | Imports 10 history sessions, then waits for memory to catch up |
| Settle timeout | 1800 s | The longest it waits for one batch to settle |
| Episode size | 20,000 tokens or 120 messages at most | Caps how much one Episode covers |
| Project Context size | 30,000 tokens | Context is kept under this size |
| Idle flush | 86,400 s (1 day of replayed time) | A quiet gap this long closes the open memory window |
| Minimum Episode coverage | 0.6 | Stops the build if under 60% of a question's messages reach an Episode. Eight's build used the default of 0.8 |

- **Topics:** from `domain-r54.json`. It has 9 topics: Identity, Relationships,
  Home, Work, Health, Travel, Interests, Money and General. Its extraction
  guidance says to keep names, dates, amounts, preferences, plans and changes
  over time, and to separate what the user said from what the assistant only
  suggested. That file was deleted along with the harness; it's in git history
  at commit `15d7cd5a`.
- **Features:** Episodes, Project Context, entity extraction and reranking
  were on. Conflict discovery, duplicate review, communities and chat
  compaction were off.

## Results

| Answer model | Newer 50 | Run 1 (all 100) | Run 2 | Run 3 | Run 4 | Average of the 4 runs |
|---|---:|---:|---:|---:|---:|---:|
| Haiku 5.5 | 82% | 86% | 82% | 86% | 86% | 85.0% |
| GPT-6 Luna | 82% | 85% | 85% | 80% | 88% | 84.5% |
| Qwen 3.8 27B | 86% | 88% | 82% | 82% | 81% | 83.2% |

- **Run 1** is the newer 50 above plus the older 50. Runs 2–4 asked all 100
  questions again with the same prompt.
- **Variation is large.** Across the 4 runs, 13 (Haiku) to 22 (Luna) of the
  100 questions were right in some runs and wrong in others.
  Grading the same answers again barely changes anything, so the variation
  comes from the answering models, not the grader.
- **Weak spots:** preference questions (38–50%), then multi-session (about 77–80%).
  Temporal reasoning, knowledge update and single-session questions score
  87–100%. The one exception is Luna on single-session user questions, at 80%.
- **Missed by every model in every run:** the Jimmy Choo savings question (the
  retail price was never extracted), the commute question (Project Context
  dropped "wants to branch out into history podcasts") and the Miami hotel question.

## Persona change

The default persona was changed to build advice around the user's tastes.

| Answer model | Preference, before (4 runs) | Preference, after (3 runs) | All 100, after (1 run) |
|---|---:|---:|---:|
| Haiku 5.5 | 38% | 58% | 84% |
| GPT-6 Luna | 50% | 50% | 80% |
| Qwen 3.8 27B | 38% | 46% | 87% |

- The preference scores went up for Haiku and Qwen. The Miami hotel question,
  which every model used to miss, now often works.
- The overall score stayed about the same.
- There are only 8 preference questions, and the persona was tuned while
  looking at them, so treat the gain as a sign, not proof.

## Known issues

- **Haiku "I'm ready" answers:** about 4–6 times per 100 questions, Haiku
  answered "I'm ready for your next question" without searching.
  - The likely cause: the question's project still had messages from earlier
    asks, so the prompt showed a recent "last turn" right after "the user is
    about to speak", and Haiku read the question as already answered.
  - This isn't confirmed. The diagnostic runs in `runs/ready-independent/`
    asked the fish question once per variant, and even the unchanged variant
    didn't reproduce the bug.
  - It costs Haiku a few questions in every run above.
- **Hedging:** models sometimes find the right answer, then undercut it, e.g.
  "Trader Joe's, but your notes don't record the brand."

## Files

Result files are in [runs/](runs/README.md): the graded answers, traces and
token usage for every run above, plus the question lists. The harness, datasets,
driver scripts and benchmark databases have been removed, so none of these runs
can be repeated without rebuilding them.
