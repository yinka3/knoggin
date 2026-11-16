# BEAM results

As of November 15, 2026. These are results on every history of BEAM's 1M and
500K splits: 35 histories each, 20 questions per history across 10
categories. That is 700 questions per split.

## Headline

| Split | Answer model | Histories | Overall |
|---|---|---:|---:|
| 1M | Haiku 5.5 | 35 | **0.675** |
| 1M | Qwen 3.8 27B | 35 | **0.688** |
| 500K | Qwen 3.8 27B | 35 | **0.702** |
| 500K | Haiku 5.5 | 15 (1–15) | 0.719 |

On 500K, Haiku answered only histories 1–15. Qwen scored 0.728 on those same
15, so the two models are about even there.

Published numbers for comparison. Most are self-reported, and they use
different judges and models:

| System | 1M | 500K |
|---|---:|---:|
| Hindsight | 0.739 | 0.711 |
| Honcho | 0.631 | 0.646–0.649 |
| AutoMem | | 0.656 |
| LIGHT, the paper's best (Llama-4-Maverick) | 0.336 | 0.359 |
| RAG baseline (paper) | 0.307 | 0.330 |

Knoggin is in the same range as the best published systems and about twice
the paper's baselines. It is not a like-for-like ranking (see Caveats).

## How it was run

- **Memory:** each history was imported into its own project, one BEAM
  session at a time at its own date. Memory had to settle after each session
  before the next one was imported. Only the `chat` messages went in;
  questions, rubrics and the persona/plan metadata never entered memory.
- **Asking:** each question was asked in a fresh session against the finished
  memory. Answer sessions were kept out of memory and deleted afterwards.
  Memory was built once per history and reused by both answer models.
- **Retrieval:** the full Knoggin agent with its normal system prompt. The
  agent could use Project Context, search Episodes, list dated events, search
  the entity graph, and read the source messages of an Episode it had found.
  Free-text search over all raw messages was off. The default tool limits
  applied: 12 tool calls, 15 attempts, `search_episodes` 6,
  `read_episode_messages` 4, `list_events` 4.
- **Grading:** BEAM's own rubric prompt, used verbatim, with
  `openai/gpt-4.1-mini` through OpenRouter at temperature 0. That model is the
  original repository's default judge. Each question's score is the mean of
  its rubric items, where each item scores 0, 0.5 or 1.
- **Event ordering:** scored the way the BEAM paper reports it, as normalized
  Kendall tau. The answer is split into lines, each line is matched to a key
  event with an LLM yes/no call, and the matched order is compared with the
  key. F1 is also recorded in the score files.
- **Code:** commit `fa88c145`, with the committed system prompt for every
  reported score. The tree also held uncommitted workspace-sandbox work, which
  the manifests don't record.

## Ingestion settings

1M and 500K used the same settings:

| Setting | Value | What it does |
|---|---|---|
| Extraction model | `anthropic/claude-haiku-5.5` | Pulls entities, facts and Episodes out of messages |
| Merge model | `anthropic/claude-sonnet-5.5` | Merges entities and updates Project Context |
| Maintenance model | `anthropic/claude-sonnet-5.5` | Keeps Project Context up to date |
| Semantic window | 128,000 tokens (40% of model context) | One window per BEAM session, split only when a session is larger |
| Project Context size | 30,000 tokens | Context is kept under this size |
| Episode size | 20,000 tokens or 120 messages at most | Caps how much one Episode covers |
| Accumulated Episodes | 16 | Most Episodes held before they are flushed |
| Idle flush | 86,400 s | A quiet gap this long closes the open window |
| Settle timeout | 1,800 s | The longest it waits for one session to settle |
| Spending cap | $10 per history | Knoggin's own budget check |

- **Topics:** from [domain.json](domain.json), covering personal, work and
  technical conversations. No question labels, rubrics or answers appear in it.
- **Features:** Episodes, Project Context and entity extraction were on.
  Conflict discovery, duplicate review, communities and chat compaction were
  off.
- **Coverage gate:** 1M started with a check that stops a build if under 60%
  of messages reach an Episode after session 3. 13 histories failed it. They
  were resumed from their last checkpoint with the gate off, and 500K ran with
  the gate off from the start.

## Results by category

| Category | 1M Haiku | 1M Qwen | 500K Qwen | 500K Haiku (1–15) |
|---|---:|---:|---:|---:|
| Abstention | .950 | .900 | .829 | .967 |
| Contradiction resolution | .632 | .579 | .621 | .692 |
| Event ordering | .245 | .285 | .295 | .281 |
| Information extraction | .700 | .734 | .786 | .810 |
| Instruction following | .931 | .882 | .863 | .894 |
| Knowledge update | .771 | .807 | .714 | .747 |
| Multi-session reasoning | .625 | .656 | .687 | .738 |
| Preference following | .900 | .896 | .910 | .919 |
| Summarization | .500 | .720 | .733 | .548 |
| Temporal reasoning | .495 | .420 | .580 | .594 |
| **Overall** | **.675** | **.688** | **.702** | **.719** |

- **Episode coverage** (the share of messages that reached an Episode):
  - 1M averaged 69%, from 33% to 99.5%. The 13 histories under 60% scored
    .641 (Haiku) and .677 (Qwen), against .695 for both on the other 22.
  - 500K averaged 91%, from 68% to 99.5%. Its sessions fit in one window each.
- **Spread between histories:**
  - 1M: Haiku .474–.855, Qwen .533–.858.
  - 500K: Qwen .536–.873.
- **Haiku is weak on summarization** (.500 and .548). Qwen is not (.720 and
  .733).

## Weak spots

**Event ordering (about .25–.30).** Two causes:

- The metric splits the whole answer on newlines, so headings, intros and
  wrapped lines count as extra events.
- Knoggin's events are broader than the key's: one line often covers two key
  events.

Rescoring by hand reproduced the reported scores exactly, so this is not a
scorer bug.

**Temporal reasoning (.42–.59).** On 1M, both models scored 0 on 21
questions:

- About 10 have keys that look wrong or can't be reached from the chat. In
  history 10, for example, the user says the exam is on January 10, but the
  key uses March 20.
- About 5 hinge on which version of a revised plan counts.
- About 6 are real retrieval failures or give-ups.

Temporal scores higher on 500K, where memory coverage is better.

## Answer experiments

Each experiment re-answered one category on the finished memory. The baseline
answers were left untouched, and none of these changes were kept. The system
prompt is back to its committed version.

| Experiment | Scope | Baseline | Changed | Kept? |
|---|---|---:|---:|---|
| Higher tool limits (20 calls, 25 attempts, search 10, events 8, read 8) | 1M Haiku, temporal, 35 histories | .495 | .502 | No |
| Prompt rule asking for one key-style event per line | 1M event ordering, histories 6–15 | Haiku .274, Qwen .313 | Haiku .244, Qwen .324 | No, reverted |
| Prompt rule: "if only the occasion is recorded, say its details weren't" | 500K Qwen, abstention, 35 histories | .829 | .871 | No, dropped |
| `history_focus` persona | 1M history 1 only | Haiku .708, Qwen .761 | Haiku .703, Qwen .801 | No |

- **Higher tool limits:** temporal misses weren't caused by running out of
  tool calls. 25 questions still scored 0.
- **Event-ordering rule:** the per-line rubric judge scored both models lower
  on these answers. The real limit is how fine-grained the events are, not
  their format.
- **Abstention rule:** abstention was rerun 3 more times with the committed
  prompt. Those runs scored .829, .857 and .871 (mean of all 4 runs .846). Two
  runs of the same setup disagree on 8–13 of the 70 questions, so the rule's
  .871 is within normal variation. The rule did fix the pattern it targeted:
  Qwen filling in details of a real event from nearby facts or the assistant's
  suggestions. But that gain could not be told apart from noise.
  - The 500K abstention drop in histories 21–35 (.700, against .925 in 1–20)
    was mostly that noise. Reruns of 21–35 scored .800–.867.

## Caveats

- **Not like-for-like with the published numbers.** Judges, answer models and
  setups differ, and most published numbers are self-reported. The BEAM
  paper's baselines used older models.
- **The scorer wasn't checked against BEAM's own evaluator code** on a shared
  set of answers. It follows the same rubric prompt and ordering formula. One
  change: when the judge quoted LaTeX such as `\(` in its reason, the JSON was
  invalid, so the score was read directly from it. That affected one judgment.
- **Answers vary between runs.** The abstention reruns flipped 8–13 of 70
  answers with nothing changed, so differences of a few points between
  categories or models are within noise.
- **Some answer keys look wrong**, especially in temporal reasoning (see
  above). They were scored as given.
- **Failed answers were re-asked, not counted:**
  - One 1M Haiku answer in history 26 (a tool error during the answer step).
  - One 500K Qwen answer in history 29 (a tool-call formatting error).
  - 26 500K answers in histories 16–20, after credits ran out.
- **The manifests list GPT-6 Luna** as an answer model, but only Haiku and
  Qwen were run.
- **Cost:**
  - 500K: about $1.60 per history for ingest, Qwen answers and grading, taken
    from the OpenRouter balance.
  - Knoggin's own spending counter undercounts: it showed about $0.75 for a
    500K ingest and $0.95 for a 1M ingest.
  - Haiku answering cost about $0.22 per history.

## Files

The result files are in [runs/](runs/README.md). They hold the answers with
their retrieval traces, the per-question scores, ingestion progress and
manifests. The harness, datasets, driver scripts, judge-call logs and benchmark
databases have been removed, so none of these runs can be repeated without
rebuilding them. The dataset is `Mohammadta/BEAM` on Hugging Face, and the
evaluator is the public BEAM repository at
`b2da22eac88bb0874c64665f13457eb99835774a`.
