# Agent and Retrieval Evaluation

Date: 2026-09-21  
Evaluated commits: `6635ac8` through `0408fef`

## Deterministic results

The fixed Phase A message scenarios contain four relevant message IDs across
lexical-only, semantic-paraphrase, and overlapping-channel cases. Lexical search
alone nominates two of them, for 50% recall. Hybrid retrieval nominates all four,
for 100% recall, while the irrelevant scenario remains empty.

| Area | Result | Evidence |
| --- | ---: | --- |
| Message retrieval recall | 4/4, 100% | Four fixed retrieval scenarios |
| Lexical-only baseline recall | 2/4, 50% | Same scenarios with semantic nominations removed |
| Irrelevant-query rejection | 1/1 | Empty lexical and semantic channels stay empty |
| Ambiguous identity abstention | 1/1 | Equal `Bob` owners produce a new identity instead of a false merge |
| Context-backed identity resolution | 1/1 | Explicit `Robert Chen` context selects the matching `Bob` owner |
| False merges in fixed identity cases | 0 | Close candidates require the configured margin |
| Unsupported LLM NER mentions | 1/1 rejected | Missing literal/source support cannot enter resolution |
| Unsupported relationship endpoints | 1/1 rejected | Diagnostic cannot create an entity or relationship |
| Research material-question coverage | 2/2 accounted for | One cited part and one explicit unresolved gap |
| Invalid research references | 2/2 rejected | Discovery-only and unknown references fail validation |
| Parallel safe-read batch | 2 calls overlap | Controlled calls both enter before either is released |
| Parallel result order | 2/2 preserved | Starts, completion events, and model results retain request order |
| Parallel sibling failure isolation | 1/1 | One failed read retains the other result |
| Parallel cancellation cleanup | 2/2 children awaited | Both in-flight operations run their cleanup before cancellation returns |

The final bounded cross-phase command completed in 8.99 seconds with 146 tests
passing. Ruff, the architecture checker, compilation checks, and `git diff
--check` also pass for the changed paths.

## Model-dependent evaluation

No external-model Agent run was performed during this closeout. Therefore tool
count, end-to-end latency, answer quality, and token totals are not claimed here.
A meaningful comparison still requires the same fixed prompts, model identifier,
provider settings, temperature/reasoning settings, and starting database state
for both the Phase A baseline commit and the current commit.

Record each manual run with:

| Input | Revision | Model/config | Tool calls | Latency | Prompt tokens | Completion tokens | Coverage/gaps | Result quality |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| Fixed input 1 | baseline/current | pending | pending | pending | pending | pending | pending | pending |

## Remaining limitations

- Semantic message retrieval depends on an embedded Episode and exact
  `episode_messages` membership. Messages not yet represented by an Episode use
  lexical retrieval only.
- The LLM NER and unknown-endpoint recovery paths are deterministically tested
  with structured fake model output. Provider accuracy remains model-dependent.
- Parallel execution is deliberately limited to `search_messages`,
  `search_entity`, `episode_check`, and `search_documents`. Web reads, mutations,
  health snapshots, and other stateful reads remain sequential.
- PostgreSQL remains the canonical backend. SQLite work should begin only after
  its parity cases cover these retrieval, identity, provenance, and ordering
  contracts.
