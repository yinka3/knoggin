# Extraction and Knowledge follow-up audit

Reviewed 2026-10-08 against source and test assertions. The earlier review notes remain working notes; this page separates fixed code defects from evaluation and coverage work.

## Fixed code defects

| Finding | Resolution | Verification limit |
| --- | --- | --- |
| A domain update could leave a no-op Context window at the old version and strand Knowledge. Section removal could invalidate live blocks. | The Context version bridge, section-removal guards, and legacy retry repair were committed in `2c0237ea`. | Focused tests pass. The full activation-to-next-window Postgres flow has not run locally. |
| Relationship extraction and storage lost separate sufficient support sets or observed labels; later retirement could remove a still-supported edge. Stale reviews and reclassification could use retired evidence. | Per-support observation identity, targeted retirement, review invalidation, and reclassification guards were committed in `2c0237ea`. | The Postgres storage regression is written but has not run locally. The one-time upgrade SQL also needs execution against an existing database before it is considered verified. |
| A bounded merge shortlist was limited before ranking, and merge reviews could accept unsupported or stale evidence. | SQL ranking and review validation were committed in `2c0237ea`. | The real Postgres ranking contract has not run locally. |
| The configured `merge_model` had no execution call from an entity merge tool. | After explicit approval for this data flow, `advise_entity_merge` now sends bounded, user-scoped message and episode excerpts to `merge_model` and validates its cited recommendation. A separate `propose_entity_merge` call opens a state-checked review; the advisor never applies a merge. | Fake-model tests verify the exact model argument, evidence bounds, citation checks, and no-review behavior. The hosted model and real Postgres evidence queries were not run here. |
| JEV extraction prepared candidates by scanning every alias against every gap and fetching profiles before its observation cap applied. | Candidate preparation now stops after a fixed number of literal checks, respects the shared remaining observation budget, and fetches a profile only for an alias found in an eligible block. The regression uses 601 aliases and checks both the work bound and profile lookup count. | This is a deterministic work cap, not a real-world latency benchmark. |
| A clean generated `CONTEXT.md` could remain stale after a domain section-title change if no later window created a Context bridge. | Reconciliation now accepts a same-revision rerender only when the file still matches its recorded generated hash. Tests cover repair before and after a bridge and refusal to overwrite a user edit. | The projection writer's real Postgres CAS was not run in this follow-up. |
| Missing Docling model artifacts were reported as invalid document content, preventing the normal dependency retry path. | Docling raises the existing `DependencyError`, and the indexer classifies it as retryable `transient_dependency`. A malformed document remains nonretryable. | Retries are still bounded by the document policy; an installation after attempts are exhausted requires an explicit reindex. |

The NLI loader, runtime configuration, and model artifact were removed in `174b5afa`. The accepted source-only rule keeps a message-only name unresolved after validating its local message reference and exact quote.

## Work that is not a completed defect fix

- Scheduled maintenance remains deterministic and does not call `merge_model`; only the explicit advisor tool does.
- VP-02 validates handles, block membership, and domain labels, but a syntactically valid relationship can still be semantically unsupported by its cited prose. There is no measured real-model false-positive rate or reliable local entailment rule yet. This is a model-quality decision requiring adversarial examples, not a verified deterministic regression.
- The accepted source-only rule records a valid message-only candidate as a transient build issue. Keeping such proposals for later human review would require a typed durable queue; it was not part of the accepted rule.

## Coverage status

The earlier follow-up suite passed **181 tests**. The model advisor and related tool/runtime suite passed **70 tests**; Ruff passed on their changed Python files. Those tests use a fake LLM and fake-backed storage. Docker's Postgres daemon is unavailable on this machine, and no `psql` binary was found, so the real evidence queries, earlier Postgres contracts, and one-time relationship upgrade were not executed here. Hosted `merge_model` and real JEV, VP-02, embedding, reranker, and Docling behavior were not measured by these runs.
