# Extraction and Knowledge follow-up audit

Reviewed 2026-10-08 against source and test assertions. The earlier review notes remain working notes; this page separates fixed code defects from evaluation and coverage work.

## Fixed code defects

| Finding | Resolution | Verification limit |
| --- | --- | --- |
| A domain update could leave a no-op Context window at the old version and strand Knowledge. Section removal could invalidate live blocks. | The Context version bridge, section-removal guards, and legacy retry repair were committed in `2c0237ea`. | Focused tests pass. The full activation-to-next-window Postgres flow has not run locally. |
| Relationship extraction and storage lost separate sufficient support sets or observed labels; later retirement could remove a still-supported edge. Stale reviews and reclassification could use retired evidence. | Per-support observation identity, targeted retirement, review invalidation, and reclassification guards were committed in `2c0237ea`. | The Postgres storage regression is written but has not run locally. The one-time upgrade SQL also needs execution against an existing database before it is considered verified. |
| A bounded merge shortlist was limited before ranking, and merge reviews could accept unsupported or stale evidence. | SQL ranking and review validation were committed in `2c0237ea`. | The real Postgres ranking contract has not run locally. |
| JEV extraction prepared candidates by scanning every alias against every gap and fetching profiles before its observation cap applied. | Candidate preparation now stops after a fixed number of literal checks, respects the shared remaining observation budget, and fetches a profile only for an alias found in an eligible block. The regression uses 601 aliases and checks both the work bound and profile lookup count. | This is a deterministic work cap, not a real-world latency benchmark. |
| A clean generated `CONTEXT.md` could remain stale after a domain section-title change if no later window created a Context bridge. | Reconciliation now accepts a same-revision rerender only when the file still matches its recorded generated hash. Tests cover repair before and after a bridge and refusal to overwrite a user edit. | The projection writer's real Postgres CAS was not run in this follow-up. |
| Missing Docling model artifacts were reported as invalid document content, preventing the normal dependency retry path. | Docling raises the existing `DependencyError`, and the indexer classifies it as retryable `transient_dependency`. A malformed document remains nonretryable. | Retries are still bounded by the document policy; an installation after attempts are exhausted requires an explicit reindex. |

The NLI loader, runtime configuration, and model artifact were removed in `174b5afa`. The accepted source-only rule keeps a message-only name unresolved after validating its local message reference and exact quote.

## Work that is not a completed defect fix

- `merge_model` still has no application call. The proposed advisor would send stored message and episode contents to the configured model. Automatic approval review previously rejected that outbound data flow, so the candidate and review fixes above deliberately stop before it. The existing agent tool can still create a reviewed merge from evidence supplied by the agent.
- VP-02 validates handles, block membership, and domain labels, but a syntactically valid relationship can still be semantically unsupported by its cited prose. There is no measured real-model false-positive rate or reliable local entailment rule yet. This is a model-quality decision requiring adversarial examples, not a verified deterministic regression.
- The accepted source-only rule records a valid message-only candidate as a transient build issue. Keeping such proposals for later human review would require a typed durable queue; it was not part of the accepted rule.

## Coverage status

The follow-up focused suite passed **181 tests**, and Ruff passed on the changed Python files. Those tests check the local branches and fake-backed storage behavior. Docker's Postgres daemon is unavailable on this machine, and no `psql` binary was found, so the prior Postgres contracts and the one-time relationship upgrade were not executed here. Real JEV, VP-02, embedding, reranker, and Docling model behavior was not measured by this run.
