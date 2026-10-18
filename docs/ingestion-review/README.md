# Ingestion code scan

Working notebook, 2026-10-08. This review follows source code and test assertions; the root README may be stale. It records how the system behaves today and which behavior has not been verified.

## Notes

- [Extraction](extraction.md): local and hosted model paths, entity and relationship validation, and evidence loss in relationship deduplication.
- [Knowledge](knowledge.md): Context, persistence, retrieval, and neighboring document ingestion.
- [Model routing and coverage](models-and-coverage.md): configured models, actual call sites, and what tests really assert.
- [Extraction and Knowledge handoff](intersection.md): durable window stages, Context-to-Knowledge rules, and cascading handoff defects.
- [General ingestion](general-ingestion.md): conversation admission, semantic job execution, document indexing, model work, retries, and coverage.
- [Episodes](episodes.md): generation, source grounding, persistence, enrichment, retrieval, and the semantic-window handoff.
- [Conflict discovery and relationship advisories](conflict-discovery-and-advisories.md): model review, durable cursor, review freshness, dormant advisory rules, and coverage.
- [Artifact retention and cleanup](artifact-retention-and-cleanup.md): deleted-session expiry, project file cleanup, document admission compensation, and coverage.
- [Health](health.md): runtime, resource, ingestion, and background health rules and false-green paths.
- [General ingestion and health intersections](general-health-intersections.md): defects that cross admission, health, Knowledge, and maintenance.
- [Remediation plan](remediation-plan.md): implementation order, acceptance checks, and design decisions.

## Current reading

The system makes Context the canonical semantic input. A window freezes the policy and domain, then runs Episode, Context, Knowledge, and finalization at durable checkpoints. Extraction proposes entities and relationships from eligible Context blocks; Knowledge validates provenance and commits graph changes in a transaction. The code favors omitting uncertain facts over adding unsupported ones.

The current model split is task based: local alias matching and GLiNER handle routine entity candidates; JEV is an optional, default-disabled judge for block-literal candidates; the hosted structured LLM handles Episode, Context, entity fallback, and relationships. A name found only in a supporting message remains unresolved and does not create an entity association. `generate_structured()` defaults to `extraction_model` unless a call selects another model (`server/src/infrastructure/llm_client.py`). The explicit `advise_entity_merge` tool now selects `merge_model` after user approval for bounded stored-evidence text. It returns cited advice only; proposal and confirmed merge remain separate. Candidate inspection and scheduled preflight remain deterministic (`server/src/core/knowledge/entity/merge_advisor.py`; `server/src/core/knowledge/jobs/application_maintenance_scheduler.py`).

## Findings to discuss

1. A no-op Context window after domain activation used to keep the old revision and strand Knowledge. The working tree now writes a zero-impact version bridge, guards section removal, and repairs a clean projection after the bridge. Focused tests pass; the Postgres handoff/following-window case remains unverified. See [handoff notes](intersection.md).
2. Relationship extraction used to collapse independent supports and distinct observed labels for one canonical edge. The working tree now persists each sufficient support set and label separately, and retires stale review/reclassification actions with that evidence. The storage contract is written but awaits a working Postgres fixture. See [extraction notes](extraction.md) and [handoff notes](intersection.md).
3. Domain edits that change Context rendering can temporarily leave `CONTEXT.md` projection pending. The title-change repair and unsafe section-removal guards now have unit coverage. See [handoff notes](intersection.md).

## General ingestion and health pass

Conversation closure wakes a project job; admission freezes whole exchanges into a durable window, and the job advances Episode, Context, Knowledge, then finalization. Health reads scheduler state, claimed-window counts, Context projection state, and document-index work. Its model-related checks report runtime objects and queue capacity, not inference quality. See [general ingestion](general-ingestion.md) and [health](health.md).

The highest-risk new cross-layer findings are: failed/cancelled exchanges are skipped by admission but still counted as pending by entity-merge maintenance; scheduler timeouts do not consume a semantic window's durable retry budget; and concurrent same-path document uploads can leave an accepted catalog row without its source file. Cancelling an upload during catalog insertion can also leave an orphan file that later reconciliation ingests. Terminal document-index failures can disappear from background health even when a new or changed document is absent from search. Health misses unclaimed exchange backlog and can report an old, unprogressed window as healthy. Admission scans already-claimed history on each cadence and repeatedly tokenizes growing pending prefixes. Evidence, qualifications, and suggested regression checks are in the [intersection notes](general-health-intersections.md).

The focused admission, scheduler-health, ingestion-health, and maintenance unit suites passed **39 tests**. A document filesystem/parser/source-batch run had **40 passes and 3 symlink setup failures** because this Windows account cannot create symlinks (`WinError 1314`). These fake-backed or local assertions do not verify the SQL-to-admission, scheduler-timeout-to-durable-retry, concurrent-upload, or real Docling boundaries. This pass did not rerun the Postgres integration contracts; the prior local attempt could not complete database setup.
