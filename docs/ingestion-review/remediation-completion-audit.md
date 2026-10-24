# Ingestion remediation completion audit

> Current verification (2026-10-19): see [verification status](verification-status.md) for the passing PostgreSQL runs, remaining coverage limits, and planned full Docling ingestion on Linux. Older database-unavailable statements below describe earlier runs.

Reviewed 2026-10-10, with the project-file follow-up completed 2026-10-11. This audit compares current code with the original extraction, Knowledge, general ingestion, health, and intersection notes and the assertions in affected tests. Earlier notebooks describe the code at scan time; this page records what is complete now. Passing fake-backed tests do not establish PostgreSQL behavior.

## Defect checklist

| Proposed defect | Current state | Remaining evidence or work |
| --- | --- | --- |
| Domain activation could strand a no-op Context/Knowledge window; section removal could invalidate live blocks. | Code fixed: version bridge, writer guard, section checks, and legacy retry repair. | The full activation → no-op Context → Knowledge → next-window PostgreSQL flow passed on 2026-10-21. |
| Relationship observations collapsed distinct labels or independent support sets; retirement/reclassification could remove surviving evidence or use stale evidence. | Code fixed through support-set identity, targeted retirement, and stale-review guards. | Semantic commit storage contracts passed on 2026-10-19. Fresh databases use the final schema directly. |
| Merge shortlist ranked after its limit, and proposals accepted unsupported or stale evidence. | Code fixed. Explicit `advise_entity_merge` tool calls `merge_model` and keeps review/application separate. | Real Postgres candidate/evidence reads and hosted model behavior remain unrun locally. |
| NLI linked names present only in supporting messages. | NLI machinery removed; source-only candidates remain unresolved after local reference/quote checks. | The issue is transient within one build. A durable proposal queue was deferred, not left half wired. |
| JEV candidate preparation scanned past its useful cap. | Work bound and profile-fetch filter fixed. | Large-catalog alias × gap latency benchmark remains open. |
| Failed/cancelled exchanges blocked maintenance frontiers. | Quiescence predicate now excludes them. | Postgres contract collects but has not run locally. |
| Semantic scheduler timeout bypassed durable retry accounting. | Stage-aware timeout callback and attempt compare-and-set implemented. | A PostgreSQL-backed scheduler timeout followed by a fresh processor retry passed on 2026-10-21. |
| Admission fetched claimed history and repeatedly tokenized full prefixes. | SQL excludes claimed membership. Exact prefix counts are batched, improving the measured 512-exchange case from 3.86 s to 1.29 s. | Unclaimed-prefix tokenization is still quadratic. Large project SQL and health-aggregate latency remain unmeasured. |
| Concurrent same-path uploads could lose the accepted source file. | Atomic create-only hard-link publication and synchronized local test are in place. | A real PostgreSQL concurrent public-submission and reconciliation contract passed on 2026-10-21; deployment filesystem support remains an environment requirement. |
| Cancelled uploads could leave an orphan later reconciled as new work. | Owned file/write tasks are settled; cancellation cleanup tombstones a completed insert before removing owned bytes. | A failed compensating storage operation is logged and may still require repair. |
| Missing Docling artifacts were classified as bad content; exhausted PDFs lacked a verified recovery path. | Dependency classification, explicit reindex recovery test, and real cached PDF conversion smoke pass. | Release CI model lane has not been observed in this local audit. |
| Inline indexing failure made a saved upload look rejected. | Public single/batch responses return saved identity and current indexing state after admission. | PostgreSQL-backed `submit_document` success and cancellation paths passed; the HTTP adapter remains covered separately with controlled services. |
| Terminal document-index failures, unavailable maintenance repair reads, scheduler check failures, and unrelated work could each leave a false-green health result. | All four code paths now degrade the relevant snapshot; targeted producer-to-health tests pass. | Most tests use fake storage or runtime components; durable Postgres health joins remain unrun. |
| Due windows and unclaimed/FIFO backlog were absent from semantic health. | Due/retry-aware and payload-free admission aggregates feed ingestion health. The FIFO age now uses the oldest blocked closed exchange. | Context-window and admission storage contracts passed on 2026-10-19. Query cost on a long history remains unmeasured. |
| Windows project deletion left native files pending. | Windows walker and junction/symlink tests pass. | Concurrent hostile directory-entry replacement remains a documented platform limit. |

## Additional findings from this audit

1. **Fixed here: fresh blocked work could hide older FIFO blockage.** The backlog aggregate used the *latest* close time among ready exchanges behind a blocked session head. Adding a new closed exchange reset the health grace period even though an earlier exchange was still blocked. It now returns the oldest blocked close time; a reader-to-health unit test and an extended Postgres contract check the boundary. The Postgres contract remains unrun locally.
2. **Fixed: expected-hash project-file edits could both succeed.** The hash is now rechecked while holding a project mutation lock before replacement. Delete and move use the same lock. A forced race across separate filesystem instances and a second process leaves one winner and a stale-hash conflict for the other operation. The lock is project-wide, so independent edits in one project wait for each other.
3. **Fixed: a later catalog update could restore an older file hash.** A chained edit could write newer bytes while an earlier edit was still reconciling its catalog row. Upload admission and document deletion had similar file/catalog gaps. These service operations now hold the project lock through their catalog write, and a cancelled caller waits for its owned operation to settle. A periodic full-tree scan records the persistent file-mutation generation and skips its catalog write if a Knoggin file mutation occurred before commit. Controlled interleavings reproduced the stale catalog before the fix and now verify the final file and catalog hash agree.
4. **Fixed: deleting a readable foreign document could remove its source file.** The reader can return documents from another readable project, while the deletion writer is scoped to the active project. Deletion now checks ownership before touching the filesystem. The ownership test includes a foreign document that is readable but must remain intact.

## Evaluation decisions still open

- VP-02 relationship grounding checks handles, citations, and labels, but the proposed adversarial model evaluation for semantically unsupported prose has not run. A new verifier should follow measured false positives.
- Scheduled entity maintenance intentionally stays deterministic; only the explicit advisor calls `merge_model`.
- Storing source-only entity proposals for later review was deferred. Original messages and Context remain durable, while the model's proposal does not.

## Verification limit

The FIFO follow-up passed 31 health unit/integration tests and Ruff. Its two storage-contract cases collected, but Docker's Postgres daemon is unavailable in this workspace, so their SQL assertions were not executed locally. The linked real-PDF model smoke passed earlier with cached artifacts. Do not treat the remaining storage and model-evaluation gaps as completed simply because unit tests pass.

The project-file follow-up passed 121 document, Context projection, and workspace-flow tests plus 17 filesystem tests; Ruff passed. The process test uses a real second Windows process, while catalog assertions use an in-memory Postgres stand-in. A real Postgres concurrency run remains open.

The composed Postgres ingestion tests replace Episode generation with a zero-Episode generator. They cover persisted stage movement, but do not prove a rich Episode → Context → Knowledge flow. A green health snapshot also reports available runtime objects and queue state; it does not test live model inference or extraction quality.

The project-file lock and process test ran on Windows. The POSIX `flock` branch has not run locally. External editors do not take Knoggin's lock; periodic reconciliation will observe their changes, but an expected-hash edit cannot claim an atomic compare-and-swap against an uncooperative external writer. The lock files live outside project folders in the library's `.knoggin-file-locks` directory so reconciliation does not ingest them.

The project-wide lock also serializes unrelated file mutations while their catalog writes finish. A cancelled project-file request may still complete its edit before reporting cancellation; this keeps the file and catalog together, and the caller can read the current file hash before retrying.
