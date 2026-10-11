# Ingestion remediation completion audit

Reviewed 2026-10-10 against current code, the original extraction, Knowledge, general ingestion, health, and intersection notes, and the assertions in affected tests. Earlier notebooks describe the code at scan time; this page records what is complete now. Passing fake-backed tests do not establish PostgreSQL behavior.

## Defect checklist

| Proposed defect | Current state | Remaining evidence or work |
| --- | --- | --- |
| Domain activation could strand a no-op Context/Knowledge window; section removal could invalidate live blocks. | Code fixed: version bridge, writer guard, section checks, and legacy retry repair. | Full activation → no-op Context → Knowledge → next-window Postgres flow has not run locally. |
| Relationship observations collapsed distinct labels or independent support sets; retirement/reclassification could remove surviving evidence or use stale evidence. | Code fixed through support-set identity, targeted retirement, and stale-review guards. | Storage contract and one-time upgrade against an existing Postgres database remain unverified locally. |
| Merge shortlist ranked after its limit, and proposals accepted unsupported or stale evidence. | Code fixed. Explicit `advise_entity_merge` tool calls `merge_model` and keeps review/application separate. | Real Postgres candidate/evidence reads and hosted model behavior remain unrun locally. |
| NLI linked names present only in supporting messages. | NLI machinery removed; source-only candidates remain unresolved after local reference/quote checks. | The issue is transient within one build. A durable proposal queue was deferred, not left half wired. |
| JEV candidate preparation scanned past its useful cap. | Work bound and profile-fetch filter fixed. | Large-catalog alias × gap latency benchmark remains open. |
| Failed/cancelled exchanges blocked maintenance frontiers. | Quiescence predicate now excludes them. | Postgres contract collects but has not run locally. |
| Semantic scheduler timeout bypassed durable retry accounting. | Stage-aware timeout callback and attempt compare-and-set implemented. | Real Postgres timeout/restart contract remains unrun locally. |
| Admission fetched claimed history and repeatedly tokenized full prefixes. | SQL excludes claimed membership. Exact prefix counts are batched, improving the measured 512-exchange case from 3.86 s to 1.29 s. | Unclaimed-prefix tokenization is still quadratic. Large project SQL and health-aggregate latency remain unmeasured. |
| Concurrent same-path uploads could lose the accepted source file. | Atomic create-only hard-link publication and synchronized local test are in place. | Real Postgres concurrency/reconciliation contract and deployment filesystem hard-link support remain unchecked. |
| Cancelled uploads could leave an orphan later reconciled as new work. | Owned file/write tasks are settled; cancellation cleanup tombstones a completed insert before removing owned bytes. | A failed compensating storage operation is logged and may still require repair. |
| Missing Docling artifacts were classified as bad content; exhausted PDFs lacked a verified recovery path. | Dependency classification, explicit reindex recovery test, and real cached PDF conversion smoke pass. | Release CI model lane has not been observed in this local audit. |
| Inline indexing failure made a saved upload look rejected. | Public single/batch responses return saved identity and current indexing state after admission. | Postgres-backed public-flow verification remains open. |
| Terminal document-index failures, unavailable maintenance repair reads, scheduler check failures, and unrelated work could each leave a false-green health result. | All four code paths now degrade the relevant snapshot; targeted producer-to-health tests pass. | Most tests use fake storage or runtime components; durable Postgres health joins remain unrun. |
| Due windows and unclaimed/FIFO backlog were absent from semantic health. | Due/retry-aware and payload-free admission aggregates feed ingestion health. The FIFO age now uses the oldest blocked closed exchange. | Storage contract for SQL semantics collects but has not run locally. Query cost must be measured on a long history. |
| Windows project deletion left native files pending. | Windows walker and junction/symlink tests pass. | Concurrent hostile directory-entry replacement remains a documented platform limit. |

## Additional findings from this audit

1. **Fixed here: fresh blocked work could hide older FIFO blockage.** The backlog aggregate used the *latest* close time among ready exchanges behind a blocked session head. Adding a new closed exchange reset the health grace period even though an earlier exchange was still blocked. It now returns the oldest blocked close time; a reader-to-health unit test and an extended Postgres contract check the boundary. The Postgres contract remains unrun locally.
2. **Open: expected-hash project-file edits are not atomic.** `ProjectFilesystem.write_bytes(overwrite=True, expected_content_hash=...)` checks the hash before preparing a temporary file, then calls `os.replace` without rechecking under a lock. A synchronized local run forced two edits past the same check; both returned success and one edit was lost. `DocumentService.update_project_file` and `append_project_file` call this path. `delete_file` and `move_file` also check a hash before their final mutation. This requires a per-path concurrency protocol and a persisted-state test; the create-only upload fix does not cover it.

## Evaluation decisions still open

- VP-02 relationship grounding checks handles, citations, and labels, but the proposed adversarial model evaluation for semantically unsupported prose has not run. A new verifier should follow measured false positives.
- Scheduled entity maintenance intentionally stays deterministic; only the explicit advisor calls `merge_model`.
- Storing source-only entity proposals for later review was deferred. Original messages and Context remain durable, while the model's proposal does not.

## Verification limit

The FIFO follow-up passed 31 health unit/integration tests and Ruff. Its two storage-contract cases collected, but Docker's Postgres daemon is unavailable in this workspace, so their SQL assertions were not executed locally. The linked real-PDF model smoke passed earlier with cached artifacts. Do not treat the remaining storage and model-evaluation gaps as completed simply because unit tests pass.

The composed Postgres ingestion tests replace Episode generation with a zero-Episode generator. They cover persisted stage movement, but do not prove a rich Episode → Context → Knowledge flow. A green health snapshot also reports available runtime objects and queue state; it does not test live model inference or extraction quality.
