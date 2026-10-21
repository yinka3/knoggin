# Current ingestion review verification

Updated 2026-10-19. This page supersedes older statements that PostgreSQL or Docker is unavailable. Original review notes retain their dated findings and test history; those findings are not automatically current defects.

## Verified in the latest reviews

| Run | Result | Scope |
| --- | --- | --- |
| Extraction, Context/Knowledge stages, admission, Episode generation, conflicts, advisories, merge advisor, project lifecycle | 107 unit tests passed | Controlled model/runtime dependencies; does not measure real model quality |
| Context windows, semantic commits, Episode writer, conflict reviews, artifact retention | 89 PostgreSQL contracts passed | `test_project_context_window_contract.py`, `test_semantic_commit_contract.py`, `test_episode_writer_contract.py`, `test_conflict_review_contract.py`, `test_artifact_retention_contract.py` |
| Common contracts, health, scheduler | 309 unit tests passed | `tests/unit/common`, `tests/unit/health`, `test_scheduler_contract.py` |
| Admission participation and domain storage | 7 PostgreSQL contracts passed | `test_semantic_window_participation_admission_contract.py`, `test_domain_config_store_contract.py`, `test_domain_constraints_contract.py` |
| Document and filesystem follow-up | 198 affected Windows unit/PostgreSQL tests; 5 Linux POSIX tests passed | Details in [document service remediation](document-service-remediation.md#locator-and-filesystem-follow-up-2026-10-19) |

These runs overlap earlier verification and should not be added into a unique-test total. In particular, the relationship support persistence/retirement contract and Context window retry contracts are now executed, not merely collected. The database runs do not prove every proposed composed API, restart, or migration scenario.

## Planned Linux check

The user plans to run full Docling document ingestion on the Linux PC. Use the complete local Stella backend and an isolated database to submit `Database_Internals_-_Alex_Petrov.pdf` through `DocumentService`. Wait for terminal indexing status, then inspect page count, stored parse snapshots, first/middle/last-page chunk locators, and retrieval. Record runtime, resource use, and failures. Selected-page OCR and mixed-provenance checks have already run on Windows; they do not establish full-book ingestion success.

## Remaining engineering verification and evaluation

These are not additional confirmed correctness defects, and are not all prerequisites for the planned Linux run:

- The complete domain activation -> no-op Context -> Knowledge -> following-window flow still lacks verified composed coverage. Its individual writer, stage, and domain guards have coverage.
- The one-time relationship support-key SQL upgrade still needs a test against an existing database. Fresh-schema storage contracts do not verify that migration.
- Public upload/concurrency/reconciliation and timeout/restart scenarios must not be marked covered solely from narrower storage and unit tests.
- Real model relationship grounding, pooled Episode retrieval quality, and large-catalog/advisory-query latency remain evaluation work. The full-book run will provide useful document evidence but will not automatically cover those other subsystems.
- Admission still performs quadratic prefix tokenization. The existing benchmark on 2026-10-19 measured 9,615,255 characters / 0.652 seconds for 256 exchanges and 38,427,671 characters / 2.293 seconds for 512 exchanges in batch mode. This is a known performance limitation; batching has not removed repeated text processing.
- Handle-relative filesystem operations now protect against the tested junction swaps. Moving an already opened directory and edits from processes that do not take Knoggin's lease remain limits described in the document remediation note.

The latest two review passes found no new confirmed correctness defect. That conclusion applies to the inspected paths and checks above, not an exhaustive guarantee.
