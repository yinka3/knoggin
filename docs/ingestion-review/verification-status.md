# Current ingestion review verification

Updated 2026-10-23. This page supersedes older statements that PostgreSQL or Docker is unavailable. Original review notes retain their dated findings and test history; those findings are not automatically current defects.

## Verified in the latest reviews

| Run | Result | Scope |
| --- | --- | --- |
| Extraction, Context/Knowledge stages, admission, Episode generation, conflicts, advisories, merge advisor, project lifecycle | 107 unit tests passed | Controlled model/runtime dependencies; does not measure real model quality |
| Context windows, semantic commits, Episode writer, conflict reviews, artifact retention | 89 PostgreSQL contracts passed | `test_project_context_window_contract.py`, `test_semantic_commit_contract.py`, `test_episode_writer_contract.py`, `test_conflict_review_contract.py`, `test_artifact_retention_contract.py` |
| Common contracts, health, scheduler | 309 unit tests passed | `tests/unit/common`, `tests/unit/health`, `test_scheduler_contract.py` |
| Admission participation and domain storage | 7 PostgreSQL contracts passed | `test_semantic_window_participation_admission_contract.py`, `test_domain_config_store_contract.py`, `test_domain_constraints_contract.py` |
| Document and filesystem follow-up | 198 affected Windows unit/PostgreSQL tests; 5 Linux POSIX tests passed | Details in [document service remediation](document-service-remediation.md#locator-and-filesystem-follow-up-2026-10-19) |
| Pre-full-ingestion lifecycle checks | 4 PostgreSQL-backed flows passed | Domain activation/no-op bridge/following window, scheduler timeout/restart, concurrent same-path public submission, and cancelled submission cleanup |

These runs overlap earlier verification and should not be added into a unique-test total. In particular, the relationship support persistence/retirement contract and Context window retry contracts are now executed, not merely collected. The database runs do not prove every proposed composed API, restart, or migration scenario.

## Linux full-document checks

`server/scripts/run_full_document_ingestion.py` completed a 764-page ESLII PDF run with BGE-base ONNX and the BGE-base reranker. Its local JSON report records indexed status, 764 parsed pages, 1,088 located chunks with 768-dimensional embeddings, and retrieval results. A BGE-large run also passed using its 1,024-dimensional model; the current default/schema contract is BGE-base at 768 dimensions. These checks establish the recorded ingestion and retrieval execution, not human-graded answer quality across arbitrary books. Generated reports remain local under `doc_testing/reports/`.

## Remaining engineering verification and evaluation

These are not additional confirmed correctness defects:

- Real model relationship grounding, pooled Episode retrieval quality, and large-catalog/advisory-query latency remain evaluation work. The full-book run does not cover those other subsystems.
- Admission still performs quadratic prefix tokenization. The existing benchmark on 2026-10-19 measured 9,615,255 characters / 0.652 seconds for 256 exchanges and 38,427,671 characters / 2.293 seconds for 512 exchanges in batch mode. This is a known performance limitation; batching has not removed repeated text processing.
- Handle-relative filesystem operations now protect against the tested junction swaps. Moving an already opened directory and edits from processes that do not take Knoggin's lease remain limits described in the document remediation note.

The latest two review passes found no new confirmed correctness defect. That conclusion applies to the inspected paths and checks above, not an exhaustive guarantee.
