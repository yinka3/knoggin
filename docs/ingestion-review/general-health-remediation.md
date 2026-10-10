# General ingestion and health remediation

Working plan, 2026-10-10. Source and persisted outcomes take priority over test names or old README text. The [general ingestion](general-ingestion.md), [health](health.md), and [intersection](general-health-intersections.md) scans hold the original evidence.

## Implemented in this pass

1. **Concurrent same-path upload:** create-only publication uses a hard link from the completed temporary file, so the destination is created atomically and cannot replace a concurrent winner. A synchronized two-upload test checks that exactly one catalog row wins, its hash matches the surviving file, and no temporary file remains. Filesystems that do not support hard links will reject create-only upload; this needs deployment compatibility review.
2. **Failed/cancelled exchange maintenance blockage:** the maintenance pending-work count excludes these two outcomes, matching semantic admission. A real Postgres contract checks that they do not block a frontier, while an unclaimed valid exchange still blocks until it joins a completed window. The contract is written but not executed: the Docker daemon is unavailable locally.
3. **Terminal document-index health:** a scoped SQL count of failed documents with no retry flows through `DocumentIndexer` and `DocumentService` into background health. Health degrades and shows only the count, not the parser error. The parser-failure test now checks this producer-to-health boundary. The affected unit/integration suite passed 121 tests.
4. **Unavailable maintenance repair read:** engine health now degrades when the expected maintenance repair metric cannot be read, rather than treating an unavailable count as zero. The health suite passed 17 tests; the failure detail stays out of the public payload.
5. **Scheduler preflight failures:** consecutive job-check errors are recorded by job name with a fixed count cap and no exception text. Ingestion health degrades for a failing `project_semantic` check; a later successful check clears the signal. A real scheduler-to-health test and the affected scheduler/health suites passed 39 tests.

## Next implementation units

1. Make upload file/catalog admission cancellation-safe, including cancellation while a worker thread is writing, and make inline index failure return a durable document identity and status. Check later reconciliation and same-path retry behavior.
2. Make scheduler-imposed semantic timeouts consume a stage-scoped durable attempt after cancellation unwinds. Distinguish shutdown cancellation; test retry/backoff/exhaustion through scheduler and persisted health.
3. Add health signals for unclaimed exchange backlog/FIFO barriers and due windows with no progress. Keep legitimate model work and retry backoff distinct from stalls.
4. Bound admission SQL to unclaimed work and eliminate repeated prefix tokenization; test membership and work scaling.
5. Check pending document indexing against actual indexing activity rather than unrelated scheduler/background jobs. Keep terminal failures and future retries separate.
6. Verify Docling artifacts and the exhausted-dependency recovery path in an environment with model artifacts. Run the storage-backed contracts when Postgres is available.

## Coverage and limits

- Focused affected suites: **121 passed** using system Python and the repo environment's installed packages, with the async pytest plugin loaded explicitly. Ruff passed on changed files.
- The storage contract for maintenance eligibility is unrun because Docker's Postgres daemon is unavailable. The concurrent upload test uses a real local filesystem but a fake catalog writer; a real Postgres uniqueness/concurrency contract remains outstanding.
- Passing tests verify the stated paths only. The Windows project deletion suite separately passed 37 tests after Developer Mode was enabled; its concurrent hostile mutation limit remains recorded in the intersection notes.
