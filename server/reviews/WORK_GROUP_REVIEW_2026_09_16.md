# Work group review — 2026-09-16

Comparison: working-tree changes against `4b769b7` on `aadedewe/refactor`.
Scope: all five Knowledge/ingestion simplification groups, all three
Archive/Delete groups, and their earlier embedding/retrieval prerequisites.
Followed `agent_behavior/ENGINEERING_BEHAVIOR.md`, `REVIEW_BEHAVIOR.md`,
and the debugging/implementation guidance for the small corrections below.

## Verdict

The simplification work is coherent. Keep the new boundaries and the existing
durable checkpoints. Archive/Delete has substantial working coverage, but its
completion claim is too strong: three remaining cases need follow-up.

| Work group | Review result |
| --- | --- |
| Simplification 1: conflict packets | Keep. Seeds form a contiguous eligible prefix; optional neighbors do not advance the cursor. Provenance is bounded and cached per build; completion writes remain atomic. |
| Simplification 2: entity candidates | Keep. One SQL catalog snapshot per batch supplies exact, alias, and fuzzy matching. Failed reads propagate; committed publication owns the runtime alias cache. |
| Simplification 3: window admission | Keep. Readiness does not assemble another proposal. Claim-time selection keeps exact whole-exchange counts; the documented 30-second cadence remains intentional. |
| Simplification 4: episodes | Keep. Generation uses one frozen window; persistence rejects historical sources and reused episode IDs. Bounded repair and checkpoint replay remain. One missed old schema fixture was corrected. |
| Simplification 5: automation/advisories | Keep. Unused trusted automation and duplicate advisory merge are removed. Scheduled proposals and explicit audited decisions remain. |
| Archive/Delete 1: name provenance | Keep the project/user support records and merge journaling. Storage contracts cover source preservation, rollback, and transactional ingestion. |
| Archive/Delete 2: shared cleanup | SQL, merge-audit invalidation, and AGE cleanup are covered. Finish resolver invalidation for indirect readers. |
| Archive/Delete 3: files/recovery | Same-root retry and restart recovery are covered. Finish cleanup-location persistence and shutdown failure handling. Dot-path deletion was fixed during this review. |

Stage 6 provenance, chronology, notebook, and Context contracts passed the
combined regression suite. No new Agent redesign is justified by this review.
The folder moves collect and import successfully.

## Remaining findings

### P2 — A changed library setting can falsely complete file cleanup

`ProjectDeletionWriter` stores only project ID and user in
`project_file_cleanup_tasks`. `ProjectManager._finish_project_file_cleanup`
uses its current filesystem factory. If cleanup fails, then the library setting
changes before restart, the new location may contain no directory. Removal
returns normally and the task is cleared, leaving the original owned files.

Smallest change: record the original absolute library location in the same
durable task as deletion; retry that location. Test failure, a changed setting,
and restart together. Existing restart tests use the same library location.
This is a new group-3 gap, not a request for distributed filesystem machinery.

### P2 — Indirect readers can retain unsupported aliases in memory

`ProjectDeletionWriter` returns surviving projects containing the affected
entity plus projects directly reading the deleted project.
`ProjectManager._invalidate_entity_caches` invalidates only that returned list.
For an entity shared by projects A and B, a loaded project C that reads only B
can have A's alias cached. Deleting A cleans SQL and B's cache but misses C.
Candidate resolution uses a fresh snapshot, but cached profile/phrase lookup
does not share that protection.

Smallest change: remove the affected entity IDs from every loaded resolver in
this single-user engine. Test A/B sharing with C reading B. Do not add a new
cache-dependency subsystem for this case.

### P2 — Retrying Delete can bypass a failed runtime shutdown

`ProjectRuntime.shutdown` sets `_closed = True` before raising collected
shutdown failures. `ProjectManager.delete_project` retains the runtime after
that exception. A second request calls shutdown again, which returns immediately
because `_closed` is already true; database/file deletion can then proceed
without establishing that the failed resource stopped.

This behavior exists in the comparison commit; it is not introduced by the
work-group diff. It nevertheless contradicts group 3's completion guarantee.
Smallest change: retain/rethrow the shutdown failure, or retry incomplete
shutdown phases. Cover a failed scheduler/indexer stop followed by Delete retry.

## Corrections made during review

- Reproduced `remove_project_directory('.')` deleting a sibling marker inside
  a temporary library. Rejected both `.` and `..`; two tests now assert that
  unrelated project files survive.
- Updated the stale structured episode-output fixture to the create-only
  proposal schema and explicitly tested rejection of the retired consolidation
  action. Production validation was correct; no compatibility branch was added.
- Fixed import formatting left by the maintenance folder move.
- Corrected stale completion text in the simplification and Stage 6 plans;
  qualified the Archive/Delete completion claim with the remaining findings.

## Validation and limits

- Full server suite after the behavioral/test corrections: **1,363 passed**,
  including real PostgreSQL/AGE contracts and runtime integration tests.
- The earlier run had 1,360 passes and one stale episode fixture failure;
  the two additional tests are the dot-path regressions.
- Project filesystem focused suite: **12 passed**.
- Touched Python paths: Ruff, compile checks, architecture-import check, and
  `git diff --check` are the closeout gates.
- Test databases use the current schema. This review does not migrate/reset
  the user's development database or claim it already contains the new tables.
- Existing dependency/model warnings remain. Tests do not establish long-term
  retrieval quality or real-provider episode quality over three months.
- Unrelated untracked apps, behavior documents, CI/configuration work, and older
  review documents are outside these commits. No push is authorized or performed.
