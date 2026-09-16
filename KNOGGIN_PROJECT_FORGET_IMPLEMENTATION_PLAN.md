# Project Archive and Delete Implementation Plan

Status: work groups 1–3 implemented; the 2026-09-16 review found remaining
cache-invalidation and failure-recovery gaps in groups 2–3. See
`server/reviews/WORK_GROUP_REVIEW_2026_09_16.md` before treating deletion as complete.
Scope: one user, one local engine. Current source is authoritative.

## Product contract

- **Archive** keeps the project and its information, and is reversible.
- **Delete** permanently removes project-owned data and removes contributions
  that have no independent surviving support.

Do not introduce a separate Forget operation or a terminal hidden-but-retained
Delete state. Delete does not ban facts: independent future evidence may teach
Knoggin the same information again.

## Existing implementation to retain

`ProjectManager.delete_project()` excludes active project leases, closes the
project runtime, and invokes `ProjectDeletionWriter` under the maintenance lock.
The writer removes project-owned SQL state through cascades, clears project AGE
relationships, and deletes entity identities with no remaining project context.
Keep that hard-delete lifecycle. Do not replace it with soft deletion.

## Required cleanup work

1. Inventory ownership across canonical tables, shared entities and aliases,
   merge audit payloads, derived projections, and project-owned filesystem paths.
   Preserve authored files outside the owned project directory.
2. Capture affected shared entity IDs before removing project rows. Keep an
   identity when surviving independent evidence supports it; remove unsupported
   identities. Do not treat a copied summary or merge audit as independent proof.
3. Add the minimum source provenance needed to establish support for each alias
   and canonical name. Current `entity_aliases(entity_id, alias)` cannot answer
   which project's deletion should remove an alias. Track actual support at the
   write boundary, including explicit user-authored names. Do not infer it from
   an embedding or have an LLM reconstruct it.
4. For surviving identities, remove unsupported aliases and choose a canonical
   name only from surviving supported names using an explicit deterministic
   rule. If identity continuity cannot be established, do not silently invent a
   replacement; settle that policy before implementing this case.
5. Remove project-derived semantic payloads from global merge journals and make
   any affected rollback unavailable when its evidence has been deleted. Retain
   only nonsemantic operational metadata needed to explain that outcome.
6. Remove the entire owned project directory, including document contents and
   workspace projections. SQL deletion alone is not complete deletion.
7. Refresh affected live caches and derived projections after canonical cleanup.
   Confirm reads cannot rediscover deleted contributions through another project.

## Recovery and ordering

Keep canonical database cleanup atomic where possible. Database and filesystem
removal cannot share one transaction: record only the minimal durable cleanup
work needed to retry filesystem removal after a crash. Hide a deleted project
immediately and never report complete deletion while owned files remain.

Reuse existing maintenance exclusion and durable commit checks. Verify that
in-flight ingestion, document indexing, and maintenance cannot recreate removed
state. Introduce additional guards only for demonstrated races.

## Validation gates

- Delete a project containing sessions, episodes, Context, documents and AGE
  relationships; verify all owned database and filesystem state is removed.
- Preserve another project's independently supported shared identities/aliases.
- Remove aliases and names supported only by the deleted project.
- Verify merge journals and rollback cannot restore deleted semantic content.
- Inject failures around database commit and filesystem cleanup; retry must be
  idempotent and the result must accurately distinguish pending from complete.
- Verify active-work exclusion and prevent post-delete publication.
- Independently reintroduce the same fact later and allow normal ingestion.

## Out of scope

Separate Forget APIs, model-driven reconciliation, identity quarantine, new
human-review workflows, permanent fact suppression, and distributed coordination.
These are not prerequisites for a clear Archive/Delete product contract.

## Work group 1 — Know where shared information came from

Status: complete.

**Why:** Two projects can mention the same person. Before deleting one project,
we need to know which names and aliases the other project still supports.

Tasks:

1. Trace the current deletion flow and list project-owned data, shared data,
   merge-history copies, caches, and owned files. Include cross-project references.
2. Record the minimum source support for canonical names and aliases at their
   write boundaries, including ingestion, explicit user naming, and entity merges.
   Reuse existing source links where they are sufficient.
3. Define a deterministic rule for keeping an identity and choosing its name
   after some evidence disappears. Settle the case where a shared identity has
   no surviving supported name before starting group 2.
4. Add two-project fixtures proving that independent evidence can be distinguished
   from copied summaries and merge history. Use a development reset if needed;
   do not build legacy-data migration machinery for this unreleased system.

### What is now recorded

`entity_name_supports` is the single durable record of a name's source:

- `project` rows are written by semantic ingestion and project-scoped alias
  writes. The row is deleted automatically with its project.
- `user` rows are written for the configured user's canonical name and aliases.
  They are not owned by a project.
- Identity refresh removes support for aliases it removes, while retaining a
  project's support for a name that remains configured.
- A confirmed entity merge moves these rows to the survivor and journals that
  move so a safe rollback restores the original sources.
- Semantic ingestion writes its support rows in its existing transaction, so a
  later failed validation rolls those rows back with the rest of the window.

Episodes, copied Context text, and global merge audit payloads do not create a
support row. They are not independent name evidence.

### Decided identity and name rule for group 2

- Keep the identity entity because its configured user name is user-supported.
- Keep any other identity only when it has a remaining user-supported name, or
  both a remaining project context and a remaining project-supported name.
- Remove aliases with no remaining support. If the current canonical name is
  still supported, keep it. Otherwise choose from surviving names in this
  order: a user-supported name first, then the name supported by the most
  distinct remaining projects, then case-insensitive name order and original
  spelling as deterministic tie breakers.
- Never infer a replacement name from an embedding, episode text, or merge
  journal. If no supported name remains, remove the non-identity entity.

The current schema makes canonical names immutable. Group 2 must permit only
this deletion-time, source-backed canonical reassignment; it must not make
ordinary renames freely mutable.

### Deletion inventory carried into group 2

- `ProjectManager` holds the maintenance lock, rejects active leases, and
  shuts down a loaded runtime. `ProjectDeletionWriter` locks the project,
  clears its AGE projection, captures project entity contexts, removes semantic
  window memberships, deletes the project cascade root, and removes orphaned
  entity projections.
- Project-owned rows cascade from `projects`, including sessions, messages,
  Context, episodes, documents, and the new project name-support rows.
- Shared state still needing source-aware cleanup is `entities`,
  `entity_aliases`, and merge-audit/rollback content. Live resolver caches and
  owned filesystem paths are also not yet handled by this group.

**Done:** Storage contracts show user identity, ingestion canonical and alias,
project-alias, and merge sources; a two-project fixture retains only the
surviving project's source after deletion and ignores copied episode/merge-
history text.

## Work group 2 — Remove the project's data and unsupported leftovers

Status: complete.

**Why:** Deleting the project should remove what it contributed without erasing
information that another project independently supports.

Tasks:

1. Capture affected shared entity IDs, then remove project-owned database rows
   and unsupported shared names, aliases, and identities in one transaction where
   possible. Apply group 1's naming rule to surviving identities, with a narrow
   deletion-only path for source-backed canonical reassignment.
2. Remove project-derived content from merge journals and rollback payloads.
   Disable affected rollback operations so they cannot restore deleted content;
   retain only the operational metadata needed to explain why.
3. Refresh affected search/graph projections and live entity caches. Audit
   cross-project reads so surviving projects cannot retrieve removed contributions.
4. Test full project database cleanup, preservation of independently supported
   shared data, rollback prevention, and transaction failure/retry behavior.

**Done when:** Database and retrieval tests find no unsupported contribution from
the deleted project, while the other project's supported information still works.
File removal is completed in group 3; group 2 alone is not complete deletion.

### What is now implemented

- `ProjectDeletionWriter` captures entities touched by the project context or
  name support under the same user-global lock used by entity merges. It deletes
  the project and cleans the remaining aliases, source rows, and identities in
  that transaction.
- A surviving canonical name stays when it has support. Otherwise deletion may
  reassign it only to a current supported name, using the settled user,
  project-count, and alphabetical ordering rule. The schema still rejects every
  ordinary canonical rename.
- A merge audit that used the deleted project is marked `failed`, has its plan
  and mutation payload removed, and cannot be rolled back or repaired. A
  redirected identity from such an unsupported merge is removed with its
  survivor so its foreign key cannot block deletion.
- AGE now replaces aliases from canonical SQL instead of accumulating old ones.
  The transaction rebuilds directly affected surviving project projections, and
  `ProjectManager` invalidates the affected IDs from loaded resolver caches,
  including projects that previously read the deleted project.
- Real PostgreSQL/AGE contracts cover shared names, user-name preference,
  rollback prevention, redirected merge cleanup, projection failure rollback and
  retry. Group 3 still owns filesystem removal and durable retry after a
  database commit.

## Work group 3 — Finish file cleanup and make deletion survive failures

Status: complete.

**Why:** Deletion is unfinished if uploaded files remain on disk or a crash lets
background work bring the project back.

Tasks:

1. Remove the entire owned project directory, including documents and workspace
   files. Preserve authored files outside it and do not follow links outside it.
2. Record minimal cleanup work durably with the database deletion so a restart
   can retry file removal. Keep the project unavailable once its database deletion
   commits, and report cleanup as pending until owned files are gone.
3. Verify existing runtime shutdown, active-work exclusion, and commit checks
   prevent ingestion, indexing, or maintenance from recreating deleted state.
   Add guards only where a test demonstrates a gap.
4. Run end-to-end deletion and crash/retry tests, including repeated requests,
   cleanup failures, and restart recovery. Verify Archive remains reversible and
   new independent evidence can introduce the same facts later.

**Done when:** Deletion removes database content and owned files, interrupted
cleanup finishes after retry, and background work cannot restore removed state.

### What is now implemented

- `ProjectFilesystemFactory` owns one whole-project removal operation. It
  deletes only the direct project directory below the configured library root,
  never traverses a file or directory symlink, and rejects a symlinked library
  root.
- `ProjectDeletionWriter` writes a minimal `project_file_cleanup_tasks` row in
  the same transaction as the project deletion. The row deliberately has no
  project foreign key, so it survives the project cascade until the native
  directory is gone.
- `ProjectManager` removes the directory after the database commit, then clears
  the task. It returns `file_cleanup_status: complete` only after both steps;
  a filesystem or cleanup-ledger failure returns `pending`. A repeated Delete
  request and `ProjectManager.start()` both retry pending cleanup.
- Existing lifecycle control already closes the loaded runtime, stops its
  scheduler and document indexer, cancels project-owned background work, and
  rejects active session leases before the database deletion. Project session
  acquisition and maintenance require a surviving project row, so no extra
  recreation guard was needed.
- Real PostgreSQL plus filesystem tests cover a failed post-commit cleanup and
  restart recovery. Unit coverage verifies whole-directory ownership,
  symlink safety, repeated Delete retry, active-work exclusion, runtime
  shutdown ordering, Archive file retention, and later independent
  reintroduction of a deleted fact.

The shared-state, audit, and filesystem checks now pass. A Delete result is
fully complete only when its `file_cleanup_status` is `complete`; `pending`
means the project remains unavailable while its owned files await retry.

### Review follow-ups — 2026-09-16

- Group 2: invalidate affected entity IDs in every loaded resolver that could
  have read a surviving shared identity. A project that reads a surviving
  project can retain a deleted alias even if it never read the deleted project.
  For this single-user engine, invalidating those IDs in all loaded resolvers
  is simpler than discovering every indirect reader.
- Group 3: freeze the original absolute library location in the durable cleanup
  task. A retry currently uses the new configuration and can report complete
  while the original directory remains.
- Group 3: preserve shutdown failures across retries or retry unfinished phases.
  The existing runtime marks itself closed after a failed shutdown; a second
  Delete call can therefore skip shutdown and proceed. This predates these
  groups but prevents the claimed active-work exclusion guarantee.
- Fixed during review: reject `.` and `..` at the filesystem factory boundary;
  deletion must never target the library itself or its parent. Both cases now
  have regression coverage.
