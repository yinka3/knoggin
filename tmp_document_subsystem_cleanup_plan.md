# Document Subsystem Cleanup Plan

## Goal

Make document ownership and behavior easier to understand without weakening the
durable indexing, immutable parse snapshots, provenance, hybrid search, or local
filesystem safety that already work well.

This file is a temporary implementation checklist. Each numbered unit should be
implemented, verified, and committed separately.

## Areas reviewed

- `core/knowledge/documents/service.py`
- `core/knowledge/documents/indexer.py`
- `core/knowledge/documents/storage.py`
- `core/knowledge/documents/filesystem.py`
- `core/knowledge/documents/scanning.py`
- `core/knowledge/documents/read_service.py`
- document database readers and writers
- project runtime/factory ownership
- agent document-search and workspace tools
- application API, Python SDK, and UI-facing client
- document, workspace, indexing, storage, focus, and health tests

## Keep intact

- `DocumentIndexer` as the owner of durable claims, retries, recovery, bounded
  admission, cancellation repair, and atomic index publication.
- Immutable parse snapshots and source-hash verification before publication.
- Format-aware extraction/chunking and exact provenance locators.
- `ProjectFilesystem` path, symlink, atomic-write, and stale-write protections.
- `DocumentReadService` as the narrow read-only capability used by community
  composition.
- Document-specific hybrid retrieval and reranking.

## Journal findings to resolve

1. Decide whether a document ID represents a stable project path or one exact
   byte version. Reconciliation currently tombstones and replaces the document
   ID after an external edit, while explicit reindexing preserves the ID and
   advances its immutable snapshot.
2. Remove the unused `recovery_interval_seconds` and `recovery_batch_size`
   settings unless a real runtime need for tuning them appears.
3. Stop rereading every eligible project file every reconciliation interval when
   unchanged files can first be rejected using trustworthy filesystem metadata.
   Keep a correctness-preserving safety sweep for external changes.
4. Rename `documents/storage.py` around its real extraction/parsing/chunking
   responsibility. Do not split formats into unnecessary classes.
5. Either expose or remove the backend-only folder preview/import and persisted
   scan-settings workflow.
6. Either expose or remove saved-link list/update/delete management; only source
   promotion currently reaches bookmark creation.
7. Remove the unused `admit_user_sources()` batch wrapper unless a concrete
   caller is introduced.
8. Make the capture-time lifecycle of document settings explicit. Add live
   mutation only if the product promises it.

## Additional findings from associated areas

### Workspace listing performs an unbounded content scan

`list_project_files()` calls `filesystem.iter_files()` without a limit and only
slices afterward. `iter_files()` reads and hashes every file. A request for ten
results can therefore read the entire project, and the agent-facing list tool can
trigger this behavior repeatedly.

The result limit must be enforced during traversal. If hashes are required in
the response, only the bounded result set should be read.

### Knoggin-owned writes trigger project-wide reconciliation

Create, update, append, move, and delete each mutate one known path and then run
the full reconciliation algorithm. This repeats the journal's periodic-I/O cost
on interactive workspace operations even though the changed path and bytes are
already known.

Add a narrow catalog update path for Knoggin-owned mutations. Retain periodic
reconciliation for recovery and external editor changes.

### Reconciliation catalog changes are not one database unit

The reconciliation loop tombstones and inserts rows through separate writer
calls. If a later operation fails, earlier changes remain committed and the
catalog is only repaired by a future pass. This is especially awkward when a
changed path is deleted before its replacement row is inserted.

Add one writer operation that applies a bounded reconciliation plan in a single
transaction. Filesystem and database state cannot be globally atomic, so the
periodic repair pass remains the recovery mechanism.

### The normal document product surface is incomplete

Agent tools expose document list/info/read/search and workspace file edits. The
HTTP API and Python SDK expose document focus and source promotion, but no normal
document upload, catalog listing, document read, reindex, or deletion boundary.
This is broader than the journal's folder-import and saved-link notes.

Before adding endpoints, decide which client owns document management. Expose
one coherent minimal surface if external clients are intended to own it;
otherwise explicitly keep document management agent/internal-only and remove
backend feature islands that assume a future client.

## Decisions required before implementation

1. Prefer stable document identity for a stable project-relative path. External
   content changes should queue a new snapshot on the existing document ID;
   moves may remain delete-plus-create unless path-independent identity is a
   product requirement.
2. Prefer deleting the two unused recovery settings.
3. Prefer direct, path-local catalog updates after Knoggin-owned file mutations,
   with periodic reconciliation as repair rather than the normal write path.
4. Prefer removing currently unreachable folder-import, scan-setting mutation,
   saved-link management, and batch-admission APIs unless a near-term client is
   confirmed before their unit begins.
5. Treat document configuration as runtime-capture settings and document that a
   project runtime rebuild is required for changes.
6. Decide the external document-management surface before changing API/SDK code.

## Implementation units

### Unit 1: Lock identity and reconciliation contracts

- Add tests defining stable document ID behavior for same-path content changes.
- Add or adjust writer support for updating source metadata and requeueing the
  existing document while retaining old snapshots.
- Apply a complete reconciliation plan transactionally.
- Preserve tombstones and historical provenance for truly removed paths.
- Commit this unit independently.

### Unit 2: Bound filesystem discovery and avoid repeated reads

- Enforce list limits during filesystem traversal, before file reads/hashing.
- Introduce a cheap metadata comparison before hashing during reconciliation.
- Keep a bounded correctness sweep so same-size or timestamp-preserving external
  edits are eventually detected.
- Add tests proving bounded reads and detection of external changes.
- Commit this unit independently.

### Unit 3: Use path-local updates for owned workspace mutations

- Update catalog state directly after create/update/append/move/delete.
- Define recovery behavior when the filesystem mutation succeeds but catalog
  persistence fails.
- Avoid a whole-project rescan on the success path.
- Add focused workspace/catalog consistency tests.
- Commit this unit independently.

### Unit 4: Remove stale configuration and lifecycle wrappers

- Remove unused recovery settings from schemas, YAML, examples, and tests.
- Remove facade forwarding methods that have no caller and do not form a useful
  public capability.
- Document capture-time settings behavior near runtime construction.
- Commit this unit independently.

### Unit 5: Clarify extraction module ownership

- Rename `storage.py` to an extraction/parsing-oriented name.
- Update imports and tests without changing extraction or chunking behavior.
- Keep the module cohesive unless a concrete dependency boundary justifies a
  smaller split.
- Commit this unit independently.

### Unit 6: Resolve unreachable product features

- Recheck production callers immediately before removal.
- Remove or deliberately expose folder preview/import and scan-setting mutation.
- Remove or deliberately expose saved-link management.
- Remove `admit_user_sources()` if it is still unused.
- Keep scan policy used by reconciliation and single-source admission used by
  source promotion.
- Commit each independently meaningful feature decision separately.

### Unit 7: Resolve the client-facing document boundary

- Decide whether API/SDK clients manage documents or only focus/promote sources.
- If exposed, add one consistent port, HTTP, SDK, and contract-test surface for
  the chosen minimal operations.
- If internal-only, document that boundary and avoid unused public-looking
  service methods.
- Commit this unit independently.

### Unit 8: Final subsystem audit

- Search for stale settings, old module imports, dead wrappers, and feature-island
  callers.
- Run focused unit/contract tests, then the non-Postgres and Postgres suites used
  by CI.
- Confirm health projections still report durable indexing state correctly.
- Record final decisions and remove this temporary plan only after all units are
  complete.
- Commit the final audit independently.

## Non-goals

- Replacing durable indexing with in-memory-only tasks.
- Deleting immutable snapshots or historical provenance.
- Replacing format-aware extraction with generic text splitting.
- Weakening path, symlink, size, content-hash, or project-scope checks.
- Combining document retrieval with unrelated message or episode retrieval.
- Creating a separate manager class for every document feature.

## Verification baseline

- Focused document service, indexer, extraction, folder, source-admission,
  workspace-tool, focus, and runtime composition tests.
- Storage contract tests for document readers/writers and snapshot publication.
- Integration tests for format indexing and workspace health.
- Full non-Postgres suite.
- Postgres-marked suite and coverage command used by CI.
- `git status --short` before every commit so unrelated user files and deleted
  temporary plans are not staged.
