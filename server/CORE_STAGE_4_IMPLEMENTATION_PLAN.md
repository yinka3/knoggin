# Core Stage 4 — Versioned Evidence and Editable Memory

Status: Chunk A complete 2026-09-11. Chunks B–D remain planned.

Baseline inspected: `aadedewe/refactor`, `bcb354e`, after completed Core Stages
1 and 2. Stage 4 may begin after Stage 3, but its document and Episode work is
architecturally independent of participation. Recheck current callers before
implementation.

## Objective

Make every versioned memory surface describe the bytes or narrative it actually
represents:

1. Document extraction/chunks cannot be published under a stale source hash.
2. A document encountered during an authorized Agent run remains persistable as
   historical provenance even if the document changes before answer commit.
3. Graph paths expose their real relationship-observation support.
4. Manual Episode edits update narrative and embedding atomically.

The shared idea is version truth: current indexes are tied to current source;
historical encounters retain the exact captured version; editable semantic
records update their retrieval representation in the same commit.

## Finding map

| Work | Review source | Stage 4 outcome |
| --- | --- | --- |
| Stale document index hash | Core F4 | Hash bytes read before extraction and refuse mismatched publication. |
| Changed document blocks answer commit | Provenance/Agent PA4 | Persist the authorized captured encounter as historical provenance. |
| Graph path loses evidence | Provenance/Agent PA3; Stage 1 graph note | Hydrate observation support through the existing bounded evidence service. |
| Episode edit leaves old vector | Core F11; locked Knowledge §25 | Generate first, then atomically CAS narrative/vector/user-edit state. |

## Scope boundaries

Included:

- Project document index claim/version validation.
- Existing `SourceReferenceCandidate` and `message_source_refs` encounter
  persistence.
- Relationship-observation evidence hydration for ordinary graph paths.
- Manual Episode narrative edit and embedding consistency.
- Deterministic unit tests plus real PostgreSQL transaction/version tests.

Deferred:

- Document extraction-library replacement remains a separate evaluation.
- SQLite migration remains separate; Stage 4 contracts become parity gates for
  it.
- Agent notebook rendering of the newly correct evidence is Stage 6.
- Project Forget source invalidation/scrubbing remains in the Forget plan.
- Automatic rewriting of historical Episode prose after current Knowledge
  correction remains out of scope.

## Required invariants

- Derived document text, chunks, and embeddings are labeled only with the hash
  of the exact bytes supplied to extraction.
- A hash mismatch is reconciliation work, not an indexing failure and never a
  successful index publication.
- A source candidate already captured by the server during an authorized run is
  historical evidence. Finalization rechecks answer scope and source ownership,
  but does not require that captured version to remain current.
- Graph path evidence preserves observation identity and expands only through
  bounded, project-scoped traversal.
- Episode summary fields, `user_modified`, embedding, and optimistic stale-edit
  check commit together or remain entirely unchanged.

## Chunk A — Document read-hash publication discipline

Compute SHA-256 immediately after `DocumentIndexer` reads filesystem bytes and
before extraction/model work. Compare it with the claimed catalog hash. On a
mismatch:

- publish no extraction, chunks, or embeddings;
- release/reconcile the stale index claim through the existing Project document
  reconciliation owner;
- leave the newly observed file version eligible for normal indexing.

Make `DocumentWriter.persist_indexed_chunks()` require the exact read hash and
revalidate it against the locked catalog row. Persist
`document_extractions.extracted_content_hash` from that verified read hash,
never by copying an unchecked row value.

Primary files:

| File | Planned change |
| --- | --- |
| `src/core/knowledge/documents/indexer.py` | Hash the bytes read, short-circuit stale claims, and invoke existing reconciliation. |
| `src/core/knowledge/db/writers/document_writer.py` | Require/read-hash equality at the transactional publication boundary. |
| `src/core/knowledge/documents/service.py` | Keep catalog reconciliation as the owner of changed-file version creation. |
| `tests/unit/core/knowledge/test_document_service.py` | Cover mismatch control flow without extraction/embedding. |
| `tests/contract/storage/test_document_writer_transaction_contract.py` | Prove stale/mid-inference versions cannot publish. |
| `tests/contract/storage/test_document_storage.py` | Assert extraction/chunk hash agreement. |

Acceptance:

- Changing bytes after queue/claim cannot produce text under the old hash.
- Changing the catalog while extraction runs still loses the writer CAS.
- Cancellation/retry behavior remains bounded and does not strand `indexing`.
- Stable bytes retain the existing successful indexing path.

Commit boundary: current document-index version correctness.

## Chunk B — Historical document encounters survive finalization

Keep capture-time authorization in the server-owned source-candidate path. At
answer finalization, validate:

- assistant message/run/session/project scope;
- `source_project_id` belongs to the run's captured readable Project scope;
- document ownership matches `source_project_id` when the catalog row remains;
- the candidate has a valid immutable hash, locator, excerpt, encounter kind,
  and idempotency identity.

Do not require the catalog's current hash/status to equal the captured hash.
The source reader already reports replaced/deleted material as historical or
unavailable. Preserve atomic answer/source/artifact persistence; do not swallow
invalid source writes.

Primary files:

| File | Planned change |
| --- | --- |
| `src/core/knowledge/db/writers/source_reference_writer.py` | Separate captured encounter validity from current-version status. |
| `src/core/knowledge/db/readers/source_reference_reader.py` | Verify current/historical/unavailable presentation remains accurate. |
| `src/core/knowledge/store.py` | Preserve the single answer/source transaction. |
| `src/runtime/session_runtime.py` | Retain captured readable scope and retry/idempotency behavior. |
| `tests/contract/storage/test_source_reference_storage_contract.py` | Add replace/delete-between-capture-and-finalize cases. |
| `tests/contract/storage/test_knowledge_store_source_reference_transaction.py` | Prove answer commits with valid historical encounter and rolls back on invalid scope. |

Acceptance:

- Authorized version A can be saved after the document becomes version B.
- The saved source reports historical/unavailable status without pretending B
  was consulted.
- Cross-project or fabricated candidates still fail atomically.
- Finalization retries do not duplicate answer or source rows.

Commit boundary: answer-linked historical source persistence.

## Chunk C — Observation-aware graph evidence hydration

Stop passing graph `observation_id` references through the message-ID
normalizer. Add an explicit evidence-ref branch in `KnowledgeRetrieval`:

- message references continue through message hydration;
- relationship-observation references call the existing bounded
  `KnowledgeStore.get_relationship_observations_evidence()` service;
- rendered results retain observation identity, active/historical status,
  Context blocks, messages, and external sources within existing traversal
  limits.

Do not reinterpret an observation ID as a message ID and do not add a duplicate
evidence graph.

Primary files:

| File | Planned change |
| --- | --- |
| `src/core/knowledge/retrieval.py` | Dispatch evidence references by kind and retain observation handles. |
| `src/core/knowledge/db/readers/graph_reader.py` | Keep active observation refs explicit and typed enough for hydration. |
| `src/core/knowledge/evidence_service.py` | Reuse its bounded traversal; edit only if a missing projection is demonstrated. |
| `src/core/knowledge/store.py` | Reuse existing evidence facade operations. |
| `tests/unit/core/agent/test_graph_retrieval_contract.py` | Assert path results include inspectable observation support. |
| `tests/unit/core/knowledge/test_evidence_service.py` | Preserve traversal scope/limit/status contracts. |
| `tests/contract/storage/test_graph_reader_contract.py` | Cover real active observation references. |

Acceptance:

- A supported path never becomes `evidence=[]` because of reference-shape
  mismatch.
- Retired observations stay excluded from ordinary current paths.
- Missing support is explicit; it is not silently converted into a message.
- Traversal limits and readable-project scope remain enforced.

Commit boundary: Knowledge retrieval evidence shape. Stage 6 later renders it
to the model.

## Chunk D — Atomic manual Episode narrative and vector edit

Use the existing canonical Episode embedding text builder. The Knowledge facade
should generate and validate the replacement vector before opening the writer
transaction. The writer then updates all narrative fields, embedding,
`user_modified`, and `updated_at` under a required expected revision/timestamp
CAS.

Knoggin has no released caller for this method, so strengthen the contract
directly instead of preserving the loose signature.

Primary files:

| File | Planned change |
| --- | --- |
| `src/core/knowledge/episodes/embedding.py` | Remain the one canonical text builder for create/rebuild/edit. |
| `src/core/knowledge/store.py` | Own pre-transaction embedding generation and validation. |
| `src/core/knowledge/db/writers/episode_writer.py` | Atomically CAS narrative/vector/user-modified state. |
| `src/core/knowledge/db/readers/episode_reader.py` | Supply the current edit token if the facade needs it. |
| `tests/unit/core/knowledge/episodes/test_episode_embedding.py` | Prove create/edit/rebuild input parity. |
| `tests/contract/storage/test_episode_writer_contract.py` | Cover successful edit, stale edit, and rollback on vector/write failure. |

Acceptance:

- Retrieval vector reflects the edited narrative immediately after commit.
- Embedding failure performs no database write.
- Late SQL failure preserves the prior narrative/vector pair.
- A stale edit cannot overwrite a newer user edit.
- Automatic Episode generation still refuses to replace `user_modified`
  Episodes.

Commit boundary: editable Episode consistency.

## Combined Stage 4 scenarios

Use deterministic embeddings/extraction and real PostgreSQL:

1. Claim document version A, replace filesystem bytes with B, and prove A's
   queued index publishes nothing before reconciliation indexes B correctly.
2. Capture an authorized passage from A, change/delete the document before
   answer finalization, and prove the answer plus A encounter commit atomically
   with historical/unavailable presentation.
3. Create a Context-backed relationship, read a graph path, and prove the
   returned observation expands to its block/message/source support.
4. Edit an Episode, inject both embedding and SQL failures, then prove only the
   successful attempt changes narrative and semantic retrieval.

These may be two focused PostgreSQL files if combining document and graph/
Episode setup obscures the invariants.

## Validation and closeout

Run focused files per chunk, then the document, provenance, graph, Episode, and
answer-finalization suites listed above. Retain Stage 1/2 semantic integration
gates because document/source changes must not alter Context ownership.

Also run touched-path Ruff, compileall, architecture checks when imports move,
the configured narrow mypy scope after its stale path is repaired separately,
and `git diff --check`. No live model or external network call is required.

Record exact commands/results, changed files, limitations, and local commit IDs.
Retire F4/F11/PA3/PA4 probes only after normal desired-behavior regressions
exist. Append status to the core, Knowledge, and provenance/Agent reviews.

- [x] A: exact-byte document index publication.
- [ ] B: historical encounter persistence across document changes.
- [ ] C: observation-aware graph evidence hydration.
- [ ] D: atomic Episode narrative/vector edits.
- [ ] Combined real-PostgreSQL scenarios pass.
- [ ] Review/probe/operations closeout is recorded and committed locally.

Chunk A validation on 2026-09-11:

- `uv run pytest -q tests/unit/core/knowledge/test_document_service.py tests/contract/storage/test_document_writer_transaction_contract.py tests/contract/storage/test_document_storage.py tests/contract/storage/test_document_format_indexing.py tests/contract/storage/test_document_reader_scope_contract.py tests/integration/ingestion/test_document_format_runtime.py tests/integration/test_workspace_health_flow.py` → 106 passed; the environment emitted its existing Requests dependency warning.
- Focused Ruff and compileall passed. The initial sandboxed Ruff command could not open the shared `uv` cache; the approved rerun passed. `git diff --check` passed.

The new normal regressions cover stale bytes before extraction, a catalog
replacement during derivation, and the real-PostgreSQL publication boundary.
No historical probe is retired yet: F4 remains represented by its normal
regressions and the other Stage 4 review probes belong to later chunks.

Next implementation task: Stage 4 Chunk B.
