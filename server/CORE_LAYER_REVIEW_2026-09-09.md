# Knoggin core-layer review — 2026-09-09

## Scope and verdict

Reviewed the checked-out working tree at `acc0cac` (current branch: `aadedewe/refactor`), using `CODE_REVIEW_GUIDE.md` and the locked Ingestion/Knowledge reviews as navigation and decision records, not proof. Primary scope: `server/src/core/ingestion`, `server/src/core/knowledge`, and their directly required schema/storage/runtime bridges. No README, SDK, frontend, or broad Project/Session lifecycle review. The existing modification to `server/PROJECT_SEMANTIC_OPERATIONS.md` was left untouched.

**Keep the architecture. Fix the transitions between its representations.** Whole-exchange admission, immutable Context, PostgreSQL-owned checkpoints, and source-backed episodes justify their complexity for one local user. The important problems are identity reuse, distinguishing new evidence from reprocessing, and keeping retrieved content consistent with its version and current status. A replacement memory architecture would not resolve these automatically.

This is a targeted core review, not certification of every server path. Model output was controlled in reproductions; no claim is made about real-model extraction accuracy, latency, or user experience.

Priority: **P1** = core correctness or a semantic queue blocked by ordinary supported input; **P2** = material consistency/retrieval defect or incomplete intended operation. New findings are distinguished from already locked work.

## Newly established findings

### F1 — P1 / Fix: a no-change Context window reuses an earlier impact set

**Trigger:** A completed window created Context revision R and extracted a relationship from block B. A later conversation produces no Context edits.

**Trace:** [ProjectSemanticJob](src/core/ingestion/project_semantic_job.py) lines 432–435 reuses the prior snapshot. Its Knowledge stage, lines 512–540, then reads R's original revision impact set and runs extraction again. [SemanticCommitWriter](src/core/knowledge/db/writers/semantic_commit_writer.py) lines 509–551 identifies observations partly by the new `semantic_window_id`. Retirement only removes observations with noncurrent supporting blocks; B remains current.

**Result:** The same supporting block can acquire another active observation under a different window. This adds model/graph work without new knowledge and can inflate observation counts, apparent strength, recency, and maintenance signals. Different model output on a later pass can also produce different interpretations of unchanged evidence.

**Evidence:** Existing no-op unit coverage explicitly expects revision reuse, but stops before downstream reconciliation. A review probe follows the no-op checkpoint and Knowledge-stage impact selection. A real PostgreSQL probe commits two windows referencing the same revision: **two active observations, two window IDs, one support block**. This is cross-window replay, not the already tested same-window retry case.

**Repair:** Give an unchanged window an explicitly empty reconciliation impact, or checkpoint it without re-extracting already reconciled blocks. Keep the unchanged Context revision if useful; separate its historical impact from this window's work. Preserve episode completion and real human-edit reconciliation.

### F2 — P1 / Finish: an identity reused from a linked project has no local classification write

**Trigger:** Project A can read B. An identity exists only in B, is visible/cached in A's resolver, and is confidently matched in A's new Context.

**Trace:** [EntityResolver](src/core/knowledge/entity/resolver.py) lines 377–462 accepts readable existing identities, but stages entity writes only for newly allocated identities. [SemanticCommitWriter](src/core/knowledge/db/writers/semantic_commit_writer.py) lines 225–283 creates Project entity contexts only for those pending new entities. The Context association trigger in [schema.sql](src/infrastructure/schema.sql), lines 104–137, requires local membership; relationship validation independently requires it too.

**Result:** A valid cross-project identity match can fail the Knowledge commit and repeatedly block that frozen window. Simply allowing foreign associations through the trigger would weaken the intended project-local interpretation model.

**Evidence:** A controlled resolver probe accepts a warmed, readable full-name identity and produces no pending identity writes. A real PostgreSQL probe with the corresponding existing-identity build fails with `CheckViolation: context block entity must be visible to its project scope`, wrapped as `StorageWriteError`; the transaction rolls back. The first probe uses a full name to satisfy the actual conservative matching policy, not a forced acceptance mock.

**Repair:** Stage local classification/membership for reused identities separately from creation of global identity. Commit it before block associations and relationships. Relationship candidate types should use that intended local classification, not blindly inherit another project's type.

### F3 — P2 / Fix: cold-cache resolution can miss an exact durable alias

**Trigger:** After restart or eviction, the database contains an exact alias such as `IBM`; the canonical-name embedding does not meet the vector threshold for the alias query.

**Trace:** [EntityResolver.get_candidate_ids](src/core/knowledge/entity/resolver.py) lines 983–1086 collects exact/fuzzy candidates only from the local index, then queries durable vectors. It never performs the durable name/alias lookup available elsewhere in the resolver/store. Runtime construction initially verifies the user identity; it does not populate every durable alias.

**Result:** Matching depends on cache history. In ingestion, an empty candidate list can allocate another identity even though the exact durable alias exists. Fixing post-commit publication alone does not fix restart/eviction behavior.

**Evidence:** A controlled store/embedding probe verifies that durable exact lookup finds `IBM`, then observes `get_candidate_ids(..., strict=True)` returning no candidates and making no durable name lookup when vector candidates are empty. This demonstrates the lookup gap, not an assertion that a particular embedding model will always miss `IBM`.

**Repair:** Include bounded durable exact name/alias candidates on a cold lookup, retaining all ambiguous owners and existing compatibility checks. Do not require loading the full entity catalog at startup.

### F4 — P2 / Fix: indexing can label changed file bytes with an old version hash

**Trigger:** A file is cataloged, then edited locally before queued indexing reads it; reconciliation has not yet updated the catalog.

**Trace:** [DocumentIndexer](src/core/knowledge/documents/indexer.py) lines 133–157 reads current filesystem bytes but passes the claimed catalog hash to persistence without hashing those bytes. [DocumentWriter](src/core/knowledge/db/writers/document_writer.py) lines 334–383 compares the expected hash with the catalog row and stores extraction under that catalog hash. Both hashes can agree while describing different bytes from those read.

**Result:** New extracted text/chunks can be marked indexed under the old source version. Later scanning may repair the catalog, but an answer or selected passage can encounter the incorrect version association in the meantime.

**Evidence:** Real filesystem + PostgreSQL + production document service/indexer/writer, with deterministic embedding output: catalog `Launch is Friday.`, replace the file with `Launch is Monday.`, index. The persisted extraction contains Monday while both metadata and `extracted_content_hash` retain Friday's SHA-256.

**Repair:** Verify the hash of the bytes actually read against the claim before extraction/publication. A mismatch should return the document to the catalog/reconciliation path without publishing mislabeled derived data. Keep the existing commit-time catalog check to handle changes discovered while inference runs. Apply the same version discipline to direct byte reads returning versioned source metadata.

## Confirmed findings already represented in locked reviews

These remain live work; they are not new discoveries.

### F5 — P1 / Fix: names still act as identity in two extraction stages

[EntityResolver.mention_dedupe_key](src/core/knowledge/entity/resolver.py) lines 144–154 discards type/topic/policy. `resolve_context_block_mentions` reuses a newly allocated ID by that key without reconsidering compatibility. Different typed same-name mentions can collapse.

Independently, [ContextRelationshipExtractor._candidates](src/core/ingestion/relationship_extractor.py) lines 95–104 throws when different candidate IDs share a canonical name. Valid homonyms can therefore block a window rather than merely reduce extraction quality.

**Repair:** Evaluate pending identities as candidates and use local entity handles for relationship output. A `(name, type, topic)` key is a partial guard, not a general solution for two people sharing a name. **Evidence:** source and callers; locked Ingestion §§13–15. No new real-model homonym benchmark was run.

### F6 — P2 / Finish: successful semantic writes are not published to the resolver

[ProjectSemanticJob](src/core/ingestion/project_semantic_job.py) lines 538–550 returns after durable commit. The store is a delegating writer call, not a cache publisher. `apply_committed_entity_writes` and `commit_new_aliases` exist on the resolver but have no production callers.

**Impact:** Known-alias matching and exact/fuzzy resolution continue using stale indexes until another read happens to hydrate the entity. Durable vector lookup can rescue some matches; duplicates are not inevitable on every next window.

**Repair:** One post-commit resolver publication operation covering new identities and aliases, with recoverable cache failure behavior. Address F3 as well. **Evidence:** source/caller search; locked Ingestion §17.

### F7 — P2 / Fix: current retrieval includes retired observations

[EntityReader](src/core/knowledge/db/readers/entity_reader.py) lines 831–897 aggregates observations without an active-only predicate. [GraphReader](src/core/knowledge/db/readers/graph_reader.py) lines 107–142 includes retired path-evidence references. [KnowledgeQueryReader](src/core/knowledge/db/readers/knowledge_query_reader.py) lines 159–173 attaches retired observations to recent message activity.

**Impact:** A relationship with remaining active support can still report old support, inflated counts, and outdated context. Explicit historical evidence traversal should retain retired records; ordinary current retrieval should not.

**Evidence:** SQL inspection plus a real PostgreSQL path-reference probe: after retiring one of two observations, current path evidence still returns both. `RelationshipObservationReader.get_active_context_supports` already filters correctly; this is inconsistent coverage, not absence of a retirement model. **Repair:** Active-only predicates on current reads, with explicit historical status preserved in audit/evidence APIs. Locked Knowledge §9.

### F8 — P2 / Fix: merge and cleanup omit Context/entity associations

[GlobalEntityMergeWriter](src/core/knowledge/db/writers/global_entity_merge_writer.py) snapshots, migrates, and journals several identity uses but never `context_block_entities`. [GraphWriter cleanup](src/core/knowledge/db/writers/graph_writer.py) lines 314–343 likewise removes message/episode/local identity uses without removing block associations.

**Impact:** Redirect entities retain stale block links after merge. If a cleaned identity survives in another project, its block associations can survive in the cleaned project. Episode enrichment consumes these associations, so this is more than unused residue.

**Repair:** Include associations in merge state hashes, migration, deduplication, inverse journal, cleanup, and preview counts. **Evidence:** writer/reader/caller inspection; existing maintenance storage tests pass but do not establish this newer table's coverage. Locked Knowledge §§11–12.

### F9 — P2 / Fix: event time is lost at semantic commit

[SemanticCommitWriter](src/core/knowledge/db/writers/semantic_commit_writer.py) lines 263–283 does not populate/advance `last_mentioned_ms`; lines 470 and 547 use current processing time for observation time. The upstream Context model already carries source time.

**Impact:** Delayed processing looks like recent activity, and entity recency remains missing/stale. **Repair:** Derive timestamps from cited Context blocks, advance entity time monotonically, and implement the accepted-time fallback for untimed human edits. Keep storage timestamps separate. **Evidence:** source assignments and downstream ordering queries; locked Knowledge §§7,13–14.

### F10 — P2 / Fix + simplify: entity embedding input changes during rebuild

[EntityResolver.prepare_pending_entity](src/core/knowledge/entity/resolver.py) lines 1153–1155 embeds name plus Project type. [EmbeddingRebuilder](src/core/knowledge/db/embedding_rebuilder.py) lines 85–89 calls the same helper with `None`, yielding `name (unknown)` instead.

**Impact:** Rebuilding changes the identity representation even with no identity change; initial embeddings also depend on whichever project created the identity. **Repair:** One identity-only embedding input shared by creation/rebuild, and remove classification-only rebuild requirements. **Evidence:** exact input construction; locked Knowledge §15.

### F11 — P2 / Finish: manual episode editing does not update its vector

[EpisodeWriter.edit_episode](src/core/knowledge/db/writers/episode_writer.py) lines 315–363 changes narrative fields and `user_modified`, leaving embedding unchanged. The store adds no embedding step. No production caller outside that facade was found in `server/src`.

**Impact:** When this intended operation is exposed, semantic retrieval can still rank the old narrative. This is unfinished feature wiring, not evidence of an already shipped user-facing edit failure.

**Repair:** Generate the replacement vector before opening the write transaction, then persist narrative/vector/user-edit state together with appropriate stale-edit protection. **Evidence:** source/caller search; locked Knowledge §25.

### F12 — P2 / Fix: maintenance quiescence ignores message-free semantic work

[EntityMaintenanceService.capture_frontier/revalidate_frontier](src/core/knowledge/entity/maintenance_service.py) lines 646–779 derives pending work from user messages and their semantic membership. A human Context edit creates an active reconciliation window with no message membership.

**Impact:** A project with otherwise completed conversations can appear quiescent while that reconciliation remains active. **Repair:** Check active semantic windows of every origin as well as eligible unclaimed exchanges. Incorporate the already planned participation/frontier rules when that feature is wired. **Evidence:** both SQL queries and the human-edit importer; locked Knowledge §§20–21. A concurrent merge/semantic-commit interleaving was not reproduced in this review.

## Improvements within the existing scope

### I1 — Delete: remove the unused transcript resolver path

`EntityResolver.resolve_mentions` and `candidate_entries_for_mentions` have no production callers; the first only calls the second. Current ingestion calls `resolve_context_block_mentions`. `register_entity` and `compute_embedding` also have only test callers. Remove genuinely retired entry points and tests that only preserve them; retain shared candidate/identity logic and the needed post-commit publication primitives.

Do not build a generic dual-pipeline abstraction to support a retired path. This reduces the places where identity fixes must be understood. Caller search is server-local; the unstarted SDK was intentionally not treated as a legacy compatibility obligation.

### I2 — Delete/simplify: remove inert policy knobs

`llm_ner` is stored and serialized but selects no current extraction branch. `EpisodeGenerationPolicy.target_message_count` is carried through hashes/metadata even though semantic-window admission owns selection. The locked Ingestion review already identifies both. Keep actual source/narrative safety limits; remove configuration that suggests behavior users cannot obtain.

### I3 — Finish: expose why an episode was omitted

`ProjectEpisodeBuild.apply_llm_output` and `create_episodes` intentionally drop over-cap proposals and still permit the window to complete. Raw messages survive and Context still receives the frozen evidence, so this is not lost conversation data.

However, a valid zero-memory decision and a capacity-rejected proposal are hard to distinguish from the durable result alone. Record a bounded outcome/reason count at the existing stage/trace boundary. Consider one bounded regrouping attempt for a splittable over-cap proposal. Do not add a second conversation-window scheduler or an unbounded retry loop.

### I4 — Improve: preserve meaning and origin in existing model-facing material

Explicitly preserve tentative versus decided, user statement versus assistant suggestion, and historical versus current wording in episode/Context prompts. For cross-project episode retrieval, include a compact owning-project label/identifier: current serialization omits an explicit project field even though searches span readable projects.

These are modest prompt/contract improvements, not proof that model judgments become reliable. Evaluate them with held examples; provenance validation cannot itself establish entailment or decision status. Do not introduce numeric confidence as a substitute for those distinctions.

## Keep, and concerns not promoted to bugs

- Keep whole-exchange admission and durable claim revalidation: the read/claim separation is justified by local asynchronous work.
- Keep durable stage checkpoints and SQL+AGE atomic Knowledge publication. Existing real storage tests cover replay/retraction/failure behavior; F1 is a distinct cross-window case.
- Keep Context source validation, immutable revisions, and timestamp rejection for older replacement evidence. They enforce useful structural guarantees without pretending to verify semantic truth.
- Keep episode consolidation's source-message reload and final capacity checks. An apparent capacity-bypass concern was ruled out by `create_episodes` revalidation.
- Keep the scheduler cadence: it synchronizes human Context edits even when `should_run` finds no due conversation. An idle-sync concern was ruled out by the caller and its test.
- Keep `KnowledgeStore` as the facade, focused storage writers, and `SemanticWindowBuild` as the temporary aggregate. Size alone is not a reason to split them.
- Keep the narrow document-read adapter: its purpose is constraining AAC capabilities while sharing the real document implementation.
- Do not introduce distributed ownership, extra managers, new memory engines, or a generic repair framework.

**Qualification of locked Ingestion §8:** The claimed domain/policy interleaving is not established for the current admission call sequence. `await capture_domain()` returns within the same task, immediately followed by synchronous `capture_ingestion_policy()`; there is no suspension point between them, and normal domain installation is event-loop-owned. A unified capture may still simplify invariants, especially across the human-file importer, which does await work before capturing policy. Do not cite the illustrated admission race as a reproduced bug without a concrete alternate mutation path.

**Remaining validation gate:** The locked global-merge/semantic-commit concurrency concern deserves a dedicated interleaving test before choosing additional locks. Existing row locks and FK locks already order some cases; the missing human-window frontier and missing association migration are independently actionable now.

## Validation and reproducibility

- Existing focused unit coverage: **415 passed, 2 deselected** across `tests/unit/core/ingestion`, `tests/unit/core/knowledge`, and `tests/unit/knowledge`, excluding model/slow/integration markers.
- Existing semantic storage coverage: **18 passed** across `test_semantic_commit_contract.py`, `test_project_context_window_contract.py`, and `test_project_semantic_postgres_flow.py`.
- Additional existing storage coverage: **13 passed** across graph reader, episode writer, document transaction, and maintenance application contracts.
- **446 existing tests passed in total.** This is selected coverage, not a full-server suite claim.
- The historical review-probe file
  `reviews/core_review_probes_2026_09_09.py` retains unresolved reproductions.
  Its F1 and F7 cases are now skipped historical records; normal regression
  tests cover the desired behavior for those implemented findings.
- Temporary probe mistakes were corrected without editing production code or existing tests: the linked-entity database exception is a wrapped trigger `CheckViolation`, and the resolver acceptance example needs a properly formed mention and a name satisfying its actual conservative policy.
- No model inference or external provider calls were used. PostgreSQL/AGE checks used the existing fixture's fresh temporary databases and cleanup, not application data.
- `.venv/bin/python` points at a missing snap-259 interpreter. An installed Python 3.12.12 under snap-262 was used with the existing `.venv` site-packages; no environment files or dependencies were changed. Sandbox socket access was initially blocked; approved local-service access allowed the database checks.

Run the probes from `server/` with a working server Python environment:

```bash
python -m pytest reviews/core_review_probes_2026_09_09.py -c pyproject.toml -q
```

They import existing test helpers and require the same test PostgreSQL/AGE/pgvector setup as the storage contract suite. The filename is intentionally outside normal `test_*.py` discovery.

## Stage 1 implementation status — 2026-09-10

- F1 is implemented by `98414ff`: a no-op Context reuse has empty
  window-specific impact and cannot replay an earlier revision's work.
- F7 is implemented by `71112d0`: current retrieval is active-only while
  historical evidence remains explicit and reachable.
- F9 is implemented by `06d04ec`: semantic publication uses persisted Context
  event time for relationship observations and monotonic entity activity.
- `e344e24` adds the combined correction → no-op → restart PostgreSQL
  regression in
  `tests/integration/ingestion/test_project_semantic_postgres_flow.py`.

At the end of Stage 1, all other findings and recommended work remained open.

## Stage 2 implementation status — 2026-09-11

The following findings are resolved within the evidence boundary stated in this
review. The original findings remain above as historical rationale.

- **F2:** `4eac23e` stages a receiving project's explicit classification for a
  readable foreign identity in the same atomic Knowledge commit, before its
  Context associations and relationships. The normal storage contract is
  `test_semantic_commit_admits_a_currently_readable_foreign_identity`.
- **F3:** `a0b49af` hydrates scoped durable exact canonical-name and alias
  candidates before vector fallback. `beb68b6` additionally prevents an
  unrelated shared fuzzy alias from invalidating a unique direct exact match.
  The normal regression is
  `test_cold_resolver_hydrates_a_durable_exact_alias`.
- **F5:** `4eac23e` evaluates pending identities as compatibility candidates,
  and `343e3a5` gives VP-02 opaque local entity handles instead of canonical
  names. This does not claim semantic selection between otherwise
  indistinguishable same-name, same-type identities; that ambiguous case still
  does not reuse an identity automatically.
- **F6:** `84d3eb0` publishes only committed entity/alias state to the live
  resolver and treats a publication failure as a recoverable finalization
  checkpoint, without replaying the canonical Knowledge mutation.
- **F10:** `52cbacb` derives global entity embeddings from normalized canonical
  identity only, so project-local reclassification does not change or rebuild
  the identity vector.
- `beb68b6` adds the combined real PostgreSQL/AGE regression
  `test_project_semantic_job_composes_real_resolution_extraction_and_recovery`,
  covering F2/F3/F5/F6/F10 together with Stage 1 history/time behavior.

## Recommended implementation order

1. F8 and F12: complete maintenance participation in Context-first state, then validate concurrency ordering.
2. F4 and F11: make document version publication consistent and complete Episode editing before exposing it.
3. I1–I4: remove inert paths/settings and improve bounded diagnostics/model context.

These are repairs and simplifications of the existing engine. No implementation changes were made during this review; only the report and standalone reproduction artifact were added.
