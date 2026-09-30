# KnowledgeStore cleanup plan

Status: source inspection complete; implementation pending.
Baseline: `8502179` (completed tool-runtime audit).
Journal reference: section 11, plus the SDK ownership amendment in section 13.2.

## Scope and rules

Keep one internal durable persistence facade. Keep focused readers/writers, atomic assistant finalization, semantic checkpoints/recovery, and eager construction of lightweight components. Do not introduce repository interfaces or export storage methods directly through the SDK.

No caller alone is not grounds for deletion: preserve real product capabilities and identify their application owner first. Commit each completed implementation unit separately.

## Current source findings

- Health SQL has already moved into `SemanticWindowReader.get_health_summary()`. The live store forwarding method must stay; this journal task is already complete.
- Duplicate Context wrappers still exist, although `ContextProjection` directly owns its reader/writer.
- Direct alias mutation and the old merge-evidence helper still exist, with no production callers found in server source or SDK. Underlying methods still have storage tests; replace obsolete coverage with coverage of the current commit/maintenance routes where necessary.
- Old combined source reads are used by integration/storage tests, but no production consumer was found. Migrate tests to the current scoped source contracts before removing these helpers.
- Generic entity/message browsing has no production caller found, but remains a potential SDK capability. Retain it pending an intentional public contract.
- Episode editing still has meaningful storage behavior and tests, but no application/API/SDK wiring found. Preserve it and record public wiring as a separate follow-up.

## Additional work the journal missed

1. `finalize_assistant_exchange()` still contains replay-source SQL in the facade. Inspect moving only that lookup to `SourceReferenceReader`, accepting the existing transaction cursor. Preserve transaction locking, deterministic reference order, and idempotent replay; do not split the finalization transaction.
2. `test_knowledge_store_message_facade.py` passes `before_message_id` positionally although the store declares it keyword-only. Correct this stale test call and run the focused test to establish a useful baseline.
3. Removing wrappers can remove facade-only dependencies such as `ProjectContextWriter` or `RelationshipObservationReader`. Check every remaining use before dropping constructor fields/imports; keep underlying components used by other owners.

## Implementation units

### Unit 1: remove superseded Context and diagnostic wrappers

- [ ] Recheck exact and reflective callers before each deletion.
- [ ] Remove `ensure_project_context()` and `record_project_context_projection()` from the store; retain ContextProjection's live writer operations.
- [ ] Review `get_project_semantic_window()`, `get_project_semantic_window_messages()`, and `get_active_context_relationship_supports()` against current Maintenance/Health ownership.
- [ ] Review the unused single-observation evidence wrapper separately from live scoped evidence APIs; keep any genuine application capability.
- [ ] Migrate affected tests to the proper reader/service and prune only newly unused facade dependencies.
- [ ] Correct the message facade test's keyword-only call.
- [ ] Verify focused Context, semantic-window, evidence, and facade tests; commit.

### Unit 2: remove obsolete mutation and merge paths

- [ ] Remove the unused store and GraphWriter direct alias mutation route after checking canonical name-support coverage.
- [ ] Remove the unused store and EpisodeReader merge-evidence helper after checking Maintenance evidence coverage.
- [ ] Remove the unused store `save_message_logs()` wrapper, but keep MessageWriter's transaction-aware operation used by finalization.
- [ ] Verify entity/name-support, merge maintenance, and finalization contracts; commit.

### Unit 3: consolidate source reads and replay ownership

- [ ] Replace test-only combined assistant/source and unscoped episode-source reads with current scoped contracts, then remove obsolete wrappers/helpers where no other caller remains.
- [ ] Move replay-source lookup SQL behind the source reader using the same transaction cursor, if this keeps replay behavior straightforward.
- [ ] Verify fresh finalization, replay, source order, rollback, and scoped source reads; commit.

### Unit 4: finish facade organization and audit

- [ ] Group remaining methods by message/exchange, Context/windows, artifacts/sources, episodes, entity/graph retrieval, and maintenance/rebuild.
- [ ] Retain episode editing and generic browsing; document their missing application ownership/wiring rather than deleting them.
- [ ] Check imports, constructor fields, reflective consumers, and relevant typing/lint checks.
- [ ] Run applicable storage contract and affected unit tests. Run PostgreSQL-dependent coverage when the local database is available; report any unavailable checks.
- [ ] Record completed work and remaining product follow-ups; commit.

## Deferred product work

Episode-edit API/SDK wiring and the entity/message browsing public contract need intentional typed application operations. This cleanup does not silently create or delete those product capabilities.

## Workspace note

Existing changes to `time_utils.py`, deleted earlier temporary plans, and untracked user files are outside this unit and must be preserved.
