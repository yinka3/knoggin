# Core Stage 2 — Entity Identity and Resolution

Status: In progress; Chunks A-E completed 2026-09-11.

Baseline inspected: `aadedewe/refactor`, `4e47481`, following completed
[Stage 1](CORE_STAGE_1_IMPLEMENTATION_PLAN.md). Recheck HEAD and the working
tree before each chunk. Preserve the existing operations-document health edit
and unrelated untracked material. This plan changes no production code.

## Objective and finding map

Keep global entity identity consistent through Context extraction, project-local
classification, relationship extraction, durable commit, and resolver recovery.

| Work | Review source | Stage 2 outcome |
| --- | --- | --- |
| Reuse an identity from a readable project | Core F2; locked Knowledge §4 | Create the receiving project's classification explicitly without recreating the identity. |
| Exact names and aliases after restart | Core F3 | Consult durable exact matches and preserve ambiguous owners regardless of cache contents. |
| Same-name mentions and relationship endpoints | Core F5; locked Ingestion §§13–15 | Pending identities require compatibility checks; VP-02 uses local entity handles. |
| Publish committed resolver state | Core F6; locked Ingestion §17 | Refresh the live resolver after SQL commit, including recovery from a lost publication. |
| Stable entity embedding input | Core F10; locked Knowledge §15 | Creation and rebuild use the same identity-only text. |
| Retired resolver entry points | Core I1 | Remove only verified unused paths encountered while changing the resolver. |

Sources: [core review](CORE_LAYER_REVIEW_2026-09-09.md),
[locked Ingestion](../KNOGGIN_INGESTION_SUBSYSTEM_REVIEW_LOCKED.md),
[locked Knowledge](../KNOGGIN_KNOWLEDGE_SUBSYSTEM_REVIEW_LOCKED.md), and
[SQLite/retrieval plan §8](../KNOGGIN_SQLITE_AND_AGENT_RETRIEVAL_PLAN.md).
Review line numbers describe their original revision; current symbols and
callers were inspected for this plan.

## Scope and implementation order

Work stays in `server/`, including the directly required runtime factory and
classification-maintenance bridge. Keep PostgreSQL/AGE, the existing resolver,
Context-first extraction, and durable semantic-window checkpoints.

Implement A → B → C → D → E. Each chunk gets focused validation and a separate
local commit. B establishes the classification contract C consumes; D depends
on A's durable recovery and B's explicit classification. E is logically
independent but follows the correctness work to keep changes reviewable.

Deferred: SQLite migration; new retrieval ranking; model/provider replacement;
general ambiguous-alias disambiguation, score fusion and margin tuning from
SQLite plan §§8.2–8.4; participation/policy capture; document/Episode embedding
changes; merge/cleanup/concurrency work; Agent behavior; SDK/UI. No global type
ontology, new cache service, event bus, publication table, or full catalog load.

## Shared decisions

- Global identity is the durable ID. A canonical name, alias, type, or topic is
  not a unique identifier. Topic remains project classification.
- Existing local classification is authoritative. Ordinary ingestion may
  create missing local membership but must not silently reclassify it.
- Foreign project classification may inform conservative identity matching;
  it must not supply local classification by default. The receiving frozen
  domain and locally extracted mention determine a new local classification.
- Same topic alone must not make explicitly incompatible entity types match.
  Do not claim arbitrary cross-domain equivalence without evidence or invent
  new global identity classes to implement this stage.
- Ambiguous exact owners remain candidates, not permission to pick the first.
  Preserve conservative rejection when available evidence cannot distinguish
  them. Temporary duplicates remain preferable to unsupported identity reuse.
- Pending identities and aliases stay private until SQL succeeds. A resumed
  semantic commit is not proof that the current in-memory pending IDs persisted.
- Reserved entity `1` retains Identity semantics and needs no manufactured
  local context or activity timestamp.
- Keep every Stage 1 invariant: window-owned impact, empty no-op work, active
  current observations, retained history, authoritative event time, and atomic
  SQL/AGE publication.

## Chunk A — Durable exact candidates and safe pending identity reuse (completed)

### Verified behavior

`EntityResolver.get_candidate_ids()` consults the alias cache, fuzzy cache, and
durable vectors. It omits the existing durable name/alias lookup. A partially
warmed cache can also make a shared alias appear unique.

`mention_dedupe_key()` discards type/topic/policy. The Context resolver reuses
`created_in_build[name]` without evaluating a pending candidate. Candidate
acceptance also permits same-topic compatibility. After selecting a durable ID,
alias staging calls `validate_existing(canonical_name, ...)`, which looks the ID
up again by name and can lose a valid homonym selection.

### Implementation

1. Add scoped durable exact lookup to the normal candidate path, even if a warm
   cache already reports one owner. Reuse searches within one build, but do not
   cache completeness across writes without an invalidation contract.
2. Hydrate all returned visible owners; combine signals by durable ID. Preserve
   ambiguity before acceptance and use deterministic ordering for equal scores.
   Keep strict ingestion read errors as failures, never as evidence of absence.
3. Keep query reuse separate from identity reuse: two mentions can share a
   name/vector search while requiring different identity decisions.
4. Replace unconditional pending-name reuse with explicit candidate evaluation
   using the frozen domain and supporting Context. Incompatible typed mentions
   stay separate even when their topics match. Clear compatible repetitions
   reuse one pending identity; unresolved homonyms do not force a match.
5. Stage aliases against the already-selected durable ID. Do not resolve the
   canonical name again. Keep alias collision/ambiguity handling conservative.
6. Recheck production callers of `resolve_mentions`,
   `candidate_entries_for_mentions`, `register_entity`, and `compute_embedding`.
   Remove retired entry points and tests that only preserve them if still unused;
   retain shared matching logic and publication primitives needed by D.

| File under `server/` | Change |
| --- | --- |
| `src/core/knowledge/entity/resolver.py` | Durable exact candidates, pending candidate decisions, compatibility checks, ID-based alias staging, verified dead-path removal. |
| `src/core/knowledge/db/readers/entity_reader.py`, `src/core/knowledge/store.py` | Reuse `get_entities_by_names`; extend only if bounded lookup needs completeness/overflow reporting. Never truncate owners into a false unique match. |
| `src/core/knowledge/entity/index.py` | Conditional: preserve multiple authoritative alias owners and coherent hydration if existing operations cannot express this. |
| `tests/unit/core/knowledge/test_entity_manager_candidates_contract.py`, `test_entity_manager_alias_maintenance_contract.py` | Cold/partial/warm cache, ambiguity, strict read failure, selected-ID alias staging. |
| `tests/unit/core/ingestion/test_context_entity_build.py` | Typed pending reuse and conservative ambiguous cases. |
| `tests/contract/storage/test_entity_name_lookup_contract.py` | Real durable alias collisions, active/scoped identities, normalization behavior. |

### Gate A

- A cold resolver finds a durable exact alias with no vector match; it reuses a
  qualifying full-name identity instead of allocating another.
- Warming just one of two visible alias owners does not hide the other.
- Inaccessible identities are excluded; missing/deactivated candidates cannot
  acquire associations. SQL and cache normalization are checked on whitespace,
  case, and non-ASCII examples without a speculative schema-wide rewrite.
- Same-name incompatible types in one topic do not collapse; compatible
  repetitions do not allocate on every mention.
- Selecting one same-name durable ID never stages aliases on another ID.

### Completion record

- `get_candidate_ids()` now treats the scoped durable exact lookup as the
  authoritative exact-owner set, hydrates every returned owner, retains
  ambiguity, and orders equal-score candidates by ID. Cached fuzzy/vector
  candidates are revalidated through the active scoped durable reader before
  they can be returned.
- Candidate-search reuse is keyed only by normalized surface text. Pending
  identity reuse is separate: it requires frozen-domain compatibility and the
  same normalized Context support. Distinct support stays separate when the
  system cannot establish a same-type homonym identity.
- Reader name/ID/vector resolution now excludes inactive entities and normalizes
  surrounding whitespace for exact names and aliases. Alias staging takes the
  candidate ID already selected by resolution.
- Removed the unused transcript resolver, `validate_existing`, `register_entity`,
  and `compute_embedding` paths. Retained committed-write publication support
  for Chunk D.
- Validated with focused resolver and Context tests (55 passed), reader snapshot
  contracts (9 passed), and the real PostgreSQL name-lookup contract (2 passed).

## Chunk B — Explicit project classification through extraction and commit

### Verified behavior

`ContextEntityResult` has global `pending_entity_writes` but no independent
local-classification payload. `SemanticCommitWriter._write_entities()` creates
contexts only for new identities. Reused linked identities therefore fail local
association/endpoint validation.

There is an upstream authority problem too: resolver hydration falls back to a
foreign profile, and `TextProcessor.extract_context_mentions()` uses that type
for a known alias and suppresses the VP-01 occurrence. Merely copying the
resulting mention into a local context would preserve the wrong authority.

### Implementation

1. Add a narrow typed classification payload keyed by resolved ID, separate from
   global identity creation. Carry both intended effective local classification
   and the distinction between existing membership and missing membership.
2. Use already-local profile classification for alias shortcuts. A foreign-only
   alias must not suppress current-domain classification; let VP-01 classify the
   occurrence and use durable exact lookup to resolve its identity afterward.
3. Fix surface-only coverage/deduplication so incompatible typed occurrences are
   not dropped before resolution. Preserve occurrence/span evidence where needed
   in the temporary mention contract; do not add a durable occurrence table.
   A duplicated detector span is different from distinct same-name occurrences.
4. Existing local classification remains unchanged. Reject conflicting proposals
   for one ID explicitly; never choose a type by dictionary overwrite or iteration
   order. Foreign vocabulary differences alone are not proof of different people,
   but unresolved identity compatibility remains grounds to abstain.
5. Insert global identities, then missing local contexts, then aliases,
   associations, recency, relationships and projections in the existing transaction.
   Verify insert conflicts against the intended context; a conflicting concurrent
   classification causes retry/rebuild, not last-write-wins reclassification.
6. Validate reused IDs against the user's active identities and current durable
   readable project scope before creating membership. Locate/reuse the existing
   project-scope query and lock convention; do not trust a caller-supplied list or
   weaken the association trigger. If scope changed, fail safely and rebuild with
   refreshed scope. No new admission-policy capture work is included.
7. Feed VP-02 the same effective local type that storage will validate. Empty
   Stage 1 builds include empty classification payloads; the writer rejects
   classification mutations on reused/no-op Context.

| File under `server/` | Change |
| --- | --- |
| `src/common/schema/ingestion/contracts.py` | Typed classification result and, if necessary, request-local occurrence evidence. |
| `src/core/knowledge/entity/resolver.py`, `profile.py` | Explicit local/foreign classification authority and staged membership. Reuse reader context lists where possible. |
| `src/core/ingestion/text_processor.py` | Alias authority and typed occurrence/overlap handling. |
| `src/core/ingestion/context_entity_build.py`, `batch.py` | Carry classification and valid empty results. |
| `src/core/ingestion/relationship_extractor.py` | Effective project-local candidate type before commit. |
| `src/core/knowledge/db/writers/semantic_commit_writer.py` | Scoped membership verification, insert/verify local contexts, payload validation, atomic ordering. |
| `src/core/knowledge/store.py` and existing scope reader | Conditional: expose only missing scoped identity/context reads. No schema migration is expected. |
| `tests/unit/core/ingestion/test_context_entity_build.py`, `test_context_relationship_extractor.py` | Foreign aliases, same-surface typed occurrences, effective local type. |
| `tests/contract/storage/test_semantic_commit_contract.py` | Membership, scope, conflicting classification, atomic rollback, retry, reserved identity and no-op rejection. |

### Gate B

- A reads B, accepts B's qualifying identity, and creates A's intended context
  under the same ID. B's classification and global identity/embedding stay intact.
- Different allowed local classifications are used consistently by VP-01,
  VP-02, storage, and later local profile reads; warmed foreign aliases cannot
  override them.
- Existing A classification survives incoming disagreement; invalid, unreadable,
  wrong-owner or retired IDs cannot be admitted through the new payload.
- A late relationship failure rolls back membership, aliases, associations and
  recency together. Replay creates no duplicate identity/context/observation.
- Same-name Person/Company occurrences reach the resolver even within one block.
  General same-name/same-type disambiguation is not claimed: if occurrence context
  cannot establish identity, retain ambiguity and avoid unconditional reuse.

### Completion record

- Added `ProjectEntityClassification` as the explicit target-project payload.
  It covers every non-identity resolved ID, distinguishes existing from missing
  membership, and keeps identity `1` outside project contexts.
- Foreign profiles can establish an identity candidate but cannot classify the
  receiving project. Local alias shortcuts retain local authority; foreign
  aliases go through VP-01, and typed source spans prevent same-surface
  incompatible occurrences from being dropped before resolution.
- The commit writer now locks current readable scope for reused IDs, verifies
  ownership/status/visibility, inserts or verifies target contexts without
  reclassification, then writes aliases and the remaining semantic payload in
  the existing transaction. VP-02 consumes the staged local type.
- Validated with the focused extraction/resolver/commit/integration suite
  (`79 passed`), touched-path Ruff, compileall, and `git diff --check`.
- Local implementation commit: `4eac23e fix(knowledge): stage local entity classifications`.

## Chunk C — Local entity handles for Context VP-02

Replace name-keyed transport with one deterministic `eN` handle per durable ID.
Keep canonical names, aliases, effective local types, and supporting `bN` blocks
as descriptive candidate metadata. Keep handles request-local; storage continues
using entity IDs and immutable Context UUIDs.

Reserved identity gets its own handle keyed by ID, explicitly identified in the
prompt. A different entity sharing the user's name must not suppress entity `1`.
Reject unknown handles, canonical names used as endpoints, raw durable IDs,
unknown blocks, and self-relations after mapping. Keep frozen-domain normalization,
minimal multi-block evidence, duplicate-write handling, and writer revalidation.
There is no legacy name-output fallback in this unreleased project.

| File under `server/` | Change |
| --- | --- |
| `src/core/ingestion/relationship_extractor.py` | Handle table and endpoint mapping by ID with B's effective classification. |
| `src/common/schema/ingestion/extraction.py` | Context-only endpoint fields accept local handles; retain existing field names where clear. |
| `src/common/utils/core_utils.py` | `format_context_vp02_input` renders entity handles and allowed endpoints. |
| `src/common/templates/prompts/extraction.md` | Context relationship instructions and explicit user-identity handle. |
| `tests/unit/core/ingestion/test_context_relationship_extractor.py`, `tests/unit/common/test_structured_llm_contracts.py` | Homonyms, Identity collision, malformed/unknown endpoints, provenance and schema contracts. |
| `tests/integration/ingestion/test_project_semantic_postgres_flow.py` | Persist a selected homonymous endpoint through actual extractor and storage with controlled model output. |

### Gate C

Two already-resolved entities with the same name, including the same type, can
participate independently. User-name collision does not impersonate Identity.
Invalid outputs cannot persist, and valid multi-block support still reaches the
original messages/sources. Handle support does not assert that the resolver can
infer every same-type homonym from ambiguous prose.

### Completion record

- VP-02 now creates an opaque, ascending request-local `eN` table after all
  eligible candidates are known. Reserved identity `1` is always `e1`; an
  identity-name collision therefore retains two distinct candidate handles.
  Durable IDs remain outside the prompt and are restored before relationship
  normalization, block validation, deduplication, and `ContextRelationshipWrite`
  construction.
- The Context-only structured schema rejects canonical names, aliases, raw IDs,
  and malformed handles. An unknown but well-formed handle remains a per-result
  validation issue so other valid model connections can still persist.
- The formatter and Context VP-02 prompt identify handle-only endpoint output,
  preserve candidate metadata for model reasoning, and explicitly mark the
  reserved user identity. There is no name-output fallback.
- Added unit coverage for same-name/same-type candidates, identity collisions,
  schema and runtime rejection, multi-block evidence, and effective local type.
  The existing real PostgreSQL source-provenance flow now uses the actual
  extractor; a new controlled homonym flow commits both selected IDs and their
  original source reference.
- Validated with the direct VP-02/schema tests (`9 passed`), the focused Stage 2
  unit suite (`72 passed`), and real PostgreSQL storage/integration suite
  (`15 passed`), plus touched-path Ruff, compileall, and `git diff --check`.
- Local implementation commit: `343e3a5 fix(ingestion): use local VP-02 entity handles`.

## Chunk D — Durable commit before resolver publication, with restart recovery

`ProjectSemanticJob` currently commits and returns without publishing entity
changes. The writer's `resumed=True` path returns before validating fresh pending
IDs, so publishing an arbitrary reconstructed build would expose uncommitted IDs.

Use one resolver operation for committed Context entity publication, wired by
`ProjectRuntimeFactory`. Prefer hydration of authoritative affected entity rows,
including aliases and local contexts, instead of several cache-internal calls.

Make publication recoverable at the existing `knowledge_committed` finalization
checkpoint, before completion permits the next window. Derive affected IDs from
the owning window's committed revision impact and durable block associations;
include every committed identity/alias/classification affected by the build.
At implementation, verify that this durable set covers all valid payloads. If
not, tighten the build coverage invariant or expose the existing durable writes
through a narrow reader; do not add a publication journal by default.

Retry publication and Episode enrichment without repeating extraction or SQL
Knowledge mutation. A publication failure retains `knowledge_committed` and uses
the existing bounded failure/retry mechanism with an accurate failure-stage
diagnostic. Rehydration must replace stale affected profiles and reconcile alias
ownership, bumping alias version when needed so PhraseMatcher rebuilds. No-op
windows skip publication of the earlier owner's impact. Cold lookup from A also
handles restart after an already-completed window; full alias preload is not required.

| File under `server/` | Change |
| --- | --- |
| `src/core/knowledge/entity/resolver.py`, `index.py` | Idempotent authoritative publication/hydration, affected profile refresh and alias-version coherence. |
| `src/core/ingestion/project_semantic_job.py` | Recoverable publication at finalization; never publish a speculative replay build. |
| `src/runtime/project_factory.py` | Wire the actual project resolver publication operation. |
| `src/core/knowledge/db/readers/project_context_reader.py`, `src/core/knowledge/store.py` | Narrow scoped committed-window affected-entity read if absent. |
| `tests/unit/core/ingestion/test_project_semantic_knowledge_stage.py` | Commit/publication ordering, failure-stage diagnostics and restart. |
| `tests/unit/core/knowledge/test_entity_manager_cache_contract.py`, `test_entity_manager_alias_maintenance_contract.py` | Repeated/partial publication, profile refresh, ambiguous aliases, version behavior. |
| `tests/integration/ingestion/test_project_semantic_postgres_flow.py` | Real persisted IDs after a crash between commit and publication. |

### Gate D

- SQL failure exposes no pending identities/aliases to the shared resolver.
- Lost or partially failed publication resumes from `knowledge_committed` with
  only durable IDs; one canonical mutation and one observation set remain.
- Successful publication makes new aliases available to the next window and
  refreshes a reused foreign profile to the new local context.
- A resumed writer result cannot publish newly allocated but uncommitted IDs.
- Publication/enrichment retries are idempotent; no-op windows do no extra work.
- Scope remains project-owned; no cross-runtime cache broadcast is introduced.

### Completion record

- Finalization now reads only durable entity IDs from a knowledge-committed
  window, publishes them through the project resolver, then enriches Episodes
  and advances the completion checkpoint. Publication failure records
  `resolver_publication` and retains `knowledge_committed` for bounded retry.
- The affected-ID reader requires the Context revision to be owned by the same
  window. A later no-op window therefore returns no IDs and cannot republish an
  earlier window's impact.
- `ContextEntityResult` now requires every resolved identity to have a durable
  Context-block association. That makes revision impact plus associations a
  complete recovery source without a publication journal.
- Resolver publication hydrates authoritative scoped rows, reconciles removed
  aliases and local classifications, preserves durable alias ambiguity, evicts
  inaccessible rows, and changes the alias version only when ownership changes.
- Added unit ordering/retry/no-op coverage, a factory-wiring contract, durable
  PostgreSQL reader coverage, and a PostgreSQL crash-boundary composition test
  that reconstructs a cold resolver without rerunning extraction or Knowledge
  mutation.
- Validated with affected ingestion/resolver units (93 passed), runtime wiring
  tests (6 passed), PostgreSQL storage contracts (76 passed), and project
  semantic integration tests (4 passed), plus touched-path Ruff, compileall,
  and `git diff --check`.
- Local implementation commit: `84d3eb0 fix(knowledge): recover committed resolver publication`.

## Chunk E — Identity-only entity embeddings

Change `build_entity_embedding_text` to take only the canonical identity name.
Creation currently includes local type while rebuild passes `None` and embeds
`(unknown)`. Use the same normalized text for creation, rebuild, and reserved
Identity. Do not invent a project-context embedding representation here.

Remove the automatic embedding rebuild from historical entity reclassification:
the current call rebuilds Episodes as well as identities even though this is a
classification change. Retain its required graph projection work and make its
result accurately report the operations performed. The explicit embedding
rebuild operation remains available for existing development data and model
changes; no automatic data migration or mixed-format compatibility layer.

| File under `server/` | Change |
| --- | --- |
| `src/core/knowledge/entity/embedding.py`, `resolver.py`, `db/embedding_rebuilder.py` | One identity-only helper/signature and all live callers. |
| `src/core/project/maintenance_service.py` | Stop re-embedding solely because local classification changed. |
| `tests/contract/storage/test_embedding_rebuilder_contract.py` | Creation/rebuild text and vector parity with deterministic encoder output. |
| `tests/unit/core/knowledge/test_entity_identity_models.py`, existing reclassification workflow tests | Type/topic independence, reserved identity, and no unnecessary embedding calls. |

### Gate E

The same canonical identity produces identical encoder input on creation/rebuild
and across project classifications. Reclassification makes no identity/Episode
embedding calls. Explicit rebuild still validates dimensions and commits
replacement vectors atomically. Input parity is not a real-model quality benchmark.

### Completion record

- Entity embedding text now consists solely of the normalized canonical name.
  Pending creation, explicit rebuild, and the reserved Identity all use that
  same representation; project-local type/topic remain on their Context rows.
- Historical entity reclassification now rebuilds the project graph only when
  classifications changed. It neither re-embeds global identities nor Episodes
  nor reports obsolete embedding-rebuild fields; explicit embedding rebuild
  remains available for development data and model changes.
- Added deterministic creation/rebuild parity coverage across distinct local
  classifications, reserved-Identity input/vector coverage, and pre-write
  validation coverage for malformed entity and Episode vectors. A real
  PostgreSQL trigger now proves a late Episode write failure rolls back entity,
  Identity, and Episode vector replacements together.
- Validated with direct embedding, identity, reclassification, and maintenance
  tests (`31 passed, 2 deselected`), affected Context/semantic tests (`32
  passed, 11 deselected`), and PostgreSQL embedding contracts (`2 passed, 9
  deselected`), plus touched-path Ruff, compileall, and `git diff --check`.
- Local implementation commit: `52cbacb fix(knowledge): make entity embeddings identity-only`.

## Validation and closeout

Use deterministic model/embedding outputs for contract tests, real PostgreSQL/AGE
for membership, visibility, alias collisions and transaction behavior. The final
composition test must exercise the real resolver and relationship extractor;
the Stage 1 fake entity builder alone cannot establish Stage 2 correctness.

Combined scenario:

1. Seed an identity and alias in readable project B, plus a valid homonym.
2. With A's cold resolver and no useful vector hit, resolve qualifying evidence
   to the intended ID and stage A's local classification.
3. Extract relationships by handles and commit exact IDs, classification,
   source-time recency and immutable block provenance.
4. Stop after Knowledge commit, reconstruct the job/resolver and recover
   publication/finalization from durable state. Inject publication failure and
   ensure retry never replays the canonical mutation.
5. Process another changed window using the committed alias, then a no-op.
   Verify cache visibility, conservative homonym handling, no duplicate identity
   for the unambiguous match, and Stage 1 observation/time/history invariants.

Run focused files per chunk, then affected ingestion/resolver unit tests,
`pytest -q tests/contract/storage -m requires_postgres`, and
`pytest -q tests/integration/ingestion/test_project_semantic_postgres_flow.py`.
Use the existing `uv run --project server --extra dev` environment from the
appropriate directory. Run touched-path Ruff, compile checks, architecture
checks when imports change, and `git diff --check`. Check touched typed contracts
against `MYPY_BASELINE.md` without expanding into a repository-wide annotation sweep.

Record exact commands/results at execution time. Stage 1 counts are historical,
not Stage 2 validation. The previously missing FastAPI dependency is an environment
limitation to recheck only if broader collection is needed; do not modify tests
to hide source defects or unrelated missing dependencies. No tests were executed
while drafting this plan.

- [x] A: durable exact candidates and safe pending reuse.
- [x] B: local classification authority, occurrence preservation and atomic membership.
- [x] C: entity handles and provenance-preserving endpoint validation.
- [x] D: committed resolver publication and recovery.
- [x] E: identity-only embeddings and classification-only maintenance cleanup.
- [ ] Combined real-resolver/extractor PostgreSQL scenario passes.
- [ ] Verify retained Stage 1 gates; record changed files, commands, limitations and local commit IDs.
- [ ] Annotate completed F2/F3/F5/F6/F10 and relevant locked sections; preserve deferred findings.
- [ ] Retire resolved historical core probes with normal regression references.
- [ ] Add a narrow operations-document update while preserving existing user edits.

Next implementation task: run the combined Stage 2 real-resolver/extractor
PostgreSQL scenario, then close the retained Stage 1 gates and operations notes.
