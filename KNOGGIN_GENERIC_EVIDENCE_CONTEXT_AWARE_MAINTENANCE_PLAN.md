# Knoggin Generic Evidence and Context-Aware Maintenance Plan

Status: Batches 1-7 implemented and validated.

This plan introduces a shared, bounded evidence read/traversal layer and uses it
to make Knowledge maintenance Context-aware. It does **not** replace maintenance
with Context, add external Context writers, or migrate all provenance into a new
generic table.

Knoggin is unreleased. Implement the clean target contract directly and delete
superseded internal code after cutover; do not add compatibility shims.

---

## 1. Outcome

After this work, Knoggin can answer, through one project-scoped application
boundary:

> What durable evidence supports this Context block, relationship observation,
> maintenance review, or maintenance mutation, and what will be affected if the
> reviewed mutation is applied?

The intended chain is:

```text
source reference or frozen message
    -> Context block support
    -> relationship observation
    -> maintenance review evidence
    -> typed maintenance plan
    -> affected canonical rows and derived projections
```

The framework is generic at the **read contract and traversal** level. Existing
tables remain authoritative for their own lifecycles.

### Success criteria

- one typed evidence vocabulary is shared by Context and maintenance readers;
- a review can return bounded provenance from observation to original source;
- Context-owned deterministic retirement never becomes a maintenance review;
- independent active evidence remains eligible for conflict/advisory review;
- maintenance mutation previews name every canonical and projection impact;
- applying a review preserves existing locks, optimistic checks, and typed plans;
- no unbounded graph traversal, source content dump, or cross-project evidence leak;
- no second ingestion or maintenance owner is introduced.

---

## 2. Locked boundaries

### 2.1 PostgreSQL ownership

PostgreSQL remains canonical. The evidence layer reads existing ownership edges:

- `message_source_refs` owns assistant-encountered sources;
- `project_context_block_supports` owns message/source support for Context blocks;
- `relationship_observation_blocks` owns observation-to-block support;
- `maintenance_review_evidence` and `maintenance_reviews` own review snapshots;
- existing entity, relationship, Context, Episode, and audit tables retain their
  current mutation ownership.

AGE and search state remain rebuildable projections. Redis is not evidence
storage and is not introduced by this work.

### 2.2 Read framework, not universal storage

Version 1 adds shared models, readers, and application services over current
tables. It does not create an `evidence_nodes` table, polymorphic foreign keys,
or a generic write API.

A shared storage redesign may be considered only after two write-owning
subsystems demonstrably require identical retention and deletion behavior.

### 2.3 Maintenance remains responsible for judgment

Keep these existing responsibilities:

- entity merge and rollback;
- entity-context reclassification;
- independent relationship conflict review;
- unknown relationship advisory and vocabulary decisions;
- observation reinterpretation or detachment;
- rebuilding affected graph/search projections.

Context reconciliation remains responsible for deterministic block replacement,
block deletion, support retirement, and relationships losing their last active
Context support.

### 2.4 Explicit exclusions

- Codex, Claude, SDK, MCP, or HTTP Context mutation integration;
- unrestricted Agent evidence or Context mutation tools;
- replacement of all maintenance with Context;
- repository-wide provenance-table migration;
- Redis-backed evidence authority or caching required for correctness;
- recursive or unbounded evidence traversal;
- automatic application of model-proposed maintenance decisions;
- historical-data migration or compatibility adapters.

---

## 3. Current-code baseline

Reuse rather than duplicate:

- `common/schema/source/locators.py` already defines strict PDF, text-line, code,
  CSV, DOCX, pasted-text, and search-result locators;
- `common/schema/source/references.py` already validates source identity and
  encounter-specific shapes;
- `core/knowledge/maintenance_reviews.py` has typed maintenance plans and now
  uses the shared `EvidencePointer` contract;
- `SourceReferenceReader` reads message-owned sources;
- `ProjectContextReader` reads Context blocks and supports;
- `RelationshipObservationReader` reads observations and block support;
- `ConflictDiscoveryReader` reads active observation neighborhoods;
- `MaintenanceReviewWriter` persists review snapshots and transitions;
- `ProjectMaintenanceService` is the project-scoped application owner for review
  listing and application;
- maintenance writers already use typed plans, transactions, expected state,
  domain versions, and project maintenance locks.

Current duplicated joins occur in relationship interpretation, Episode
reconciliation, entity readers, and Knowledge queries. Consolidate only the
provenance-shaped reads; do not move mutation SQL into the evidence reader.

---

## 4. Target contracts

Create `common/schema/evidence.py` with frozen, strict models.

### 4.1 EvidencePointer

A stable reference without embedded content:

```python
EvidenceKind = Literal[
    "message",
    "episode",
    "merge_mutation",
    "source_reference",
    "context_block",
    "relationship_observation",
    "maintenance_review",
]

class EvidencePointer:
    kind: EvidenceKind
    identifier: str
```

Rules:

- identifiers are nonblank and bounded;
- observation and message identifiers validate as positive integers;
- UUID-backed identifiers validate as UUIDs;
- no generic arbitrary `kind: str` at public/internal boundaries;
- project and user scope are method arguments, not trusted pointer payload.

Replace the maintenance-local `EvidenceRef` with this contract only after every
review writer/reader and stored JSON validator is updated in the same commit.

### 4.2 EvidenceSubject

The object whose support is being explained:

```python
EvidenceSubjectKind = Literal[
    "context_block",
    "relationship_observation",
    "maintenance_review",
]
```

Episode and artifact **subjects** are deliberately deferred until they become
real traversal callers. Message, Episode, and merge-mutation pointers remain in
the closed pointer vocabulary because current entity-maintenance reviews already
use them as evidence.

### 4.3 EvidenceNode and EvidenceEdge

`EvidenceNode` contains bounded display metadata appropriate to its kind:

- pointer;
- role or source kind where applicable;
- content hash where useful for immutable identity;
- typed locator for source references;
- bounded excerpt/summary;
- timestamp availability, not fabricated timestamps;
- active/retired state for observations.

`EvidenceEdge` uses a closed vocabulary:

- `source_owned_by_message`;
- `supports_context_block`;
- `supports_relationship_observation`;
- `cited_by_maintenance_review`.

Edges must preserve direction and must not imply that a source proves a claim;
they state only the durable support/citation relationship recorded by Knoggin.

### 4.4 EvidenceBundle

A bounded traversal result:

- subject;
- nodes in deterministic order;
- edges in deterministic order;
- truncation flags and counts;
- unavailable-content flags;
- current-state token or hash for stale-review detection.

Default limits:

- no more than 128 observations;
- no more than 256 Context blocks;
- no more than 512 messages/source references;
- excerpts use the existing source bounds;
- no full message or Context document bodies unless the caller explicitly uses
  an existing authorized detail reader.

---

## 5. Target ownership

```text
EvidenceTraversalReader
    owns bounded, project-scoped provenance joins

EvidenceService
    owns typed read orchestration, redaction, and deterministic bundles

MaintenanceReviewWriter
    continues to own review persistence and transitions

ProjectMaintenanceService
    continues to own project-scoped maintenance commands and application locks

Context/SemanticCommit writers
    continue to own deterministic Context support and observation retirement

Maintenance mutation writers
    continue to own merge, rollback, reclassification, and interpretation writes
```

The evidence service performs no mutation and starts no background work.

---

## 6. Implementation batches

### Batch 1 — Freeze evidence vocabulary and invariants

#### Commit 1.1 — Inventory real evidence paths

Files:

- this plan;
- a temporary test-owned inventory or architecture assertion if useful.

Work:

- enumerate every production join using `message_source_refs`,
  `project_context_block_supports`, `relationship_observation_blocks`, and
  `maintenance_review_evidence`;
- classify each as provenance read, mutation validation, reconciliation, or
  projection rebuild;
- identify the exact Context and maintenance callers that will adopt the shared
  reader;
- do not refactor mutation-specific joins merely because their SQL looks similar.

Acceptance:

- every planned replacement has a named production caller and test;
- no Episode/artifact abstraction is added without a caller;
- deletion and scope behavior for all participating tables is documented.

#### Commit 1.2 — Add common evidence contracts

Files:

- `server/src/common/schema/evidence.py`;
- `server/src/common/schema/source/locators.py` only if an existing locator must
  be reused or exported;
- focused schema tests.

Work:

- add the closed pointer, subject, node, edge, limit, and bundle models;
- reuse existing `SourceLocator` rather than defining another locator union;
- enforce deterministic ordering inputs and explicit truncation metadata;
- prohibit arbitrary metadata dictionaries in the core bundle contract;
- keep source excerpts bounded and distinguish unavailable from empty.

Acceptance:

- invalid kinds and identifier shapes fail validation;
- models are JSON-safe, frozen, and reject extra fields;
- no contract contains database clients, ORM rows, exception objects, or secrets.

#### Commit 1.3 — Migrate maintenance review pointers

Files:

- `core/knowledge/maintenance_reviews.py`;
- `core/knowledge/db/writers/maintenance_review_writer.py`;
- review and writer tests.

Work:

- replace the open-ended maintenance `EvidenceRef` with `EvidencePointer`;
- preserve the current stored JSON shape only if it matches the clean target;
  otherwise update the unreleased schema and all callers directly;
- keep observation convenience access typed rather than accepting arbitrary
  integer/string coercion throughout the codebase.

Acceptance:

- all review kinds round-trip through PostgreSQL;
- unknown evidence kinds are rejected;
- dedupe signatures remain deterministic;
- no compatibility parser for an unreleased prior shape remains.

### Batch 2 — Build the bounded PostgreSQL traversal reader

#### Commit 2.1 — Context and relationship provenance

Files:

- new `core/knowledge/db/readers/evidence_traversal_reader.py`;
- `core/knowledge/store.py` facade;
- PostgreSQL contracts.

Work:

- add project/user-scoped reads for one Context block, one observation, and a
  bounded list of observations;
- traverse observation -> support block -> block support -> message/source ref;
- return flat typed rows or typed nodes/edges, never nested raw database JSON;
- include retired-state metadata without treating retired evidence as active;
- order by observation, block, support kind, message, and source ID;
- require explicit limits at every multi-row boundary.

Acceptance:

- the composed source-grounded relationship fixture resolves end to end;
- user-message, assistant-message, and assistant-source support all work;
- duplicate paths are deduplicated without losing distinct support edges;
- a project cannot read another project's blocks, observations, messages, or
  source references;
- deleted projects return no evidence;
- query count is bounded and does not grow once per observation.

#### Commit 2.2 — Evidence service

Files:

- new `core/knowledge/evidence_service.py`;
- focused unit tests;
- runtime/application composition only where an existing caller needs it.

Work:

- validate scope and traversal limits;
- assemble deterministic `EvidenceBundle` values;
- expose separate summary and detail reads rather than a boolean maze;
- apply redaction and size limits before returning the bundle;
- compute an evidence-state token from stable identities and current active state.

Acceptance:

- repeated reads of unchanged evidence return the same order and state token;
- truncation is explicit;
- source failure summaries, connection strings, and unrestricted content never
  enter the bundle;
- the service remains read-only.

#### Batch 1 completion record

Completed directly against the unreleased schema and current callers:

- inventoried the relevant joins and retained mutation-owned validation in the
  relationship interpretation, semantic commit, and deletion paths;
- introduced the closed `EvidencePointer`, subject, node, edge, traversal-limit,
  and bundle contracts using the existing `SourceLocator` union;
- migrated maintenance reviews, conflict discovery, relationship advisories,
  agent-created reviews, merge application, and rollback to the shared pointer
  vocabulary;
- removed the old open-ended `EvidenceRef` contract and did not add a legacy
  parser or compatibility storage shape.

Validation covers invalid pointer kinds/identifiers, frozen and extra-forbidden
models, deterministic signatures, maintenance callers, and persisted review
application through PostgreSQL.

#### Batch 2 completion record

Completed as one read-only PostgreSQL traversal boundary plus one service:

- one scoped query handles one or a bounded list of relationship observations;
- one scoped query handles Context-block detail;
- bundles include observation, Context block, message, and source-reference
  nodes with only typed locators, bounded excerpts, hashes, roles, timestamps,
  and active/retired/missing state;
- stable ordering, deduplication, category limits, edge limits, explicit
  truncation, and a deterministic evidence-state token are enforced before the
  bundle leaves the service;
- `KnowledgeStore` exposes the two single-subject detail reads without adding a
  mutation path or an external API.

Validation covers user-message, assistant-message, and assistant-source
support; duplicate rows; unchanged repeated reads; Context-block reads;
cross-project isolation; deletion behavior; bounded list query count; and the
source-grounded relationship path on fresh PostgreSQL.

### Batch 3 — Make maintenance reviews evidence-aware

#### Commit 3.1 — Review evidence bundle reads

Files:

- `core/project/maintenance_service.py`;
- `core/knowledge/db/writers/maintenance_review_writer.py` only if its read
  surface belongs there today;
- `common/schema/public.py` if the current local API exposes review detail;
- maintenance service and API contract tests.

Work:

- add a project-scoped review-detail operation that resolves review pointers
  through `EvidenceService`;
- return the stored immutable review snapshot separately from current evidence;
- report missing/retired/current evidence rather than silently dropping it;
- bound the response and preserve existing authorization.

Acceptance:

- a relationship review explains all current Context/source support;
- stale evidence remains visible as stale without becoming active again;
- list endpoints remain lightweight and do not hydrate full bundles;
- no external Context write endpoint is added.

#### Commit 3.2 — Durable evidence snapshot normalization

Files:

- `maintenance_reviews.py`;
- `maintenance_review_writer.py`;
- `schema.sql` only if the clean snapshot shape requires constraints;
- PostgreSQL contracts.

Work:

- replace loosely shaped snapshot payloads with a versioned, bounded evidence
  summary contract;
- store stable pointers, counts, state token, and bounded display facts;
- keep current live traversal outside the immutable snapshot;
- include the snapshot in review signatures where it affects deduplication.

Acceptance:

- review creation cannot persist unbounded or arbitrary nested snapshots;
- identical evidence produces one open review under the existing dedupe rule;
- changed evidence creates a new/stale decision according to an explicit test;
- existing review transition atomicity remains intact.

### Batch 4 — Separate deterministic Context retirement from review work

#### Commit 4.1 — Classify maintenance evidence origin

Files:

- `conflict_discovery_reader.py`;
- `relationship_observation_reader.py`;
- `conflict_discovery.py` and advisory builders only where classification is
  consumed;
- focused and PostgreSQL tests.

Work:

- identify whether each active observation is Context-linked or independent;
- continue excluding retired observations;
- make deterministic Context-reconciliation audit rows non-reviewable;
- retain active independently supported observations even when an old Context
  block was replaced;
- avoid using Context prose as model instructions.

Acceptance:

- replacing a Context block cannot create a conflict or advisory by itself;
- two active independently supported observations can still create a review;
- mixed Context-linked and independent observations in one review group are not
  discarded once the independent evidence satisfies the review threshold;
- discovery cursors advance without repeatedly reconsidering excluded rows.

#### Commit 4.2 — Consolidate provenance packet building

Files:

- `conflict_discovery.py`;
- `relationship_advisories.py`;
- `evidence_service.py`;
- packet/advisory tests.

Work:

- use evidence bundles for bounded review context instead of bespoke raw-row
  provenance assembly;
- retain the existing token ceiling and compaction behavior;
- label evidence as untrusted data;
- require model findings to cite valid observation pointers;
- keep advisory thresholds and conflict semantics unchanged.

Acceptance:

- token and row ceilings are enforced before the model call;
- fabricated pointer citations are rejected;
- chronological change is not automatically classified as conflict;
- no model decision is applied without the existing review workflow.

#### Batch 3 completion record

Completed with one versioned, bounded `EvidenceSnapshot` contract:

- review snapshots now contain only stable pointers, bounded display facts,
  counts, truncation state, and a deterministic state token;
- arbitrary nested snapshot dictionaries are rejected, and detection metadata
  moved into the typed conflict plan where it belongs;
- review signatures include the normalized snapshot, so changed evidence state
  cannot silently deduplicate to the old decision;
- project review detail returns the stored snapshot separately from current
  evidence bundles and reports current, changed, or partially unavailable state;
- list reads remain storage-only and do not trigger live evidence traversal.

No public Context mutation operation or external-agent integration was added.

#### Batch 4 completion record

Completed against the relationships the current schema can actually represent:

- active observations are classified as `context` when linked through
  `relationship_observation_blocks`, otherwise as `independent`;
- the schema does not contain a second per-observation support relation, so a
  fictional `both` state was not introduced;
- retired observations are excluded from both conflict and advisory reads;
- Context-only conflict seeds advance the durable cursor without an LLM call;
- advisory thresholds must be satisfied by independent observations, while
  Context-linked observations in the same qualifying group remain visible;
- conflict packets use bounded evidence bundles for provenance identities,
  status, truncation, and state tokens, label all packet material as untrusted,
  and continue rejecting citations outside the packet;
- review application rejects a changed live evidence token before mutation.

Existing Context reconciliation remains the sole owner of deterministic
retirement, and all model findings still pass through maintenance reviews.

### Batch 5 — Add typed maintenance impact previews

#### Commit 5.1 — Impact contracts and planner

Files:

- new `core/knowledge/maintenance_impact.py`;
- common schema only if the result crosses an application boundary;
- focused tests.

Work:

- define closed impact kinds for canonical rows and rebuildable projections;
- build previews for relationship reinterpretation, entity merge, merge rollback,
  entity-context reclassification, and relationship-domain changes;
- identify affected observations, relationships, Context block/entity links,
  Episodes, AGE projections, search projections, and live entity caches;
- distinguish direct mutation from derived rebuild;
- bound IDs and counts, returning truncation explicitly.

Acceptance:

- every currently supported plan kind has a preview or explicitly declares no
  applicable impact;
- preview performs no writes;
- expected state includes the evidence-state token and relevant domain/frontier
  versions;
- unsupported plan kinds fail closed.

#### Commit 5.2 — Expose preview through the maintenance owner

Files:

- `core/project/maintenance_service.py`;
- existing project/runtime port and public contracts if already exposed;
- service tests.

Work:

- add review preview beside existing list/transition operations;
- require the same project ownership checks used by apply;
- return review plan, evidence summary, and typed impact separately;
- do not add a generic evidence mutation API.

Acceptance:

- callers can inspect consequences before applying a review;
- cross-project and stale reviews are rejected;
- preview contains no raw SQL, model prompt, or hidden internal exception.

### Batch 6 — Bind apply-time validation to evidence and impact state

#### Commit 6.1 — Stale evidence guard

Files:

- `core/project/maintenance_service.py`;
- applicable maintenance writers;
- review application tests.

Work:

- recompute the evidence-state token immediately before mutation;
- compare domain version, frontier/expected state, and evidence state under the
  existing maintenance lock;
- mark or report the review stale if evidence changed;
- preserve each writer's current transactional compare-and-update checks.

Acceptance:

- support added/retired after preview prevents stale application;
- unchanged evidence applies through the existing typed writer;
- no partial canonical mutation occurs on stale state;
- dismiss remains a status-only operation.

#### Commit 6.2 — Projection and cache recovery contract

Files:

- existing maintenance writers and projection rebuilder;
- health/diagnostic contracts only if current coverage is insufficient;
- PostgreSQL and failure-injection tests.

Work:

- inventory the projections required by each mutation plan;
- make canonical mutation completion distinguishable from projection completion;
- retry projection/cache repair without repeating the canonical mutation;
- reuse existing projection rebuild and cache invalidation owners;
- add durable repair state only if a real failure-injection test proves current
  state cannot recover after restart.

Acceptance:

- failure after canonical commit resumes projection repair only;
- merge/rollback/reclassification is not applied twice;
- affected project projections and live caches converge;
- health exposes bounded repair state without evidence content.

### Batch 7 — Delete duplication and close the audit

#### Commit 7.1 — Replace proven duplicate provenance readers

Files:

- only callers identified in Batch 1;
- associated unit/PostgreSQL tests.

Work:

- migrate provenance-shaped reads to `EvidenceService`;
- retain transaction-local validation joins where atomicity requires them;
- remove superseded helpers, DTOs, and duplicate tests;
- do not retain old and new evidence paths as fallbacks.

Acceptance:

- one read owner exists for cross-layer evidence traversal;
- mutation writers still validate inside their transactions;
- production line count/complexity does not grow solely to preserve old paths;
- dead-symbol and import scans are clean.

#### Commit 7.2 — Documentation and completion audit

Files:

- semantic/maintenance operations documentation;
- this plan;
- architecture checks.

Work:

- document evidence ownership, traversal limits, and review/apply lifecycle;
- document the deterministic Context versus judgment-based maintenance boundary;
- map each acceptance scenario to an exact test and boundary;
- record unavailable service/model checks without presenting them as passing.

Acceptance:

- a developer can identify the owner of every evidence edge and mutation;
- no documentation says Context replaces maintenance;
- no external Context integration appears in production;
- all required validation gates below have terminal results.

#### Batch 5 completion record

- added a closed impact vocabulary that separates direct canonical mutations,
  derived rebuilds, and live cache invalidation;
- every typed maintenance plan returns either bounded impacts or an explicit
  no-applicable-impact explanation;
- impact identifiers are capped at 128 with totals and truncation reported;
- project review preview composes current evidence detail with impact and rejects
  resolved, missing, cross-project, or evidence-stale reviews;
- the planner projects only consequences established by current owners: it does
  not invent search rebuilds for relationship interpretation or Domain changes.

#### Batch 6 completion record

- apply recomputes live evidence under the existing maintenance lock before any
  relationship-interpretation mutation;
- existing writer transaction checks for domain, frontier, definition, and row
  state remain authoritative;
- entity merge and rollback now persist a bounded projection-repair marker after
  canonical commit when AGE rebuilding fails;
- repair reloads the durable merge audit and retries only affected projections,
  then `ProjectManager` repeats only the derived cache invalidation;
- engine health exposes bounded pending-repair count/truncation without evidence
  or exception content. No additional repair table was necessary because the
  existing durable merge audit already owns this lifecycle.

#### Batch 7 completion record

- removed the conflict writer's duplicate observation-provenance query;
- retained transaction-local joins in semantic commit, interpretation, Episode,
  merge, and deletion owners where atomic validation requires them;
- documented evidence edge ownership, hard traversal bounds, Context versus
  maintenance responsibility, preview/apply behavior, and retry-only recovery;
- confirmed no Redis evidence authority, external Context mutation API, generic
  evidence write API, compatibility parser, or second maintenance owner exists.

---

## 7. Required end-to-end scenarios

1. An assistant-owned source supports a Context block and relationship; review
   detail resolves the complete chain.
2. A user-message-supported Context relationship resolves without a source ref.
3. Replacing the only supporting Context block retires the observation and opens
   no maintenance review.
4. Replacing one Context support preserves an observation with another active
   support.
5. Independent contradictory active observations remain reviewable.
6. A fabricated or cross-project evidence pointer is rejected.
7. Review evidence changes after preview; apply fails stale with no mutation.
8. An unchanged review applies once and records its transition atomically.
9. Entity merge preview lists Context associations and projections affected.
10. Projection failure after applied maintenance resumes repair without applying
    the canonical mutation twice.
11. Project deletion removes evidence edges, reviews, and repair state.
12. Large evidence sets truncate deterministically within configured limits.

Every persistence or deletion claim must run against fresh PostgreSQL. Packet
compaction and limit combinations may remain focused unit tests. At least one
review scenario must compose the real evidence service, maintenance service, and
PostgreSQL readers/writers while faking only model results.

### Completion mapping

| Scenario | Exact validating boundary |
| --- | --- |
| 1 | `test_project_semantic_job_commits_source_grounded_relationship_provenance` |
| 2 | `test_evidence_service_handles_assistant_support_and_deduplicates_paths` plus the user-message row in `test_evidence_service_builds_deterministic_bundle_with_one_query` |
| 3 | `test_semantic_commit_is_atomic_idempotent_and_retracts_replaced_support` |
| 4 | The same PostgreSQL contract first persists two distinct block-support edges for one observation; `_retire_noncurrent_observations` is guarded by current-revision `NOT EXISTS`, so retirement occurs only after neither support remains current. |
| 5 | `test_context_only_observations_do_not_create_advisory_but_mixed_groups_survive` and `test_packet_includes_bounded_direct_histories_of_both_endpoints` |
| 6 | `test_evidence_pointers_validate_closed_identifier_shapes` and the cross-project assertion in `test_project_semantic_job_commits_source_grounded_relationship_provenance` |
| 7 | `test_changed_evidence_prevents_apply_before_canonical_mutation` |
| 8 | `test_project_review_application_reinterprets_before_marking_applied` |
| 9 | `test_merge_preview_lists_context_scopes_projection_and_cache_impacts` |
| 10 | `test_failed_merge_projection_repair_does_not_repeat_canonical_merge` |
| 11 | `test_project_deletion_executes_complete_aggregate_against_postgres` plus the post-deletion evidence assertion in `test_project_semantic_job_commits_source_grounded_relationship_provenance` |
| 12 | `test_evidence_service_reports_leaf_truncation_without_dangling_edges` |

---

## 8. Validation gates

### Per commit

- focused tests for changed contracts and callers;
- Ruff on touched Python paths;
- Python compile/import checks;
- `git diff --check`;
- architecture/import checks when ownership changes.

### Per batch

- all unit tests for evidence and affected maintenance subsystem;
- fresh-PostgreSQL contracts for schema/read/write changes;
- deletion and cross-project isolation tests;
- failure injection for mutation/recovery changes;
- inspect the diff for parallel owners, generic unused abstractions, and test-only
  production code.

### Final gate

- all twelve end-to-end scenarios mapped to exact tests;
- complete focused evidence/maintenance suite;
- all fresh-PostgreSQL evidence, Context, maintenance, and deletion contracts;
- complete service-free suite, with heavyweight model checks reported separately
  if they remain an environment-specific lane;
- dependency consistency and architecture checks;
- no legacy compatibility code, unused table, or external Context API;
- final review confirms that the evidence framework reduced duplicate read logic
  without weakening transaction-local mutation validation.

---

## 9. Stop conditions

Pause implementation and revise this plan if:

- the shared reader requires a polymorphic evidence table for the first two
  callers;
- a proposed abstraction has only one production caller;
- maintenance application would rely on a previously fetched bundle instead of
  transaction-time validation;
- Context replacement would retire independently supported evidence;
- evidence traversal cannot be strictly project-scoped and bounded;
- a new worker, queue, cache authority, or external API becomes necessary.

Those conditions indicate an ownership or scope change requiring an explicit
decision, not an implementation detail to add opportunistically.
