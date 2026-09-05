# Knoggin Project Context + Context-First Ingestion Refactor

## Status

This is the implementation plan for replacing Knoggin's message-local semantic
ingestion path with a project-scoped, Context-first path.

The architectural direction and batch/commit boundaries are approved.

Implementation progress:

- Batch 1 — complete: contracts, configuration, target-state schema, and
  Context/window storage ports are in place and covered by focused real
  PostgreSQL contracts.
- Batch 2 — complete: deterministic Context materialization, atomic revision
  publication, controlled `CONTEXT.md` projection/import, and document/workspace
  reservation are in place and covered by focused unit and PostgreSQL contracts.
- Batch 3 — complete: assistant/user exchanges now close durably and
  idempotently; whole exchanges are admitted into frozen project windows under
  the configurable 128K policy; and a disabled project semantic job owns the
  future scheduler boundary. The old session ingestion worker remains active
  until Batch 8.
- Batch 4 — complete: Episode generation now consumes frozen project-window
  evidence through a standalone generator. Its narrative result, including an
  explicit zero-result, is persisted idempotently against the window without
  touching legacy Episode checkpoints or graph links.
- Batch 5 — complete: the disabled project semantic job now reconciles one
  Context revision from a frozen window after its durable Episode result.
  Model operations use local Context/message/source/Episode handles; canonical
  Context remains authoritative if `CONTEXT.md` projection is temporarily
  unavailable. Context stage completion remains disabled in normal runtime
  until the Batch 8 cutover.
- Batch 6 — complete: Context-first VP-01 entity extraction now builds typed,
  block-version-scoped pending results from a frozen Context impact closure.
  GLiNER2.5 receives only typed domain labels/descriptions; evidence and exact
  policy state reload durably, but no live Knowledge mutation occurs until
  Batch 7's single reconciliation transaction.
- Batch 7 — complete: Context-native VP-02 now uses only configured LLM calls
  over current eligible block versions. One atomic reconciliation transaction
  validates the frozen window/revision, retracts stale block-backed evidence,
  updates SQL and AGE graph state, and advances `context_committed` to
  `knowledge_committed`. Episode enrichment, completion, and separately
  retryable maintenance recording are durable. The semantic runtime remains
  disabled until Batch 8 cutover.
- Batch 8 — complete: Agent reads the committed database Context directly;
  the enabled project semantic job is the sole normal owner; and the old
  per-session message-local worker, claims, checkpoints, and provenance shape
  are deleted.
- Batch 9 — complete: routine Context replacement now retires evidence with an
  audit rather than a review and retired evidence cannot seed conflict
  discovery. Runtime uses the blank spaCy English tokenizer for alias matching,
  without downloading or loading `en_core_web_md`. The GLiNER2.5-only VP-01
  benchmark has frozen Context fixtures, quality/resource gates, and no
  fallback path; VP-02 remains LLM-only. Batch 10 remains pending.

This document supersedes the earlier exploratory version. Choices described as
**locked** are the target behavior. Explicit exclusions are outside this
refactor and must not be added opportunistically.

Knoggin is unreleased. Prefer a clean target-state schema and delete obsolete
code after cutover. Do not build compatibility shims for the old ingestion path.

---

## 1. Outcome and Scope

### 1.1 Goal

Keep canonical messages as exact evidence, but stop using individual messages
as the primary semantic extraction surface.

The new flow is:

```text
closed conversation exchanges
        ↓
one durable project semantic window
        ↓
Episode narrative generation
        ↓
Project Context revision
        ↓
changed Context impact closure
        ↓
entity extraction and resolution
        ↓
relationship extraction
        ↓
atomic Knowledge reconciliation
        ↓
Episode graph enrichment
        ↓
maintenance only for unresolved ambiguity
```

### 1.2 Representation boundaries

```text
Messages
= exact canonical conversation evidence

Episodes
= bounded historical narrative: what happened

Project Brief (PROJECT.md)
= user-owned project description and instructions

Project Context (CONTEXT.md projection)
= engine-maintained current understanding

Knowledge
= structured entities, relationships, and provenance

Notebook
= temporary Agent-run working state
```

These representations must not silently become aliases for one another.

### 1.3 In scope

- explicit exchange completion;
- project-scoped semantic admission;
- one durable multi-session window;
- Episode generation from that window;
- revisioned Project Context with block provenance;
- user-editable `CONTEXT.md` imported through a controlled Context revision;
- entity and relationship extraction from Context blocks;
- deterministic retraction after Context replacement/deletion;
- Agent reads of the latest committed Context;
- removal of the old per-session semantic ingestion owner;
- recovery, observability, and focused evaluation.

### 1.4 Explicit exclusions

- external Context-write integration, including a Codex/Claude connector or an
  SDK, MCP, or HTTP endpoint that lets another process mutate Context;
- a generic repository-wide evidence framework;
- replacing all maintenance with Context logic;

These are not required follow-on tasks for this refactor. They would add a new
integration surface or a broader abstraction without a present caller.

| Excluded item | What remains in this refactor | Consider only when |
|---|---|---|
| Codex/Claude or other external Context writer; SDK/MCP/HTTP API | Internal readers plus the controlled user-file importer only | An actual integration is being built and its authorization/provenance contract is known |
| Generic evidence framework | Context-specific block/support tables | A second real subsystem needs the same evidence lifecycle |
| Full maintenance replacement | Deterministic Context retraction plus existing review paths | Conflict/review metrics show a narrower service is justified |

The following are **not** excluded: Context persistence, user editing through
the controlled importer, the project semantic window, exchange completion,
Episode/Context ordering, Context-first entity and relationship extraction,
graph retraction, Agent reads, and restart recovery. VP-02 remains an LLM call;
GLiNER2.5 is the default VP-01 model.

---

## 2. Current-Code Constraints

The implementation must begin from these current facts.

### 2.1 Semantic ingestion is session-owned

- `runtime/session_runtime_factory.py` creates one `IngestionWorker` per
  session.
- `core/ingestion/worker.py` claims and processes only that session's user
  messages.
- `MessageLifecycleWriter` claims `role='user'` rows by session.
- `GraphWriter` verifies the exact single-session claim before committing.

Therefore Project Context cannot be inserted as another stage inside the
current worker. Cross-session Context needs a new project owner and checkpoint.

### 2.2 The exchange is not the current claim unit

- The user message is persisted and signals ingestion immediately.
- The assistant message is saved later.
- Assistant messages are currently marked `ingestion_state='excluded'`.
- `messages.user_msg_id` already links an assistant response to its user turn.
- A user message can exist without an assistant row after cancellation/failure.

The new path needs an explicit durable exchange outcome. Assistant-row presence
alone is not an adequate completion marker.

### 2.3 `PROJECT.md` already has a separate contract

`PROJECT.md` is created per project, user-owned, supplied to Agent runs, and
handled through the project-file/document service. It must not be silently
converted into model-owned current memory.

### 2.4 Episodes already have useful project-wide behavior

`EpisodeJob` merges ready exchanges across sessions while preserving per-session
FIFO. Its generation and consolidation behavior is useful, but it owns an
independent trigger/window and `EpisodeWriter` currently performs graph
enrichment before persistence.

Keep the narrative behavior. Remove the competing window ownership and separate
narrative persistence from graph enrichment.

### 2.5 Relationship evidence is currently message-shaped

`RelationshipWrite` and `relationship_observations` currently require one
`message_id`. Context relationships may need several Context blocks, and each
block may be supported by several messages. Repeating one observation per
message would inflate provenance and complicate retraction.

### 2.6 Domain entity descriptions already exist

`DomainConfig` already accepts and compiles entity-type descriptions. Do not add
a second description field. The GLiNER2.5 VP-01 adapter consumes the existing
compiled descriptions as its model-facing typed extraction guidance.

### 2.7 The scheduler is already generic underneath

`infrastructure.job.scheduler.Scheduler` can run project jobs, while
`EpisodeScheduler` is a narrow one-job wrapper. Reuse the generic scheduler;
do not introduce a second orchestration framework.

---

## 3. Locked Architectural Decisions

### 3.1 Ownership

One `ProjectSemanticJob` owns semantic processing for a project.

It is registered on the existing project scheduler and serializes windows for
that project. Session runtimes only:

1. persist canonical conversation state;
2. close the exchange durably;
3. wake the project scheduler.

The database remains authoritative. A missed wake-up must only delay work until
the next scheduler poll; it must never lose work.

Redis is not reintroduced as semantic storage, a Context queue, or a checkpoint
authority. Any existing Redis conversation buffer/wake behavior remains
transient infrastructure; PostgreSQL is the recovery source for messages,
windows, Context revisions, and Knowledge.

### 3.2 Canonical Context and file projection

PostgreSQL Context revisions and blocks are canonical.

`CONTEXT.md` is a deterministic projection of the latest committed revision and
is also user-editable. A user file edit is not accepted by simply overwriting
the database. The Context importer parses the file, compares it with the last
known projection hash, and creates a new human-authored Context revision through
the same block/retraction path.

The database remains authoritative after an edit is accepted. If the file is
stale relative to a newer database revision, the importer reports a conflict
and preserves the database revision; it does not silently discard either side.
If projection fails after a database commit, a reconciler rewrites the file.
`PROJECT.md` remains the separate user-owned Project Brief and instruction
source.

`CONTEXT.md` is excluded from ordinary document indexing. Generic Agent
workspace tools do not receive unrestricted Context mutation; user edits enter
through the controlled importer. Public/external mutation APIs are outside this
refactor.

### 3.3 Context structure

Context is natural Markdown organized into configured sections. Initial default
sections are:

- Current State
- Active Work
- Decisions and Constraints
- Preferences
- Open Questions

Durable Context does not contain extraction instructions. Stable extraction
guidance belongs in `DomainConfig`; user project instructions remain in
`PROJECT.md`.

### 3.4 Context blocks and revisions

A block is a paragraph or coherent list item, not necessarily one atomic fact.
The model sees revision-local handles such as `C1` and `C2`; those handles do not
persist in Markdown. The server assigns durable UUID block-version IDs.

Unchanged blocks reuse their immutable block version in the next revision.
Replacement creates a new block version with `supersedes_block_id`; deletion
omits the prior block from the new materialized revision.

Every committed revision is a complete materialized snapshot, not a replay-only
delta. The updater returns structured `ADD`, `REPLACE`, and `DELETE` operations,
but the server validates and applies them to create the next full snapshot.

For editable `CONTEXT.md`, the renderer writes a non-visible block marker before
each block. The marker is an implementation detail, not a user-facing ID. The
importer uses it to preserve support for an unchanged block. Its rules are:

- an unchanged marked block reuses its prior block version and support;
- edited marked text becomes a new `human_asserted` block version and drops the
  old support unless a later semantic window independently supports it;
- an unmarked added block becomes `human_asserted` with no fabricated message
  support;
- a removed marked block is a DELETE and enters normal Knowledge retraction;
- malformed sections/markers fail closed and leave the prior revision current.

An accepted file edit creates a Context revision with `origin=human_edit` and
an empty-message semantic window already positioned at `context_committed`.
The project job then runs the same Context impact closure and Knowledge
reconciliation used for conversation updates. This keeps manual edits possible
without pretending they came from a chat message.

### 3.5 Evidence and trust

Every generated Context block stores one assertion kind:

```text
user_asserted
source_grounded
agent_derived
human_asserted
```

Rules:

- `user_asserted` requires at least one supporting canonical user message.
- `source_grounded` requires an assistant message plus one or more
  `message_source_refs` owned by that assistant message.
- `agent_derived` may be retained as useful working understanding, but is not
  eligible for Knowledge extraction in the first implementation.
- `human_asserted` is created only by an accepted user edit to `CONTEXT.md`.
  It may be eligible for Knowledge extraction with Context-block provenance,
  but it has no invented canonical-message support and creates no
  `message_entity_refs` unless a real message also supports the block.
- An assistant restatement of a user claim should cite the user message and be
  stored as `user_asserted`, not treated as independent assistant evidence.
- The server validates ownership of every referenced message/source reference.
- No model-generated local handle is accepted as a durable identifier.

This prevents ungrounded assistant prose from automatically becoming graph
truth while still allowing it to be visible as lower-trust Context.

### 3.6 Exchange completion

Add explicit exchange state to the canonical user-message row:

```text
exchange_state   = open | closed
exchange_outcome = assistant_final | clarification | failed | cancelled | user_only
exchange_closed_at_ms
```

Rules:

- A normal assistant save atomically writes the assistant message, sources and
  artifact, then closes the linked user exchange.
- Clarification is a closed exchange and may enter a window.
- Failure/cancellation closes the exchange without inventing an assistant row;
  the user evidence remains eligible.
- Direct user-only callers explicitly close with `user_only`.
- Admission waits for the selected user revision to be sealed.
- `open` exchanges never block eligible exchanges from other sessions.
- Per-session FIFO is preserved: later exchanges in the same session cannot
  pass an earlier open/unsealed exchange.

### 3.7 Window admission

A semantic window contains whole closed exchanges from one project, possibly
from several sessions.

The sole new size policy is:

```text
semantic_window_tokens = 128_000
```

`semantic_window_tokens` is configurable per developer policy and is the
admission trigger. Do not introduce separate user-facing target, hard-cap,
context-cap, or overfill settings for the same window.

A window closes when any of the following is true:

- adding the next complete exchange reaches or crosses the target;
- the semantic job's fixed idle-flush safety interval or an explicit project
  flush closes a partially filled window;
- a participating session closes;
- one oversized exchange must run alone.

The token estimator is the existing LLM token counter. Persist the estimator
name/version and measured token count with the window so retries do not select a
different boundary. Start the semantic job with a fixed 300-second idle flush;
it is an operational safety constant, not a second Context-size tuning knob.

#### Good overfill

Overfill is expected because an exchange cannot be split. Let `T` be the
configured target, `B` the source-token count before the crossing exchange, and
`E` the complete crossing exchange:

```text
B < T
F = B + E
overfill = F - T
```

This is **good overfill** when:

1. `B < T`;
2. `E` is one complete closed exchange;
3. `F` is the final window count—no second exchange is added after the target
   is crossed; and
4. the stage can execute `F` through bounded model-input packing.

The overfill is therefore unavoidable and bounded by the exchange that crossed
the target (`overfill <= E`). A 128K target followed by a 6K exchange producing
134K is good overfill. Continuing to add another 20K exchange after that is not.

If a single exchange is itself larger than the provider request budget, it is an
oversized exchange: keep it as one logical window, execute its stages in bounded
chunks, and retain one provenance/checkpoint boundary. Do not add a second
`max_source_tokens` setting merely to force a split.

The Context/episode/prompt overhead is handled by stage-local input packing. It
does not change window membership, and it must never silently truncate Context,
messages, or provenance. Record `source_token_count`, `overfill_tokens`, and
`overfill_ratio` for evaluation.

If the complete Context plus the admitted evidence cannot fit in one provider
request, use bounded stage-local packing/repair while preserving the same
logical window and all block/source references. Never silently truncate
canonical Context or discard block provenance. A Context-size setting is not
added separately from the 128K admission target.

### 3.8 Cross-session ordering

- Preserve FIFO within each session.
- Sort eligible exchanges by user source timestamp, then message ID.
- Do not globally block a project because one session has an incomplete turn.
- Serialize committed Context revisions by project.
- Store processing time separately from source time.
- Late older evidence is shown to the updater with its real source timestamp and
  must not replace newer Context merely because it was processed later.

### 3.9 Stage boundaries and recovery

The durable window checkpoint is intentionally small:

```text
claimed
context_committed
knowledge_committed
completed
```

Failure metadata is stored separately: attempt count, last failure stage/code/
time/summary, and next retry time. A failure never erases the last successful
stage.

- Before `context_committed`, retry Episode generation/Context update for the
  same frozen message membership.
- After `context_committed`, retry from the persisted Context revision without
  calling the updater again.
- After `knowledge_committed`, retry only Episode enrichment/maintenance and
  finalization.
- Maintenance failure does not roll back Context or Knowledge.
- Exhausted retries remain inspectable and manually retryable; messages are not
  silently released into a differently shaped window.

An exhausted active window intentionally blocks newer Context revisions for the
same project until it is retried successfully. This preserves revision order
and must surface as degraded project health rather than becoming a silent gap.

### 3.10 Episode ordering

Episode narratives are generated before Context and persisted idempotently for
the same semantic window. Graph associations are added only after Knowledge is
committed. Zero generated Episodes is a successful recorded result.

### 3.11 Context impact closure

Downstream extraction processes a bounded impact closure:

1. every added/replaced block;
2. the prior block version for every replacement/deletion, for retraction;
3. immediate neighboring blocks in the same section;
4. unchanged blocks explicitly referenced as dependencies by updater output.

Only current, Knowledge-eligible blocks are extraction inputs. Prior versions
are reconciliation inputs, not new factual evidence.

### 3.12 Knowledge reconciliation

Normal Context evolution is deterministic reconciliation, not a maintenance
review. In one transaction:

1. persist entity writes/alias updates;
2. persist current block-to-entity associations;
3. retire relationship observations whose support intersects replaced/deleted
   block versions;
4. insert new observations and their block support set;
5. update/remove relationship aggregates with no active support;
6. write literal `message_entity_refs` only for messages that actually mention
   the entity or are explicitly validated as its evidence;
7. advance the window to `knowledge_committed`.

Maintenance remains for identity ambiguity, independent contradictory evidence,
and interpretations that cannot be resolved by block supersession.

#### What “replace maintenance with Context logic” means

The earlier proposal did **not** mean deleting the maintenance subsystem or
pretending every conflict is solved by the updater. It meant changing the first
line of responsibility:

1. A normal current-state change is represented as Context `REPLACE`/`DELETE`.
2. The semantic commit retires graph observations supported only by the old
   block version and writes observations for the new current block.
3. Routine supersession therefore does not create a maintenance review.
4. Maintenance still handles unresolved identity ambiguity, independently
   sourced contradictions, uncertain interpretations, and explicit review or
   detachment decisions.

In short, Context owns deterministic current-state reconciliation; maintenance
owns cases that require judgment after reconciliation. The first cut narrows
maintenance input but does not fully replace it.

### 3.13 Source time

Context revisions have processing timestamps. Extracted observations use source
time. For a current-state observation supported by multiple messages, use the
most recent supporting timestamp as `observed_at_ms`, while retaining all
support links. Missing source timestamps remain explicit; do not replace them
with ingestion time.

### 3.14 Model sequencing

VP-01 switches from the current `gliner` integration to the local
`gliner2` package and `fastino/gliner2.5-base-v1` as the default English model.
Load it through `AutoExtractor.from_pretrained(...)`, which dispatches safely by
checkpoint architecture. Use `fastino/gliner2.5-multi-v1` only when a project
is explicitly configured for multilingual extraction.

GLiNER2.5 is chosen over the GLiNER2 span default because it is the current
recommended English multi-task checkpoint, uses sparse boundary decoding rather
than a fixed-width span grid, and supports spans of any length inside its encoded
input window. The base checkpoints are comparable in size: GLiNER2 base is 205M
parameters, while GLiNER2.5 base is 194M.

GLiNER2/2.5 take typed schemas: entity labels plus model-facing descriptions.
For Knoggin, `DomainConfig.entity_types[].description` is the VP-01 instruction
channel. It can express guidance such as “a person actively responsible for
project work, not a merely mentioned public figure.”

This is schema-conditioned extraction guidance, not an arbitrary LLM system
prompt. Do not concatenate free-form project instructions into the local model
input and assume it will obey them. General interpretation remains the Context
updater/VP-02 LLM responsibility.

VP-02 is always an LLM call over Context blocks and resolved entity candidates.
Its output remains subject to server-side vocabulary, direction, symmetry,
endpoint, and provenance validation. GLiNER relation/classification features
are deliberately out of scope for VP-02.

The Context updater uses the existing `merge_model` role because its job is
state reconciliation. Episode generation and VP-02 retain their current LLM
roles. VP-01 uses GLiNER2.5. Do not add a fourth configurable Context model
unless evaluation shows the existing role is unsuitable.

Replace `en_core_web_md` with `spacy.blank("en")` only after lowercase/alias
quality tests cover removal of the current POS fallback.

### 3.15 Migration and cutover safety

There must never be two live semantic writers for the same project evidence.

- Batches 1-7 build and test the new path without registering it as the normal
  runtime owner.
- Evaluation uses fixture/test databases or an explicit non-writing harness.
- The new path may persist its own Context/window rows during controlled tests,
  but it must not write production entities, relationships, or duplicate
  Episodes while the old jobs remain authoritative.
- Batch 8 is the single cutover: it disables/removes old ownership and enables
  the complete new path together.
- Any temporary development switch or comparison hook is deleted in Batch 8.

Do not run message-first and Context-first graph commits side by side and try to
reconcile their output afterward.

---

## 4. Target Durable Model

Names may be adjusted to repository conventions, but ownership and constraints
should remain.

### 4.1 Message exchange fields

Add `exchange_state`, nullable `exchange_outcome`, and nullable
`exchange_closed_at_ms` to `messages`. Constraints apply meaningful values to
user rows. Assistant rows continue to link through `user_msg_id`.

### 4.2 Semantic windows

`project_semantic_windows` stores:

```text
window_id uuid primary key
user_name / project_id
origin = conversation | human_edit
stage
domain_version
policy_snapshot jsonb
source_token_count / token_estimator
episode_result_recorded
context_revision_id nullable
attempt/failure/retry fields
claimed_at / updated_at / completed_at
```

`project_semantic_window_messages` stores:

```text
window_id / message_id / session_id
exchange_user_message_id / role / ordinal
primary key (window_id, message_id)
unique (message_id)
```

Membership stores identity/order, not copied content. Only one non-completed
window per project is allowed, enforced by a partial unique index.

### 4.3 Context revisions and blocks

Use:

```text
project_contexts
  user_name / project_id / current_revision_id
  projection_hash / projection_synced_at

project_context_revisions
  revision_id / project_id / revision_number
  parent_revision_id / window_id / origin / domain_version
  edit_summary / content_hash / created_at

project_context_blocks
  block_id / project_id / section_key / markdown / content_hash
  assertion_kind / supersedes_block_id / source_time_ms / created_at

project_context_revision_blocks
  revision_id / block_id / ordinal

project_context_block_supports
  block_id / message_id / source_ref_id nullable / support_kind
```

Use a PostgreSQL-safe uniqueness shape for nullable `source_ref_id`; do not rely
on ordinary nullable uniqueness to reject duplicate supports.

### 4.4 Knowledge support

Add `context_block_entities(block_id, entity_id, mention_text)` and
`relationship_observation_blocks(observation_id, block_id)`.

In the final target schema, `relationship_observations` no longer requires one
`session_id/message_id`. Provenance resolves:

```text
relationship observation
    → Context block versions
    → supporting messages/source refs
    → sessions and canonical sources
```

Do not add message-ID arrays to relationship rows.

### 4.5 Context edit contract

The updater returns typed `ContextAdd`, `ContextReplace`, and `ContextDelete`
operations plus explicit dependencies on unchanged local Context refs.

Validation requires:

- only rendered local handles;
- one change per target;
- canonical configured section keys;
- bounded nonblank Markdown;
- support from the same project and frozen window;
- source refs owned by a cited assistant message;
- at least one current-window reason for add/replace/delete;
- valid assertion-kind/support combinations;
- server-assigned durable IDs.

The LLM operation contract requires current-window support for every factual
change. The file-import contract is separate: it accepts a validated
`expected_projection_hash`, creates `human_asserted` blocks where text changed,
and may create an empty-message `human_edit` window for downstream
reconciliation.

---

## 5. Runtime Component Shape

### Add

Suggested modules:

```text
common/schema/context.py
common/schema/semantic_window.py
core/knowledge/context/models.py
core/knowledge/context/policy.py
core/knowledge/context/render.py
core/knowledge/context/importer.py
core/knowledge/context/prompts.py
core/knowledge/context/updater.py
core/ingestion/project_semantic_job.py
core/ingestion/semantic_window.py
core/knowledge/db/readers/project_context_reader.py
core/knowledge/db/readers/semantic_window_reader.py
core/knowledge/db/writers/project_context_writer.py
core/knowledge/db/writers/semantic_window_writer.py
core/knowledge/db/writers/semantic_commit_writer.py
```

Use fewer modules if responsibilities remain cohesive. Do not create manager,
service, repository, factory, and controller layers for the same operation.

### Repurpose

- `EpisodeJob` generation/consolidation → `EpisodeGenerator` accepting a frozen
  semantic window.
- `ProjectEpisodeBuild` → build from supplied canonical messages.
- `TextProcessor` → Context-block entity extractor.
- `EntityResolver` → resolve block mentions and return block evidence.
- `RelationshipExtractor` → return multi-block support.
- generic `Scheduler` → project semantic job runner.
- useful `GraphWriter` logic → project-scoped semantic commit writer.

### Delete after cutover

- per-session `IngestionWorker` semantic ownership;
- `EpisodeScheduler` narrow wrapper;
- independent Episode readiness/checkpoint ownership;
- old `IngestionBatch` and `session_text` contract;
- single-message VP-02 source assumptions;
- message ingestion claim fields/methods with no remaining owner;
- trained spaCy load/prefetch and POS tie-break after its quality gate;
- temporary shadow comparison code.

---

## 6. Implementation Batches and Commit Stages

Every commit stage must compile and pass its focused non-service tests. Database
stages additionally require real-PostgreSQL contract tests. Do not combine
batches merely to reduce commit count; these are review and recovery boundaries.

Dependency order:

```text
Batch 1 ─┬─→ Batch 2 ─┐
         └─→ Batch 3 → Batch 4 ─┤
                                ↓
                              Batch 5 → Batch 6 → Batch 7 → Batch 8
                                                            ├→ Batch 9
                                                            └→ Batch 10
```

### Batch 1 — Contracts, settings, and target-state schema

**Purpose:** establish stable vocabulary and persistence without changing live
runtime behavior.

#### Commit 1.1 — Domain and policy contracts

Files:

- `server/src/common/conf/domain_config.py`
- `server/src/common/schema/settings.py`
- new Context/semantic-window schema modules
- configuration/domain tests

Work:

- add ordered Context section definitions to `DomainConfig`;
- add stable `extraction_guidance` separate from Context sections;
- add `vp01_language = en | multilingual`, defaulting to `en`, solely to
  choose the English or multilingual GLiNER2.5 checkpoint;
- compile section lookup/order into `CompiledDomain`;
- add one configurable `semantic_window_tokens` setting with retry policy;
- keep idle flush and overfill behavior as derived/implementation policy rather
  than additional user-facing size settings;
- retain existing entity descriptions without duplication;
- add strict enums/models for assertion kind, window stage, exchange outcome,
  edit operations, and block references.

Acceptance:

- unknown section keys and duplicate normalized names fail validation;
- extraction guidance is never rendered as factual Context;
- an invalid VP-01 language selection fails validation and existing domains
  receive the English default;
- existing domain fixtures receive deterministic defaults;
- settings enforce a positive target and retry limits.

#### Commit 1.2 — Bootstrap schema

Files:

- `server/src/infrastructure/schema.sql`
- schema bootstrap/domain constraint tests

Work:

- add exchange completion columns/checks;
- add semantic-window tables and indexes;
- add Context current/revision/block/snapshot/support tables;
- add `context_block_entities` and relationship block-support table;
- add project-scope foreign keys, cascade behavior, and uniqueness constraints;
- update project-deletion ownership for all new rows.

Because Knoggin is unreleased, edit the canonical bootstrap schema directly and
recreate local test databases. Do not add a production migration framework or
nullable compatibility columns solely for the old pipeline.

Acceptance:

- fresh schema bootstrap succeeds;
- cross-project references are rejected;
- a message cannot belong to two semantic windows;
- more than one active window per project is rejected;
- Context current revision must belong to the same project;
- project deletion removes all new project-owned state.

#### Commit 1.3 — Read/write ports and storage contracts

Files:

- new Context/window readers and writers
- Knowledge store composition/export modules
- storage fakes/fixtures
- focused unit and PostgreSQL contract tests

Work:

- implement Context current-revision reads;
- implement immutable block/snapshot reads;
- implement atomic window claim and exact membership reload;
- implement stage advancement with expected-stage checks;
- implement failure recording without regressing stage;
- expose narrow ports used by later jobs.

Acceptance:

- duplicate claim attempts return the existing active window or no work;
- stage compare-and-set rejects stale writers;
- membership reload returns stable order/IDs across retries;
- no LLM/runtime wiring exists yet.

### Batch 2 — Context persistence and `CONTEXT.md` projection

**Depends on:** Batch 1.

#### Commit 2.1 — Context renderer and edit applier

Files:

- `core/knowledge/context/models.py`
- `core/knowledge/context/render.py`
- unit tests

Work:

- render natural Markdown in configured section order;
- render local `C1...Cn` handles only for model/debug input;
- validate/apply ADD/REPLACE/DELETE to a materialized snapshot;
- reuse unchanged block versions;
- create replacement lineage through `supersedes_block_id`;
- compute deterministic hashes from normalized content and order;
- compute the changed impact closure.

Acceptance:

- equal inputs render byte-identically;
- local handles never appear in canonical Markdown;
- invalid/double operations fail before persistence;
- no-op updates produce no new revision;
- replacement/deletion impact closure is deterministic.

#### Commit 2.2 — Atomic Context revision writer

Files:

- `project_context_writer.py`
- `project_context_reader.py`
- PostgreSQL contract tests

Work:

- lock the current Context row;
- verify expected parent revision;
- persist new block versions/supports and full snapshot;
- advance the current pointer in one transaction;
- make `window_id` idempotent;
- return the already-committed revision on retry.

Acceptance:

- concurrent writers cannot create two children of the same current revision;
- a failed transaction leaves the prior revision current;
- retrying the same window does not create another revision;
- support refs outside project/window are rejected.

#### Commit 2.3 — Generated file projection and reservation

Files:

- `core/project/project_files.py`
- project filesystem/document reconciliation code
- Context projection component
- workspace/document tests

Work:

- reserve `CONTEXT.md` as a controlled user-editable Context file;
- exclude it from ordinary document discovery/indexing;
- write it with existing atomic filesystem replacement and non-visible block
  markers;
- store/compare projection hash;
- parse accepted user changes through a Context importer using the expected
  projection hash;
- create human-authored revisions and empty-message reconciliation windows;
- reconcile missing/stale projections from the current DB revision;
- keep `PROJECT.md` behavior unchanged.

Acceptance:

- generated Context never appears in `project_documents`;
- projection failure does not invalidate the DB revision;
- unchanged/edited/added/removed blocks receive the documented support behavior;
- stale file edits are rejected as conflicts without losing either version;
- accepted user edits trigger downstream Knowledge reconciliation.

### Batch 3 — Durable exchange completion and semantic admission — complete

**Depends on:** Batch 1. May proceed alongside Batch 2 after shared contracts
settle.

#### Commit 3.1 — Atomic exchange closure

Files:

- `runtime/session_runtime.py`
- message writer/lifecycle writer
- source-reference/artifact writer integration
- message persistence tests

Work:

- replace split assistant-save paths with one transaction that writes assistant
  content, source references, optional artifact, and closes the user exchange;
- add explicit failure/cancellation/user-only closure paths;
- preserve idempotency for repeated finalization;
- prepare the project wake edge, but retain the old user-message worker signal
  until the Batch 8 cutover.

Acceptance:

- a crash cannot leave an assistant row committed with its exchange open;
- duplicate finalization does not create a second assistant response;
- failed/cancelled turns retain the user message as closed evidence;
- open or editable user turns remain ineligible.

#### Commit 3.2 — Project window selector and claimer

Files:

- `semantic_window_reader.py`
- `semantic_window_writer.py`
- admission policy/tests

Work:

- adapt the useful per-session FIFO merge from `EpisodeReader`;
- select whole exchanges across sessions;
- apply the sole configurable target plus the fixed idle-flush, explicit flush,
  session-close, and whole-exchange crossing triggers; do not add a second cap;
- persist exact ordered membership and policy/domain snapshots;
- isolate oversized exchanges;
- use row locking/unique constraints for safe claim races.

Acceptance:

- no exchange is split;
- one blocked session does not starve another;
- later turns cannot pass an earlier blocked turn in the same session;
- concurrent claimers cannot overlap membership;
- retries load original policy/domain versions and membership.

#### Commit 3.3 — Project semantic job ownership

Files:

- `core/ingestion/project_semantic_job.py`
- `infrastructure/job/scheduler.py`
- `runtime/project_factory.py`
- `runtime/project_runtime.py`
- `runtime/session_runtime_factory.py`
- runtime composition tests

Work:

- construct one `ProjectSemanticJob` on the existing generic Scheduler boundary
  without enabling it as the regular runtime writer yet;
- add a scheduler wake event while retaining periodic polling;
- expose the wake edge through `ProjectRuntime`;
- exercise registration in focused composition tests;
- keep comparison non-writing and non-authoritative.

Acceptance:

- one enabled project job runs at a time in composition tests;
- a missed wake is recovered by polling;
- project shutdown cancels only project-owned work;
- multiple sessions signal the same owner without duplicate jobs.

#### Batch 3 completion record

- **3.1:** `KnowledgeStore.finalize_assistant_exchange` now wraps assistant
  persistence, source references, optional artifacts, and user-exchange closure
  in one transaction. Repeated finalization returns the original assistant;
  clarification, failure, cancellation, and explicit user-only paths close and
  seal the user evidence. The legacy worker is still signaled after closure.
- **3.2:** `SemanticWindowAdmission` merges FIFO-safe per-session streams,
  selects only whole sealed/closed exchanges, records the immutable policy and
  compiled-domain snapshots, measures tokens through `llm.count_tokens`, and
  stops immediately after the crossing exchange. The 300-second idle flush is
  fixed operational safety policy, not another size setting. Oversized logical
  exchanges are isolated for later bounded stage-local packing.
- **3.3:** `ProjectSemanticJob` is registered on the generic `Scheduler`, but
  is disabled in normal composition and performs preview-only admission when
  explicitly enabled by a focused test. `ProjectRuntime` exposes its wake edge;
  scheduler polling remains the durable recovery path. The obsolete narrow
  `EpisodeScheduler` wrapper was removed.

Validation completed:

- 49 focused non-service tests passed;
- 14 focused PostgreSQL contracts passed, including duplicate finalization,
  terminal closure, exact membership reload, and concurrent project claims;
- the full non-PostgreSQL/non-Redis lane passed: 1120 passed, 94 deselected;
- touched-path Ruff and compilation checks passed.

### Batch 4 — Episode generation from the shared window — complete

**Depends on:** Batch 3.

#### Commit 4.1 — Extract `EpisodeGenerator`

Files:

- `core/knowledge/episodes/job.py`
- `core/knowledge/episodes/build.py`
- a generator/service module if useful
- Episode unit tests

Work:

- move generation/consolidation behind a method accepting frozen window
  messages;
- remove `should_run` and next-window selection from generation logic;
- preserve local references, narrative limits, consolidation preflight, and
  canonical message membership;
- represent zero output explicitly.

Acceptance:

- frozen messages produce stable validated build input;
- generator never advances checkpoints or selects more messages;
- zero Episodes is distinguishable from “not run.”

#### Commit 4.2 — Idempotent narrative persistence

Files:

- Episode writer/reader and schema additions if needed
- Episode PostgreSQL contract tests

Work:

- associate the generation result with `window_id`;
- persist Episode narrative and message membership before Context;
- remove graph lookup/enrichment from initial persistence;
- prevent duplicate Episode writes on retry;
- retain historical Episode semantics.

Acceptance:

- retry returns the recorded Episode result;
- zero output records completion;
- Episode identity does not depend on graph enrichment;
- consolidation source membership remains valid.

#### Commit 4.3 — Coordinator Episode stage

Files:

- `project_semantic_job.py`
- job tests/fakes

Work:

- load or generate the Episode result for a claimed window;
- provide it to the upcoming Context stage;
- record stage-specific failures without advancing the Context checkpoint;
- leave current runtime Episode registration unchanged until Batch 8.

Acceptance:

- Episode failure retries the same window;
- Context is not called yet;
- independent Episode windows/checkpoints are no longer authoritative.

#### Batch 4 completion record

- **4.1:** `EpisodeGenerator` owns only build preparation, structured
  generation, narrative repair, consolidation re-grounding, and embeddings for
  supplied frozen messages. `EpisodeJob` retains legacy selection, checkpoint,
  and runtime registration until Batch 8, but delegates its generation work to
  the same service. Empty model output returns a completed build with no
  Episodes.
- **4.2:** `project_semantic_window_episodes` records the ordered narrative
  result for each semantic window. `episode_result_recorded` is the durable
  zero-result/completion marker, so `None` means not run while `[]` means a
  successful zero result. The semantic writer atomically validates frozen
  membership, persists narratives and canonical episode messages, maps their
  window result, and clears retry metadata. It neither advances legacy Episode
  checkpoints nor derives entity/relationship links; a consolidation may retain
  complete prior canonical evidence without changing episode identity.
- **4.3:** The still-disabled `ProjectSemanticJob` now claims due windows when
  explicitly enabled, reloads a recorded result before generating, and writes
  only the Episode stage. Episode failures stay at `claimed`, record
  `episode_generation` failure metadata, and retry the same window with the
  configured backoff. Its frozen policy snapshot prevents setting reloads from
  changing a retry's narrative limits or candidate count. No Context dependency
  or checkpoint transition was introduced.

Validation completed:

- 23 focused non-service tests passed, covering the standalone generator,
  frozen policy replay, zero output, retry/backoff, legacy episode behavior, and
  runtime configuration fan-out;
- 4 focused fresh-schema PostgreSQL contracts passed, covering exact semantic
  membership reload, idempotent persistence, zero-result completion, no legacy
  checkpoint/graph side effects, and consolidation membership;
- touched-path Ruff, compilation, and `git diff --check` passed.

The broader non-service lane was stopped after 60 seconds while an existing
fully monkeypatched fake-engine integration test was still running; it does not
construct the Batch 4 runtime path. This is recorded as an environment/test
duration limitation, not a Batch 4 source failure.

### Batch 5 — Project Context updater

**Depends on:** Batches 2 and 4.

#### Commit 5.1 — Prompt and structured-output contract

Files:

- `core/knowledge/context/prompts.py`
- `core/knowledge/context/updater.py`
- `common/schema/context.py`
- prompt/contract tests

Work:

- render current Context with local block refs;
- render frozen messages with role, session, source time, and local refs;
- include assistant source refs with separate local handles;
- render Episode narratives as interpretation aids;
- instruct for current-state reconciliation, late-source-time handling, natural
  prose, minimal support sets, and explicit uncertainty;
- request structured operations/dependencies only.

Acceptance:

- prompt separates evidence, Episodes, instructions, and current state;
- generated Markdown remains natural rather than extraction-shaped;
- unsupported assistant claims cannot validate as grounded;
- malformed or oversized output fails closed.

#### Commit 5.2 — Trust/provenance validator

Files:

- Context updater/application service
- source-reference reader additions
- unit and PostgreSQL tests

Work:

- resolve local refs to canonical IDs;
- verify project/window membership;
- enforce assertion-kind rules;
- expand Episode influence back to canonical Episode messages when selected;
- compute block source time and impact closure;
- reject invented, cross-project, or stale refs.

Acceptance:

- every Knowledge-eligible block reaches canonical evidence;
- source-grounded blocks reach refs owned by their assistant message;
- Episode IDs alone are never terminal evidence;
- `agent_derived` blocks are marked non-extractable;
- `human_asserted` blocks are accepted only from the file importer and retain
  block provenance without fabricated message refs.

#### Commit 5.3 — Context stage commit and recovery

Files:

- `project_semantic_job.py`
- Context reader/writer/projection integration
- recovery tests

Work:

- load the expected current revision;
- call the updater once for the window;
- validate/apply/persist revision;
- store `context_revision_id` and advance to `context_committed`;
- attempt file projection without making it canonical;
- on retry, load the committed revision without recalling the model.

Acceptance:

- crash after revision commit resumes from that exact revision;
- no-op result advances with the current revision recorded;
- stale-parent conflict reloads/retries without losing window evidence;
- zero Episode output still permits Context processing.

#### Batch 5 completion record

- **5.1:** The Context LLM now receives a server-owned catalog with `C` Context
  handles, `M` frozen-message handles (role, session, and source time), separate
  `S` assistant-source handles, and `E` Episode interpretation aids. It returns
  bounded structured ADD/REPLACE/DELETE operations, evidence handles, and
  dependencies—not durable IDs or a full document. The prompt requires natural
  prose, minimal support, explicit uncertainty, and source-time-aware
  reconciliation; malformed, model-timed, human-authored, and over-large
  responses fail before persistence.
- **5.2:** The updater resolves every local handle only against frozen
  project/window data. `user_asserted` changes retain canonical user-message
  support; `source_grounded` changes retain assistant-owned source refs;
  `agent_derived` remains visibly lower-trust/non-extractable; and `E` aids
  expand back to current-window canonical messages instead of becoming terminal
  evidence. The server derives block source time, blocks older/untimed evidence
  from replacing newer Context, and rejects invented, cross-window, or stale
  handles. Accepted `CONTEXT.md` edits now create their human-edit window
  directly at `context_committed` with the committed revision ID.
- **5.3:** The still-disabled `ProjectSemanticJob` advances a claimed window
  from its recorded Episode result through Context. It first reloads a revision
  already associated with the window, which recovers a crash between revision
  persistence and the stage CAS without another model call. A no-op records the
  current (or explicit initial empty) revision, Context failures retry at
  `claimed`, and a filesystem projection failure is logged for later reconcile
  without rolling back canonical Context. Batch 7 remains responsible for all
  Knowledge writes after `context_committed`.

Validation completed:

- focused Context prompt/provenance, Context-stage recovery, legacy Episode,
  semantic admission, and runtime composition unit tests passed;
- the 12-test fresh-schema PostgreSQL Context/window contract suite passed,
  including frozen assistant-source catalogs, committed-revision recovery, and
  human-edit `context_committed` publication;
- touched-path Ruff and compilation checks passed.

### Batch 6 — Context-block entity extraction

**Depends on:** Batch 5.

#### Commit 6.1 — Replace message-local extraction input

Files:

- `core/ingestion/batch.py`
- `core/ingestion/text_processor.py`
- `runtime/resources.py`
- `server/pyproject.toml`
- extraction schemas/prompts/tests

Work:

- introduce a project-scoped `SemanticWindowBuild` containing the Context
  revision, impact closure, policy snapshot, trace, and pending writes;
- replace the current `gliner` dependency/API with a GLiNER2.5 `AutoExtractor`
  VP-01 adapter;
- load `fastino/gliner2.5-base-v1` for English and pass compiled entity labels
  plus `DomainConfig` entity descriptions as the typed VP-01 schema;
- select `gliner2.5-multi-v1` only for explicitly multilingual projects;
- preserve known-alias matching before the GLiNER2.5 pass;
- allow cross-block VP-01 input and multi-block output refs;
- remove `session_text` from the new path;
- exclude instruction/non-Knowledge-eligible blocks.

Acceptance:

- entity mentions resolve to block-version IDs;
- VP-01 can cite the smallest multi-block support set;
- alias-only/GLiNER2.5-only mode remains valid;
- fixture tests prove entity descriptions change the passed VP-01 schema;
- no extraction code treats one Context block as one canonical message.

#### Commit 6.2 — Resolver block-evidence contract

Files:

- `core/knowledge/entity/resolver.py`
- ingestion contracts and tests

Work:

- change resolver input from `(message_id, name, type, topic)` to typed block
  mentions;
- preserve project/domain normalization and entity-ID allocation;
- return block-to-entity associations separately from literal
  message-to-entity evidence;
- derive `message_entity_refs` only from validated mention support.

Acceptance:

- existing entity/alias identity rules remain unchanged;
- broad block support does not fan every entity onto every message;
- retries preserve stable identity through the commit boundary.

#### Commit 6.3 — Entity result assembly and shadow evaluation

Files:

- semantic build/result contracts
- comparison/evaluation harness
- focused extraction tests

Work:

- assemble pending entity/alias writes, block associations, and validated
  message refs without committing live Knowledge;
- compare Context-first candidates/resolutions with the old message path on
  frozen fixtures;
- keep all Knowledge mutation for the single Batch 7.3 transaction;
- emit temporary comparison traces.

Acceptance:

- Context-block and message provenance resolve correctly in the pending result;
- running the evaluation harness cannot mutate canonical Knowledge;
- traces contain IDs/counts, not duplicated evidence payloads.

#### Batch 6 completion record

- **6.1:** `SemanticWindowBuild` is now a project-scoped, non-writing build
  boundary. It contains the committed Context revision, durable impacted block
  closure, typed support catalog, canonical evidence text, frozen ingestion
  policy, trace, validation issues, and pending result. The closure and typed
  supports are persisted/read through Context storage, including deletion
  neighbors needed to reassess an affected Context seam after a block is
  removed. Conversation admission and human `CONTEXT.md` imports both capture
  the exact policy used for later replay. A restart may reopen only a
  `context_committed` window whose Context revision and frozen policy match.
- **6.1:** The server now depends on `gliner2[local]` and uses a local
  `AutoExtractor` adapter for VP-01. English projects preload
  `fastino/gliner2.5-base-v1`; `gliner2.5-multi-v1` loads only when an active
  domain explicitly chooses `multilingual`. The adapter passes each configured
  extraction label with its domain entity description, preserves offsets, and
  filters output against the supplied Context text. Each build selects its
  adapter from its own frozen language before extraction, while a live
  domain-language activation also swaps the project runtime's cached adapter.
  The local prefetch script and resource tests no longer retain the old GLiNER
  model dependency. Internal work/provenance labels use `vp01`; `gliner2`
  appears only where it is the actual package or checkpoint identifier.
- **6.1:** Context VP-01 input consists solely of eligible current blocks in
  the durable impact closure—never `session_text`. Known aliases run first;
  GLiNER2.5 then operates over one offset-preserving multi-block surface.
  `agent_derived` blocks, and therefore model-like instruction text, cannot
  enter that surface. Output cites the smallest current block-version set it
  touches.
- **6.2:** The resolver has a separate typed Context-block mention contract.
  It retains the existing project/domain normalization, candidate lookup, and
  ID-allocation rules while returning block-to-entity associations separately
  from message evidence. Message refs are created only when a canonical
  message supporting a cited block contains the resolved mention literally;
  broad Context support cannot fan an entity out to unrelated messages.
- **6.3:** Entity/alias writes, block associations, and validated message refs
  assemble in memory as `ContextEntityResult`. The shadow evaluator accepts
  already-finished Context and legacy outputs and emits only IDs and counts;
  it has no store, writer, or allocator dependency. Batch 7 remains the sole
  owner of live Knowledge reconciliation and stage advancement.

Validation completed:

- 74 focused Context VP-01, resolver/provenance, frozen-policy/replay,
  admission, render, legacy message-contract, runtime-resource, and
  project-runtime unit tests passed;
- the 13-test fresh-schema PostgreSQL Context/window contract suite passed,
  including durable impact closure/support reads and human-import frozen-policy
  persistence;
- touched-path Ruff and compilation checks passed; the lock resolves
  `gliner2[local]` 2.0.0 with the required newer Transformers API.

### Batch 7 — Context relationships and deterministic retraction

**Depends on:** Batch 6.

#### Commit 7.1 — Relationship contracts and storage shape

Files:

- ingestion relationship contracts
- relationship observation readers/writers
- schema/storage tests

Work:

- remove required single-message provenance from the new contract;
- require one or more current Context block-version refs;
- preserve endpoint/type/domain validation;
- add active-support queries and deterministic orphan cleanup;
- retain old columns only until Batch 8 cutover.

Acceptance:

- one observation supports multiple blocks without duplication;
- cross-project block refs fail;
- unsupported observations cannot remain in the aggregate.

#### Commit 7.2 — Refactor VP-02 around Context

Files:

- `core/ingestion/relationship_extractor.py`
- prompts/contracts/tests

Work:

- build candidates from resolved entities in the impact closure;
- provide relevant neighboring/dependency blocks;
- call the configured LLM for VP-02 and accept multi-block support refs;
- do not invoke GLiNER relation/classification features;
- remove per-message event heuristics that no longer describe the input;
- keep vocabulary, direction, symmetry, and endpoint checks.

Acceptance:

- cross-block pronouns/relations are supported;
- output cites the smallest sufficient current block set;
- relations cannot cite prior/deleted or agent-derived blocks.

#### Commit 7.3 — Atomic Knowledge reconciliation

Files:

- `semantic_commit_writer.py`
- relationship aggregate helpers
- atomic commit/recovery tests

Work:

- lock/verify the window at `context_committed`;
- retire observations touching replaced/deleted prior block versions;
- write pending entities, aliases, block-entity associations, message refs,
  relationships, observations, and block supports in one transaction;
- update/remove aggregate relationships;
- advance to `knowledge_committed` atomically;
- make retry at expected stage idempotent.

Acceptance:

- replacing “Sarah owns Delta” with “John owns Delta” cannot leave an active
  Sarah ownership relation without independent current support;
- deleting a block retires its unsupported observations;
- an unchanged independent supporting block keeps the aggregate alive;
- rollback cannot expose half-reconciled graph state.

#### Commit 7.4 — Episode enrichment and finalization

Files:

- Episode writer enrichment path
- `project_semantic_job.py`
- integration tests

Work:

- associate committed entities/relationships with Episodes after Knowledge;
- make enrichment idempotent by window/Episode identity;
- mark the window `completed` even when no Episode exists;
- record maintenance work after completion without rolling back the semantic
  commit.

Acceptance:

- enrichment retry creates no duplicate associations;
- maintenance failure leaves the completed window separately retryable;
- all stage transitions are visible in diagnostics.

#### Batch 7 completion record

- `ContextRelationshipWrite` requires one or more immutable Context block
  versions rather than a message/session. Legacy message provenance columns
  remain only for the Batch 8 owner cutover. The schema adds semantic-window
  provenance, retirement state, active-support indexing, scoped block/window
  foreign keys, and an idempotent semantic-observation identity.
- `ContextRelationshipExtractor` is the separate Context-native VP-02 path.
  It constructs candidates from resolved impact-closure entities, provides only
  eligible current Context blocks, uses the configured LLM with local `bN`
  references, selects the smallest duplicate support set, and revalidates
  endpoint types, vocabulary, direction, symmetry, and domain version before
  durable write. It has no GLiNER relationship/classification dependency.
- `SemanticCommitWriter` locks and verifies `context_committed`, exact Context
  membership, and durable impact closure. It writes entities, aliases,
  block/entity associations, literal message refs, aggregates, observations,
  and multi-block supports as one transaction; retires observations touching a
  block absent from the new revision; removes aggregates without active support;
  rebuilds the AGE projection in that transaction; and checkpoints
  `knowledge_committed` idempotently.
- Episode enrichment runs only after that checkpoint and is idempotent by
  window/Episode identity. A window with zero Episodes still completes. After
  completion, `project_semantic_window_maintenance` records an independent
  pending item; a maintenance failure updates that item without reopening or
  rolling back the semantic window.
- Validation passed: 60 focused unit tests; 15 fresh-schema PostgreSQL Context,
  semantic-commit, retraction, rollback, and maintenance-contract tests; plus
  touched-path Ruff, compilation, and whitespace checks.

### Batch 8 — Production cutover and deletion of old ownership — complete

**Depends on:** Batches 1-7 and accepted shadow/evaluation results.

#### Commit 8.1 — Agent read integration

Files:

- `core/agent/executor.py`
- `core/agent/system_prompt.py`
- Context reader
- Agent prompt tests

Work:

- load latest committed Context directly from the canonical DB reader;
- label `PROJECT.md` as user-owned Project Brief;
- label Project Context as engine-maintained current understanding;
- do not read `CONTEXT.md` as the authoritative runtime source;
- bound rendered Context size deterministically.

Acceptance:

- Agent sees both layers with distinct authority;
- stale/missing projection does not change Agent input;
- empty Context is handled without placeholder factual text.

#### Commit 8.2 — Switch semantic production owner

Files:

- project/session runtime factories
- scheduler/runtime lifecycle tests
- integration ingestion tests

Work:

- enable `ProjectSemanticJob` as the only normal semantic owner;
- remove per-session worker creation/signaling/reset;
- remove independent Episode job registration;
- route exchange-closure wake-ups to the project job;
- remove temporary dual-write/shadow execution.

Acceptance:

- a completed exchange reaches Context and Knowledge through one window;
- two sessions contribute to one serialized project Context;
- restart resumes an unfinished durable window;
- no old worker can concurrently write semantic graph state.

#### Commit 8.3 — Delete obsolete message-local machinery

Files:

- old worker/batch/pipeline helpers
- message lifecycle claim code
- settings, exports, fakes, tests, and bootstrap schema

Work:

- delete `IngestionWorker` and old `IngestionBatch` when no caller remains;
- delete `session_text` and per-session graph-claim APIs;
- remove obsolete `messages.ingestion_*` columns if no lifecycle owns them;
- remove old Episode processing checkpoints/window selection;
- remove single-message relationship provenance columns;
- update architecture import checks.

Acceptance:

- `rg` finds no runtime caller of deleted contracts;
- schema has one semantic checkpoint model;
- no compatibility adapter preserves the old path;
- focused and service-free suites pass.

#### Batch 8 completion record

- **8.1:** `AgentExecutor` loads the latest committed Context snapshot through
  the canonical database reader and renders it with a deterministic 24,000
  character bound. `PROJECT.md` is presented separately as the user-owned
  Project Brief; `CONTEXT.md` is never read as runtime authority, so a stale,
  missing, or unprojected file cannot change Agent input.
- **8.2:** `ProjectSemanticJob` is enabled as the one normal semantic owner.
  `ProjectRuntimeFactory` registers it with the project scheduler, while
  `SessionRuntimeFactory` creates only session-local runtime shells. Exchange
  closure wakes the shared project job; acceptance no longer creates, signals,
  resets, or shuts down a session-local semantic worker.
- **8.3:** Deleted the old worker, pipeline, graph-commit helper, message batch,
  Episode job/checkpoint ports, message claim APIs, and obsolete test fixtures.
  The bootstrap schema no longer contains `messages.ingestion_*`,
  `episode_processing_checkpoints`, or single-message relationship-observation
  provenance. Relationship observations now belong to a semantic window and
  Context block support; window deletion cascades that derived evidence during
  project deletion. No compatibility adapter preserves the old writer path.
- Runtime health reports the project semantic job and durable semantic-window
  aggregate rather than a session worker or message queue. Maintenance frontiers
  wait for closed exchanges to appear in completed semantic windows.

Validation completed:

- 100 focused unit/runtime/semantic-stage tests passed;
- 1,056 service-free tests passed, with 79 PostgreSQL/Redis tests deliberately
  deselected;
- 45 fresh-schema PostgreSQL contracts passed for bootstrap, lifecycle,
  Context/window, provenance, reconciliation, Episode, maintenance, and
  project deletion behavior;
- 12 fresh-schema integration tests passed for durable acceptance, document
  provenance, workspace health, and the project semantic runtime boundary;
- Ruff, Python compilation, dead-path scans, and `git diff --check` passed.

### Batch 9 — Maintenance and local-model simplification — complete

**Depends on:** Batch 8. These are follow-up commits, not cutover prerequisites
unless benchmarks expose a blocker.

#### Commit 9.1 — Narrow maintenance inputs

Files:

- maintenance candidate readers/services
- `core/knowledge/db/writers/semantic_commit_writer.py`
- maintenance unit/contract tests

Work:

- remove routine Context supersession from conflict candidate generation;
- keep independent conflicting observations, identity ambiguity, and manual
  review paths;
- ensure deterministic retraction emits audit data without fabricating a review.

Acceptance:

- normal replacements do not open maintenance reviews;
- independently sourced contradictions still can;
- detachment/reclassification audit contracts remain intact.

#### Commit 9.2 — spaCy cleanup

Files:

- `runtime/resources.py`
- `core/ingestion/text_processor.py`
- dependency/model resource configuration
- NLP regression tests

Work:

- replace trained pipeline loading with `spacy.blank("en")` where tokenizer/
  matcher vocabulary remains required;
- remove `en_core_web_md` prefetch/load and POS tie-break;
- delete spaCy entirely only if matcher/tokenization can also be removed safely.

Acceptance:

- lowercase/common-word false-positive fixtures remain within accepted bounds;
- alias matching behavior is preserved;
- runtime startup no longer requires the trained model.

#### Commit 9.3 — GLiNER2.5 VP-01 regression benchmark

Files:

- GLiNER adapter/resource composition
- model evaluation harness and frozen Context fixtures
- VP-01 benchmark tests

Work:

- benchmark the required production adapter on frozen Context fixtures;
- verify compiled labels and entity descriptions reach the extractor unchanged;
- measure CPU latency, memory, precision/recall, and VP-01 fallback rate;
- leave VP-02 on its required LLM path in every benchmark variant.

Acceptance:

- the GLiNER2.5 adapter remains the production VP-01 path;
- regression gates identify tuning/resource problems before cutover;
- do not retain the old GLiNER path as a permanent fallback.

#### Batch 9 completion record

- **9.1:** Conflict discovery reads only active relationship observations, so
  a Context block replacement cannot turn its deterministic retirement into a
  candidate review. `SemanticCommitWriter` records every such retirement in
  `maintenance_reinterpretation_audits`, including old relationship identity
  and the Context-reconciliation reason; it never creates a maintenance review.
  Active independently sourced evidence remains available to the existing LLM
  conflict-discovery and manual-review paths.
- **9.2:** Runtime and prefetch no longer load or prefetch `en_core_web_md`.
  `spacy.blank("en")` supplies the tokenizer and vocabulary required by the
  case-insensitive `PhraseMatcher`; spaCy remains installed because that
  matcher is still the safe known-alias boundary. Lowercase/common-word
  filtering and alias fixtures cover the change.
- **9.3:** `run_vp01_benchmark` accepts only `GLiNER25VP01Adapter` matching
  the frozen domain language. For each frozen Context fixture it measures
  synchronous CPU inference latency and process high-water RSS, then computes
  exact span precision/recall. It exposes explicit quality, latency, memory,
  and zero-fallback gates. The adapter-call contract verifies that compiled
  label descriptions reach GLiNER2.5 unchanged. The harness has no VP-02
  capability; relationship extraction remains on the separate required LLM
  path.

Validation completed:

- 32 focused VP-01, Context entity, runtime-resource, and conflict-discovery
  unit tests passed;
- 4 fresh-schema PostgreSQL contracts passed for semantic reconciliation and
  maintenance application behavior;
- 1,060 service-free tests passed, with 79 PostgreSQL/Redis tests deliberately
  deselected;
- Ruff, Python compilation, dependency-lock refresh, trained-model removal
  scan, and `git diff --check` passed.

### Batch 10 — Recovery, end-to-end validation, and documentation

**Depends on:** Batch 8; include Batch 9 results if completed.

#### Commit 10.1 — Failure injection and restart coverage

Files:

- semantic job/recovery tests
- storage failure-injection fakes
- project health/diagnostic tests

Work:

- add failure injection after exchange close, window claim, Episode persistence,
  Context commit, projection write, entity extraction, Knowledge rollback/
  commit, Episode enrichment, and maintenance scheduling;
- add degraded-health coverage for exhausted active windows;
- verify explicit retry resumes the frozen window rather than reselection.

Acceptance:

- every restart resumes from the last committed stage;
- committed Context is never regenerated;
- no message enters two windows;
- exhausted work remains diagnosable and manually retryable.

#### Commit 10.2 — End-to-end provenance scenarios

Files:

- ingestion/runtime integration tests
- PostgreSQL provenance/retraction tests
- document/project deletion contract tests

Work and required scenarios:

1. one exchange adds Context and a relationship;
2. a later exchange replaces it and retracts the old relationship;
3. two sessions contribute to one Context revision;
4. one blocked session does not starve another;
5. assistant-cited support resolves through `message_source_refs`;
6. agent-derived Context renders but does not enter Knowledge;
7. zero Episode output still completes;
8. an oversized exchange remains one logical window;
9. stale `CONTEXT.md` is repaired from PostgreSQL;
10. project deletion removes all Context/window evidence.

Acceptance:

- every scenario passes through public/runtime composition rather than testing
  only isolated helper methods;
- provenance queries resolve observation → blocks → messages/source refs;
- graph state and current Context agree after replacement/deletion;
- real-PostgreSQL coverage is reported separately from fake-store coverage.

#### Commit 10.3 — Architecture and operator documentation

Work:

- document ownership and restart semantics near runtime composition;
- document Project Brief versus Project Context;
- document settings and trace fields;
- update architecture checks and remove temporary migration notes;
- record real-service checks that could not run.

Acceptance:

- a developer can locate one owner for admission, each stage, and each durable
  checkpoint;
- no documentation describes the deleted message-local path as current;
- `git diff --check`, focused Ruff, compile, and applicable tests pass.

---

## 7. Test and Validation Matrix

### Per-commit minimum

- tests directly covering changed contracts;
- Ruff on touched Python paths;
- Python compile/import check for touched modules;
- `git diff --check`;
- architecture/import checks when ownership changes.

### Per-batch minimum

- all unit tests for the changed subsystem;
- relevant existing integration tests;
- PostgreSQL contract tests for schema/writer changes;
- fake/store contracts updated alongside real storage contracts;
- no unrelated test weakening to accommodate the refactor.

### Cutover gate

Before Batch 8 merges:

- Context updater fixture evaluation accepted;
- old-versus-Context-first extraction comparison reviewed;
- replacement/deletion retraction proven on real PostgreSQL;
- multi-session ordering and concurrent admission proven;
- restart from every durable checkpoint proven;
- Agent prompt authority separation reviewed;
- generated Context exclusion from document indexing proven;
- no unresolved old-worker call sites;
- service-free suite passes;
- real PostgreSQL results and any existing Redis buffer/wake coverage reported
  separately and precisely.

If the full suite is blocked by an unrelated environment/dependency problem,
record the exact collection/runtime blocker. Do not change product contracts or
tests merely to make the command green.

---

## 8. Diagnostics

Every semantic-window run should expose bounded identifiers and counts:

```text
window_id / project_id / stage / attempt_count
message_count / exchange_count / session_count / source_token_count
domain_version / context_parent_revision / context_revision
context_add/replace/delete counts
changed_block_count / impact_closure_count
entity candidate/resolved/new counts
relationship candidate/committed/retired counts
episode created/consolidated/zero-result counts
projection status / failure stage / failure code
```

Do not log entire messages, Context, source excerpts, or prompts by default.
Targeted debug expansion must remain explicit.

---

## 9. Definition of Done

The refactor is complete only when all of the following are true:

1. Messages remain exact canonical evidence.
2. One project-scoped owner admits complete exchanges across sessions.
3. Episodes and Context consume the same frozen window.
4. PostgreSQL is the single authority for current Context.
5. `CONTEXT.md` is a recoverable projection and cannot self-ingest.
6. `PROJECT.md` remains user-owned and semantically distinct.
7. Context updates are structured, revisioned, and provenance-validated.
8. Unsupported assistant prose cannot silently become Knowledge.
9. Entity and relationship extraction operates on Context impact closures.
10. Relationship evidence supports multiple Context blocks without duplication.
11. Replacement/deletion retracts unsupported graph state deterministically.
12. Episodes remain historical when current Context changes.
13. Source time remains distinct from processing time.
14. Restart resumes without regenerating committed Context.
15. The old per-session worker, `session_text`, independent Episode window, and
    single-message relationship source contract are deleted.
16. Maintenance handles unresolved ambiguity rather than routine state changes.
17. VP-01 uses GLiNER2.5 with compiled entity descriptions as typed guidance.
18. VP-02 remains an LLM call and does not use GLiNER relation features.

The final architecture should be explainable as:

```text
raw exact evidence
→ coherent historical Episodes
→ maintained current Context
→ structured Knowledge
```

There should be one obvious owner, one durable checkpoint chain, and one
traceable provenance path for each derived assertion.
