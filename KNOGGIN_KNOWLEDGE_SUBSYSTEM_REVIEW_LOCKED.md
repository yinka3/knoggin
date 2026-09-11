# Knoggin — Knowledge Subsystem Review (Locked)

## Scope

This review covers the server-side Knowledge subsystem as the durable semantic state layer for Knoggin.

The review focuses on:

- canonical PostgreSQL knowledge state
- AGE graph projection
- entities and aliases
- Project entity contexts
- Context revisions and immutable Context blocks
- Context evidence/supports
- Context block/entity associations
- relationships and relationship observations
- Episodes and Episode enrichment
- Knowledge retrieval
- evidence traversal
- entity maintenance
- global entity merge/rollback
- Project-scoped cleanup
- historical reclassification/normalization
- projection and embedding rebuilds

The intended deployment model remains:

- one user
- one local machine
- local concurrency is real and must be handled
- no distributed or multi-tenant redesign

Previously locked Project, Session, Runtime, and Ingestion decisions remain authoritative unless a direct contradiction is found.

---

# 1. Knowledge Role

Knowledge is the durable semantic state layer beneath Ingestion and above retrieval/Agent use.

The ownership model is:

```text
                         KNOWLEDGE
                            │
          ┌─────────────────┼──────────────────┐
          │                 │                  │
     PostgreSQL          AGE Graph        Local Runtime
     canonical           projection          caches
          │                 │                  │
          ▼                 ▼                  ▼
     Entities          Entity nodes       EntityResolver
     Aliases           Relationships      retrieval caches
     Project contexts
     Context revisions
     Context blocks
     Relationships
     Observations
     Episodes
     Evidence
     Reviews/audits
```

The primary architectural rule is:

```text
PostgreSQL = canonical truth
AGE        = rebuildable traversal projection
Resolver   = live acceleration/cache
```

---

# 2. `KnowledgeStore` — LOCKED

`KnowledgeStore` is the main core-facing persistence facade.

Although it exposes a broad API, it delegates actual storage responsibilities to focused readers, writers, and rebuilders.

Conceptually:

```text
KnowledgeStore
├── MessageReader / Writer
├── EntityReader
├── GraphReader / Writer
├── EpisodeReader / Writer
├── ProjectContextReader / Writer
├── SemanticWindowReader / Writer
├── SemanticCommitWriter
├── EvidenceService
├── SourceReferenceReader / Writer
├── ArtifactReader / Writer
├── reclassification writers
└── projection / embedding rebuilders
```

It should not be split merely because the facade is large.

## Classification

**KEEP**

---

# 3. PostgreSQL Canonical State + AGE Derived Projection — LOCKED

PostgreSQL is authoritative.

AGE is a derived graph traversal projection.

Project-specific identity interpretation remains in relational state rather than being treated as global graph identity metadata.

AGE can be rebuilt from PostgreSQL canonical state.

## Required invariant

> AGE must never become an independent source of truth for Knowledge identity or evidence.

Projection failures may require repair, but must not silently redefine canonical Knowledge.

## Classification

**KEEP / VERY GOOD**

---

# 4. User-global Entity Identity + Project-local Classification — LOCKED

Entity identity is user-global.

Project classification is Project-scoped.

Conceptually:

```text
Entity
│
├── entity_id ............... user-global identity
├── canonical_name .......... immutable identity name
├── aliases ................. user-global
└── embedding ............... user-global

ProjectEntityContext
│
├── project_id
├── entity_id
├── entity_type
├── topic
└── last_mentioned_ms
```

The same identity may appear in several Projects while carrying different Project-specific type/topic interpretation.

Canonical names remain canonical and should not be casually rewritten.

During entity merge:

- the survivor retains its canonical name
- the retired canonical name becomes an alias
- the retired entity becomes a redirect

The reserved Identity entity remains protected from merge and Project cleanup.

## Classification

**KEEP / VERY GOOD**

---

# 5. Context Revision Model — LOCKED

Project Context uses immutable revisions and immutable block versions.

```text
Context Revision
      │
      ├── immutable block membership
      ├── immutable block versions
      ├── parent revision
      └── impact closure

Context Block
      │
      ├── assertion kind
      ├── source_time_ms
      ├── supersedes_block_id
      └── support rows
             ├── messages
             └── assistant sources
```

The model is strong because semantic history is preserved without requiring mutable in-place statements.

## Classification

**KEEP / VERY GOOD**

---

# 6. Context Provenance Rules — LOCKED

Context writes are validated against their evidence.

Important rules:

- model-generated Context cannot create `human_asserted`
- `user_asserted` requires current user-message evidence
- `source_grounded` requires assistant source evidence
- `agent_derived` remains distinguishable
- older or untimed evidence cannot replace newer timed Context
- `source_time_ms` is server-derived from evidence
- supports are explicit durable references

Human `CONTEXT.md` edits become explicit `human_asserted` block versions rather than being rewritten as model assertions.

## Classification

**KEEP / VERY GOOD**

---

# 7. Human Context Event-time Fallback — LOCKED

Conversation-derived Context follows source/event time from canonical evidence.

```text
message.timestamp_ms
        ↓
ContextBlock.source_time_ms
```

Human-authored Context may have no earlier message/source timestamp.

For a genuinely human-authored Context assertion with no prior source time:

> Use the time the human edit is accepted as the block's `source_time_ms`.

This preserves the unified event-time model without inventing historical timestamps.

Keep storage lifecycle time separate:

```text
source/event time
    → source_time_ms

storage lifecycle time
    → created_at
    → updated_at
```

## Classification

**LOCKED SEMANTIC RULE**

---

# 8. Relationship Aggregate vs Observation History — LOCKED

The current relationship model separates graph identity from evidence history.

```text
Relationship
    = current aggregate graph edge

RelationshipObservation
    = one evidence-backed observation or interpretation
```

Context reconciliation can retire observations whose supporting Context block versions are no longer current.

Retired observations remain historical evidence.

When a Relationship no longer has any active observations:

```text
retired observations remain
        +
aggregate Relationship is removed
```

This preserves both current state and historical explainability.

## Classification

**KEEP / VERY GOOD**

---

# 9. FIX — Current Retrieval Must Exclude Retired Relationship Evidence

Several current-state retrieval paths join `relationship_observations` without consistently filtering:

```sql
retired_at IS NULL
```

This can cause retired evidence to affect:

- relationship observation counts
- evidence message counts
- first/last observed timestamps
- relationship ordering/strength
- graph path observation references
- recent-activity enrichment

That conflicts with the storage distinction between current Knowledge and historical provenance.

## Required rule

```text
normal Knowledge retrieval
        ↓
ACTIVE observations only

explicit provenance / audit traversal
        ↓
active + retired observations
        ↓
retired evidence explicitly marked retired
```

Evidence/history APIs should continue exposing retired observations.

Current retrieval should not.

## Classification

**FIX**

---

# 10. `context_block_entities` — LOCKED STRUCTURE

`context_block_entities` is a first-class Context-first Knowledge structure.

It represents:

```text
Context block version
        ↓
resolved entity identity
        ↓
literal/contextual mention text
```

This table is not disposable glue.

It is used by later Knowledge behavior, including Episode enrichment.

## Classification

**KEEP**

---

# 11. FIX — Global Entity Merge Must Migrate `context_block_entities`

Global entity merge currently covers:

- entities
- aliases
- Project entity contexts
- message entity references
- Episode entity links
- relationships
- relationship observations
- Episode relationship links

But the newer Context-first association:

```text
context_block_entities
```

must also be migrated.

Current problematic shape:

```text
Context Block B
    ↓
entity 72

merge 72 → 18

project_entity_context → 18
message refs           → 18
Episode links          → 18
relationships          → 18

but:

context_block_entities
B → 72
```

Because the retired entity survives as a redirect, the entity FK does not automatically remove that block association.

The stale association can later affect Episode enrichment.

## Required merge behavior

```text
retired block/entity association
             ↓
survivor block/entity association
             ↓
collapse duplicate survivor association safely
             ↓
remove retired association
             ↓
journal mutation
```

The global merge snapshot/state hash should include these associations.

Rollback must also understand them.

## Classification

**FIX / IMPORTANT**

---

# 12. FIX — Project Entity Cleanup Must Remove `context_block_entities`

Project-scoped cleanup currently removes several Project-derived uses of an entity, but if the same global entity survives in another Project, the target Project's Context block/entity associations can survive.

Therefore cleanup may appear to remove an entity from a Project while current Context still points to it.

## Required cleanup behavior

When entity X is explicitly cleaned from Project P:

```text
delete P's relationships for X
delete P's relationship observations as appropriate
delete P's message entity refs for X
delete P's Episode entity links for X
delete P's context_block_entities for X
delete P's project_entity_context
```

Only then decide whether the global Entity row itself has become orphaned.

## Recommended preview improvement

Cleanup preview should expose current Context association count/evidence so the user can see whether the entity is represented in current Context.

## Classification

**FIX**

---

# 13. FIX — Entity `last_mentioned_ms` Must Follow Event Time

`project_entity_contexts.last_mentioned_ms` is used by:

- entity list ordering
- entity search ordering
- hot-topic ordering
- cleanup ordering
- AGE projection
- entity context hydration

However, the normal Context-first semantic path does not consistently advance it.

This leaves current entities with stale or null activity timestamps while downstream readers treat the field as meaningful.

## Required event-time rule

During semantic Knowledge commit:

```text
last_mentioned_ms
    = max(
        current last_mentioned_ms,
        relevant Context block source_time_ms
      )
```

For human-authored Context with no earlier source time, use the accepted human-edit time as defined in Section 7.

## Classification

**FIX**

---

# 14. FIX — Relationship `observed_at_ms` Must Follow Evidence Time

The semantic relationship commit currently risks using semantic commit time as `observed_at_ms`.

That produces ingestion-time semantics:

```text
message happened at 10:00
semantic commit at 10:04
relationship observed_at_ms = 10:04
```

The intended Knoggin clock is event/source time.

## Required rule

For a relationship observation:

```text
observed_at_ms
    = max(source_time_ms of its cited Context support blocks)
```

The chosen timestamp should reflect when the supporting evidence happened, not when PostgreSQL processed it.

For genuinely human-authored Context with no earlier evidence time, use accepted human-edit time.

## Keep separate

```text
event/source time
    → message.timestamp_ms
    → ContextBlock.source_time_ms
    → entity.last_mentioned_ms
    → relationship_observation.observed_at_ms

storage lifecycle time
    → created_at
    → updated_at
    → retired_at
```

## Classification

**FIX**

---

# 15. FIX + SIMPLIFY — Entity Embeddings Must Be Identity-only

Entity embeddings are user-global.

Project type/topic are Project-local.

Therefore Project classification must not alter the vector representing one user-global identity.

The intended rule already exists in parts of the code:

```text
entity embedding
    = canonical identity representation
```

But the Context-first pending-entity path can currently include `entity_type` in embedding text.

This makes the initial vector depend on Project classification while rebuild logic later ignores Project classification.

That can change a vector merely because maintenance ran.

## Required simplification

Prefer:

```python
build_entity_embedding_text(canonical_name)
```

instead of:

```python
build_entity_embedding_text(canonical_name, entity_type)
```

Remove the ability for Project-local type/topic to leak into user-global entity embedding semantics.

## Consequence

Historical entity type/topic reclassification should no longer report:

```text
embedding_rebuild_required
```

solely because Project classification changed.

Likewise, Project entity reclassification should not trigger global entity embedding rebuilds.

## Classification

**FIX + SIMPLIFY**

---

# 16. Knowledge Retrieval Boundary — LOCKED

Knowledge retrieval should answer:

> What is currently known and visible in the requested Project read scope?

It should not silently behave like a historical audit traversal.

Current-state retrieval should:

- respect visible Project IDs
- resolve user-global entity identity
- hydrate Project-local contexts
- use current relationships
- use active observations
- preserve bounded query behavior

## Classification

**KEEP, WITH RETIRED-EVIDENCE FIX**

---

# 17. Evidence Traversal — LOCKED

Explicit evidence traversal is a separate concern from ordinary retrieval.

It should answer:

> Why does or did Knoggin believe this?

It may return:

- current Context blocks
- retired relationship observations
- messages
- assistant source references
- evidence status
- bounded evidence snapshots

Retired evidence should remain available here and should be explicitly labeled as retired rather than hidden.

## Classification

**KEEP / VERY GOOD**

---

# 18. Global Entity Maintenance Architecture — LOCKED

Global identity maintenance is explicitly user-global.

The architecture correctly separates:

```text
deterministic candidate discovery
        ↓
preview
        ↓
typed merge plan
        ↓
durable maintenance review
        ↓
explicit apply
        ↓
mutation journal
        ↓
optional rollback
```

The model does not silently choose a merge and mutate canonical identity.

Merge conflicts in Project-specific type/topic require explicit choices.

## Classification

**KEEP**

---

# 19. Global Merge Rollback Model — LOCKED

Rollback is not a blind database snapshot restore.

The mutation journal allows the system to distinguish:

```text
safe inverse mutation
```

from:

```text
post-merge state changed
→ user decision required
```

Conflicting inverse operations can become a new maintenance review.

This is the correct model for explicit identity repair.

## Classification

**KEEP / VERY GOOD**

---

# 20. FINISH — Maintenance Frontier Must Respect Semantic Participation

The current maintenance frontier treats a closed user exchange without completed semantic membership as pending semantic work.

That is no longer sufficient after Session semantic participation.

A deliberately excluded Session interval is expected to remain unclaimed.

Without participation-aware logic:

```text
Session participation disabled
        ↓
closed exchange intentionally not ingested
        ↓
no completed semantic window
        ↓
maintenance sees "pending" forever
```

## Required rule

Maintenance must share the same semantic eligibility concept as Ingestion.

Only a conversation exchange that is semantically eligible should count as pending.

Conceptually:

```text
Session participates?
        ↓ yes
message is after participation frontier?
        ↓ yes
exchange is closed and semantically eligible?
        ↓ yes
then unfinished semantic processing counts as pending
```

Disabled intervals do not count.

Deleted Sessions do not count.

## Classification

**FINISH EXISTING PARTICIPATION DECISION**

---

# 21. FIX — Maintenance Frontier Must Include Non-conversation Semantic Windows

Human Context edits create semantic reconciliation windows with no message membership.

Example:

```text
origin = human_edit
stage = context_committed
messages = none
```

A message-only maintenance frontier can therefore claim the Project is semantically quiescent while a human-edit Knowledge reconciliation is still active.

## Required semantic-quiescence rule

For an affected Project:

```text
1. no active semantic window of any origin
   with stage != completed

AND

2. no semantically eligible closed conversation exchange
   waiting to be claimed
```

Only then may global entity maintenance treat the Project semantic frontier as stable.

## Classification

**FIX**

---

# 22. FIX / HARDEN — Global Entity Merge Must Serialize with Semantic Knowledge Commit

Global identity merge has strong stale-state checks:

- state hash
- semantic frontier
- domain definition versions
- user-global merge advisory lock
- row locking for target entities

However, Project semantic commit does not share the Project maintenance lock.

Therefore a semantic build can be prepared against an entity that is merged while the build is still in flight.

Possible shape:

```text
Global Merge                         Semantic Job

revalidate frontier
revalidate state hash
                                     build uses entity 72
lock 72
merge 72 → 18
commit
                                     attempts Knowledge commit using 72
```

Some stale writes will fail through active endpoint validation, but this is not a complete durable exclusion boundary.

## Required invariant

> A semantic Knowledge commit using entity X and a global merge retiring entity X must serialize at the durable identity boundary.

The implementation may use:

- advisory locks
- entity row locks
- final commit-time identity revalidation
- another equivalent durable ordering mechanism

The exact locking implementation should be chosen with the current SQL writers in view.

## Classification

**FIX / LOCAL CONCURRENCY**

---

# 23. AGE Entity Deletion Concern — NOT A BUG

The AGE projection contains a `DETACH DELETE` path for globally deleted identities.

This initially appears risky because identity nodes are user-global.

Current cleanup callers correctly remove the target Project context first and only globally delete an Entity when no `project_entity_contexts` remain anywhere.

Therefore the AGE node is only deleted when the identity is globally orphaned.

## Classification

**KEEP / NO FINDING**

---

# 24. Episodes — LOCKED

Episodes remain Project-owned semantic narratives grounded in canonical messages.

Important properties:

- Episode source-message ownership is durable
- semantic-window Episode output is checkpointed
- Episode enrichment attaches Knowledge entities/relationships after Knowledge commit
- Episode consolidation uses prior Project Episodes as interpretation aids
- automation must not overwrite explicitly user-modified Episodes

## Classification

**KEEP**

---

# 25. Manual Episode Editing — LOCKED AS KEEP + FINISH

Manual Episode editing is an intended feature.

A production caller may eventually come from the server API and/or SDK surface.

The server currently supports narrative edits and marks the Episode:

```text
user_modified = true
```

Once user-modified:

> automated Episode consolidation/regeneration must not overwrite the user's edited narrative.

That behavior should remain.

## Current loose end

Editing:

- summary
- new developments
- updates
- unresolved items

changes the semantic text but currently risks leaving the old Episode embedding.

That would make vector retrieval represent the previous Episode narrative.

## Required invariant

> A successful user Episode edit must leave the narrative and its embedding representing the same Episode version.

## Recommended flow

Do not hold a PostgreSQL transaction open during model inference.

Prefer:

```text
validate edit
    ↓
generate replacement embedding
    ↓
transactionally persist:
    ├── edited narrative
    ├── replacement embedding
    ├── user_modified = true
    └── updated_at
```

If embedding generation fails, do not leave a new narrative paired with the old vector.

## API bridge

Verify the production Episode-edit endpoint/caller during the later API bridge review.

SDK remains outside the current server-only review.

## Classification

**KEEP + FINISH**

---

# 26. Historical Entity Reclassification — LOCKED DIRECTION

Historical Project entity type/topic reclassification is explicit maintenance, not automatic hidden mutation.

It should continue using:

- Project scope
- frozen/expected domain version
- preview/planning
- bounded batches
- compare-and-update semantics
- conflict accounting

After Section 15's embedding simplification, a Project type/topic update should not require rebuilding the user-global identity embedding.

AGE Project-domain metadata may still require projection refresh when Project classification changes.

## Classification

**KEEP, WITH EMBEDDING CLEANUP**

---

# 27. Historical Relationship Normalization — LOCKED

Historical relationship normalization is observation-first.

It should remain:

- explicit
- bounded
- Project-scoped
- domain-version-aware
- evidence preserving
- conflict aware

Relationship observations remain canonical evidence.

Maintenance may reinterpret how observations attach to current aggregate relationships without destroying the historical evidence.

## Classification

**KEEP**

---

# 28. Project Entity Cleanup — LOCKED DIRECTION

Entity cleanup remains an explicit user action.

It should not become automatic heuristic deletion.

The reserved Identity entity remains undeletable.

Messages remain canonical historical evidence.

The cleanup operation must fully remove the selected entity's Project-derived Knowledge footprint, including the newly recognized `context_block_entities` requirement.

## Classification

**KEEP + FIX COMPLETE COVERAGE**

---

# 29. Delete vs Forget — DEFERRED

Exact deletion/forget semantics remain intentionally deferred.

The Knowledge review does not redefine:

- whether historical evidence is eventually physically deleted
- whether deleted Session transcripts remain browseable
- whether Forget removes derived Knowledge, evidence, or both
- which audit structures survive Forget

Current canonical evidence retention remains in place until the Delete vs Forget contract is designed.

## Classification

**DEFERRED**

---

# 30. Final Knowledge Stability Map

```text
KNOWLEDGE
│
├── KnowledgeStore facade ...................... KEEP
│
├── PostgreSQL canonical state ................. KEEP / VERY GOOD
├── AGE rebuildable projection ................. KEEP
│
├── user-global Entity identity ................ KEEP / VERY GOOD
├── immutable canonical names .................. KEEP
├── Project entity classification .............. KEEP
│
├── entity embedding identity semantics ........ FIX + SIMPLIFY
├── last_mentioned event time .................. FIX
│
├── Context revisions / immutable blocks ....... KEEP / VERY GOOD
├── Context provenance/supports ................. KEEP / VERY GOOD
├── human-edit accepted-time fallback .......... KEEP / LOCKED RULE
├── context_block_entities ..................... KEEP
│   ├── global merge migration ................. FIX
│   └── Project cleanup coverage ............... FIX
│
├── Relationship aggregate ..................... KEEP
├── Relationship observations .................. KEEP / VERY GOOD
├── retired historical evidence ................ KEEP
│   └── current retrieval filtering ............ FIX
├── relationship observed event time ........... FIX
│
├── Episodes ................................... KEEP
├── Episode source-message ownership ........... KEEP
├── Episode enrichment ......................... KEEP
├── manual Episode editing ..................... KEEP + FINISH
│
├── Knowledge retrieval boundary ............... KEEP
├── Evidence traversal ......................... KEEP / VERY GOOD
│
├── typed maintenance reviews .................. KEEP
├── global entity merge + rollback ............. KEEP
│   ├── Context association migration .......... FIX
│   ├── semantic participation frontier ........ FIX
│   ├── human-edit active-window detection ..... FIX
│   └── semantic-commit exclusion .............. FIX / HARDEN
│
├── historical entity reclassification ......... KEEP
├── historical relationship normalization ...... KEEP
│
└── Delete vs Forget historical visibility ..... DEFERRED
```

---

# 31. Locked Implementation Priorities

## Priority 1 — canonical Knowledge correctness

1. Add `context_block_entities` migration to global entity merge.
2. Add `context_block_entities` handling to merge rollback/state hashing.
3. Remove Project `context_block_entities` during explicit Project entity cleanup.
4. Exclude retired observations from normal/current Knowledge retrieval.
5. Propagate Context event time into `project_entity_contexts.last_mentioned_ms`.
6. Propagate Context event time into `relationship_observations.observed_at_ms`.

## Priority 2 — identity consistency

7. Make entity embedding text identity-only.
8. Remove Project type/topic from new-entity embedding generation.
9. Stop rebuilding global entity embeddings solely because Project classification changed.

## Priority 3 — maintenance correctness

10. Make semantic maintenance frontier participation-aware.
11. Include active semantic windows of every origin, including human-edit windows, in semantic quiescence checks.
12. Serialize global entity merge against semantic Knowledge commit at the durable identity boundary.

## Priority 4 — intended feature completion

13. Keep manual Episode editing.
14. Regenerate the Episode embedding before committing the edited Episode.
15. Persist edited narrative + replacement embedding + `user_modified=true` coherently.
16. Verify production API exposure during the API bridge pass.

---

# 32. Regression Tests to Add

## Retired observations

- current relationship retrieval excludes retired observations
- path retrieval excludes retired observation refs
- recent activity does not attach retired relationship evidence
- explicit evidence traversal can still return retired observations with retired status

## Global entity merge

- merge migrates `context_block_entities` from retired ID to survivor
- duplicate survivor/block association collapses safely
- merge state hash changes if Context entity associations change
- rollback restores Context block/entity identity where safe
- merge conflict handling remains correct when Context associations exist

## Project entity cleanup

- cleanup removes target Project `context_block_entities`
- cleanup does not remove another Project's Context associations
- global Entity row survives if another Project still owns the identity
- cleaned entity cannot reappear in later Episode enrichment through stale block associations

## Event time

- new entity context receives `last_mentioned_ms` from Context source time
- repeated mention advances `last_mentioned_ms` with max source time
- relationship observation receives source-derived `observed_at_ms`
- delayed semantic processing does not change event time
- human Context edit with no earlier source time receives accepted-edit time
- storage `created_at` remains independent from semantic source time

## Entity embeddings

- newly created identity embedding ignores Project entity type
- embedding rebuild reproduces the same identity vector input
- Project type/topic reclassification does not require identity embedding rebuild
- same global entity used with different Project types retains one identity embedding

## Maintenance frontier

- excluded Session exchanges do not block global entity maintenance
- disabled-then-reenabled frontier semantics are respected
- deleted Sessions do not count as pending semantic work
- active human-edit semantic window blocks maintenance
- completed human-edit semantic window no longer blocks maintenance
- active conversation semantic window blocks maintenance

## Merge/semantic concurrency

- semantic commit using an entity cannot succeed against stale retired identity after merge wins
- merge cannot apply over a Knowledge commit that has already acquired its required identity locks
- no partially migrated Context/entity state is possible

## Episode editing

- manual edit sets `user_modified=true`
- manual edit updates embedding
- embedding failure leaves prior narrative/vector pair unchanged
- user-modified Episode is not selected as an automatic consolidation target
- later vector retrieval uses the edited Episode representation

---

# 33. Final Conclusion

The Knowledge subsystem should **not** be redesigned.

Its primary structures are strong:

- PostgreSQL is canonical.
- AGE is a rebuildable graph projection.
- Entity identity is user-global.
- Project classification is Project-local.
- canonical names remain stable.
- Context is immutable and evidence-backed.
- relationships separate current aggregate state from historical observations.
- evidence traversal is distinct from current Knowledge retrieval.
- maintenance is explicit, typed, previewable, and auditable.
- merge rollback is mutation-aware rather than blindly restorative.
- Episodes remain grounded in canonical source messages and may be explicitly user edited.

The important remaining work is mostly integration debt caused by the newer Context-first Knowledge model:

- `context_block_entities` must participate in merge and cleanup,
- current retrieval must stop leaking retired observations,
- event-time must propagate all the way into entities and relationships,
- identity embeddings must stop depending on Project-local classification,
- maintenance frontiers must understand semantic participation and non-conversation semantic windows,
- global merge and semantic commit need a real durable serialization boundary,
- and manual Episode editing needs embedding consistency before production exposure.

These changes strengthen the existing architecture rather than replacing it.

---

## Stage 1 implementation status — 2026-09-10

The locked rationale remains intact. The following items are implemented and
validated; the later-stage work in this review remains open.

- §7: `06d04ec` assigns one accepted time to a newly authored untimed human
  block and preserves the durable value through retry and projection repair.
- §9: `71112d0` filters retired observations from current readers while
  explicit evidence traversal retains their historical status and support.
- §§13–14: `06d04ec` derives entity recency and relationship observation time
  from persisted eligible/cited Context support, with monotonic entity updates.
- `e344e24` adds the real-PostgreSQL combined correction → no-op → restart
  regression in
  `tests/integration/ingestion/test_project_semantic_postgres_flow.py`.
