# Knoggin — Ingestion Subsystem Review (Locked)

## Scope

This review covers the server-side Ingestion subsystem and the direct bridges required to move canonical Session exchanges into durable Project Knowledge.

Primary ingestion files reviewed:

- `server/src/core/ingestion/semantic_window_admission.py`
- `server/src/core/ingestion/project_semantic_job.py`
- `server/src/core/ingestion/batch.py`
- `server/src/core/ingestion/policy.py`
- `server/src/core/ingestion/text_processor.py`
- `server/src/core/ingestion/vp01.py`
- `server/src/core/ingestion/context_entity_build.py`
- `server/src/core/ingestion/relationship_extractor.py`

Directly relevant bridges reviewed:

- semantic-window reader/writer
- Project Context reader/writer/projection
- entity resolver
- semantic Knowledge commit writer
- ProjectRuntime / ProjectRuntimeFactory composition
- semantic-window schemas and ingestion contracts
- real PostgreSQL integration tests and ingestion unit tests

The intended deployment model remains:

- one user
- one local machine
- local concurrency is real and must be handled
- no distributed or multi-tenant redesign

---

# 1. Ingestion Role

Ingestion is the bridge that converts closed canonical conversation exchanges into durable Project semantic state.

The intended flow is:

```text
Canonical Session Exchanges
        ↓
SemanticWindowAdmission
        ↓
Frozen Project Semantic Window
        ↓
Episode Generation
        ↓
Project Context Revision
        ↓
Context-first Entity Extraction
        ↓
Entity Resolution
        ↓
Context Relationship Extraction
        ↓
Atomic Knowledge Reconciliation
        ↓
Episode Enrichment
        ↓
Semantic Window Completed
```

The key architectural rule is:

> Once the Context stage exists, Context — not the raw transcript — is the semantic input for entity and relationship Knowledge extraction.

The transcript remains provenance/evidence, but Context is the durable semantic interpretation boundary.

---

# 2. Semantic Window Admission — LOCKED

Semantic-window admission is structurally strong.

The current design correctly preserves:

- whole exchanges
- per-Session FIFO
- independence between different Sessions
- global ordering by source/event time
- unavoidable whole-exchange overfill
- exact frozen message membership
- one active semantic window per Project

Conceptually:

```text
Session A FIFO ─┐
Session B FIFO ─┼─→ global event-time merge → complete-exchange semantic window
Session C FIFO ─┘
```

An open or editable exchange blocks later exchanges from the same Session, but does not block independent Sessions.

A semantic window may exceed its target size only when preserving one complete exchange requires it.

## Classification

**KEEP / VERY GOOD**

---

# 3. Atomic Claim Revalidation — LOCKED

Admission selection does not blindly become durable.

The storage claim path:

1. locks the owning Project row,
2. checks whether another active semantic window already exists,
3. revalidates selected durable messages,
4. confirms the selected user exchanges are still sealed and closed,
5. confirms assistant membership still matches the exchange,
6. then persists the window and exact membership.

This closes the normal selection-to-claim race.

## Required invariant

> Semantic membership is not canonical until it passes durable revalidation under the Project claim lock.

## Classification

**KEEP / VERY GOOD**

---

# 4. Semantic Window State Machine — LOCKED

The durable semantic-window stages are:

```text
CLAIMED
   ↓
CONTEXT_COMMITTED
   ↓
KNOWLEDGE_COMMITTED
   ↓
COMPLETED
```

Each stage is a restart checkpoint.

A later failure does not erase a successful earlier checkpoint.

Examples:

```text
Episode failure
    → remains CLAIMED

Context already committed
Knowledge failure
    → remains CONTEXT_COMMITTED

Knowledge committed
Episode enrichment failure
    → remains KNOWLEDGE_COMMITTED
```

The stage writer only permits one-step forward transitions.

## Classification

**KEEP / VERY GOOD**

---

# 5. Retry Model — LOCKED

Automatic retries belong to the durable semantic window as a whole.

The window retains:

- exact membership
- frozen policy
- stage
- Context checkpoint
- failure stage/code
- retry timestamp
- attempt count

An exhausted window remains active and diagnosable.

An explicit operator retry resets retry metadata while retaining the exact frozen window and its successful checkpoints.

This is coherent with the existing design.

## Classification

**KEEP**

---

# 6. `SemanticWindowBuild` — LOCKED

`SemanticWindowBuild` is a justified high-level temporary structure.

It owns one Context-first Knowledge reconciliation operation:

```text
SemanticWindowBuild
│
├── window identity
├── Project/user scope
├── exact Context revision
├── durable impact closure
├── Context supports
├── frozen canonical message evidence
├── frozen IngestionPolicy
├── extraction trace
├── validation issues
├── pending Context mentions
├── pending entity result
└── pending relationship writes
```

It does not mutate Knowledge directly.

It is the correct temporary aggregate for passing one coherent semantic operation across several internal services.

## Classification

**KEEP**

---

# 7. `IngestionPolicy` — LOCKED

`IngestionPolicy` correctly freezes the decision inputs required for one semantic operation.

It captures:

- VP-01 threshold
- entity candidate thresholds
- entity-resolution threshold
- common-word threshold
- sparse-context verbs
- compiled domain

The policy is persisted into the semantic-window snapshot and can be reconstructed after restart.

This prevents configuration changes halfway through an active semantic window from silently changing identity decisions.

## Classification

**KEEP**

---

# 8. FIX — Capture Domain and Ingestion Policy as One Coherent Runtime Snapshot

The current Project runtime exposes separate capture operations:

```text
await capture_domain()
capture_ingestion_policy()
```

`capture_domain()` uses the Project domain lock.

`capture_ingestion_policy()` separately reads the live `compiled_domain`.

A domain activation can therefore occur between the two reads:

```text
semantic operation              domain activation

capture domain v4
                              → install domain v5
capture policy
    └── policy contains v5
```

The admission path detects the mismatch instead of persisting corrupt state, which is good, but the semantic operation can still fail unnecessarily.

## Required fix

Expose one Project-runtime capture edge that obtains the semantic configuration under the same domain lock.

Conceptually:

```text
capture_semantic_policy()
    ↓
exact CompiledDomain
+ IngestionPolicy built from that exact domain
```

A tuple or existing aggregate is sufficient; no large new public model is required.

## Required invariant

> One semantic operation must never assemble its frozen configuration from two different Project-runtime moments.

## Classification

**FIX**

---

# 9. DELETE — Dead `llm_ner` Setting

`llm_ner` currently exists in:

- `TextProcessorSettings`
- `TextProcessor`
- `IngestionPolicy`
- persisted ingestion-policy snapshots

However, the current Context-first entity-extraction pipeline does not contain an LLM-NER branch.

Current entity mention extraction uses:

```text
known aliases
    +
VP-01 / GLiNER
```

Changing `llm_ner` currently does not change entity extraction behavior.

Unless LLM-NER is intentionally returning soon, remove it from the current runtime/settings/policy surface.

## Classification

**DELETE / SIMPLIFY**

---

# 10. Context-first Entity Extraction — LOCKED

The current entity-extraction direction is correct.

`TextProcessor` receives only eligible current Context blocks in the durable impact closure.

The model-facing text is assembled while preserving offsets back to the exact block versions.

Model spans are mapped back to the smallest set of current blocks they touch.

The system does not convert Context-block evidence into message IDs.

## Classification

**KEEP**

---

# 11. VP-01 Offset/Provenance Contract — LOCKED

VP-01 requires exact span offsets.

If the model returns invalid bounds, mismatched text, missing span metadata, or out-of-range offsets, the candidate is discarded instead of guessed onto a Context block.

That is the correct provenance rule.

## Classification

**KEEP / VERY GOOD**

---

# 12. Message Entity References — LOCKED

Context support does not automatically imply that every supporting message literally mentioned every extracted entity.

Therefore `message_entity_refs` are created only after checking the exact frozen canonical message text for a literal token-bounded occurrence.

This keeps Context-block semantic association distinct from direct message mention evidence.

## Classification

**KEEP / VERY GOOD**

---

# 13. FIX — Name-only In-build Entity Deduplication

The current `EntityResolver.mention_dedupe_key()` effectively reduces an in-build mention identity to normalized name and discards entity type, topic, and contextual identity evidence.

This can collapse incompatible same-surface mentions inside one semantic build.

Example:

```text
Apple acquired Acme.

I ate an apple after lunch.
```

If the frozen domain distinguishes the two mentions, a newly allocated `"apple"` identity from the first mention can be reused for the second simply because the normalized name matches.

## Required invariant

> Two typed mentions that are incompatible under the frozen domain must never be automatically collapsed solely because their text matches.

Simply changing the key to `(name, type, topic)` improves the immediate bug but should not be treated as a complete general homonym solution.

Different people may still share the same name and type.

The better conceptual rule is:

> A pending in-build identity should be evaluated as an identity candidate, not as an unconditional name-to-ID map.

## Classification

**FIX**

---

# 14. FIX — VP-02 Must Not Use Canonical Names as Wire Identifiers

The current Context relationship extraction contract asks the model to return canonical entity names and then maps those names back to durable IDs.

This fails for legitimate homonyms.

The database identity model correctly treats `entity_id` as identity and does not require canonical names to be globally unique.

Therefore the extraction transport is weaker than the Knowledge model.

## Required change

Use opaque local entity handles.

Example:

```text
Candidate Entities

e1 = Apple [Company]
e2 = Apple [Food]
e3 = Sarah Chen [Person]

Current Context Blocks

b1 = ...
b2 = ...
```

Model output:

```text
entity_a = e3
entity_b = e1
block_ids = [b1]
relationship = ...
```

Server-side mapping:

```text
e1 → durable entity_id
e2 → durable entity_id
e3 → durable entity_id
```

Canonical names remain canonical names. They simply stop serving as transport-level identity keys.

## Classification

**FIX**

---

# 15. Context Relationship Extraction — KEEP AFTER IDENTITY FIX

Outside the endpoint-identity issue, Context VP-02 is well constrained.

It:

- accepts only resolved candidate entities
- receives current eligible Context blocks
- uses local block handles
- requires one or more supporting block IDs
- rejects unknown blocks
- rejects unknown entities
- rejects self relationships
- normalizes relationship semantics through the frozen domain
- records validation issues for rejected outputs
- deduplicates equivalent writes

## Classification

**KEEP, AFTER FIXING ENTITY HANDLE IDENTITY**

---

# 16. Atomic Knowledge Reconciliation — LOCKED

`SemanticCommitWriter` is an important and valid ownership boundary.

For one `context_committed` window it atomically:

- locks the semantic checkpoint
- validates the exact Context revision
- validates the exact impact closure
- writes new entities
- writes aliases
- writes Context-block/entity associations
- writes literal message/entity refs
- retires stale Context-backed relationship observations
- writes replacement relationship observations
- removes orphan relationships
- rebuilds the graph projection
- advances the semantic window to `knowledge_committed`

All of this occurs as one durable transaction.

## Required invariant

> Knowledge must never advertise `knowledge_committed` unless SQL state and its rebuildable graph projection correspond to the same committed semantic snapshot.

## Classification

**KEEP / VERY GOOD**

### Stage 1 implementation status — 2026-09-10

A later no-change window now reuses the completed Context revision with empty
window-specific impact. It still advances through the normal durable Knowledge
and completion checkpoints; storage accepts reuse only after the owning window
published Knowledge. Restart and retry rebuild this decision from durable
state. The combined real-PostgreSQL regression is
`tests/integration/ingestion/test_project_semantic_postgres_flow.py::test_project_semantic_job_preserves_correction_history_through_noop_restart`
(`e344e24`).

---

# 17. FIX — Publish Successful Semantic Entity Changes into the Live `EntityResolver`

This is a concrete Ingestion/Knowledge bridge bug.

New entities are correctly prepared as pending writes and deliberately kept out of shared resolver indexes until durable commit succeeds.

`EntityResolver` already provides post-commit publication primitives for newly durable entity writes and aliases.

However, the semantic Knowledge path does not currently publish the successfully committed entity result back into the live Project resolver.

This allows:

```text
Window 1
    ↓
new entity / alias committed to PostgreSQL
    ↓
live Project EntityResolver remains stale
    ↓
Window 2
    ↓
known-alias matcher and resolver operate from stale local indexes
```

## Recommended repair

After the semantic Knowledge transaction succeeds:

```text
commit Knowledge
    ↓
publish committed entity result into Project EntityResolver
    ↓
bump alias version where needed
    ↓
TextProcessor PhraseMatcher lazily rebuilds
```

Prefer one high-level resolver method, conceptually:

```text
apply_committed_context_entity_result(result)
```

rather than making `ProjectSemanticJob` understand several cache internals.

PostgreSQL remains authoritative.

## Classification

**FIX**

---

# 18. FINISH — Session Participation Must Be Enforced Before FIFO

This decision was already locked in Project and Session reviews.

The Ingestion implementation must apply it before semantic FIFO/admission.

Required order:

```text
Project exchanges
    ↓
Session semantic participation enabled?
    ↓
message_id > participation frontier?
    ↓
per-Session FIFO
    ↓
global semantic-window merge
```

An intentionally excluded exchange must not become a FIFO barrier for a later eligible exchange.

## Classification

**FINISH EXISTING DECISION**

---

# 19. FINISH — Participation Must Be Revalidated During Atomic Claim

Filtering only during the first admission read is not enough.

Possible race:

```text
semantic selection sees Session enabled
    ↓
user disables participation
    ↓
participation frontier advances
    ↓
old selection attempts durable claim
```

The semantic-window claim must re-check the relevant Session participation state while performing durable membership validation.

Desired linearization:

```text
participation change commits before claim
    → exchange excluded

semantic claim wins first
    → exchange becomes frozen window evidence
```

## Classification

**FINISH EXISTING DECISION / REQUIRED CLAIM-TIME VALIDATION**

---

# 20. Deleted Sessions and Ingestion

Once semantic participation is correctly enforced:

```text
deleted Session
    ↓
semantic participation disabled
    ↓
no new semantic-window admission
```

Already-created Knowledge remains historical Project state.

This resolves the Session deletion ingestion bug identified in the Session review.

The current `session_closed` admission concept should be reconsidered because Session has only `open` and `deleted`, and deleted Sessions should not act as a reason to flush unclaimed material.

## Classification

**FIX THROUGH PARTICIPATION WIRING / SIMPLIFY OBSOLETE BRANCH**

---

# 21. REMOVE — Legacy Episode Window-size Policy Remnants

This was already locked during the Project review.

The semantic window is the authoritative conversation-selection window.

Remove the second-window concepts, including:

- Project Episode window size
- `EpisodeGenerationPolicy.target_message_count`
- hardcoded reference window size used only to construct that policy
- related alternate Episode-selection branches

Keep Episode safety caps such as maximum source messages/tokens and narrative limits.

## Classification

**REMOVE — ALREADY LOCKED IN PROJECT REVIEW**

---

# 22. Human `CONTEXT.md` Reconciliation — LOCKED

The controlled Context file boundary is structurally strong.

Important behavior:

- PostgreSQL Context remains authoritative.
- generated Context file writes use content-hash preconditions.
- unknown/stale user changes are not overwritten.
- valid user edits become explicit `human_edit` Context revisions.
- a human edit creates a semantic reconciliation window already at `context_committed`.
- file projection failure does not roll back a successfully committed Context revision.
- later reconciliation repairs the file projection.

## Classification

**KEEP / VERY GOOD**

---

# 23. Final Ingestion Stability Map

```text
INGESTION
│
├── whole-exchange admission ................. KEEP / VERY GOOD
├── per-Session FIFO ......................... KEEP
├── global event-time ordering ............... KEEP
├── atomic membership claim/revalidation ..... KEEP / VERY GOOD
│
├── semantic participation
│   ├── pre-FIFO filtering ................... FINISH
│   └── claim-time revalidation .............. FINISH
│
├── frozen semantic window ................... KEEP
├── semantic-window stage machine ............ KEEP / VERY GOOD
├── retry/restart model ...................... KEEP / VERY GOOD
│
├── SemanticWindowBuild ...................... KEEP
├── IngestionPolicy .......................... KEEP
│   ├── coherent domain/policy capture ....... FIX
│   └── dead llm_ner field ................... DELETE
│
├── Episode generation checkpoint ............ KEEP
│   └── duplicate target-message policy ...... REMOVE
│
├── Context revision checkpoint .............. KEEP / VERY GOOD
├── human CONTEXT.md reconciliation .......... KEEP / VERY GOOD
│
├── Context-first entity extraction .......... KEEP
├── VP-01 offset/provenance contract ......... KEEP / VERY GOOD
├── literal message/entity references ........ KEEP
│
├── in-build entity identity
│   └── name-only collapse ................... FIX
│
├── Context VP-02
│   └── canonical-name endpoint identity ..... FIX
│
├── atomic Knowledge reconciliation .......... KEEP / VERY GOOD
│   └── live resolver publication ............ FIX
│
├── Episode enrichment checkpoint ............ KEEP
└── final semantic completion ................ KEEP
```

---

# 24. Locked Implementation Priorities

## Priority 1 — semantic correctness

1. Enforce Session semantic participation before per-Session FIFO.
2. Revalidate participation/frontier during atomic semantic-window claim.
3. Fix name-only in-build identity collapse.
4. Replace VP-02 canonical-name identifiers with local entity handles.
5. Publish successful semantic entity/alias commits into the live Project `EntityResolver`.

## Priority 2 — frozen-policy correctness

6. Capture the Project domain and ingestion policy from one coherent locked runtime snapshot.

## Priority 3 — cleanup

7. Remove dead `llm_ner` configuration unless the feature is intentionally returning.
8. Remove the already-rejected Episode target-message/window-size policy remnants.
9. Simplify deleted-Session/session-closed admission behavior after participation wiring is complete.

---

# 25. Regression Tests to Add

## Participation

- disabled Session exchange never enters semantic admission
- disabled exchange does not FIFO-block later eligible exchanges after re-enable
- re-enable frontier excludes the disabled interval
- participation toggle committed before claim invalidates the stale selection
- claim committed before participation toggle preserves the already-frozen window
- deleted Session cannot contribute previously unclaimed exchanges

## Entity identity

- two same-name mentions with incompatible types are not collapsed into one pending entity
- same canonical name for two durable entities does not crash VP-02
- VP-02 local entity handles resolve to the correct durable IDs
- relationship extraction remains correct when two candidates have identical canonical names

## Resolver publication

- semantic commit creates new entity
- same live Project runtime immediately recognizes that entity on the next semantic window
- newly committed alias is immediately visible to the known-alias matcher
- failed Knowledge transaction does not publish pending entities into the resolver

## Policy capture

- concurrent domain activation cannot produce a semantic operation with mismatched frozen domain/policy versions

---

# 26. Final Conclusion

The Ingestion subsystem should **not** be redesigned.

Its principal architecture is strong:

- canonical exchanges are frozen into complete semantic windows,
- semantic progress is checkpointed durably,
- Context is the semantic interpretation boundary,
- entity and relationship extraction operate on immutable Context evidence,
- Knowledge reconciliation is atomic,
- retries resume from durable checkpoints.

The remaining important problems are concentrated around entity identity and runtime publication:

- name-only pending identity collapse,
- canonical names acting as VP-02 transport identifiers,
- successfully committed entities/aliases not being published into the live resolver,
- and semantic participation not yet being wired into the complete admission/claim boundary.

Fixing those edges should preserve the current architecture while making the durable Knowledge identity model and the live ingestion runtime agree.
