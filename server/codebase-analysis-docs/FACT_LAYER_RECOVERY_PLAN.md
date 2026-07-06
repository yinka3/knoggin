# Fact Layer Recovery Plan

This document consolidates the working plan for fact correction, lightweight
audit, and rollback/recovery in the Knoggin fact layer.

Working assumption: entity extraction should remain broad. It is better to
capture possible entities and let later resolution/merge logic clean them up
than to miss important entities. Facts need stricter recovery behavior because
facts are the system's active beliefs.

## Core Principle

The fact layer should not become a full event-sourcing system.

Use the existing invalidation model as the primary mechanism:

- Active facts have `invalid_at IS NULL`.
- Removed, replaced, or superseded facts get `invalid_at` set.
- Corrections create new active facts rather than mutating old fact content.
- Rollback is reserved for undoing a bad fact-change batch, not normal user
  correction.

The model may propose fact changes. Python/Postgres must enforce scope,
ownership, confirmation, and durable mutation.

## Current Useful Guards

The current code already has several useful properties:

- Facts are durable Postgres rows with `valid_at`, `invalid_at`, source message
  pointers, and project/user scope.
- `FactResolver.apply_fact_changes()` creates new facts before
  invalidating older ones.
- Fact creation failure skips invalidation, avoiding data loss when writes fail.
- Invalid source message IDs are stripped if they are not in the valid
  conversation window.
- Source session mapping is checked before linking a fact to a message.
- `process_extracted_facts()` supports explicit `supersedes` and `invalidates`
  fields from structured profile extraction.
- `invalidate_fact()` updates `facts`, AGE projection, and `fact_search`.
- Invalidated facts are eventually hard-deleted by the archival job.

## User-Facing Operations

Keep the user-facing vocabulary simple.

### Remove Memory

Use when a fact should no longer be active.

Example:

```text
Memory:
"Yinka uses Asana for roadmap planning."

User action:
Remove from memory
```

Backend action:

```text
invalidate_fact(fact_id)
```

This should not hard-delete the fact. It should set `invalid_at` so the memory
is no longer retrieved as active, while preserving auditability until archival.

### Correct Memory

Use when the current fact is wrong or incomplete and the user provides the
correct wording.

Example:

```text
Current memory:
"Yinka uses Asana for notes."

User correction:
"No, I use both Notion and Asana."

Replacement fact:
"Yinka uses both Notion and Asana for notes."
```

Backend action:

```text
invalidate_fact(old_fact_id)
create_fact(replacement_content)
```

This avoids compatibility markers and partial restoration logic. The corrected
compound fact becomes the active memory.

### Merge Facts

Fact merge should mean: replace multiple related facts with one clearer fact.

It should not be a separate semantic graph operation.

Example:

```text
Selected facts:
- "Yinka uses Notion for notes."
- "Yinka uses Asana for notes."

Replacement:
- "Yinka uses both Notion and Asana for notes."
```

Backend action:

```text
invalidate_fact(fact_id_1)
invalidate_fact(fact_id_2)
create_fact(replacement_content)
```

Minimum checks:

- All selected facts belong to the same authorized user.
- All selected facts belong to the same visible project scope.
- All selected facts belong to the same entity.
- Replacement content is non-empty.
- User/admin confirmation is required.

### Report Bad Extraction

Use when the system extracted or revised memory incorrectly.

User-facing reason choices should be strict and finite:

- `not_true`
- `misread_source`
- `outdated`
- `belongs_elsewhere`
- `should_not_remember`
- `duplicate`

Initial backend behavior can stay simple:

- `not_true`: invalidate the fact.
- `misread_source`: invalidate the fact and record a disputed extraction
  report.
- `outdated`: ask for replacement content, then use Correct Memory.
- `belongs_elsewhere`: invalidate or queue an admin/project-scope review.
- `should_not_remember`: invalidate and optionally add a future do-not-remember
  policy later.
- `duplicate`: propose Merge Facts with a clearer replacement.

## Internal Operations

### Invalidate Fact

This is the base operation.

Required behavior:

- Scope by `user_name`, `project_id`, and `fact_id`.
- Set `facts.invalid_at`.
- Set `fact_search.invalid_at`.
- Update AGE projection.
- Return whether the fact was actually changed.
- Be idempotent.

Current `FactWriter.invalidate_fact()` already does most of this, though it
currently accepts only `fact_id` and `project_id`.

### Create Fact

Required behavior:

- Scope by `user_name`, `project_id`, and `entity_id`.
- Verify parent entity exists in the same project.
- Preserve source message info when valid.
- Write to `facts`, AGE projection, and `fact_search`.
- Be auditable as part of a fact change.

Current `FactWriter.create_facts_batch()` already has most of this behavior.

### Replace Facts

Add a service-level operation rather than spreading this across UI/API code.

Suggested signature:

```python
async def replace_facts(
    *,
    user_name: str,
    project_id: str,
    entity_id: int,
    fact_ids: list[str],
    replacement_content: str,
    actor: str,
    reason: str,
) -> dict:
    ...
```

Behavior:

1. Fetch selected facts under user/project/entity scope.
2. Reject missing or cross-scope fact IDs.
3. Create one replacement fact.
4. Invalidate selected facts.
5. Record an audit row.
6. Recompute or schedule recomputation of the entity embedding.

This powers both Correct Memory and Merge Facts.

## Fact Change Audit

The lightweight audit table for user/admin corrections and profile extraction
changes is now implemented. The remaining audit-related work is a reader/history
surface and conservative manual rollback.

The goal is not full historical version control. The goal is to answer:

- What changed?
- Why did it change?
- Who or what caused it?
- Which old facts were invalidated?
- Which new facts were created?
- Can this batch still be undone?

Implemented table shape, with rollback columns still to add:

```sql
CREATE TABLE public.fact_change_audits (
    fact_change_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    entity_id BIGINT NOT NULL,
    session_id TEXT,
    actor TEXT NOT NULL,
    change_type TEXT NOT NULL,
    reason TEXT,
    source_msg_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    invalidated_fact_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    invalidated_fact_snapshots JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_fact_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    replacement_content TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'applied',
    rollback_status TEXT NOT NULL DEFAULT 'not_requested',
    rollback_actor TEXT,
    rollback_reason TEXT,
    rollback_failure_reason TEXT,
    rolled_back_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Recommended `change_type` values:

- `profile_extraction`
- `manual_remove`
- `manual_correction`
- `fact_merge`
- `bad_extraction_report`
- `admin_recovery`

Store `invalidated_fact_snapshots` for any fact invalidated by the batch. This
keeps manual rollback possible even if the archival job later deletes
invalidated facts.

## Rollback

Rollback should be an admin/service recovery path, not the normal correction
path.

Normal user correction should use Replace Facts.

Rollback is for undoing a bad fact-change batch.

The first rollback implementation should be medium complexity: manual/admin
changes only. Do not roll back `profile_extraction` audits in the first slice.
Repair bad automated extraction forward with `bad_extraction_report` or
`manual_correction`.

### Rollbackable Conditions

A fact change is rollbackable if:

- The audit row exists.
- It belongs to the authorized `user_name`, `project_id`, and `entity_id`.
- `status = 'applied'`.
- `change_type` is one of `manual_remove`, `manual_correction`, `fact_merge`,
  `bad_extraction_report`, or eligible `admin_recovery`.
- `change_type` is not `profile_extraction`.
- It has not already been rolled back.
- No later successful fact-change audit exists for the same entity.
- Facts created by the batch still exist and are still active.
- Facts invalidated by the batch are still invalidated, or full snapshots are
  available for safe restoration.

### Rollback Actions

For a simple rollback:

1. Load the `fact_change_audits` row.
2. Validate scope and rollback status.
3. Invalidate facts created by that audit.
4. Restore invalidated facts from that audit by setting `invalid_at = NULL`, or
   reinsert from snapshots if they were archived.
5. Update `fact_search`.
6. Update AGE projection.
7. Recompute or schedule recomputation of the entity embedding.
8. Mark the audit as rolled back.

If rollback cannot be completed, mark:

```text
rollback_status = rollback_failed
rollback_failure_reason = ...
```

Do not silently partially roll back.

## Handling The Notion And Asana Case

Prefer replacement over compatibility markers.

Bad state:

```text
Active:
- "Yinka uses Asana for notes."

Invalidated:
- "Yinka uses Notion for notes."
```

User says:

```text
"No, I use both Notion and Asana."
```

Simple behavior:

```text
Invalidate:
- "Yinka uses Asana for notes."

Create:
- "Yinka uses both Notion and Asana for notes."
```

Do not restore the old Notion fact as a separate active fact. Do not keep both
Asana and Notion as separate active facts. Do not add compatibility markers yet.

This keeps retrieval and contradiction handling simpler because there is one
clear active memory.

## Agent Presentation

The agent should not receive raw audit rows by default.

Present compact memory events:

```text
Memory correction:
The user corrected a stored memory.

Removed:
- Yinka uses Asana for notes.

Added:
- Yinka uses both Notion and Asana for notes.

Use the added memory going forward.
```

Agent tools should propose changes, not execute destructive mutation directly.

Possible tools:

```text
propose_memory_correction(fact_id, replacement_content, reason)
report_bad_memory(fact_id, reason, optional_replacement)
```

Server-side Python should own:

- scope validation,
- source validation,
- confirmation,
- fact invalidation,
- replacement creation,
- audit writing,
- projection/search updates.

## Where Things Can Go Wrong

### Partial Mutation

Creating a replacement fact and invalidating old facts can currently span
multiple writer calls. If one succeeds and another fails, memory can become
inconsistent.

Mitigation:

- Add a service-level transactional writer for replacement/merge.
- Or record an audit row before mutation and mark failure states explicitly.

### Later Changes Depend On The Same Facts

Rolling back an old batch can conflict with newer corrections.

Mitigation:

- Refuse rollback if a later successful fact-change audit exists for the same
  entity.
- Make a new correction against the current active facts instead.
- Add force rollback only as a later admin-only design, if needed.

### Archived Invalidated Facts

If invalidated facts were hard-deleted, rollback by ID cannot restore them.

Mitigation:

- Store `invalidated_fact_snapshots`, or
- define rollback expiration as the archival retention window.

### Search Or Projection Drift

If `facts` changes but `fact_search` or AGE projection is not updated, retrieval
can disagree with durable state.

Mitigation:

- Add one low-level operation that updates all fact surfaces together.
- Add tests that check `facts`, `fact_search`, and projection update calls.

### Entity Embedding Drift

Fact corrections change active knowledge, but entity embeddings may still
reflect old facts.

Mitigation:

- Replacement, merge, remove, and rollback should return/schedule the affected
  `entity_id` for embedding recompute.

## Proposed Implementation Phases

### Phase 1: Document And Schema

Status: complete for the audit table and indexes. Rollback status columns remain
to add.

Acceptance criteria:

- Fact changes can be durably audited by batch.
- Audits are scoped by user, project, and entity.

### Phase 2: Service Boundary

Status: complete for manual remove/replace/merge/bad-extraction service
operations. Destructive operations remain out of agent tools.

Acceptance criteria:

- User-facing remove/correct/merge operations go through one service boundary.
- Each operation writes an audit row.

### Phase 3: Profile Extraction Audit

Status: complete. Profile extraction writes audit rows for created/invalidated
facts and stores skipped/missing/failed details in metadata.

Acceptance criteria:

- A bad extraction report can identify which batch created a fact.
- The system can show what was removed and added by an extraction pass.

### Phase 4: Rollback

- Implement `rollback_fact_change(fact_change_id, actor)`.
- Start conservative: refuse rollback if newer fact-change audits exist for the
  same entity.
- Recompute or schedule recomputation of the entity embedding after rollback.

Acceptance criteria:

- The latest eligible manual/admin fact change can be undone as a batch.
- `profile_extraction` rollback is rejected with a clear reason.
- Rollback is idempotent.
- Rollback failure is recorded, not hidden.

### Phase 5: User/API Surface

- Expose memory actions:
  - Remove from memory.
  - Correct memory.
  - Merge selected memories.
  - Report bad extraction.
- Present proposed mutations before confirmation.

Acceptance criteria:

- Users can correct a memory days later without knowing the original audit ID.
- Bad extraction reports can trace back to the relevant fact-change audit when
  available.

### Phase 6: Tests

Add focused tests for:

- Removing a fact invalidates it and writes audit.
- Correcting a fact invalidates old fact and creates replacement.
- Merging facts invalidates selected facts and creates one replacement.
- Cross-project or cross-entity fact merge is rejected.
- Bad extraction audit captures created and invalidated facts.
- Rollback invalidates created facts and restores invalidated facts.
- Rollback refuses when later fact changes exist.
- Archived invalidated facts either restore from snapshots or produce a clear
  rollback-expired failure.

## MVP Recommendation

Start with:

1. `fact_change_audits`. Done.
2. `remove_fact`. Done.
3. `replace_facts`. Done.
4. user-facing bad extraction report that records the report and optionally
   invalidates/corrects the visible fact. Service path done; public surface
   remains later.
5. manual rollback for latest eligible audit. Next.

Delay:

- compatibility markers,
- pairwise fact relationship tables,
- autonomous rollback,
- general event sourcing,
- complex partial rollback UI.

This gives Knoggin practical memory correction without making the fact layer
hard to reason about.
