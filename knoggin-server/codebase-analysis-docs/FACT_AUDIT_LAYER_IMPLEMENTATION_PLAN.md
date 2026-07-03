# Fact Audit Layer Status And Manual Rollback Plan

This document is the current source of truth for fact audit work after the
manual/admin audit layer was implemented.

## Current Status

The core fact audit layer is effectively complete.

Implemented:

- `public.fact_change_audits`
- `FactAuditWriter`
- transactional fact remove/replace audit writes in `FactWriter`
- `FactChangeService`
- manual change types:
  - `manual_remove`
  - `manual_correction`
  - `fact_merge`
  - `bad_extraction_report`
  - `admin_recovery`
- profile extraction audit rows using `profile_extraction`
- dirty marking after manual fact changes
- profile-complete marker invalidation after manual fact changes
- tests for writer, service, profile extraction audit integration, and
  KnowledgeStore delegation

The system is unreleased, so direct schema changes remain acceptable.

## Product Semantics

Keep three user/admin concepts separate.

### Correct Again

If a user/admin makes a correction and then notices a small mistake, they should
make another correction against the current active fact.

This is already supported by:

- `FactChangeService.replace_facts(...)`
- `FactWriter.replace_facts_with_audit(...)`

The writer fetches active facts with:

```sql
AND invalid_at IS NULL
FOR UPDATE
```

So a correction of a correction is a normal new `manual_correction` audit row.

### View History

Fact audit history is stored, but there is not yet a clean reader/interface for
it.

The table can answer:

- what changed;
- who changed it;
- why it changed;
- which old facts were invalidated;
- which new facts were created;
- what profile extraction created or invalidated.

Needed next:

- `FactAuditReader.get_fact_change_audit(...)`
- `FactAuditReader.list_fact_change_audits_for_entity(...)`
- `FactAuditReader.list_fact_change_audits_for_project(...)`
- KnowledgeStore facade methods for these reader calls

### Rollback

Rollback means undo a specific fact-change batch and restore the previous
state. It is not the normal way to fix a current mistake.

Correction of current facts should use `replace_facts(...)`.

Rollback should be restricted to the latest eligible manual/admin change for an
entity. If a newer applied fact-change audit exists for the same entity, refuse
rollback and tell the caller to correct the current memory instead.

This prevents undoing an old correction after later corrections have already
moved the entity forward.

## Rollback Scope

Implement medium-complexity rollback only.

Rollback allowed:

- `manual_remove`
- `manual_correction`
- `fact_merge`
- `bad_extraction_report`
- `admin_recovery`, only when it has normal snapshots/created IDs and passes
  every safety check

Rollback not allowed:

- `profile_extraction`
- failed audit rows
- applying audit rows
- already rolled-back audit rows
- older audit rows when a newer applied fact-change audit exists for the same
  user/project/entity

Profile extraction should be repaired forward with `bad_extraction_report` or
`manual_correction` unless bad automated extractions become common enough to
justify a separate high-complexity rollback design.

## Schema Updates Needed

Add rollback state to `fact_change_audits`:

```sql
rollback_status TEXT NOT NULL DEFAULT 'not_requested',
rollback_actor TEXT,
rollback_reason TEXT,
rolled_back_at TIMESTAMPTZ,
rollback_failure_reason TEXT,
CONSTRAINT fact_change_audits_rollback_status CHECK (
    rollback_status IN (
        'not_requested',
        'rolling_back',
        'rolled_back',
        'rollback_failed'
    )
)
```

Add useful index:

```sql
CREATE INDEX IF NOT EXISTS fact_change_audits_rollback_idx
ON public.fact_change_audits(user_name, project_id, entity_id, rollback_status);
```

Because the system is unreleased, no compatibility shim is needed.

## Reader Work

Add `src/knoggin_server/knowledge/db/readers/fact_audit_reader.py`.

Suggested interface:

```python
class FactAuditReader:
    def __init__(self, client: PostgresClient): ...

    async def get_fact_change_audit(
        self,
        fact_change_id: str,
        *,
        user_name: str,
        project_id: str,
    ) -> dict | None: ...

    async def list_fact_change_audits_for_entity(
        self,
        *,
        user_name: str,
        project_id: str,
        entity_id: int,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]: ...

    async def list_fact_change_audits_for_project(
        self,
        *,
        user_name: str,
        project_id: str,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]: ...

    async def has_newer_applied_fact_change(
        self,
        *,
        user_name: str,
        project_id: str,
        entity_id: int,
        created_after,
        excluding_fact_change_id: str,
    ) -> bool: ...
```

Reader should parse JSON fields into Python values:

- `source_msg_ids`
- `invalidated_fact_ids`
- `invalidated_fact_snapshots`
- `created_fact_ids`
- `metadata`

## Audit Writer Updates

Extend `FactAuditWriter` with transaction-scoped rollback status methods:

```python
async def mark_rollback_started_with_cursor(
    cur,
    fact_change_id: str,
    *,
    actor: str,
    reason: str,
) -> None: ...

async def mark_rollback_succeeded_with_cursor(
    cur,
    fact_change_id: str,
) -> None: ...

async def mark_rollback_failed(
    fact_change_id: str,
    reason: str,
) -> None: ...
```

Successful rollback status changes should happen inside the same transaction as
fact mutation. Rollback failure marking may happen outside the transaction when
the mutation transaction aborts.

## Fact Writer Rollback

Add:

```python
async def rollback_fact_change_with_audit(
    *,
    fact_change_id: str,
    user_name: str,
    project_id: str,
    actor: str,
    reason: str,
) -> dict: ...
```

Inside one Postgres transaction:

1. Fetch audit row `FOR UPDATE` under `user_name/project_id/fact_change_id`.
2. Validate rollback eligibility.
3. Check there is no newer applied fact-change audit for same entity.
4. Lock current created facts with `FOR UPDATE`.
5. Lock invalidated old facts with `FOR UPDATE`.
6. Mark rollback as `rolling_back`.
7. Invalidate facts created by the original audit.
8. Restore invalidated snapshots as active facts.
9. Update AGE projection.
10. Update `fact_search`.
11. Mark rollback as `rolled_back`.
12. Return summary.

Return shape:

```python
{
    "fact_change_id": "...",
    "entity_id": 123,
    "rolled_back": True,
    "restored_fact_ids": ["old-1", "old-2"],
    "invalidated_fact_ids": ["new-1"],
}
```

## Eligibility Rules

Require:

- audit row exists under user/project scope;
- `status = 'applied'`;
- `rollback_status = 'not_requested'`;
- `change_type` is rollback eligible;
- `change_type != 'profile_extraction'`;
- all invalidated fact IDs have snapshots;
- each snapshot belongs to the same user/project/entity;
- created facts, if present, still belong to same user/project/entity;
- created facts are still active;
- invalidated old facts are still invalidated or are absent but restorable from
  snapshots;
- no newer applied fact-change audit exists for same user/project/entity after
  the target audit.

Do not add force rollback in the first implementation. If force rollback is
needed later, make it a separate admin-only operation with explicit conflict
reporting.

## Service Updates

Add to `FactChangeService`:

```python
async def rollback_fact_change(
    *,
    user_name: str,
    project_id: str,
    fact_change_id: str,
    actor: str,
    reason: str,
) -> dict: ...
```

Validation:

- non-empty `user_name`;
- non-empty `project_id`;
- non-empty `fact_change_id`;
- non-empty `actor`;
- non-empty `reason`.

After durable rollback succeeds:

- mark entity dirty;
- delete `RedisKeys.project_profile_complete(user_name, project_id)`;
- emit `job.dirty_entities_marked` with `reason='fact_rollback'`;
- keep Redis dirty marking best-effort.

## UI/API Guidance

When showing fact audit history:

- show latest eligible manual change as undoable;
- show older changes as history only when newer fact changes exist;
- show copy like: "Cannot undo because newer memory changes exist. Correct the
  current memory instead.";
- always allow a new correction against current active facts.

Do not expose rollback for `profile_extraction` rows in the first UI/API slice.

## Tests To Add

Reader tests:

- fetches scoped audit by ID;
- returns `None` for cross-user/project;
- lists entity history newest first;
- lists project history newest first;
- parses JSON fields;
- detects newer applied fact changes.

Writer/schema tests:

- schema contains rollback columns and rollback check constraint;
- rollback started/succeeded/failed SQL is correct;
- rollback status methods use transaction cursor where required.

FactWriter rollback tests:

- rejects missing audit;
- rejects non-applied audit;
- rejects `profile_extraction`;
- rejects already rolled-back audit;
- rejects newer applied audit for same entity;
- rejects missing snapshots;
- rejects changed created facts;
- invalidates created facts;
- restores invalidated snapshots;
- updates AGE and `fact_search`;
- marks rollback status in the same transaction;
- marks rollback failure when mutation fails.

Service tests:

- validates required inputs;
- delegates to store;
- marks dirty with `reason='fact_rollback'`;
- Redis failure returns `dirty_marked=false`.

KnowledgeStore tests:

- delegates fact audit reader methods;
- delegates rollback mutation method.

## Implementation Order

1. Schema rollback columns.
2. `FactAuditReader` and KnowledgeStore reader facade.
3. `FactAuditWriter` rollback status methods.
4. `FactWriter.rollback_fact_change_with_audit(...)`.
5. `FactChangeService.rollback_fact_change(...)`.
6. Focused tests.
7. API/UI wiring only after the service is stable.
