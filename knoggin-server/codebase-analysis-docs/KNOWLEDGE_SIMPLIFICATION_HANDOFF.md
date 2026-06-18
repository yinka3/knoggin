# Knowledge System Simplification Handoff

This handoff captures the simplification work done in the Knoggin knowledge
system and the reasoning behind it. It is written for continuing the work on a
different machine without needing the full prior chat history.

## Current Direction

The broad simplification goal is to reduce unnecessary data movement and reduce
the number of places that look like sources of truth.

The system was drifting toward a shape where Redis, session metadata, Postgres
side tables, and Apache AGE all held meaningful parts of the knowledge state.
That made the code harder to reason about because every write had to answer:

- Is this the real data, or a helper copy?
- Which store should reads trust?
- What happens if one write succeeds and the other fails?
- Is a field session-scoped, project-scoped, or agent-scoped?

The current target model is:

- **Postgres canonical tables** own durable knowledge state.
- **Apache AGE** is a graph traversal projection rebuilt from Postgres.
- **`*_search` tables** are derived search indexes.
- **Redis** owns runtime/session/project/job state, not durable knowledge graph
  truth.
- **Agent directives** are simple prompt guidance, not separate rule/preference/
  ick data models.

This lets us keep graph traversal, but makes Postgres the place to inspect,
repair, and reason about durable state.

## System Summary

Knoggin turns conversations and project context into memory. The agent writes:

- raw messages,
- entity profiles and aliases,
- facts about entities,
- relationships between entities,
- hierarchy edges,
- prompt-facing memory blocks,
- agent behavioral directives.

The agent later retrieves this context through search, graph traversal, and
profile/fact reads.

Before this simplification pass, Apache AGE carried much of the domain graph
state, while regular Postgres tables acted like sidecars for vector search,
full-text search, or helper lookups. The refactor changes that: regular
Postgres tables are now the durable source of truth for core knowledge, while
AGE exists to answer graph-shaped traversal questions.

## Why These Simplifications Were Discussed

The user wanted to step back from testing and look for places where the system
was moving or storing too much data without improving accuracy or reliability.

The first example raised was rule/preference/ick duplication. That opened the
larger question: where are multiple concepts actually doing the same job?

The core simplification themes became:

1. **Remove redundant scopes.**
   Session metadata should not store project-level topic configuration.

2. **Collapse duplicate prompt concepts.**
   Rules/preferences/icks were all behavioral guidance. They can be represented
   as one `Directive` concept with a mode.

3. **Separate truth from projections.**
   Postgres should own durable state. AGE should be a traversal projection.

4. **Keep repairability.**
   If AGE is derived, there must be a way to rebuild it from canonical state.

5. **Avoid accuracy changes unless necessary.**
   These changes were intentionally storage/model simplifications, not changes
   to extraction quality, retrieval ranking, or entity-resolution judgment.

## Completed Simplifications

### 1. Session-Scoped `topics_config` Removed

Session metadata no longer stores `topics_config`.

Current behavior:

- `SessionManager.create_session(...)` can still accept `topics_config`.
- Session creation requires the ID of an existing persisted project.
- That value is passed to `ProjectManager.acquire_project_for_session(...)`.
- `ProjectManager` uses it only to seed the project topic config if the project
  does not already have one.
- Session metadata keeps `project_id`, `model`, `agent_id`, and
  `enabled_tools`.
- Session metadata does not keep `topics_config`.

Reasoning:

- Topic config is project behavior, not session behavior.
- Sessions should know which project they belong to, because resume/close/delete
  need to find the owning project efficiently.
- Sessions should not carry project config copies.
- The system is not released, so no legacy migration path was needed.

Important files:

- `src/knoggin_server/session/session_manager.py`
- `src/knoggin_server/project/project_manager.py`
- `tests/runtime/test_session_lifecycle.py`
- `tests/runtime/test_project_membership.py`

### 2. Rule / Preference / Ick Collapsed Into `Directive`

Agent working memory no longer needs separate rule/preference/ick concepts.
The simplified concept is `Directive`.

Current model:

- `Directive`
  - `mode`
  - `content`

Current modes:

- `require`
- `prefer`
- `avoid`

Prompt formatting groups directives as:

- `Required:`
- `Preferred:`
- `Avoid:`

Reasoning:

- `content` plus `mode` is enough to tell the agent how to use guidance.
- Separate rule/preference/ick buckets forced the system to move more data and
  maintain more category-specific code without improving behavior.
- `mode` gives enough prompt separation without extra models.
- Internal IDs still exist for storage operations (`directive_id`) even though
  the conceptual user-facing model is just content plus mode.

Important files:

- `src/common/schema/memory.py`
- `src/knoggin_server/knowledge/services/memory_service.py`
- `tests/knowledge/test_memory_service.py`
- `src/knoggin_server/agent/tools/community_tools.py`
- `src/common/schema/aac_schema.py`

Important boundary:

- This did **not** mean graph/profile preferences were removed.
- It did **not** mean onboarding seed text was removed.
- It did **not** mean prompt-template instructions called "rules" inside
  extraction prompts were removed.
- It specifically simplified agent working memory guidance.

### 3. Canonical Postgres Knowledge Tables Added

The schema now includes canonical tables for durable knowledge state:

- `messages`
- `entities`
- `entity_aliases`
- `facts`
- `relationships`
- `relationship_evidence_refs`
- `hierarchy_edges`

These tables live in `src/infrastructure/schema.sql`.

Reasoning:

- Postgres is easier to inspect, test, repair, and migrate than AGE graph
  properties.
- AGE remains useful for traversal, but should not be the only place durable
  state exists.
- Canonical SQL rows let us rebuild derived state when projection drift happens.

### 4. `AgeProjectionWriter` Added

AGE writes were moved behind a focused projection boundary:

- `src/knoggin_server/knowledge/db/writers/age_projection_writer.py`

This class owns derived AGE projection writes for:

- messages,
- entities,
- topics,
- relationships,
- facts,
- fact-message links,
- hierarchy edges,
- merge projection repair,
- fact invalidation/deletion,
- relationship deletion,
- clearing a project projection before rebuild.

Reasoning:

- Writers should mostly write canonical SQL, then call projection methods.
- Cypher should not be scattered everywhere.
- The storage boundary becomes easier to explain:
  - SQL writer code decides truth.
  - projection writer makes AGE reflect that truth.

Important note:

- `IDENTITY_ENTITY_ID` is used in `AgeProjectionWriter` only for relationship
  paths that can involve the special user/self entity. It is not used for
  hierarchy, messages, normal entities, facts, or topics.
- Identity initialization now happens in
  `ProjectManager.acquire_project_for_session(...)` before project runtime boot.
  `EntityWriter.ensure_identity_entity(...)` reserves entity ID `1`, persists
  the configured user under the reserved `__identity__` scope in SQL and AGE, and
  treats any non-identity occupant of ID `1` as a broken development database
  invariant. No legacy identity lookup or migration path is maintained.
- PostgreSQL owns canonical entity and message ID generation through
  `entity_id_seq` and `message_id_seq`. Entity ID `1` remains reserved for the
  identity, so normal entity allocation begins at `2`.
- Redis owns only session-local conversation turn ordering. It no longer owns
  canonical entity/message counters and requires no startup synchronization.

### Project Lifecycle Status And Retained Deletion

Persisted project metadata now carries an explicit lifecycle status:

- `active`
- `archived`
- `deleted`

New projects start active. Archived projects retain their metadata, durable
session membership, topic configuration, canonical PostgreSQL knowledge, search
rows, and AGE projection. They cannot create or resume sessions. Archiving is
rejected while live runtime sessions still hold the project, and reactivation
restores session eligibility.

`allowed_projects` may explicitly include an archived project, which lets an
active project expose retained archive knowledge to agent retrieval without
making the archived project writable again. Merely archiving a project does not
make its knowledge globally readable. Entity resolution may inspect entities in
the readable scope, but ingestion only reuses entities owned by the active
project or the reserved identity. A matching entity owned by another readable
project causes a new entity to be created in the active project instead.
Project create/update rejects unknown allowed-project IDs, removes duplicates
and self-references, and scope changes are rejected while runtime sessions are
active so a live `ProjectState` cannot retain a stale read scope.

`delete_project(...)` is intentionally a soft delete. It marks retained project
metadata as `deleted`, records `deleted_at`, and preserves session membership,
topic configuration, canonical PostgreSQL knowledge, search rows, AGE
projection, and message history. Deleted projects cannot create or resume
sessions, cannot be archived or reactivated, and are excluded from
`allowed_projects` read scopes. Deletion is rejected while runtime sessions are
active. Active projects that were already reading the deleted project have that
scope and its cached entities removed immediately.

There is currently no permanent project purge operation. A future explicit
purge design can use the retained deleted metadata to identify and remove the
full aggregate deliberately.

Lifecycle enforcement exists at both session acquisition and direct project
runtime boot. Missing, archived, and deleted projects cannot bootstrap a
`ProjectState`, even if lower-level manager methods are called directly.
Cross-project readable targets must carry a recognized `active` or `archived`
status; missing, malformed, and deleted metadata is excluded.

### Development Database Reset

This work changes the fresh PostgreSQL schema by adding canonical ID sequences,
foreign keys, cascade rules, and integrity checks. There is no migration or
legacy compatibility path because the system is pre-release. Existing
development database volumes must be recreated before using this code; running
the new `schema.sql` against an old populated volume is not the supported setup.

### 5. Message Writes and Reads Moved to Canonical SQL

`GraphWriter.save_message_logs(...)` now writes canonical `messages` first, then
projects AGE `Message` nodes for compatibility.

User and assistant messages both receive canonical IDs from `message_id_seq`.
The previous assistant `turn_id + 1_000_000_000` namespace has been removed.
Canonical message retries are idempotent only when the stored payload matches;
an ID reused with different content or scope fails instead of overwriting data.
Assistant persistence is awaited by `Context.add_assistant_turn(...)`. After the
bounded retries are exhausted, the staged Redis conversation row, sorted-set
entry, message-to-turn mapping, and cached content are deleted, and the final
exception is re-raised for higher-layer user feedback. PostgreSQL and AGE writes
share a transaction, while consumed sequence and turn numbers are intentionally
not recycled.

`GraphReader` message reads now use SQL:

- `get_message_text`
- `get_messages_by_ids`
- `get_recent_project_messages`
- `get_surrounding_messages`

Reasoning:

- Messages are durable records, not graph-only state.
- Jobs like profile refinement need reliable project-scoped message reads.
- AGE `Message` nodes are still projected because fact-source links currently
  use `EXTRACTED_FROM`.

Important files:

- `src/knoggin_server/knowledge/db/writers/graph_writer.py`
- `src/knoggin_server/knowledge/db/readers/graph_reader.py`
- `tests/storage/test_graph_writer_contract.py`
- `tests/storage/test_graph_reader_contract.py`

### Entity Deletion Is A Cascading Hard Delete

Entities are active graph interpretations rather than permanent audit records.
Deleting one removes its canonical aliases, facts, relationships, relationship
evidence, hierarchy edges, and search rows through PostgreSQL foreign keys with
`ON DELETE CASCADE`. The matching AGE entity and fact projections are removed
in the same database transaction.

Deletion entry points return the exact entity IDs removed. The cleanup job uses
those IDs for `EntityManager` cache eviction, and orphan eligibility is computed
from canonical SQL rather than AGE. Future mentions may create a new entity with
a new ID; there is no archive or tombstone model.

Merge remains transfer-first: canonical dependencies and `fact_search`
ownership move to the primary entity, the writer verifies that no facts,
relationships, or hierarchy edges remain on the secondary, and only then
deletes the secondary. Existing development databases must be recreated so the
fresh `schema.sql` constraint set is applied.

### 6. Entity Writes and Metadata Reads Moved to Canonical SQL

`EntityWriter.write_batch(...)` now writes:

- `entities`,
- `entity_aliases`,
- `entity_search`,
- then projects AGE entity/topic nodes.

Topic projection replaces every existing AGE `BELONGS_TO` edge for the entity
before creating the edge for its canonical PostgreSQL topic. PostgreSQL
therefore remains the single-topic source of truth even when an existing entity
is reclassified.

Entity metadata reads now use SQL for:

- max entity id,
- entity by id,
- entities by ids,
- entities by names/aliases,
- entity list and count,
- alias collisions,
- type/topic counts,
- hydration rows,
- top connected entities,
- entity relationships and evidence.

Reasoning:

- Entity metadata is durable profile state.
- Aliases are easier and safer as rows than as graph array properties.
- Relationship metadata and evidence should be queryable and repairable from SQL.

Important files:

- `src/knoggin_server/knowledge/db/writers/entity_writer.py`
- `src/knoggin_server/knowledge/db/readers/entity_reader.py`
- `tests/storage/test_entity_writer_contract.py`
- `tests/storage/test_entity_reader_contract.py`

AGE still remains useful for neighbor/path traversal.

### 7. Fact Writes and Reads Moved to Canonical SQL

`FactWriter.create_facts_batch(...)` writes canonical `facts` first, then
projects AGE `Fact` nodes and `HAS_FACT` edges.

`FactReader` now hydrates fact bodies from SQL.

Search still uses `fact_search` as a derived vector index.

Reasoning:

- Facts are durable claims about entities and need clear invalidation semantics.
- The body of a fact should not be trapped only in AGE properties.
- Search ranking can remain a derived index, but returned fact records should
  come from truth.

Important files:

- `src/knoggin_server/knowledge/db/writers/fact_writer.py`
- `src/knoggin_server/knowledge/db/readers/fact_reader.py`
- `tests/storage/test_fact_writer_contract.py`
- `tests/storage/test_fact_reader_contract.py`

### 8. Relationships Moved to Canonical SQL

Relationships now live in:

- `relationships`
- `relationship_evidence_refs`

`EntityWriter.write_batch(...)` writes relationship rows first and projects AGE
`RELATED_TO` edges after.

`GraphWriter.delete_relationship(...)` deletes canonical relationship state and
then removes the projected AGE edge.

`EntityReader.get_entity_relationships(...)` reads SQL relationship/evidence
state.

Reasoning:

- Relationship evidence is important enough to be structured.
- Relationship weights/confidence/context should not be calculated only from AGE.
- Merge and delete behavior becomes safer when SQL owns the relationship row.

Important files:

- `src/knoggin_server/knowledge/db/writers/entity_writer.py`
- `src/knoggin_server/knowledge/db/writers/graph_writer.py`
- `src/knoggin_server/knowledge/db/readers/entity_reader.py`

### 9. Hierarchy Edges Moved to Canonical SQL

Hierarchy now lives in:

- `hierarchy_edges`

`GraphWriter.create_hierarchy_edge(...)` writes SQL first, then projects AGE
`PART_OF`.

Hierarchy reads now use SQL:

- `get_parent_entities`
- `get_child_entities`
- `has_hierarchy_edge`
- `get_hierarchy_candidates`

Hierarchy is treated as a project-scoped, multi-parent directed acyclic graph:

- An entity may have more than one parent.
- Self-edges and edges that would create a cycle are rejected.
- Hierarchy creation and merge take a transaction-scoped project advisory lock
  so overlapping jobs cannot race their cycle checks.
- Candidate rows include `parent_type` and `child_type` for the hierarchy job.
- Candidate selection and embedding hydration require the same `project_id`.
- Entity merge rewrites every parent and child edge from the secondary entity to
  the primary; it no longer selects a single parent with `LIMIT 1`.
- A merge is rejected when the primary and secondary are already connected by
  an ancestor path because contracting those nodes would create a cycle.

Reasoning:

- Parent/child facts are durable relationship state.
- Merge cleanup needs SQL-owned hierarchy state.
- AGE can still project `PART_OF` for traversal.
- Preserving all canonical edges matches the schema's multi-parent model.
- Keeping cycle checks in the SQL transaction prevents canonical and projected
  hierarchy state from diverging.

Important files:

- `src/knoggin_server/knowledge/db/writers/graph_writer.py`
- `src/knoggin_server/knowledge/db/readers/graph_reader.py`

### 10. Entity Merge Reworked Around Canonical SQL

Entity merge was the largest cleanup. Topic choice is policy owned by
`MergeDetectionJob`; `GraphWriter.merge_entities(...)` persists the supplied
final topic while it performs the canonical SQL/AGE merge.

Current merge flow:

1. Validate primary and secondary entities from SQL.
2. Combine aliases.
3. Pick strongest confidence and newest mention timestamp.
4. Update primary canonical entity row with the final topic supplied by the
   merge job, falling back to the primary topic when none is supplied.
5. Move secondary aliases to primary.
6. Move secondary facts to primary.
7. Merge secondary relationships into primary relationships.
8. Transfer relationship evidence refs.
9. Remove direct primary-secondary relationship if present.
10. Rewrite hierarchy edges away from the secondary entity.
11. Read canonical relationship/hierarchy rows for projection.
12. Update AGE primary entity projection.
13. Update `fact_search` ownership and verify no transferable secondary-owned
    dependencies remain.
14. Transfer projected fact dependencies.
15. Replace the primary AGE topic with the supplied final SQL topic.
16. Replace affected AGE relationship projection from SQL state.
17. Replace affected AGE hierarchy projection from SQL state.
18. Delete secondary AGE entity projection.
19. Delete secondary SQL entity; cascades remove residual aliases/search rows.

Reasoning:

- The old merge asked AGE to validate entities and calculate merged relationship
  shape, then patched SQL afterward.
- Cross-topic merges choose one canonical SQL topic in the merge job by active
  facts, relationship count, recency, confidence, then primary-topic tie-break.
- The merged entity never accumulates both entities' projected topic edges.
- The new merge makes SQL decide the final state and uses AGE only as projected
  traversal state.

Important files:

- `src/knoggin_server/knowledge/jobs/merge_job.py`
- `src/knoggin_server/knowledge/db/readers/graph_reader.py`
- `src/knoggin_server/knowledge/db/writers/graph_writer.py`

Important tests:

- `tests/knowledge/test_merge_detection_job_contract.py`
- `tests/storage/test_graph_reader_contract.py`
- `tests/storage/test_graph_writer_contract.py`

### 11. Projection Rebuilder Added

`ProjectionRebuilder` was added as the final repair path:

- `src/knoggin_server/knowledge/db/projection_rebuilder.py`

It exposes:

```python
await graph_client.rebuild_project_projection(project_id, user_name=...)
```

via:

- `src/infrastructure/graph_client.py`

Current rebuild behavior:

1. Clear project-scoped AGE projection.
2. Fetch canonical messages.
3. Fetch canonical entities and aliases.
4. Fetch canonical relationships and evidence refs.
5. Fetch canonical facts.
6. Fetch canonical hierarchy edges.
7. Re-project messages.
8. Re-project entities and topics.
9. Re-project relationships exactly from SQL.
10. Re-project facts and fact-message links.
11. Re-project hierarchy edges.
12. Return a summary count.

Reasoning:

- If Postgres is truth and AGE is projection, AGE must be repairable.
- This avoids needing to trust live AGE state after a partial failure.
- It was kept as a callable/manual API, not a scheduled job, so the final chunk
  did not introduce new background behavior.

Important tests:

- `tests/storage/test_projection_rebuilder_contract.py`
- `tests/storage/test_graph_client_contract.py`

## What Has Not Been Done Yet

### 1. Tool Queries Still Hydrate Too Much From AGE

`src/knoggin_server/knowledge/db/tool_queries.py` still has rich AGE queries that
return entity/fact/relationship details.

Future simplification:

- Let AGE answer traversal/path questions and return ids.
- Hydrate entity/fact/relationship details from canonical SQL.

Reasoning:

- This would make tool answers less dependent on AGE property freshness.
- It would make projection drift less dangerous.
- It preserves AGE for what it is best at: paths and graph traversal.

This is probably the strongest next cleanup candidate.

### 2. Direct Cypher Still Exists Outside `AgeProjectionWriter`

Some writers still contain direct Cypher, especially for entity update/delete
projection and graph preferences.

Future simplification:

- Move remaining projection Cypher into `AgeProjectionWriter`.
- Keep writers shaped like:

```python
write canonical SQL
write derived search index
call projection helper
```

Reasoning:

- It would make the source/projection boundary even clearer.
- It would reduce repeated `build_cypher(...)` usage in domain writers.

### 3. Graph `Preference` Nodes Still Exist

`GraphWriter.create_preference(...)`, `GraphWriter.delete_preference(...)`, and
`GraphReader.list_preferences(...)` still use AGE `Preference` nodes.

This is separate from the Redis-backed agent `Directive` simplification.

Future decision:

- Decide whether graph preferences are still needed.
- If they are durable memory, consider moving them into SQL.
- If they are old behavior, remove or replace them with directives/facts.

Reasoning:

- The current system now has both:
  - Redis-backed agent directives,
  - AGE-backed preferences.
- That may still be conceptually redundant.

### 4. Claims vs Evidence Refs Still Needs a Design Pass

The user asked about claims and `evidence_ref`.

Current thinking:

- `evidence_ref` is valuable because it answers where a fact/relationship came
  from.
- A separate "claim" concept may be redundant if facts plus evidence refs already
  express the same thing.

Future simplification:

- Audit whether "claims" exist as an independent model in the current pipeline.
- Keep evidence/source references.
- Collapse claims into facts if they do not add separate behavior.

Reasoning:

- Evidence improves reliability.
- Duplicate claim/fact concepts increase data movement and prompt/schema
  complexity.

### 5. Extraction Prompts Were Not Simplified Yet

The user asked if extraction prompts can be simplified. No implementation was
done yet.

Future simplification:

- Reduce repeated instruction blocks.
- Make schemas stricter and smaller.
- Separate "what to extract" from "how to output JSON".
- Keep prompt-template "rules" if they are just local extraction instructions,
  not agent directives.
- Run existing ingestion/knowledge tests and any custom evals after changes.

Reasoning:

- Prompt sprawl creates maintenance cost.
- But extraction accuracy is high-risk, so simplify incrementally with tests.

### 6. Projection Write Coupling Still Exists

Most canonical writes still project to AGE in the same transaction.

This is reliable in the sense that failed projection prevents a canonical write
from silently committing. But it also means AGE availability can block canonical
Postgres writes.

Future decision:

- Keep strict dual-write transaction behavior.
- Or allow canonical writes to commit and repair AGE asynchronously with the
  projection rebuilder.

Reasoning:

- Strict coupling favors immediate traversal freshness.
- Decoupling favors write availability and repairability.
- Now that `ProjectionRebuilder` exists, decoupling is possible later, but it was
  not done yet.

### 7. Derived Search Repair Is Implemented

`SearchIndexRebuilder` now reconstructs `message_search`, `entity_search`, and
`fact_search` from canonical PostgreSQL rows for one project.

Current behavior:

- All embeddings are generated before existing search rows are modified.
- Replacement occurs in one PostgreSQL transaction.
- The global identity search row is refreshed from active and archived project
  facts; deleted projects are excluded.
- Project maintenance rejects repair while any project runtime has active
  sessions and blocks new session acquisition for the duration of repair.
- Entity registration, profile refinement, merge finalization, and repair share
  one deterministic entity embedding-text recipe.
- `message_search` is project-scoped and its message id cascades from
  `messages`.

AGE projection repair remains separate because it does not require model
inference and has different failure and performance behavior.

Fresh development databases are required after these schema changes. Existing
development database volumes should be discarded and recreated; no migration
or compatibility path was added.

### 8. Documentation Is Now Stale

`codebase-analysis-docs/CODEBASE_KNOWLEDGE.md` still contains old statements like
AGE owning graph data and memory categories being `rules`, `preferences`, and
`icks`.

Future cleanup:

- Update CODEBASE_KNOWLEDGE after this refactor is committed.
- Update `AGE_POSTGRES_DATA_FLOW.md` if it still describes AGE as truth.

Reasoning:

- The code has moved to Postgres-as-truth.
- The docs should not teach the old mental model.

## Current Worktree Notes

At the time this handoff was written, the worktree contained the simplification
changes plus unrelated untracked directories/docs.

Relevant changed tracked files:

- `src/infrastructure/graph_client.py`
- `src/knoggin_server/knowledge/db/readers/entity_reader.py`
- `src/knoggin_server/knowledge/db/readers/fact_reader.py`
- `src/knoggin_server/knowledge/db/readers/graph_reader.py`
- `src/knoggin_server/knowledge/db/writers/entity_writer.py`
- `src/knoggin_server/knowledge/db/writers/fact_writer.py`
- `src/knoggin_server/knowledge/db/writers/graph_writer.py`
- `tests/storage/test_entity_reader_contract.py`
- `tests/storage/test_entity_writer_contract.py`
- `tests/storage/test_fact_reader_contract.py`
- `tests/storage/test_fact_writer_contract.py`
- `tests/storage/test_graph_client_contract.py`
- `tests/storage/test_graph_reader_contract.py`
- `tests/storage/test_graph_writer_contract.py`

Relevant new files:

- `src/knoggin_server/knowledge/db/writers/age_projection_writer.py`
- `src/knoggin_server/knowledge/db/projection_rebuilder.py`
- `tests/storage/test_projection_rebuilder_contract.py`
- `codebase-analysis-docs/KNOWLEDGE_SIMPLIFICATION_HANDOFF.md`

Unrelated/unreviewed untracked items were present and should not be assumed part
of this migration unless intentionally added:

- `../knoggin-gpt/`
- `../knoggin-website/`
- `benchmark/`
- other untracked docs/assets under `codebase-analysis-docs/`

## Verification Already Run

The following checks were run successfully after the final chunk:

```bash
uv run pytest tests/storage
uv run pytest tests/ingestion
uv run pytest tests/knowledge
uv run pytest tests/agent/test_graph_retrieval_contract.py tests/agent/test_fact_check_retrieval_contract.py
uv run pytest tests/runtime/test_fact_resolution_scope_contract.py tests/runtime/test_job_clock_contracts.py tests/community/test_community_manager_seeding_contract.py
uv run ruff check src/knoggin_server/knowledge/db/projection_rebuilder.py src/knoggin_server/knowledge/db/writers/age_projection_writer.py src/infrastructure/graph_client.py tests/storage/test_projection_rebuilder_contract.py tests/storage/test_graph_client_contract.py
git diff --check
```

Storage count after final chunk:

- `tests/storage`: 192 passed

## Recommended Next Steps

### Step 1: Commit or Transfer Current Migration Safely

Before continuing, make sure the new untracked files are included:

```bash
git add \
  src/knoggin_server/knowledge/db/writers/age_projection_writer.py \
  src/knoggin_server/knowledge/db/projection_rebuilder.py \
  tests/storage/test_projection_rebuilder_contract.py \
  codebase-analysis-docs/KNOWLEDGE_SIMPLIFICATION_HANDOFF.md
```

Then inspect:

```bash
git status --short
git diff --stat
```

### Step 2: Update Stale Docs

Update `CODEBASE_KNOWLEDGE.md` to reflect:

- Postgres canonical knowledge tables,
- AGE as projection,
- directives replacing rules/preferences/icks for agent working memory,
- project-scoped topic config.

### Step 3: Simplify `tool_queries.py`

Recommended approach:

1. Pick one tool query.
2. Let AGE return ids/path structure only.
3. Hydrate details from SQL.
4. Add/adjust contract tests.
5. Run storage plus agent retrieval tests.

Start with query methods that currently return rich entity properties from AGE,
then move toward path methods.

### Step 4: Move Remaining Projection Cypher

Scan:

```bash
rg -n "build_cypher|MATCH \\(|MERGE \\(|DETACH DELETE|DELETE r" \
  src/knoggin_server/knowledge/db/writers
```

Move projection-only Cypher into `AgeProjectionWriter` where it makes sense.

### Step 5: Decide Preference/Directive Boundary

Audit:

- `GraphWriter.create_preference`
- `GraphWriter.delete_preference`
- `GraphReader.list_preferences`
- agent directive storage in `MemoryManager`

Decide whether AGE preferences are:

- still needed,
- should become SQL-backed,
- should become directives,
- or should be removed.

### Step 6: Claims / Evidence Design Pass

Search for claim-like models and uses. Decide whether claims are distinct from
facts. Preserve evidence/source refs unless there is a strong reason to remove
them.

### Step 7: Extraction Prompt Simplification

Do this after storage/model simplifications are stable.

Recommended guardrails:

- Change one extraction prompt or schema at a time.
- Keep eval/test runs tight.
- Do not collapse prompt instructions that protect entity accuracy without a
  test/eval showing behavior stays stable.

## Mental Model To Carry Forward

Use this rule when deciding future simplifications:

> Durable knowledge belongs in canonical Postgres tables. AGE should answer graph
> traversal questions over projected state. Redis should hold runtime/session/job
> state. Prompt guidance should use the smallest model that still lets the agent
> behave correctly.

If a new field or object does not clearly belong to one of those categories, it
is probably a simplification candidate.
