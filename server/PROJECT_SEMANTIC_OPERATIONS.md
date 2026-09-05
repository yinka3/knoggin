# Project Semantic Processing

Project semantic processing is the one project-owned path that turns closed
conversation exchanges into durable Project Context, then into Knowledge. It is
local, PostgreSQL-backed work: a scheduler wake is only a prompt to inspect
durable state, never the source of truth.

## Ownership and checkpoints

`SessionRuntime` owns exchange closure and calls the single
`ProjectRuntime.signal_semantic_work()` wake edge. `ProjectSemanticJob`,
registered once by `ProjectRuntimeFactory`, owns admission and every semantic
stage for that project:

| Checkpoint | Owner | Durable result before moving on |
| --- | --- | --- |
| Admission | `SemanticWindowAdmission` plus `SemanticWindowWriter` | one frozen window, its exact message membership, domain/policy snapshots, and token accounting |
| Episode | `ProjectSemanticJob` | the Episode result, including an explicit empty result |
| Context | `ProjectSemanticJob` and `ProjectContextWriter` | immutable Context revision and `context_revision_id` stage checkpoint |
| File projection/import | `ProjectSemanticJob` through `ContextProjection` | controlled local edit import or repair of the committed PostgreSQL revision |
| VP-01 / VP-02 and Knowledge | `ProjectSemanticJob` plus `SemanticCommitWriter` | atomic entity/relationship reconciliation and `knowledge_committed` |
| Finalization | `ProjectSemanticJob` | idempotent Episode enrichment, then terminal `completed` |

There is no message-local semantic worker or message-local checkpoint path.
Window membership makes message records immutable evidence for that window;
`message_source_refs` remain owned by their assistant messages. The window is
the restart unit.

## Restart and operator recovery

The job always reads the active durable window before considering admission. A
restart therefore resumes its recorded stage and never selects its messages
again. If the Context revision was committed before a process or projection
failure, that revision is reused; the Context model is not called again.

`ProjectRuntimeFactory` performs Context-file synchronization before starting
the project scheduler. The same sole semantic job synchronizes before it admits
a conversation window and on its 30-second scheduler cadence. A valid edit
based on the current generated projection creates exactly one `human_edit`
window; a missing or known-stale projection is regenerated from PostgreSQL. A
malformed, stale, or concurrently changed file is preserved and recorded as a
bounded projection diagnostic rather than overwritten.

Automatic retries use `developer_settings.ingestion.semantic_window_retry`:
`max_attempts` (default 3), `initial_backoff_seconds` (default 30), and
`max_backoff_seconds` (default 300). An exhausted window remains active and
diagnosable in PostgreSQL. `ProjectMaintenanceService.retry_semantic_window`
is the local operator workflow: it resets only the retry state, retains frozen
membership, policy, stage, and Context checkpoint, and wakes the loaded
project's existing semantic job. It is deliberately not an external API yet.

`RuntimeHealthService.get_ingestion_health()` exposes bounded aggregate state:
`pending_count`, `claimed_count`, `failed_count`, `exhausted_count`,
`manual_retry_required`, the oldest pending age, scheduler state, and the last
processed indication. An exhausted count degrades health and emits a manual
retry warning; it does not silently admit a second window.

## Project Brief and Project Context

Project Brief is the user-authored project setup and operating intent. It is
read separately by the agent and is not generated from conversation exchange
processing.

Project Context is the evolving, revisioned semantic state. PostgreSQL Context
revisions are canonical. `CONTEXT.md` is an editable local projection: a
structured human edit produces a human-authored revision and a reconciliation
window; a missing or known-stale generated file is repaired from PostgreSQL.
Generic Agent and document tools cannot read, edit, or index it. Context
synchronization never treats its own generated file as a new user edit.
Only source-grounded, user-asserted, and human-asserted Context blocks enter
Knowledge. Agent-derived blocks still render in Project Context but are not
entity or relationship input.

## Settings and trace evidence

`developer_settings.ingestion.semantic_window_tokens` defaults to 128,000 and
is configurable. Admission keeps an entire crossing exchange, records
`source_token_count`, `overfill_tokens`, `overfill_ratio`, estimator identity,
and a frozen admission policy. Overfill is acceptable only when it is the
unavoidable remainder of one whole exchange; an exchange larger than the target
forms a single `oversized_exchange` window rather than being split.

Each durable window records `window_id`, origin, stage, domain version, frozen
policy, failure stage/code/summary, retry time, and attempt count. Context
revisions record their parent, window, origin, impact closure, blocks, and
supports. Knowledge observations retain their Context block provenance; cited
assistant support resolves through the owning `message_source_refs` rows.

VP-01 is the GLiNER2.5 entity boundary. VP-02 relationship extraction remains
an LLM call and is intentionally separate from the VP-01 model path.
