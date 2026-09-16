# Knoggin — Session Subsystem Review (Locked)

## Scope

This review covers the server-side Session subsystem and its direct runtime/storage bridges.

Primary files reviewed:

- `server/src/core/session/session_manager.py`
- `server/src/runtime/session_runtime.py`
- `server/src/runtime/session_runtime_factory.py`

Relevant bridges reviewed:

- `server/src/runtime/project_runtime.py`
- `server/src/runtime/project_factory.py`
- `server/src/runtime/api_port.py`
- `server/src/core/agent/orchestrator.py`
- `server/src/core/knowledge/store.py`
- `server/src/core/knowledge/db/writers/message_lifecycle_writer.py`
- `server/src/core/knowledge/db/writers/session_deletion_writer.py`
- `server/src/core/knowledge/db/readers/semantic_window_reader.py`
- `server/src/core/ingestion/semantic_window_admission.py`
- `server/src/infrastructure/schema.sql`
- relevant Session/runtime/storage tests

The intended deployment model remains one user on one local machine. Local concurrency still matters; distributed or multi-tenant redesign is out of scope.

## 1. Session Subsystem Role

The Session subsystem represents one durable conversation and, when loaded, one live conversational execution shell.

```text
SESSION — durable identity/configuration
│
├── session_id
├── project_id
├── model default
├── agent default
├── enabled-tools default
├── pinned document focus
├── Project-memory participation state
├── status
├── created_at
└── last_active_at

        loads into

SESSION RUNTIME — temporary execution shell
│
├── exact ProjectRuntime lease
├── shared Project DocumentService
├── application-owned AgentOrchestrator
├── session defaults for model / agent / tools
├── pinned document focus
├── one-active-run state
└── shutdown state

        each admitted run produces

CANONICAL EXCHANGE
│
├── editable user message
│   └── immutable revision history
│
├── terminal outcome
│   ├── assistant_final
│   ├── clarification
│   ├── failed
│   ├── cancelled
│   └── user_only
│
├── optional assistant message
├── source references
└── optional artifact

        after closure, if participating

PROJECT SEMANTIC ADMISSION
    ↓
Semantic Window
    ↓
Episode
    ↓
Context
    ↓
Project Knowledge
```

The Session layer should remain focused on conversation identity, configuration defaults, exchange lifecycle, and execution coordination. It should not own Project-scoped semantic or document services.

## 2. Durable Session Lifecycle — LOCKED

A Session has only two durable lifecycle states:

```text
OPEN ───────────────→ DELETED
       delete_session()
```

There is intentionally no archived, idle, inactive, timeout, or eviction Session domain state.

Loaded vs unloaded is runtime state, not domain state:

```text
durable Session = OPEN
        │
        ├── SessionRuntime loaded
        └── SessionRuntime not loaded
```

An open Session may be reconstructed later. A deleted Session may not resume. Therefore `deactivate_runtime_session()` is an internal runtime lifecycle operation, not a Session domain-state transition.

**Classification: KEEP**

## 3. SessionManager — LOCKED

`SessionManager` is the correct owner of durable Session creation, lookup/listing, runtime publication/resume/deactivation, Session deletion, Session metadata persistence, Project lease coordination, and shutdown of live Sessions.

Creation remains:

```text
persist durable Session
        ↓
acquire exact Project lease
        ↓
construct SessionRuntime
        ↓
publish SessionRuntime
```

Failure compensation runs in reverse.

**Classification: KEEP**

## 4. SessionRuntime — LOCKED

`SessionRuntime` is a valid high-level runtime container. It binds one Session to one `ProjectRuntime`, exposes Session defaults, coordinates one active normal run at a time, persists canonical exchanges through `KnowledgeStore`, signals Project semantic work after terminal exchange closure, and stops/cancels session-owned work during shutdown.

**Classification: KEEP**

## 5. SessionRuntimeFactory — LOCKED

`ProjectRuntimeFactory` is a substantial composition root. It builds domain state, entity resolution, retrieval, text processing, scheduler, Project document services/indexing, semantic jobs, Episode/Context processing, config subscriptions, and background work.

`SessionRuntimeFactory` is much smaller:

```text
ProjectRuntime
    ↓
construct SessionRuntime
    ↓
attach Project-owned DocumentService
    ↓
validate semantic owner exists
    ↓
return SessionRuntime
```

It is not equivalent to `ProjectRuntimeFactory`, but it still preserves a clean separation:

```text
SessionManager
    = durable/runtime lifecycle

SessionRuntimeFactory
    = construct a valid live Session

SessionRuntime
    = execute the live Session
```

It is not harmful abstraction and should remain unless a deliberate future simplification pass removes it.

**Classification: KEEP**

## 6. One Active Run Per Session — LOCKED

Normal conversational runs remain serialized per Session. The Session reserves the run before accepting the user message, so an overlapping run is rejected before a second user message persists. Different Sessions may run independently.

**Classification: KEEP**

## 7. Session Defaults vs AgentRun — LOCKED

Session-level model, `agent_id`, enabled tools, and document focus are defaults for future executions. The Agent layer resolves effective values and constructs an `AgentRun` for one execution.

```text
Session
    = conversation identity + execution defaults

AgentRun
    = one execution configuration
```

`enabled_tools = None` means inherit/default; `enabled_tools = []` means intentionally disable all.

**Classification: KEEP**

## 8. Canonical Exchange Model — LOCKED

A user message begins editable with an open exchange and revision 1. Edits append revisions and are allowed only during the edit window while the Session remains open. An exchange ends in exactly one terminal outcome: `assistant_final`, `clarification`, `failed`, `cancelled`, or `user_only`. Terminal closure seals the user message.

**Classification: KEEP / VERY GOOD**

## 9. Atomic Assistant Finalization — LOCKED

The successful assistant path commits the assistant message, source references, optional artifact, and user-exchange closure in one durable transaction. Retries return the existing assistant answer rather than writing a second one.

**Classification: KEEP / VERY GOOD**

## 10. FIX — Admitted Run Can Be Stranded During Shutdown

There is a lifecycle gap between durable message acceptance and active task registration:

```text
open_agent_run_stream()
    │
    ├── reserve Session run
    ├── persist canonical user message
    │
    └── return async generator
             │
             │ gap
             ↓
    generator begins
    └── register current task as active run
```

If shutdown/deletion lands in that gap, the user message already exists but no active task exists to cancel. The generator can later reject execution because runs are closed before reaching the normal terminal-exchange cleanup path. This can strand an editable/open exchange and also FIFO-block later semantic admission for that Session.

Required invariant:

> Once durable user-message acceptance succeeds, some Session-owned state must remain responsible for driving that exchange to exactly one terminal outcome.

A small private runtime state structure is enough; no new public model is required.

**Classification: FIX**

## 11. FIX — Message Idempotency Does Not Yet Provide Execution Idempotency

Durable user-message acceptance is idempotent, but `SessionRuntime` currently does not branch on whether the accepted message was newly created or already existed. A duplicate request can therefore reuse the same canonical user message while rerunning the agent and tools.

This creates repeated side-effect risk and a canonical identity mismatch: a retry can generate new response content while durable finalization correctly returns the already-existing assistant message ID.

Required invariant:

> A duplicate already-finalized exchange must not rerun the model/tools and then present new content under an existing canonical assistant-message identity.

Conceptually:

```text
new acceptance
    → execute normally

duplicate + still open
    → explicit recovery policy

duplicate + assistant_final
    → replay canonical persisted completion

duplicate + clarification
    → replay canonical clarification

duplicate + failed/cancelled
    → replay terminal result or require a new request identity
```

**Classification: FIX**

## 12. KEEP + RENAME + FINISH — Session Contribution to Project Memory

The intended capability is: “This Session should / should not contribute to Project memory.”

Current fields are Episode-specific:

- `episode_participation_enabled`
- `episode_participation_after_message_id`

The mechanism should remain, but the semantics should be renamed toward semantic/Project-memory participation, such as:

- `semantic_participation_enabled`
- `semantic_participation_after_message_id`

The frontier behavior is correct: re-enabling contribution records the current message frontier so conversation intentionally excluded while disabled does not retroactively enter Project memory.

Required admission order:

```text
all Project exchanges
        ↓
Session semantic-participation eligibility
        ↓
per-session FIFO
        ↓
semantic-window selection
```

Excluded exchanges must also not become FIFO barriers.

**Classification: KEEP MECHANISM + RENAME + FINISH**

## 13. FIX VIA PARTICIPATION — Deleted Sessions Can Currently Feed Future Semantic Windows

The Session deletion contract preserves canonical historical evidence but excludes the deleted Session from future semantic ingestion. The tombstone disables participation, but semantic admission currently does not enforce that participation state.

Because deleted Session exchanges are still returned and `session.status != open` can become a semantic-window flush reason, unclaimed closed exchanges can currently enter semantic processing after deletion.

This should be fixed through the already-decided participation boundary rather than through a separate design:

```text
deleted Session
    ↓
semantic participation disabled
    ↓
not eligible for new semantic windows
```

Previously processed knowledge remains unaffected.

**Classification: FIX THROUGH PARTICIPATION WIRING**

## 14. Session Deletion Semantics — LOCKED FOR NOW

Current Session deletion remains a tombstone operation:

```text
Delete Session
│
├── unload SessionRuntime
├── release Project lease
├── mark durable Session deleted
├── clear Session execution defaults
├── clear pinned document focus
├── disable future semantic participation
├── prevent resume / future mutation
│
├── preserve canonical messages
└── preserve Project-owned documents
```

The final treatment of deleted Session transcript visibility is not being decided in this subsystem review.

Whether deleted Session history should remain directly browsable, become hidden from ordinary conversation-history APIs, remain only as internal provenance/evidence, or behave differently under a future Forget operation belongs to the broader **Delete vs Forget semantics** design.

Therefore `get_session_history_readonly()` reading preserved deleted Session messages is not currently classified as a Session bug.

**Classification: KEEP CURRENT STORAGE BEHAVIOR / DEFER VISIBILITY TO DELETE VS FORGET**

## 15. FIX — `last_active_at` Semantics

Agreed rule:

> Update `last_active_at` on successful durable user-message acceptance.

Do not define activity as every token/tool event, metadata edit, document-focus change, or mere runtime resume. Successful user-message acceptance is the cleanest durable signal that the user actually interacted with the Session.

**Classification: FIX / SMALL**

## 16. SIMPLIFY — Setting Document Focus Should Not Necessarily Materialize a SessionRuntime

Current `set_document_focus()` resumes/loads the Session in order to access `SessionRuntime.document_service`, but that service is Project-owned.

Long-term cleaner rule:

> Changing Session metadata should not inherently require a live `SessionRuntime`.

A Project-level document boundary could resolve/validate the target, after which SessionManager persists the validated focus directly.

This is not a correctness bug and should not block the subsystem.

**Classification: SIMPLIFY / OPTIONAL**

## 17. Deleted Session History — DEFERRED

The future Delete vs Forget design should distinguish:

```text
Session deletion
        ≠
evidence retention
        ≠
conversation browse visibility
        ≠
Forget semantics
```

Do not prematurely change storage/read behavior until that broader ownership contract is designed.

**Classification: DEFER TO DELETE VS FORGET**

## 18. Final Session Stability Map

```text
SESSION
│
├── durable Session identity ................. KEEP / GOOD
├── lifecycle: open → deleted ................ KEEP
├── no idle/archive states ................... LOCKED
│
├── SessionManager ........................... KEEP
├── SessionRuntime ........................... KEEP
├── SessionRuntimeFactory .................... KEEP
│
├── Project lease ownership .................. KEEP / GOOD
│
├── one-active-run admission ................. KEEP / GOOD
│   └── admitted-but-not-started shutdown .... FIX
│
├── user-message lifecycle ................... KEEP / VERY GOOD
├── message revision history ................. KEEP / GOOD
├── terminal exchange model .................. KEEP / VERY GOOD
├── assistant/source/artifact transaction .... KEEP / VERY GOOD
│
├── request idempotency
│   ├── durable message acceptance ........... KEEP / VERY GOOD
│   └── execution/replay ..................... FIX
│
├── Session defaults → AgentRun ............... KEEP / GOOD
├── document focus ........................... KEEP
│   └── runtime materialization for metadata . SIMPLIFY
│
├── Project-memory participation
│   ├── enabled/frontier mechanism ........... KEEP
│   ├── Episode-specific naming .............. RENAME
│   └── semantic admission enforcement ....... FINISH
│
├── Session tombstone ........................ KEEP
│   └── deleted-session semantic admission ... FIX VIA PARTICIPATION
│
├── last_active_at ........................... FIX / SMALL
│
└── deleted transcript visibility ............ DELETE/FORGET DESIGN LATER
```

## 19. Locked Implementation Priorities

### Priority 1 — correctness

1. Fix the admitted-run/shutdown ownership gap.
2. Fix duplicate finalized-request execution/replay behavior.
3. Enforce Session semantic participation before semantic-window selection.
4. Ensure deleted Sessions cannot contribute new semantic evidence.

### Priority 2 — naming / consistency

5. Rename Episode-specific participation fields/APIs to semantic or Project-memory participation terminology.
6. Update `last_active_at` on successful durable user-message acceptance.

### Priority 3 — simplification

7. Consider validating/persisting document focus without loading a `SessionRuntime`.

## 20. Final Conclusion

The Session subsystem should **not** be redesigned.

Its principal architecture is sound:

- SessionManager owns lifecycle.
- SessionRuntime owns live conversation execution.
- SessionRuntimeFactory remains a useful assembly seam.
- ProjectRuntime owns Project-scoped services.
- AgentRun owns one execution.
- KnowledgeStore owns durable message/exchange persistence.
- semantic processing begins only after terminal exchange closure and participation eligibility.

The remaining important work is concentrated at lifecycle boundaries: accepted-but-not-started run ownership, durable idempotency vs execution idempotency, Session contribution to Project memory, deleted Session semantic exclusion, and precise activity timestamps.

The broader question of what Delete and Forget mean for historical conversation visibility remains intentionally deferred.

## Stage 3 implementation status — 2026-09-11

The locked rationale remains the historical review record. The Session
contribution-to-Project-memory work is now implemented and covered by normal
regressions:

- §§12–13 and priorities 3–5: `73af90b` renames the durable contract to
  semantic participation; `822a16f` applies it before per-Session FIFO; and
  `ec387a6` revalidates it in the atomic claim transaction.
- A re-enable records the durable frontier, so the disabled interval stays out
  of future windows. Deletion disables future participation without removing
  canonical message evidence.
- `5d32823` supplies the real-PostgreSQL scenario: excluded exchanges do not
  block later eligible exchanges, a toggle that wins before claim rejects the
  stale proposal without membership rows, and a later toggle cannot rewrite a
  claimed window.

The unrelated admitted-run ownership, execution idempotency, activity-timestamp,
and Delete-versus-Forget work remains open.
