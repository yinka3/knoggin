# ProjectRuntime Cleanup Plan

## Goal

Tighten ProjectRuntime and ProjectRuntimeFactory ownership without changing the
basic design: one shared project runtime, exact session leases, ordered startup,
domain activation locking, and owner-scoped background cancellation all remain.

## Current findings

The journal findings still match the current implementation:

- `ProjectRuntime.capture_semantic_policy()` bypasses dependency injection by
  reading `ConfigManager.get()` directly.
- `ProjectRuntimeFactory.create()` reads live configuration repeatedly while an
  asynchronous bootstrap is in progress, so one runtime can be assembled from
  different config versions.
- `ProjectManager` receives the application-owned `ProjectFilesystemFactory`,
  but does not pass it into `ProjectRuntimeFactory`; document and Context paths
  each construct another factory from config.
- `load_domain_config()` and `capture_domain()` have no production callers.
- `_register_background_jobs()` accepts an unused `resources` argument and an
  optional semantic processor even though production always supplies one.
- `ProjectRuntime` compiles the initial domain a second time and contains
  scheduler `None` checks that contradict its constructor contract.
- failed shutdown is recorded as closed, preventing a useful retry.

The nearby ownership audit added these details:

- final lease release removes the lease and active runtime before shutdown has
  succeeded, so failed cleanup leaves live work untracked;
- manager-wide shutdown clears all runtime tracking before cleanup results are
  known;
- project archive/delete/scope-change paths already keep the runtime tracked
  until shutdown succeeds and should remain the model;
- bootstrap cleanup failure can replace the original startup exception;
- config subscriptions are removed even when another shutdown phase fails, so
  retry behavior must track completed phases instead of blindly repeating every
  callback.

## Configuration decisions

Capture one root configuration snapshot at the beginning of `create()`.

Captured for the runtime lifetime:

- search and external-search settings;
- document extraction/index/rerank settings;
- project library root;
- document reconciliation cadence.

Hot-reloaded through explicit subscriptions:

- entity-resolution policy;
- NLP pipeline settings;
- ingestion and episode settings;
- conflict-discovery settings.

`ProjectRuntime` will receive the injected `ConfigManager` so semantic policy
capture uses the same manager as the factory. This keeps entity-resolution
policy hot-reload behavior while removing the global singleton lookup.

## Units of work

### 1. Make bootstrap dependencies coherent

- require/pass the same `ConfigManager` through factory and runtime;
- capture one root-config snapshot at the start of factory creation;
- pass the snapshot or its typed sections to construction helpers;
- remove repeated `self.dev_settings` and `_config().config` reads during one
  bootstrap;
- add tests proving a config change during bootstrap cannot mix snapshots.

Commit separately.

### 2. Reuse the canonical project filesystem factory

- pass `ProjectManager._filesystem_factory` into `ProjectRuntimeFactory`;
- require the canonical factory in runtime composition;
- use it for both `DocumentService` and `ContextProjection`;
- remove duplicate path resolution/factory construction;
- update composition tests to assert both consumers share the dependency.

Commit separately.

### 3. Remove dead and misleading runtime surface

- delete `ProjectRuntime.load_domain_config()` and `capture_domain()`;
- pass the already compiled initial domain into `ProjectRuntime`;
- remove the unused background-job `resources` argument;
- make the project semantic processor required by job registration;
- remove impossible scheduler-null checks while retaining genuinely optional
  background-work and conflict-job behavior;
- update tests to use production domain activation/capture paths.

Commit separately.

### 4. Make project shutdown retryable and trackable

- serialize shutdown attempts;
- remember which cleanup phases completed successfully;
- allow failed phases to be retried;
- preserve/re-report shutdown failure until every phase completes;
- make concurrent/repeated successful shutdown calls idempotent;
- add focused failure, retry, and concurrent-shutdown tests.

Commit separately.

### 5. Preserve manager ownership when shutdown fails

- on final lease release, keep enough lease/runtime ownership to retry when
  shutdown fails;
- only remove a runtime from `active_projects` after successful cleanup;
- during manager shutdown, retain failed runtimes instead of clearing them
  before results are known;
- confirm archive/delete/scope-change behavior follows the same rule;
- preserve the original bootstrap exception when best-effort cleanup also
  fails, while logging/chaining the cleanup problem;
- cover final-release, manager shutdown, and bootstrap failure paths.

Commit separately.

### 6. Final audit

- search for global config reads and removed APIs in the project-runtime area;
- verify capture-time versus hot-reload behavior in tests and comments;
- run focused runtime/project tests, Ruff, and the non-database server suite;
- remove this temporary plan after all units are complete.

Commit separately.

## Out of scope

- replacing ProjectManager's coarse lifecycle lock;
- splitting ProjectRuntime into more manager classes;
- changing document, retrieval, or maintenance product behavior;
- redesigning ApplicationRuntime or RuntimeResources shutdown before their own
  subsystem review;
- changing the public API or SDK contract.

## Completion criteria

- one coherent config snapshot builds each project runtime;
- no ProjectRuntime path reads the global ConfigManager singleton;
- one application-owned filesystem factory supplies document and Context files;
- dead domain/factory APIs are gone;
- failed cleanup remains owned, visible, and retryable;
- startup ordering and domain activation locking remain intact;
- focused tests and lint pass;
- every implementation unit has its own commit.
