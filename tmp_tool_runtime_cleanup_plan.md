# Tool Runtime / Registry / Reference Cleanup Plan

## Goal

Keep Knoggin's canonical tool registry, immutable per-run policy, compact local
references, bounded argument coercion, mutation authorization, and durable audit
trail. Simplify duplicated metadata and make every tool failure mean the same
thing to dispatch, auditing, the executor, the notebook, and the model.

## Current findings

The journal findings remain present in the current code:

- tool failures still mix raised exceptions, `{"error": ...}` values, error
  lists, and synthetic web-search rows;
- broad catches in Brain tools expose raw exception text as ordinary tool data;
- compact-reference `ValueError`s are converted to generic failures before the
  executor can classify them;
- `anyOf` in the merge schema is not enforced by the runtime validator;
- presentation overrides can weaken canonical validation constraints;
- `_TOOL_PARAM_TYPES`, schema fallbacks, `ToolDefinition.dispatch`, duplicate
  filtering APIs, and redundant capability permissions remain;
- AAC-only methods still have fake implementations on base `MemoryTools`;
- `Tools` retains unused resolver-derived dependencies and receives mixed
  internal/external search configuration;
- document and web concerns remain combined in `SearchTools`;
- raw tool arguments are still logged without bounded redaction.

The nearby caller/owner audit added one item:

- every normal and AAC `Tools` instance eagerly creates two HTTP clients even
  when its immutable runtime exposes no web tools. Cleanup closes them, but
  resource creation should be lazy or explicitly tied to web-tool use.

The explicit tool renames completed earlier are treated as canonical; this work
does not add compatibility aliases.

## Failure contract

Use one boundary contract:

- successful result: return normal data;
- successful retrieval with no matches: return the normal empty shape;
- invalid/unavailable request: raise non-retryable `ToolExecutionError`;
- transient dependency, storage, timeout, or network failure: raise retryable
  `ToolExecutionError`;
- optimistic workspace conflict: keep `WorkspaceConflictError`;
- health degradation: remain valid structured health data.

Expected domain absence may return an empty result only when absence is the
documented successful meaning of that read. It must not return `error` data.

## Units of work

### 1. Normalize tool failures

- convert Brain, maintenance/service availability, document-tool availability,
  workspace, and web-provider failures to the boundary contract;
- remove synthetic `Search Error` and `Timeout` evidence rows;
- ensure action audit status and executor success/failure always agree;
- keep safe user-actionable messages while preventing raw internal exception
  text from reaching the model;
- add dispatch, audit, executor, notebook, and web-search regression tests.

Commit separately.

### 2. Preserve typed local-reference failures

- add a non-sensitive tool-error detail/code for invalid compact references;
- translate reference resolution errors before the generic exception handler;
- make executor diagnostics use the typed detail instead of string matching;
- never persist the raw model-supplied handle in diagnostics;
- cover sequential and parallel execution paths.

Commit separately.

### 3. Complete and harden schema validation

- implement the bounded `anyOf` behavior used by canonical schemas;
- always validate canonical constraints;
- validate an active presentation override as an additional constraint, not as
  a replacement for the canonical safety contract;
- add direct tests for merge evidence requirements and weakened overrides.

Commit separately.

### 4. Remove duplicate registry and permission metadata

- delete `_TOOL_PARAM_TYPES` and unreachable no-schema branches;
- remove `ToolDefinition.dispatch`; derive method and parameters from canonical
  definition name/schema;
- delete `get_filtered_schemas()` and `ALL_TOOL_NAMES` after migrating tests;
- remove unused generic tag/capability filters from `get_tool_schemas()`;
- remove redundant `ToolPermissions.allowed_capabilities` while retaining
  capability metadata for write authorization and audit classification;
- keep executor-protocol definitions explicitly non-dispatchable.

Commit separately.

### 5. Validate tools against their actual owner

- validate normal definitions against `Tools`;
- validate AAC-only definitions against `AACTools` without creating a circular
  import or plugin framework;
- delete fake AAC/community methods from `MemoryTools` and update tests;
- keep the real AAC implementations and presentation override behavior.

Commit separately.

### 6. Narrow Tools composition and web resources

- pass explicit `project_id` instead of storing an unused `EntityResolver`;
- remove unused `embedding_service` and `readable_project_ids` state;
- pass only external provider/key settings to web tooling;
- update AAC composition to copy only the explicit dependencies it needs;
- lazily create web HTTP clients, or construct them only when web tools are
  active, while preserving deterministic close behavior;
- update factory/composition tests.

Commit separately.

### 7. Separate document and external-web tooling

- split `SearchTools` into document-search and external-web mixins/modules;
- retain one simple aggregate `Tools` class for executor dispatch;
- keep SSRF protection, public-address checks, redirect validation, response
  bounds, PDF/HTML extraction, and snapshot caching together in the web side;
- make no behavior or public tool-name changes during the move.

Commit separately.

### 8. Bound ordinary tool-call logging

- log tool name and bounded argument metadata rather than complete values;
- redact secret-like fields and avoid copying Brain/file/query content;
- keep the separate durable mutation-audit redaction path;
- add focused logging/redaction tests.

Commit separately.

### 9. Final audit

- search for error-as-data, stale registry APIs, duplicate dispatch metadata,
  fake AAC methods, and unused dependencies;
- verify normal/AAC registry validation and tool-client cleanup;
- run focused agent/community tests, Ruff, architecture checks, and the fast
  non-database server lane;
- remove this temporary plan after every unit is complete.

Commit separately.

## Out of scope

- dynamic plugin/tool discovery;
- redesigning notebook evidence or the executor state machine;
- changing public tool names or adding backward-compatible aliases;
- changing health results into exceptions;
- redesigning AAC discussion behavior beyond tool ownership validation;
- replacing exceptions with a large Result/Either framework.

## Completion criteria

- no operational failure is reported as successful tool data;
- local-reference failures retain a safe typed diagnostic category;
- canonical schema constraints always run and `anyOf` is enforced;
- registry selection and dispatch have one source of truth;
- normal tools no longer pretend to implement AAC-only behavior;
- Tools owns only dependencies its methods actually use;
- web clients are not eagerly opened for runs that never use web tools;
- normal logs do not contain full tool argument values;
- focused and broad verification pass, with environment-only failures recorded;
- every implementation unit has its own commit.
