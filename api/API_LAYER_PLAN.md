# Knoggin API layer plan

Planning document, checked against the server checkout on 2026-10-23. No API implementation is implied by this file.

## Decision and boundary

Build the API in the existing root `api/` directory, grouped by subsystem. The repository boundary is **`server/` engine → root `api/` HTTP/SSE bridge → `app/` client**. Request/response contracts, routing, stream translation, and HTTP error mapping belong in root `api/`. The engine may expose a small runtime entry point where composition needs one; it should not own API workflows. The old `server/src/runtime/api_port.py` adapter and `ApplicationRuntime.application_port()` were removed during Phase 5 after the root API took over their transport responsibilities. Project leases, user scoping, stream cancellation, and safe errors remain required behaviors of the new boundary.

Apply this ownership rule to every phase, including routes already built. If a server responsibility is only API request validation, response shaping, HTTP/SSE event translation, or public error wording, move it to root `api/` while implementing its replacement. Keep durable state, project/session leases, run admission, persistence, and cleanup in their existing engine owners. Do not retain a server-side API adapter for compatibility; the product is not released. Review existing Phase 1–3 code under this rule before declaring the API cut complete.

The old `server/src/common/schema/public.py` contracts were removed with the adapter. Engine session input lives in its owning module, while HTTP contracts live in root `api/`. Reuse validated domain models where they match the new boundary. The tracked `app/src/contracts/api.ts`, `app/src/client/api.ts`, and page files were deleted earlier in the worktree; inspect their provenance before implementing the client.

The application is local and single-user. API identity must match the running `ApplicationRuntime` user; a supplied user string cannot select another storage scope. Use one API lifespan to own engine startup and ordered shutdown. Routes call current project/session/service owners, not SQL or model clients directly. Give root `api/` its own package/dependencies and add it to the root uv workspace when implementation begins; the workspace listed only `server` when this plan was written. The HTTP dependency belongs to the API package, not the engine.

## Config sources and exposure rule

The persisted global configuration is `RootConfig` in `common/schema/settings.py`, `agent/settings.py`, and `jev.py`. It has **109 leaf fields** in the current checkout. `ConfigManager` owns validation, atomic YAML persistence, publication, and apply status. Project scan settings have 11 separately persisted fields. Project domain configuration has its own versioned PostgreSQL lifecycle. Session, agent, and run choices are separate again. Environment variables configure process startup.

| Class | Meaning | API treatment |
| --- | --- | --- |
| Standard | A user is likely to choose it in normal use | Read and write in its subsystem; normal UI control |
| Advanced | Real persisted tuning, but easy to misuse or rarely changed | Read and write in an advanced section, with defaults and validation; UI may collapse it |
| Secret | Credential value | Write or clear explicitly; read only a `configured` flag; never echo input |
| Startup | Process resource or environment choice | Show effective non-secret value if useful; changes require process restart and remain deployment configuration initially |
| Fixed | Code constant or internal snapshot rather than a user configuration source | No settings endpoint; revisit only for an actual user need |

The API should not invent per-project copies of global settings. A settings response must distinguish persisted value, active value where it may differ, and `restart_required_fields`. Existing `ConfigManager` status says whether subscribers applied; it does not by itself prove that startup-captured owners changed. For a running project, document and retrieval settings may still use the snapshot taken when that project loaded. A new run captures its own limits and policy; changing settings does not rewrite an in-flight run or durable semantic window.

## Complete global settings inventory

Paths below are relative to `RootConfig`. Every leaf in that schema is accounted for here. Grouped names share the prefix printed in the first column. The suggested exposure class concerns API and UI presentation, not whether the stored field exists.

| Subsystem and path prefix | Leaf fields | Class and activation |
| --- | --- | --- |
| Identity `user_aliases` | `user_aliases` | Standard; identity entity is initialized at application startup; restart required for existing identity state |
| LLM `llm` | `agent_model`, `extraction_model`, `merge_model`, `base_url` | Standard for models; advanced for URL; client updates live for later calls; in-flight calls keep their selection |
| LLM `llm` | `api_key` | Secret; client update live |
| Spending `llm.spending_budget` | `limit_usd`, `model_pricing`, `fallback_pricing` | Standard limit; advanced pricing. Pricing entries contain `input_usd_per_million_tokens` and `output_usd_per_million_tokens`; validation requires pricing for a positive limit |
| Spending `llm.spending_budget` | `reservation_output_tokens`, `reset_key` | Advanced. `reset_key` intentionally selects a new accounting window, so show that consequence before saving; never change it implicitly |
| Search provider `search` | `provider` | Standard; tools read current settings when built for a run |
| Search provider `search` | `brave_api_key`, `tavily_api_key` | Secret; write/clear, read configured flags |
| JEV `jev` | `identity_mode`, `extraction_mode`, `classification_mode`, `model` | Advanced; client changes live, semantic work captures admitted policy |
| JEV `jev` | `identity_question_version`, `extraction_question_version`, `classification_question_version`, `acceptance_policy_version`, `extraction_acceptance_policy_version`, `classification_acceptance_policy_version` | Advanced version selectors; only schema-supported values |
| JEV `jev` | `identity_min_choice_confidence`, `identity_min_choice_probability`, `identity_min_probability_margin`, `identity_min_evidence_noul`, `extraction_min_choice_confidence`, `extraction_min_entity_noul`, `classification_min_choice_confidence`, `classification_min_choice_probability`, `classification_min_probability_margin`, `classification_min_evidence_noul` | Advanced decision thresholds; admitted windows retain snapshots |
| JEV `jev` | `max_candidates`, `identity_match_sample_rate`, `max_calls_per_window`, `max_questions_per_request`, `max_options_per_choice`, `max_request_bytes`, `request_timeout_seconds`, `total_timeout_seconds`, `max_elapsed_seconds_per_window`, `accounting_timeout_seconds`, `max_retries`, `shutdown_timeout_seconds` | Advanced limits and timeouts; shutdown timeout affects later shutdown |
| JEV `jev` | `endpoint` | Advanced provider connection; validated HTTPS endpoint |
| JEV `jev` | `api_key` | Secret; client update live |
| Chat ingestion `developer_settings.ingestion` | `message_edit_window_seconds`, `semantic_window_tokens` | Advanced; edit setting is read for run/message operations; semantic admission uses current settings for later windows |
| Semantic retry `developer_settings.ingestion.semantic_window_retry` | `max_attempts`, `initial_backoff_seconds`, `max_backoff_seconds` | Advanced; processor subscribes to ingestion settings; existing admitted work and retry behavior must be described accurately |
| Document indexing `developer_settings.jobs.document_indexing` | `reconciliation_interval_seconds` | Advanced; existing document services capture it at construction; restart or project runtime rebuild required |
| Episodes `developer_settings.jobs.episode` | `enabled`, `max_episode_source_messages`, `max_episode_source_tokens`, `max_narrative_chars` | Standard toggle; advanced limits. Project processor subscribes; each admitted window keeps a policy snapshot |
| Conflict discovery `developer_settings.jobs.conflict_discovery` | `enabled`, `mode`, `interval_hours`, `max_seed_span_days`, `max_package_tokens` | Standard toggle/mode; advanced cadence and caps; job subscribes for later work |
| Agent limits `developer_settings.limits` | `agent_history_turns`, `max_tool_calls`, `tool_timeout`, `max_attempts`, `max_consecutive_errors`, `max_accumulated_messages`, `max_accumulated_profiles`, `max_accumulated_graph`, `max_accumulated_paths`, `max_accumulated_episodes`, `max_accumulated_documents`, `max_accumulated_web_discoveries`, `max_accumulated_web_reads`, `max_accumulated_actions`, `max_accumulated_next_steps`, `max_accumulated_summary_chars`, `max_notebook_render_tokens`, `conversation_context_turns`, `max_conversation_history`, `project_briefing_mode`, `tool_limit_overrides` | Advanced, except briefing mode could be standard. Orchestrator captures limits for each new run; tool overrides must be checked against the actual tool registry |
| Entity resolution `developer_settings.entity_resolution` | `candidate_fuzzy_threshold`, `resolution_threshold`, `resolution_margin`, `common_word_frequency_threshold`, `sparse_context_verbs` | Advanced; resolver receives updates, and semantic work captures a policy snapshot |
| NLP `developer_settings.nlp_pipeline` | `gliner_threshold`, `llm_ner_mode` | Advanced; processor receives updates, future work captures them |
| Retrieval `developer_settings.search` | `fts_limit`, `semantic_message_limit`, `semantic_message_threshold`, `rerank_candidates`, `default_message_limit`, `default_entity_limit`, `default_activity_hours` | Advanced; loaded project retrieval captures settings; existing project runtime needs rebuild/restart; AAC may refresh its own read context |
| Documents `developer_settings.documents` | `project_library_root`, `rerank_enabled`, `rerank_candidates` | Advanced; project library root is process-level filesystem ownership, other two are captured by document services; restart/rebuild for existing owners |
| Community `developer_settings.community` | `enabled`, `interval_minutes`, `token_budget`, `seeding_agent_id` | Standard enabled flag; advanced rest; opportunity loop subscribes, changes affect future admissions, not current discussion |
| Coordination log `developer_settings.coordination_log` | `enabled`, `path`, `retention_days`, `rotation_mb` | Advanced; runtime subscribes and applies later writes |

The current `SettingsResponse` reads most of this tree, but its LLM view omits `llm.base_url` and its JEV view omits `jev.shutdown_timeout_seconds`, while updates accept both. Fix read/write symmetry when designing the new contracts. Its JEV policy must remain credential-free. The existing global update accepts an arbitrary nested dictionary and relies on `RootConfig` validation; new HTTP request models should make accepted sections and secret clear semantics explicit while still delegating validation and persistence to `ConfigManager`.

## Other persisted or caller-selected settings

| Scope | Settings | Owner and API plan |
| --- | --- | --- |
| Project domain | `version`, `topics` (`name`, `description`, `active`), `entity_types` (`name`, `topic`, `description`, `labels`, `allowed_topics`), `relationships` (`name`, `source_types`, `target_types`, `symmetric`, `labels`), `context_sections` (`key`, `title`), `extraction_guidance`, `vp01_language` | `ProjectManager` and `DomainConfigStore` own read, validate, preview, and optimistic activation. Add project routes around those operations. Version is server-assigned on activation, not a free tuning knob; show future-only impact and conflict results |
| Project folder scan | `respect_gitignore`, `include_hidden`, `ignored_patterns`, `allowed_extensions`, `blocked_extensions`, `blocked_file_names`, `blocked_directory_names`, `max_document_size_bytes`, `max_total_size_bytes`, `max_file_count`, `max_folder_depth` | `DocumentService` persists a complete `FolderScanSettings` per project; expose get/put/reset. Changes affect subsequent scans and clear reconciliation fingerprints |
| Project metadata/access | `name`, `description`, `allowed_projects`, archive/delete state | Existing project manager/port operations; not global settings. Preserve project-scoped permissions and deletion cleanup status |
| Agent | `name`, `persona` (`attention_bias`, `reasoning_style`, `social_temperament`, `communication_signature`, `productive_flaw`), `model`, `temperature`, `enabled_tools`, `aac_enabled`, default-agent selection | `AgentManager` persists these. Add agent routes for supported user edits. `brain`, `brain_revision`, `spawned_by`, and last-turn fields are engine-owned state, not arbitrary settings writes. Current public `UpdateAgentRequest` exposes only name/tools, so it is not a complete agent-settings contract |
| Session | `model`, `agent_id`, `enabled_tools`, pinned document focus | Existing session manager has create/update/delete and focus lifecycle. Preserve omitted/null/empty allowlist meanings; these are durable session choices |
| Run | `model`, `agent_id`, `enabled_tools`, `research_mode` (`normal`, `research`, `deep_research`), request document focus | `StartRunRequest` selects one run; values are captured for that run. Research multipliers are fixed profile definitions today, not user-configurable global fields |

## Startup and operator controls

These are tunable today but are not part of persisted `RootConfig`. Keep them in deployment configuration at first. An optional read-only diagnostics response may report effective non-secret values; do not pretend an HTTP PATCH can resize running executors or swap loaded embedding models.

| Owner | Startup inputs |
| --- | --- |
| Database/resource profile | `DATABASE_URL` (never return), `RESOURCE_PROFILE`, `THREAD_WORKERS`, `EMBEDDING_BATCH_SIZE`, `BACKGROUND_JOB_WORKERS`, `FOREGROUND_MODEL_WORKERS`, `BACKGROUND_MODEL_WORKERS`, `PREFER_GPU`; `ApplicationRuntime.start(thread_workers=...)` can override worker count |
| Work queues | `BACKGROUND_JOB_QUEUE_LIMIT`, `FOREGROUND_MODEL_QUEUE_LIMIT`, `BACKGROUND_MODEL_QUEUE_LIMIT`, `FOREGROUND_MODEL_TIMEOUT_SECONDS` |
| Local models | `EMBEDDING_BACKEND`, `ONNX_PROVIDER`, `EMBEDDING_MODEL_REVISION`, `EMBEDDING_MODEL`, `RERANKER_MODEL` |
| Extraction/tracing | `DOCLING_TIMEOUT_SECONDS`, `DOCLING_PAGE_GROUP_SIZE`, `TRACE_LLM_CALLS` |

The `DocumentIndexPolicy` capture defaults, scheduler constants, five-minute semantic idle flush, research profile multipliers, and similar code constants are **fixed** for this API plan. Their existence as constructor parameters or dataclass fields does not make them user settings. Exposing them would require a real product reason plus lifecycle rules. Provider secrets, raw DSN, filesystem internals, and engine-owned agent brain state are not returned by the API.

## Proposed API sections

Names below describe contracts for root `api/`. Use versioned routes and consistent IDs/errors. `GET/PATCH /v1/settings/{section}` is an example grouping; each route maps to one current `RootConfig` subtree and calls the same config owner. A top-level `GET /v1/settings` can hydrate the settings screen. Keep the API's JSON field names and error envelope aligned with the TypeScript contracts in `app/`; generate client types from an API schema if that proves simpler than hand-maintaining two copies.

| Section | First routes/operations | Current owner |
| --- | --- | --- |
| Status/settings | Read redacted settings; patch a named section; get apply/restart status; retry failed applies; explicit reload; optional non-secret startup diagnostics | `ConfigManager`, `ApplicationRuntime` |
| Projects/domains | Project CRUD/archive; domain read/validate/preview/activate with `expected_version` | `ProjectManager`, `DomainConfigStore` |
| Agents/sessions/runs | Agent CRUD/settings/default/AAC participation; session CRUD/history/focus; run stream and cancellation | `AgentManager`, `AACRuntime`, `SessionManager`, `SessionRuntime` |
| Documents/sources | Upload/batch admission, OCR preflight, list/read/reindex/delete, saved links, project scan settings | `DocumentService`, project lease owner |
| Knowledge/maintenance | Episodes, evidence previews, review decisions, rollback, artifacts | Existing maintenance and knowledge services |
| Community/health | Participation, trigger/stop, discussions/timeline/insights; engine/project health; optional budget snapshot | `AACRuntime`, `RuntimeHealthService`, spending ledger |

The transport must preserve project lease release on success, failure, and cancellation; streaming disconnect must close the owned run; invalid user/project access must fail before side effects. Surface domain version conflicts distinctly. Return stable public error codes without leaking provider text, secrets, SQL, or file paths. Keep request and response validation at the API boundary while owners retain their own invariants. Do not create a second persisted settings store.

Client integration is outside the current API implementation scope. When `app/` work is agreed, it should have one typed API client and a stream decoder for the chosen event envelope. Its first slice should read settings/status, apply one section update, and show whether that update is active or requires restart. The deleted tracked client files need review before they are recreated or replaced so unrelated app work is not overwritten. The bridge owns CORS/local-origin policy.

### Proposed file ownership

| Location | Responsibility |
| --- | --- |
| Root `pyproject.toml`, `api/pyproject.toml` | Add the API as a workspace package depending on the installed server engine and its own HTTP dependencies |
| `api/src/app.py` | API app factory, local configuration, engine lifespan, shared error handling |
| `api/src/routes/` | Small subsystem routers: settings, projects/domains, agents/sessions/runs, documents, maintenance, community/health |
| `api/src/contracts/` | API-only requests, responses, redacted settings views, and stream event contracts |
| `api/tests/` | A small set of checks for lifecycle and failures that are hard to verify manually |
| `app/src/contracts/`, `app/src/client/`, `app/src/app/` | TypeScript wire types, one request/stream client, and screens that consume those APIs after reviewing the tracked deletions |
| `server/src/runtime/application.py` and engine contract modules | Only the minimal runtime entry/migration needed by the API; remove old `application_port()` and core imports of API contracts |

## API verification approach

Run the local API and use `curl` for direct route checks: status, settings round trips, validation errors, and redaction. Use Postman when a request body, collection, or stream is easier to inspect there. For each new subsystem, verify one normal path and one meaningful failure against the running API, including persisted effects where relevant. `curl` or Postman confirms the real HTTP wiring; passing unit tests alone does not prove the route is right.

Keep automated API tests small. Test lifecycle and failure paths that are awkward to reproduce reliably by hand, such as startup/shutdown ownership, failed updates leaving settings unchanged, secret leaks, and stream disconnect cleanup. Do not build a separate test for every simple field or mirror route implementation in assertions. Use focused engine tests where the rule belongs in the engine. Expand tests only when a concrete regression risk requires it.

## Implementation phases and gates

Phase 1 is implemented: root `api/` is a Starlette workspace package with a single runtime lifespan, process-bound user/config selection, and a health route. `SessionManager` now imports its input model from an engine-owned module. New API request and response contracts begin with the first settings route in phase 2; the old public contracts remain while the old adapter still uses them. API shutdown makes one attempt and surfaces failure; it has no retry loop.

Phase 2 has API-owned global settings routes, redacted reads, section patches, explicit credential set/clear, apply/restart status, reload, and apply retry. API checks use a real temporary `ConfigManager` over HTTP ASGI. A live HTTP check against a disposable database with the current schema covered health, settings read, patch, restart status, and read-back. The local `.env` now points to a fresh current-schema database and a 768-dimensional embedding model; the older database remains intact.

Phase 3 has project domain read/validate/preview/activate and folder scan get/put/reset routes. Scan settings now use the project owner without bootstrapping a whole project runtime for each settings request. Live HTTP checks against a disposable database covered domain activation and version conflict, scan settings after an API restart, and archived/deleted project behavior. Existing engine tests cover active-project domain fanout.

Phase 4 is in progress in root `api/` only. Project CRUD, agent/session preferences and document focus, run SSE, community/health reads and controls, document/link and source batch routes, OCR preflight, artifact reads, episode edits, and maintenance review/rollback routes have initial implementations. Focused HTTP ASGI checks cover maintenance scope and decisions, batch item results and safe errors, episode revision conflicts and lease ownership, plus run disconnect cleanup. Live HTTP checks covered settings/projects/agents/sessions, CORS, temporary project/session/agent updates and cleanup, a safe run failure, community/health reads, and document upload/list. A short project lease exposed a server lock cycle: final lease release held the project maintenance lock while scheduler shutdown waited for a relationship advisory job needing that lock. `ProjectManager` now settles final runtime shutdown outside that lock while blocking new leases for the project, and preserves the lease for retry if shutdown fails. A live repeat measured two document-list requests at 0.07 and 0.06 seconds instead of about 30 seconds each. New routes and lifecycle gates still need live review before Phase 5. The latest live API startup could not connect to local PostgreSQL (`pg_isready` also reported no response), so no new live-route result is claimed. The `app/` work attempted during this phase was removed at the user's direction; it is outside current scope.

1. **Contract/ownership cut.** Create package files and subsystem modules in root `api/`, choose the transport dependency, add an API workspace member, and establish one lifespan owner. Put API contracts there; move engine-used models out of `server/src/common/schema/public.py` where needed. Gate: the engine imports no API package, startup/shutdown is owned once, and another user cannot select this runtime's storage.
2. **Settings and status slice.** Add redacted global read, explicit section patch, apply/restart status, and secret write/clear behavior through `ConfigManager`. Cover every accepted leaf in the 109-field inventory, including LLM base URL and JEV shutdown timeout. Gate: read/write symmetry, invalid update leaves persisted and active config unchanged, secret never appears in JSON/error/log output, and restart field reporting reflects a startup-captured owner.
3. **Project-scoped settings.** Add domain read/validate/preview/activate and folder scan get/put/reset. Gate: version conflict, archived/deleted project rules, future-only domain effects, active project fanout, and persisted scan settings after reload.
4. **Remaining API subsystems.** Add project management, agent/session/run, document/source, knowledge/maintenance, and community/health routes in owner-sized slices. Gate: settings and project routes keep their earlier behavior, durable session/agent preferences hold, run snapshots and stream disconnect cleanup work, project leases release correctly, and errors are safe at HTTP/SSE seams. `app/` work is deferred until its scope is agreed.
5. **Remove the old adapter.** The old adapter, its runtime factory method, and adapter-only public contracts/tests have been removed. The document service and document provenance checks now exercise their engine owners; focused API checks cover source promotion, run focus, source/usage stream events, lease cleanup, and safe errors. The caller audit and static checks pass. PostgreSQL-dependent integration and live HTTP checks remain open because local PostgreSQL is unavailable. App typecheck/build belongs to a later app phase. A passing test is evidence, not proof that ownership and lifecycle are correct.

Each implementation phase should be reviewed before the next one. Existing mixed-worktree app deletions and unrelated `doc_testing` material must not be staged or rewritten as part of this plan.
