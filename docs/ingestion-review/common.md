# Common layer review

> Current verification (2026-10-19): see [verification status](verification-status.md) for the passing PostgreSQL runs, remaining coverage limits, and planned full Docling ingestion on Linux. Older database-unavailable statements below describe earlier runs.

Working notebook, 2026-10-11. Read source and assertions as the authority; the root README and older review notes may describe earlier code. This pass covers `server/src/common` and follows its contracts into the subsystems already reviewed.

Implementation status and new compatibility findings are recorded in [common-layer remediation](common-remediation.md). The numbered findings below preserve the original scan evidence.

## Inventory and role

- `schema/` supplies the typed boundaries for semantic windows, Context, extraction, JEV, evidence, documents, health, public API, agent tools, and artifacts.
- `conf/` validates project domain and relationship vocabulary and manages runtime configuration. Domain and policy snapshots cross ingestion and Knowledge stages.
- `scoping.py`, `document_limits.py`, `artifact_retention.py`, and `exceptions.py` centralize scope checks, limits, retention, and error types.
- `utils/` contains prompt loading, local IDs, time, task lifecycle, logging/events, text validation, and formatting used by extraction and other subsystems.
- `templates/prompts/` supplies active Episode, Context, Context entity fallback, and Context relationship prompts. `AGENT_IDENTITY.md` seeds the default agent. `topics.yaml` has no source-code reader found; only the root README references it.

There are 363 `common` import lines across `server/src`, so a common contract can affect multiple layers. This is an import count, not a coverage measure.

## Model controls in common

`schema/settings.py` defaults the local GLiNER threshold to 0.85 and enables hosted entity fallback; the hosted `extraction_model` and `merge_model` are separately named, and the spending budget is global. `schema/jev.py` defines the smaller JEV identity, extraction, and classification modes, all disabled by default, with bounded work and confidence values. Admission captures a credential-free JEV policy and a compiled domain in `IngestionPolicy`; the frozen snapshot is replayed later by Knowledge. The code does not define an NLI mode. The live model-routing details remain in [model and coverage notes](models-and-coverage.md).

## Rules visible in the templates

The Context prompt treats frozen messages as direct evidence, assistant sources as source-grounded evidence, and Episodes as interpretation aids. It rejects unsupported assistant statements and unanswered clarification premises. The Episode prompt proposes zero to three bounded, non-overlapping memories. The Context extraction prompts favor omission of weak relationships; relationship endpoints and evidence blocks use local handles. A name found only in a supporting message is an unresolved candidate, not an automatically linked entity.

The extraction prompt file also retains the old message-based `Extract Entities` and `Extract Relationships` sections. Their wrapper functions have no production callers in `server/src`; the live path uses the Context variants (`core/ingestion/text_processor.py`, `relationship_extractor.py`). The old relationship example allows a co-location edge while its rules forbid co-mention edges. This is dormant contradictory guidance, and there is no direct prompt-library test in `server/tests`.

`ConfigManager` calls `validate_prompt_library()` during configuration validation. That validator checks section presence and placeholder names, not behavioral consistency. A direct invocation of `validate_prompt_library()` passed on this checkout.

## Findings

1. **Relationship name collision:** `DomainConfig.from_mapping()` checks aliases against aliases, but `compile()` puts canonical names and aliases into one lookup. A legal label can therefore override another relationship's canonical name. The validator also normalizes runs of hyphens and spaces differently from runtime extraction. `normalize_relationship()` can interpret a configured label as a different relationship, affecting Context relationship extraction and Knowledge reclassification. See [intersection notes](common-intersections.md).
2. **Non-finite settings:** `LLMSpendingBudgetSettings.limit_usd` and pricing fields use a lower-bound constraint but accept infinity. An infinite limit is reported as enforced while never blocking new model work. The entity resolver's common-word frequency threshold also accepts infinity, which makes its common-word guard always false. See [intersection notes](common-intersections.md).
3. **Domain activation can commit and report failure:** The project domain store commits a new version before `ProjectRuntime` awaits the selected VP-01 adapter. A model-load exception raises to the API with the new domain already live and durable. The running `TextProcessor` normally selects VP-01 per frozen domain on each extraction, so the confirmed failure is the misleading activation result and unavailable-model failures on future extraction, not silent use of the old adapter.
4. **Malformed tool-argument logging:** `safe_json_loads()` logs the first 100 characters of invalid JSON. The agent executor passes raw model tool arguments to it, so stored or user text can reach ordinary Loguru sinks. A read-only repro captured private text in the emitted warning.
5. **Agent Brain size mismatch:** `normalize_agent_brain()` accepts an already structured Brain without the section-editor's 12,000-character cap. Specialist creation can persist unbounded initial directives. A 12,104-character Brain was accepted, then `replace_brain_section()` rejected an edit because the total remained too large. Duplicate required headings are also accepted and section operations target the first one; that part needs a focused path test.
6. **Non-finite document coordinates:** A Docling bounding box with `NaN` or infinity passes both `_region_bbox()` and `LayoutBoundingBox` because area comparisons do not reject non-finite values. The parse snapshot retains the raw Docling structure and `json.dumps()` emits a nonstandard `NaN` token before the SQL `jsonb` cast. The exact Postgres/indexer result still needs a storage contract check.
7. **Agent tool validator omits declared checks:** `validate_tool_arguments()` ignores JSON Schema `pattern` and accepts non-finite `number` values. A malformed merge state hash and `NaN` relationship-conflict confidence pass this shared validator; downstream handlers reject them later. This is an early-boundary gap, with late failure rather than a confirmed bad write.
8. **Coordination failure event can retain exception text:** The event retention policy allows an `error` string on `job.failed` and truncates it but does not scrub content inside it. Scheduler failure passes `str(exc)` into that field. If an exception includes private source or model text, it reaches the enabled coordination log and ordinary Loguru sinks. I did not find a demonstrated production exception with such text in this pass, so this is a conditional leak path, distinct from the confirmed malformed tool-argument leak.

## Coverage and limits

- `server/tests/unit/common/` has focused contract tests for config, scoping, Context/window, source references, public shapes, health, and utilities. Presence is not proof that collision, numeric boundary, or cross-layer behavior is tested.
- `test_domain_config.py` checks duplicate extraction labels and invalid relationships, but not a label colliding with a different canonical relationship name or normalization-equivalent labels.
- `test_context_updater.py` asserts one phrase in the loaded system prompt and the structured response model; it does not check the complete authority contract.
- `test_json_utils.py` verifies the fallback value after malformed JSON, but not that logs omit the raw input.
- `test_document_extraction_contract.py` tests malformed Docling provenance with a string coordinate, not a non-finite number.
- Agent tool tests exercise `anyOf` and several shape rules, but not declared regex patterns or non-finite numbers.
- A full `tests/unit/common` run passed **231 tests** on this checkout. The concrete counterexamples above still pass through their boundaries, so this green suite cannot close those gaps.
- Postgres-backed boundary tests and real model behavior will be distinguished from fake-backed unit checks in the final review.

| Common area | Existing checks that carry weight | Main gap seen in this scan |
| --- | --- | --- |
| Domain/config | Duplicate extraction labels, invalid relationships, snapshot immutability, config publication/retry tests | Canonical relationship/alias collisions, normalization-equivalent aliases, adapter-load failure after commit |
| Window/Context/extraction/JEV | Typed boundary tests and downstream fake-backed stage tests | Cross-stage replay with a collision; actual model evidence quality remains unevaluated here |
| Source/document | Locator validation, malformed Docling provenance, document writer contracts | Non-finite raw parser fields through the `jsonb` writer and terminal index outcome |
| Agent tools/Brain | Tool shape, evidence `anyOf`, revision-conflict tests | Regex patterns, non-finite numbers, oversized/duplicate full Brain, private-text log output |
| Health/public/events | Sanitized detail and public error tests, public stream checks, approved-event field filtering | Ordinary Loguru path bypasses health/public sanitizers; `job.failed.error` still accepts exception text |

Several utilities appear to have test-only callers and no production use: `safe_update`, `BackgroundTaskGroup`, `cosine_similarity`, `format_vp01_input`, `format_vp02_input`, and `extend_local_id_maps`. They are cleanup candidates, not confirmed behavior defects.

## Search log

- Inventoried `server/src/common` and `server/tests/unit/common` with `rg --files`; searched imports into ingestion, Knowledge, health, agent, infrastructure, and API.
- Read the prompt manifest and templates, followed actual calls from Context updater, Episode generator, Context entity fallback, and Context relationship extractor.
- Searched for `ner_prompt`, `get_connection_reasoning_prompt`, `topics.yaml`, and prompt-loader tests to identify active versus legacy material.
- Read domain relationship validation, compiled lookup, normalization, and existing domain tests; traced LLM budget settings into the spending ledger.
- Followed domain activation through the store, runtime, resource model loader, and on-demand VP-01 selection. Checked agent tool-argument parsing/logging, Brain creation/editing, and Docling region projection.
- Read event persistence, coordination logging, and scoping helpers with their assertions. Scoping normalizes/unique-filters project IDs; approved events drop raw `prompt`/`content`/`messages` fields, but do not sanitize approved error values.

Schema families inspected: source locators/references; ingestion, Context, semantic window, Episode, evidence, JEV/settings, document focus/uploads, health, public API/stream/errors, artifact/maintenance, and agent identity/research/stream/tool/community contracts. The deepest traces were domain normalization, PDF snapshot persistence, and tool dispatch. This was not a field-by-field audit of every public DTO or tool schema, and no Postgres or live-model test ran in this pass.
