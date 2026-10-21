# Common-layer intersections

> Current verification (2026-10-19): see [verification status](verification-status.md) for the passing PostgreSQL runs, remaining coverage limits, and planned full Docling ingestion on Linux. Older database-unavailable statements below describe earlier runs.

Working notebook, 2026-10-11. These paths start in `common` and continue into previously reviewed ingestion, extraction, Knowledge, or health code. Findings below preserve the original observations; [common-layer remediation](common-remediation.md) records the fixes and remaining verification limits.

## Domain relationship lookup can change the meaning of extraction output

`DomainConfig.from_mapping()` records relationship alias owners but does not reserve all canonical names (`common/conf/domain_config.py:353-361`). The compiled lookup combines names and labels (`:523-531`). `relationship_name_key()` collapses *runs* of spaces and hyphens, while config validation replaces each character once (`common/conf/relationship_config.py:25-29`). Thus accepted configs can have keys that collide at runtime.

Path: project domain activation -> compiled domain frozen in an ingestion window -> VP-02's observed label -> `normalize_relationship()` -> `ContextRelationshipWrite` -> Knowledge semantic commit. Knowledge reclassification also calls the same normalizer (`core/ingestion/relationship_extractor.py:281`; `core/knowledge/relationship_reclassification.py:146`; `core/knowledge/db/writers/semantic_commit_writer.py:1576`). Depending on endpoint types, the wrong canonical relationship may be stored or a valid label may be left unrecognized. The defect is at the vocabulary boundary, before persistence.

Reproduced cases from source reading: `DEPENDS_ON` plus a different relationship with label `DEPENDS_ON` can make `relationship_by_key('DEPENDS_ON')` resolve to the latter. Labels `moves--to` and `moves_to` on different relationships pass validation, while runtime normalizes both to `moves_to` and can select the wrong owner. A regression should verify rejection at domain activation and stable lookup after policy serialization/replay.

## LLM budget can appear enforced but be unbounded

`common/schema/settings.py:138-165` accepts an infinite positive budget limit and infinite model prices. `ExternalModelSpendingLedger.reserve()` compares spent and reserved amounts with the limit; finite totals never reach infinity (`infrastructure/external_model_budget.py:93-125,233-238`). The snapshot sets `enforced` based on whether the limit is present (`:309-324`). The same ledger gates hosted model calls used by Episode, Context, extraction, JEV, and the agent. A finite-number check belongs at configuration validation, with a test from settings load through budget admission; fake-only assertions cannot verify the Postgres numeric boundary.

The same lower-bound-only validation accepts infinity for `EntityResolutionSettings.common_word_frequency_threshold` (`common/schema/settings.py:128-133`). `_is_common_word_mention()` compares actual word frequency against that threshold (`core/knowledge/entity/resolver.py:1327-1335`), so an infinite threshold disables the common-word safeguard. This value is frozen into ingestion policy. Finite-number validation should cover both settings and policy replay.

## Domain activation can partly succeed

`ProjectRuntime.activate_domain_config()` asks the store to commit, installs the new domain fields, then awaits `_select_vp01()` (`runtime/project_runtime.py:181-195`). The resource loader may fail while loading the domain-selected language (`runtime/resources.py:144-175`). A mock runtime repro raised `model unavailable` after the store marked the new domain committed and the live domain advanced to version 1. The API caller sees failure and a retry with the old expected version conflicts. `TextProcessor` normally selects the adapter for each frozen domain (`core/ingestion/text_processor.py:211-215`), so subsequent extraction will seek the new adapter and can fail until it becomes available. The method's atomicity claim does not hold across the model-load await. A regression needs a failing loader, asserted durable version, API response, and retry behavior.

## Malformed JSON logging path

`safe_json_loads()` logs a prefix of malformed input (`common/utils/json_utils.py:7-15`). The agent executor gives it raw tool-argument JSON (`core/agent/executor.py:858-870`). A read-only repro captured the input prefix in a Loguru warning; configured ordinary warnings go to stdout and `knoggin.log` (`log/logging_setup.py:19-29`). The fallback test checks only the returned value. Diagnostic logging can keep the error category and length without copying the argument content.

The separate coordination path filters event names and raw content keys, but allows `error` on `job.failed` and copies a truncated value (`common/utils/event_persistence_policy.py:106-153,167-186`). The scheduler supplies `str(exc)` on job exceptions (`infrastructure/job/scheduler.py:368-376`). If an exception includes private source/model text, it can be retained in the enabled coordination log and ordinary Loguru sinks. This conditional path needs an assertion using an exception containing sensitive text; no real production exception with such text was demonstrated in this scan.

## Agent Brain creation can strand section edits

`normalize_agent_brain()` returns any already structured Brain containing the required heading names, with no total/section size check (`common/utils/agent_identity.py:75-99`). `replace_brain_section()` later enforces 4,000 characters per edit and 12,000 total (`:102-134`). Specialist creation accepts unbounded initial directives (`common/schema/agent/community_tools.py:133-145` -> `core/community/tools.py:107-159` -> `core/agent/services/agent_manager.py:257-303`). A 12,104-character Brain was accepted, then a section edit raised `Brain exceeds 12000 characters`. The full-update method has the same validation gap, though no production caller was found for it. Tests should cover initial creation through later section edit, not only normalization in isolation.

## Non-finite layout coordinates cross document boundaries

`LayoutBoundingBox` checks positive area using comparisons that do not reject `NaN` (`common/schema/source/locators.py:19-31`). Docling `_region_bbox()` has the same gap (`core/knowledge/documents/extraction.py:490-499`). A `NaN` left coordinate passes both. `DocumentParseSnapshot.to_storage_payload()` also retains the raw Docling structure (`core/knowledge/documents/extraction.py:146-164`), and the writer's default `json.dumps()` emits a literal, nonstandard `NaN` before a PostgreSQL `jsonb` cast (`core/knowledge/db/writers/document_writer.py:446,500`). A local serialization repro confirmed this; the DB and indexer outcome is unverified. Reject or sanitize non-finite parser values before projection, enforce finite locator coordinates, and test strict JSON serialization plus the writer boundary.

## Prompt contract handoff

`common/utils/prompt_loader.py` loads Markdown sections into system prompts. The live Context paths use `extract_context_entities` and `extract_context_relationships`; the old message prompt wrappers are unused in production. Prompt-library validation confirms file structure and placeholders, but cannot catch semantic contradictions. Coverage needs assertions at the model boundary and downstream rejection of unsupported output, especially for paraphrased relationship `context` and source-only entity names.

## Agent tool arguments pass unsupported schema constraints

`validate_tool_arguments()` implements a subset of JSON Schema but does not inspect `pattern`; its numeric min/max comparisons also let `NaN` through (`common/schema/agent/tool_contracts.py:1336-1415`). The `propose_entity_merge.expected_state_hash` schema declares 64 lowercase hex characters (`:983`), and `report_relationship_conflict.confidence` declares a 0–1 number (`:1040-1044`). Both invalid examples passed the shared validator in a read-only repro. The agent runtime calls the validator before dispatch (`core/agent/tool_runtime.py:185-193`); downstream merge hash and conflict confidence checks reject these examples, so the observed result is late tool failure. `safe_json_loads()` uses Python's `json.loads`, which also accepts `NaN`. Add targeted validator and tool-dispatch assertions for declared patterns and finite numbers.
