# JEV foundation

Phase A provides an optional HTTP adapter, configuration, shared spending, frozen
ingestion policy, and typed decision records. Identity/extraction consumers are
not implemented yet: setting observe mode alone does not issue ingestion JEV calls
at this stage. Current entity/extraction behavior stays unchanged.

## Installation and settings

No JEV SDK or local model download is required. The adapter uses the server's
existing `httpx` dependency and the
[TypeSafe HTTP API](https://docs.typesafe.ai/api).
Live requests will require a TypeSafe API key when the consumer pilots are wired.
Tests use mock transport and need no key or network.

The private runtime configuration file is `knoggin.yml` in the configured directory.
Its new top-level section accepts:

```yaml
jev:
  api_key: ""  # Private credential; do not commit it.
  endpoint: https://api.typesafe.ai/v1/systemone
  model: jev-1.13.0
  identity_mode: disabled
  extraction_mode: disabled
  classification_mode: disabled
  max_candidates: 8
  max_calls_per_window: 12
  max_questions_per_request: 16
  max_options_per_choice: 64
  max_request_bytes: 24000
  request_timeout_seconds: 5
  total_timeout_seconds: 12
  accounting_timeout_seconds: 2
  shutdown_timeout_seconds: 5
  max_retries: 1
```

Defaults are conservative initial work limits, not benchmarked acceptance
thresholds. Versioned models are required; moving aliases are rejected. Each
capability has disabled/observe/active settings, but activation policy and caller
behavior belong to the later phases. The public settings UI/API has not been
extended for JEV; configuration currently uses the private file.

The pinned model and modes, question/policy versions, and work limits are admitted
with an ingestion window. Credentials, endpoint, and the shutdown timeout remain
runtime-only. Old
development windows without JEV policy reopen with JEV disabled. Policy freezing
does not guarantee identical probabilistic responses on an uncommitted retry.

## Spending and request ownership

JEV and LLM requests share the same `ExternalModelSpendingLedger`, including the
existing durable budget reservation path. The budget remains configured under
`llm.spending_budget`; JEV does not introduce a separate spending ceiling.
When PostgreSQL is configured, accounting always uses it, including uncapped
calls. This adds database writes for uncapped calls but keeps their spending
visible if a cap is enabled later. In-flight reservations retain their original
storage, reset period, and price; settings changes cannot redirect settlement.
Settlement retries are idempotent, and snapshots read the durable balance.

Configure JEV pricing explicitly to get meaningful cost attribution, for example:

```yaml
llm:
  spending_budget:
    model_pricing:
      jev-1.13.0:
        input_usd_per_million_tokens: 0.042
        output_usd_per_million_tokens: 0
```

This example matches the [model documentation](https://docs.typesafe.ai/models)
checked on 2026-09-28; verify pricing when enabling live calls. Keep existing LLM
pricing entries. A configured spending limit requires applicable pricing;
exhaustion skips optional JEV work. Without pricing, the result reports unknown
cost rather than claiming the request was free.

Input reservation uses a conservative byte estimate plus framing allowance, not
the provider tokenizer. Valid reported usage replaces the estimate. Attempts with
unknown usage are charged conservatively. Every retry consumes the shared window
call budget and receives its own spending reservation. Total time includes retry
backoff and the foreground wait for accounting; consumers can impose a tighter
owning-stage deadline. Invalid compressed responses return typed unavailable
results just like other malformed responses.

Accounting has a separate bounded attempt. If it fails or outlasts the request,
the client retains the reservation and usage for retry and reports
`accounting_pending`. New optional JEV calls wait for that recovery. These work
items contain usage/model data, not the private request payload. After a process
loss, durable reservations still protect the balance: on later admission,
unsettled reservations older than 15 minutes are charged once at their reserved
estimate. Late settlement does not charge them again. This can overestimate cost,
including when the provider was never reached; it avoids silently refunding
possibly completed work. The existing budget tables support this behavior without
a schema change.

The application owns one lightweight JEV client. Its HTTP client is created only
on first eligible request. Settings are captured per request, so credential or
endpoint updates do not close a transport underneath active work. A request
returns when its total deadline expires, even if cleanup is still running. The
client owns that cleanup, and late reservation completion cannot dispatch a
provider call after the deadline. Caller cancellation still propagates.

Shutdown cancels owned requests, retries accounting, and closes HTTP before
PostgreSQL closes. If cleanup exceeds the shutdown deadline or accounting still
fails, shutdown raises promptly while retaining the owners for retry; it does not
close PostgreSQL underneath unfinished accounting.

## Decision provenance and pending storage work

The Phase B identity pilot calls JEV in observe mode only when deterministic
matching abstains and at least one eligible visible candidate exists. It sends
bounded occurrence support and local candidate handles with Choice and independent
Noul questions. Results include the baseline ID and hypothetical candidate
suggestion in the private in-memory identity trace. Both reconciliation passes
share a call budget. No observation changes an entity ID, alias, classification,
or commit. `identity_mode: active` still uses baseline behavior until a reviewed
acceptance policy is implemented. Awaiting JEV currently holds the resolver lock;
latency and lock occupancy need measurement before active rollout.

`JevDecisionRecord` retains scoped occurrence/evidence references, domain/model/
question versions, option mapping, Choice/Noul results, usage/cost/timing, baseline
outcome, and acceptance status. Observe records cannot be marked accepted.
This is currently a serializable contract, not a durable database integration.

Planned ownership:

- The ingestion build accumulates private records across its two bounded passes.
- Observe diagnostics use a scoped window decision store. They do not alter entity
  classification or count as accepted metadata.
- Accepted classification provenance is persisted by the semantic commit writer
  in the same transaction as the classification, linked to entity/project/window.
- Later higher-tier review reads a bounded decision record plus the original
  scoped source evidence. It does not treat the earlier judgment as proof.
- Storage must deduplicate retries, enforce project/user visibility, and define
  retention with referenced Context versions. Committed replay skips provider work.

Implementing those storage rules needs schema/reader/writer and commit-contract
changes. They are deferred to the consumer/provenance integration work rather than
adding a disconnected table in Phase A. Full private input payloads and API keys
are not part of ordinary diagnostics.

## Baseline capture

From `server`, run the project interpreter with:

```text
python -m tests.fixtures.capture_jev_baseline
```

It writes `tests/fixtures/jev_baseline_outputs.json` using the existing fake-backed
ingestion test harness and deterministic resolver. No provider calls or model
downloads occur. `jev_pilot_cases.json` contains additional proposed examples.
Both still require human review and broader held-out examples before they can
serve as quality ground truth. These artifacts do not establish live JEV quality.
