# JEV foundation

Phase A provides an optional HTTP adapter, configuration, shared spending, frozen
ingestion policy, and typed decision records. The Phase B identity consumer runs
in observe mode. Bounded extraction supports disabled, observe, and opt-in active
modes. Active identity reuse is still gated on reviewed quality evidence.

## Installation and settings

No JEV SDK or local model download is required. The adapter uses the server's
existing `httpx` dependency. Knoggin supports the direct TypeSafe System One
endpoint and OpenRouter's Decisions API. The evaluation pilots use OpenRouter's
`typesafe/jev-1.13` model and an OpenRouter API key.
Tests use mock transport and need no key or network.

The private runtime configuration file is `knoggin.yml` in the configured directory.
Its new top-level section accepts:

```yaml
jev:
  api_key: ""  # Private credential; do not commit it.
  endpoint: https://openrouter.ai/api/alpha/decisions
  model: typesafe/jev-1.13
  identity_mode: disabled
  extraction_mode: disabled
  classification_mode: disabled
  max_candidates: 8
  identity_match_sample_rate: 0  # Optional observe sample of baseline reuses.
  acceptance_policy_version: observe-v1  # Use identity-positive-v1 only after evaluation.
  identity_min_choice_confidence: 0.9
  identity_min_choice_probability: 0.8
  identity_min_probability_margin: 0.2
  identity_min_evidence_noul: 0.8
  extraction_min_choice_confidence: 0.8
  extraction_min_entity_noul: 0.8
  max_calls_per_window: 12
  max_questions_per_request: 16
  max_options_per_choice: 64
  max_request_bytes: 24000
  request_timeout_seconds: 5
  total_timeout_seconds: 12
  max_elapsed_seconds_per_window: 30
  accounting_timeout_seconds: 2
  shutdown_timeout_seconds: 5
  max_retries: 1
```

Defaults are conservative initial work limits and extraction gates, not live
benchmarked quality guarantees. Versioned models are required; moving aliases are
rejected. The public settings UI/API has not been
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
      typesafe/jev-1.13:
        input_usd_per_million_tokens: 0.042
        output_usd_per_million_tokens: 0
```

This example matches the [OpenRouter Jev model page](https://openrouter.ai/typesafe/jev-1.13)
checked on 2026-10-04; verify pricing when enabling live calls. Keep existing LLM
pricing entries. A configured spending limit requires applicable pricing;
exhaustion skips optional JEV work. Without pricing, the result reports unknown
cost rather than claiming the request was free.

Input reservation uses a conservative byte estimate plus framing allowance, not
the provider tokenizer. Valid reported usage replaces the estimate. Attempts with
unknown usage are charged conservatively. Every retry consumes the shared window
call budget and receives its own spending reservation. The shared elapsed budget
starts with the first provider attempt and spans later requests and both identity
passes. It includes retry backoff and foreground accounting waits. Invalid
compressed responses return typed unavailable results like other malformed
responses.

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

## Decision provenance

The Phase B identity pilot calls JEV in observe mode when deterministic
matching abstains and at least one eligible visible candidate exists. An optional
stable sample of deterministic reuses can audit false reuse; the default rate is
zero. It sends
bounded occurrence support and local candidate handles with Choice and independent
Noul questions. Results include the baseline ID and hypothetical candidate
suggestion in the private in-memory identity trace. Evidence-free copies of the
bounded observation records are also emitted as `jev_identity_observation` JSON
diagnostics for pilot review. Both reconciliation passes share call-count and
elapsed-time budgets. At most 128 identity observations are retained per window,
including early unavailable results that consume no provider call. No observation
changes an entity ID, alias, classification,
or commit. Active semantic reuse requires both `identity_mode: active` and
`acceptance_policy_version: identity-positive-v1`. The positive policy rejects
truncated sets and requires Choice confidence >= 0.90, selected probability >=
0.80, probability margin >= 0.20, and Noul evidence >= 0.80. Any failure returns
to the pending reuse/new-ID baseline. The default `observe-v1` policy keeps
historical baseline behavior even if an older configuration says active. Awaiting
JEV currently holds the resolver lock,
but the shared elapsed budget now bounds that wait; lock occupancy still needs
measurement before active rollout.

`JevDecisionRecord` retains scoped occurrence/evidence references, domain/model/
question versions, option mapping, Choice/Noul results, usage/cost/timing, baseline
outcome, and acceptance status. Observe records cannot be marked accepted.
Identity observe records are also stored with the semantic window in the same
Knowledge commit transaction. A scoped reader returns them for project review;
deleting the window/project cascades to its records. The full raw JEV request and
API key are never stored. All semantic commit contracts passed against a fresh
live PostgreSQL schema on 2026-10-03, covering scoped reads, idempotent replay,
unowned-evidence rollback, and deletion cascade.
When the candidate list is truncated, the record is `indeterminate` and exposes no
suggested entity ID, so it cannot be scored as an actionable identity proposal.
The record also retains up to 128 eligible candidate IDs for a separate candidate
recall audit; larger lists are marked incomplete.

The bounded extraction consumer prepares literal candidates only from validated
unknown relationship endpoints and known-alias gaps. A typed candidate receives an
entity-evidence Noul; a candidate without a usable current-project type also
receives a Choice over the frozen domain's active types. Observe mode records the
hypothetical type and runs the current generative fallback unchanged. Active mode
accepts only positive results above both configured gates, validates the literal
span and domain type again, assigns the `jev_fallback` origin, and recomputes gaps.
Negative, uncertain, unavailable, or invalid results never suppress fallback.

An originally uncovered block remains a residual gap after one bounded recovery.
This prevents recovering “Delta” from being treated as proof that other names in
the block were found. If the block already had normal coverage and every targeted
alias/endpoint gap is positively recovered, active mode may avoid the generative
NER call. `llm_ner_mode` independently controls residual generative fallback, so
active JEV can recover a bounded candidate while LLM NER is disabled. The existing
maximum of two entity/relationship passes is unchanged, and both passes share the
same JEV work budget.

The first reviewed live extraction diagnostic ran on 2026-10-05. Raw decisions
were correct on 11/12 cases; the frozen active gate accepted four candidates,
all correct, for 100% precision and 57.14% positive recall. Provider-reported
cost was $0.00027569. This is a small diagnostic, so broader held-out recovery,
latency, and fallback-savings measurement remain required.

Extraction records are private, window-scoped, and committed atomically in
`project_jev_extraction_decisions`. Trace counters report candidate judgments,
accepted/rejected positives, avoided calls, avoided blocks, prompt-character
reduction, and actual LLM fallback calls separately. Prompt characters are not
claimed as provider token savings; live usage measurement is still required.

Ownership:

- The ingestion build accumulates private records across its two bounded passes.
- Observe diagnostics use bounded application logs, and the scoped semantic
  window decision store is the durable source after a successful Knowledge commit.
- Topic-classification proposals and their post-identity aggregate are stored in
  `project_jev_classification_decisions` in the same Knowledge transaction.
  Observe records have no accepted topic and keep the configured default active.
- A later higher-tier review can read a bounded decision record plus the original
  scoped source evidence. It does not treat the earlier judgment as proof.
- The window/pass/occurrence key is unique. Project and window ownership is
  checked during writes and reads; committed replay skips provider work.
- Observe records live for the lifetime of the semantic window. Deleting the
  project removes its windows and records. The referenced Context block versions
  remain subject to existing Context retention rules.

Full private input payloads and API keys are not part of ordinary diagnostics or
the decision store. The live PostgreSQL commit/reader contracts pass.

## Topic configuration foundation

An entity type's existing `topic` field remains its default topic. Phase D adds
an optional `allowed_topics` list for types that may be classified under more
than one active topic. When omitted, the compiled list contains only the default,
so existing domain files behave exactly as before. The list must reference known
topics and include the default. Inactive topics remain in durable configuration
but are excluded from runtime choices. Existing entity classifications are not
rewritten when this configuration changes.

With `classification_mode: observe`, a first-entry entity whose known type has
multiple active allowed topics receives a bounded topic Choice plus an independent
evidence Noul. `classification-v2` does not expose the configured default to JEV,
and it explicitly directs generic, indirect, future, multi-topic, and name-only
evidence to `insufficient_evidence`. One allowed topic is derived without a
provider call. Existing project classifications skip this request and remain
authoritative. The raw answer is retained, but only a non-default answer becomes
an override proposal. The configured default still supplies the staged
classification and new-entity write. Truncated option sets expose no suggestion.
`classification_mode: active` changes the first-entry topic only when
`classification_acceptance_policy_version: override-positive-v1` is also set.
The accepted non-default aggregate must be consistent, and every contributing
proposal must meet the frozen Choice confidence, selected-probability,
probability-margin, and evidence-Noul thresholds. Otherwise the configured
default remains active.

After identity resolution, occurrence proposals are grouped by resolved entity.
The aggregate reports a consistent proposal, conflicting proposals, or no usable
proposal. A conflict marks the contributing proposed records as `conflicting` and
keeps all proposed topics for review in configured domain order. The configured
default remains the operational topic for every first-entry entity in observe
mode. Existing project classifications are excluded from aggregation.

The atomic writer stores each occurrence together with that aggregate only after
rechecking its frozen domain/model/question versions, evidence ownership, option
mapping, response-derived suggestion, and first-entry entity classification. The
bounded reader requires window, user, and project scope. Replay does not duplicate
records, and deleting the semantic window deletes its classification records.
The schema already reserves a nullable `accepted_topic`; observe mode requires it
to be null so a proposal cannot be mistaken for an applied classification.

Topic quality uses a separate human-reviewed packet and scorer. From `server`,
put the key in the repository root `.env` (which is ignored by Git):

```text
OPENROUTER_API_KEY=<your key>
```

After reviewing `tests/fixtures/jev_classification_review_cases.json`, run:

```powershell
python -m tests.fixtures.run_jev_classification_pilot --output-dir <private-directory>
```

The report separates raw and decisive accuracy, abstention accuracy,
non-default override precision and recall, entity-level aggregate accuracy,
unavailable/indeterminate coverage, and false conflicts. It reports
`active_ready` only with at least 50 reviewed occurrences, 10 non-default
examples, 10 ambiguous examples, 10 repeated-entity aggregates, 95% for every
accuracy/precision/recall gate, at most 10% unscorable results, and at most 2%
false conflicts. Provider-reported usage and cost are shown separately from the
configured spending ledger. The bundled packet contains 50 reviewed occurrences,
including 24 non-default, 10 ambiguous, and 10 repeated-entity aggregates.

The 2026-10-05 same-packet comparison improved raw accuracy from 78% with the
initial wording to 96% with `classification-v2`; v2 reached 100% abstention,
95.83% override precision/recall, and 95% aggregate accuracy. Because the current
wording was designed after inspecting the initial errors on this packet, these
results are diagnostic and did not enable active classification by themselves.

The separately reviewed 50-occurrence held-out packet passed on 2026-10-05 with
96% raw accuracy, 95% decisive accuracy, 100% abstention, override
precision/recall, and aggregate accuracy, with no unavailable results or false
conflicts. The run reported $0.00132602 of provider cost. This completed C5 and
allowed the separately gated active override path to be implemented; observe
mode remains the default.

## Baseline capture

From `server`, run the project interpreter with:

```text
python -m tests.fixtures.capture_jev_baseline
```

It writes `tests/fixtures/jev_baseline_outputs.json` using the existing fake-backed
ingestion test harness and deterministic resolver. No provider calls or model
downloads occur. `jev_identity_review_cases.json` contains additional proposed examples.
Both still require human review and broader held-out examples before they can
serve as quality ground truth. These artifacts do not establish live JEV quality.

After `jev_identity_review_cases.json` has been human-reviewed and its top-level
status changed to `reviewed`, the identity pilot reads the same root `.env` key.
Run it with a private output directory:

```powershell
python -m tests.fixtures.run_jev_identity_pilot --output-dir <private-directory>
```

The runner makes at most one provider attempt per case. It writes stable labels,
bounded observations, and a report containing candidate recall, raw judgment
accuracy, active-gate precision, wrong reuse, abstention, and recorded spend. It
does not write the API key. The current ten seed cases exercise the workflow but
cannot satisfy the 200-case operational acceptance threshold.
