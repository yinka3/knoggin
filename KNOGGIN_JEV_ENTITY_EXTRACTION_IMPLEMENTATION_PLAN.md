# JEV entity identity and extraction implementation plan

Date: 2026-09-28
Status: identity, bounded extraction, and retrieval are in scope as of 2026-10-03. Bounded extraction begins with an observe pilot; active acceptance still requires evaluation.

This plan consolidates the JEV identity, extraction, classification, provider,
spending, and lifecycle decisions that were previously spread across working
journals. It is the canonical implementation record for this work.

## 1. Agreed behavior

- Start identity and bounded extraction in observe mode: call JEV and record
  judgments while retaining baseline decisions. Observe costs time and money.
- Start with Choice and Noul. Questions use stable, versioned templates with
  occurrence-specific evidence and choices supplied by code.
- Questions in one request are independent. A Noul cannot inspect the Choice
  answer from that same request.
- Keep one accepted entity classification per project. Do not add independently
  selected topics to individual occurrences. Mentions still carry classification
  proposals through the existing ingestion pipeline.
- GLiNER labels continue to map to configured entity types. JEV does not retype
  every normal GLiNER mention; bounded missing candidates may need JEV typing.
- The user's domain configuration restricts classification choices.
- Existing project classifications remain authoritative during identity reuse.
  Changes use the existing reclassification path, not automatic overwrite.
- Identity uncertainty retains deterministic pending reuse/new-ID behavior.
- Extraction uncertainty retains residual generative fallback when enabled.
- Preserve literal evidence, scope, atomic commit, and postcommit publication.

## 2. Question contracts

### Identity

State: occurrence name, type, topic proposal, eligible supporting text, and a
bounded visible candidate list. Use actual profile fields: canonical name,
aliases, and classification status. Do not invent biographies or relationships.

Choice: "Which supplied candidate refers to the same entity as this mention?"

Options: local candidate handles, `none_of_these`, `insufficient_evidence`.

Noul: "The supplied evidence supports identifying this mention with one specific
candidate."

The Noul assesses general evidence sufficiency, not correctness of the candidate
selected by Choice. Evaluate its incremental usefulness. Pair-specific verification
would require questions per candidate or a separate request and is deferred.

### Bounded extraction

State: literal candidate name, occurrence location, bounded support, evidence
origin, and active type descriptions.

Choice: "What configured entity type best describes this mention in its context?"

Options: active entity types, `not_an_entity`, `insufficient_evidence`.

Noul: "The supplied context supports treating this mention as an entity."

An existing valid type need not be selected again. Code validates returned types,
literal spans, and eligible blocks before accepting a mention.

### Topic classification

The current domain maps each type to exactly one topic. Initially that topic is
the default. Extend configuration to permit explicitly allowed alternatives per
type, with backward-compatible behavior when alternatives are absent.

Once type is known, topic Choice options are the allowed active topics for that
type plus `insufficient_evidence`. Include topic descriptions and source evidence.
When there is only one allowed topic, derive it without a JEV call.

A proposed topic Noul is: "The supplied evidence supports assigning one specific
allowed topic to this entity in the current project." Evaluate before making it
an acceptance gate.

Build topic options in code after type validation. For a JEV-typed missing
candidate, topic selection may require a subsequent request; same-request questions
cannot depend on the type Choice answer. Budget this explicitly. For GLiNER-typed
candidates, type is already available.

Topic judgments are proposals until identity resolution determines whether the
entity already has a current-project classification. Retain that classification
on reuse. A foreign-project classification cannot supply the active project's
type or topic.

## 3. Provenance and conflicts

Introduce a typed decision record, provisionally named `jev_classification` for
classification provenance, rather than a second competing classification field.
Keep normal accepted type/topic fields as the operational classification.

Record capability, project/window/pass and occurrence references, question and
policy versions, pinned/reported model, bounded option mapping, Choice distribution
and confidence, Noul value, outcome, fallback reason, and acceptance status.
Retain source block/version references. Do not write API keys or full private
payloads to ordinary logs.

Distinguish proposed, accepted, rejected, unavailable, and conflicting judgments.
Choose and document durable storage ownership before claiming provenance survives
restart; the current in-memory trace alone is insufficient. Persist accepted
classification provenance with its semantic commit, without publishing private
state early. Observe records must not become accepted entity metadata.

A higher-tier model may later receive a bounded package containing the prior
proposal, its origin, and original evidence. Provenance identifies who made a
judgment; it does not establish correctness. Automated higher-tier adjudication
and its triggering policy are follow-up work, not part of the initial pilot.

Code detects conflicting classifications after mentions resolve to the same ID.
Retain an existing project classification. For a new entity, use a domain-valid
baseline/default classification when available and record the conflict. If types
also conflict or no valid default exists, use the existing validation/issue path;
do not arbitrarily select one. These default conflict rules are implementation
recommendations to validate against current commit contracts.

## 4. Work units and order

### Phase A — Baseline and shared foundation

- [x] A1: Capture human-reviewed identity examples and baseline outputs. Include
  same names, aliases, foreign classifications, and repeated occurrences.
  Extraction/classification examples remain proposed until separately reviewed.
- [x] A2: Add a small async provider adapter with typed Choice/Noul requests,
  response validation, timeout/retry bounds, cancellation, and fake-backed tests.
- [x] A3: Add independent capability modes, model/question versions, work limits,
  and separate acceptance policies. Use one shared external-model spending budget.
- [x] A4: Wire application-owned client lifecycle and dependency injection.
- [x] A5: Extend frozen ingestion policy and snapshot validation. Credentials stay
  runtime-only. Committed windows resume without new judgments.
- [x] A6: Verify durable decision records, retention, and atomic commit ownership
  against PostgreSQL. The implementation and scoped reader are in place.

Phase A progress (2026-09-28): A1 has proposed examples and four captured
deterministic/fake-backed baseline outputs; human review and broader quality
ground truth remain pending. A6 has typed, serializable records and a storage
ownership design; durable record storage is deferred to consumer integration
because it requires schema and commit changes. See [foundation notes](server/JEV.md).
No consumer requests or active acceptance behavior have been added.
Verification: 204 focused regression tests passed; subsequent provider/policy/LLM
checks passed, including the shared-balance check and response/shutdown failure
cases. Ruff checks passed. Live JEV and PostgreSQL integration checks were not run.

Review fixes (2026-09-29): bounded foreground and shutdown waits now retain
unfinished accounting for retry; malformed compressed responses return typed
unavailable results; reservations retain their storage/reset-period/price across
budget changes. PostgreSQL accounting also covers uncapped calls, and settlement
retries cannot double-charge. Expired unsettled reservations conservatively charge
their estimate using the existing tables. No consumer behavior or schema changed.
Validation: 183 focused regression tests passed; five isolated PostgreSQL tests
skipped because the test service was unavailable. Live SQL behavior remains to be
verified. A1 human review and A6 durable decision provenance are still pending.

Phase A review fixes (2026-10-07): admitted `classification-v1` snapshots now
reopen with their original question wording and proposal behavior; new windows
still default to v2, and active topic overrides require v2. Spending-reservation
storage failures return typed `reservation_unavailable` results without provider
dispatch, while cancellation propagates. The atomic identity/extraction writers
now recompute accepted judgments from validated responses and frozen gates, and
check the recorded reuse or staged extraction association before storing them.
Weak, unavailable, malformed, or mismatched acceptance rolls back the commit.
The unrelated missing-`DATABASE_URL` test now isolates dotenv loading.
Validation: 243 combined policy/provider/runtime/ingestion/evaluation/LLM and
live PostgreSQL tests passed; Ruff and diff checks passed. No live provider calls
were made. A1's captured baseline outputs still require human review; these fixes
do not mark that requirement complete.

Phase A baseline review (2026-10-07): the user approved the captured baseline
behavior with the ordinary-word false reuse, PG candidate-discovery miss, and
partial-block extraction coverage limitation explicitly recorded. The baseline
file is now marked reviewed. This completes A1's baseline capture/review; it does
not approve those errors as desired behavior or establish representative active
readiness. The broader Phase B/V4 evaluation requirements remain separate.

### Phase B — Entity identity observe pilot

- [x] I1: Separate visibility/scope/type eligibility from deterministic acceptance
  heuristics; verify disabled behavior remains unchanged.
- [x] I2: Build bounded candidate handles from the durable candidate snapshot.
  Keep per-occurrence evidence even when searches share a normalized name.
- [x] I3: Run Choice/Noul for eligible uncertain cases in observe mode. Record the
  baseline result and hypothetical semantic result without changing resolution.
- [ ] I4: Measure candidate recall separately from judgment quality. Optionally
  sample deterministic matches to estimate false reuse.
- [x] I5: Establish held-out acceptance criteria before adding active acceptance.
  Do not compare JEV probabilities with the existing score capped at 1.5.
- [x] I6: Add active decisions behind mode configuration. Preserve hard boundaries,
  pending reuse/new-ID fallback, existing classifications, and postcommit publication.

Phase B start (2026-09-30): uncertain deterministic abstentions now receive
bounded, occurrence-specific Choice and Noul observations from the scoped
candidate snapshot. The request uses local handles; incompatible and invisible
candidates are excluded, while foreign-project classifications are neutral.
Both identity passes share one in-memory call and elapsed-time budget. The current
baseline ID and JEV suggestion appear in the private in-memory identity trace and
an evidence-free bounded JSON diagnostic. Truncated candidate sets are marked
indeterminate and expose no suggested ID. The JEV result never selects an ID or
writes an alias. Disabled and active settings retain
baseline behavior until the active acceptance gate is designed and evaluated.
At that point, I1 hard-boundary extraction, I4 reviewed candidate-recall
measurement, A1 human-reviewed examples, and A6 durable database provenance
remained pending. The follow-up below updates that status.

Phase A/B follow-up (2026-10-03): hard JEV candidate eligibility now has its own
resolver boundary. The semantic commit writer stores scoped, bounded identity
observations atomically with Knowledge, and a project-owned reader exposes them
for review. Window deletion cascades to these records; otherwise they live with
the semantic window. A candidate audit retains up to 128 eligible IDs and marks
larger sets as incomplete. A scoring script separates candidate discovery and
offer failures from JEV judgment mistakes, but refuses unreviewed labels. A stable
sample of deterministic reuses can audit false reuse and is off by default. The
identity review packet is proposed, not yet human-approved. PostgreSQL storage
contracts and live provider quality are still unverified; active identity reuse
remains gated on reviewed acceptance criteria.

Phase A storage verification (2026-10-03): a live fresh PostgreSQL run exposed
and fixed a reserved-word alias in both scoped decision readers. All 18 semantic
commit contracts then passed, including identity/extraction decision persistence,
owner-scoped reads, idempotent replay, unowned-evidence rollback, and window
deletion cascade. The Windows storage fixture now selects the Psycopg-compatible
Selector event loop and uses the direct IPv4 test address by default.

Phase B active implementation (2026-10-03): `identity-positive-v1` adds a second
explicit gate beyond `identity_mode: active`. It requires a complete candidate
set, Choice confidence >= 0.90, selected probability >= 0.80, probability margin
>= 0.20, and identity-evidence Noul >= 0.80. Failure retains the current pending
reuse/new-ID path. Accepted IDs must be eligible supplied candidates and must be
the reused committed ID recorded by the atomic writer. Existing project
classification remains authoritative. `active` with the default `observe-v1`
policy retains baseline behavior for backward compatibility.

Before operational enablement, a held-out set must contain at least 200 reviewed
occurrences, including at least 50 ambiguous/no-match cases. Candidate recall must
be reported separately and reach 95%. Scored JEV judgments must reach 95%
accuracy. The active gate must reach at least 99% acceptance precision and no more
than 1% wrong reuse, with zero scope, type, truncation, or existing-classification
violations. The evaluator reports candidate discovery, raw judgment quality,
active acceptance precision, wrong reuse, and abstention separately. These are
established criteria, not achieved results; A1/I4 and live JEV evaluation remain
open.

Phase A/B evaluation tooling (2026-10-03): the live pilot runner now converts an
approved review packet into stable labels, exercises the real resolver and JEV
client in observe mode, and writes private observations plus a report. It limits
each case to one provider attempt. Missing observations with a known correct
entity count as candidate-discovery misses; candidate recall, raw judgment
accuracy, active-gate precision, wrong reuse, and abstention are reported
separately. The runner refuses proposed packets until a person marks them
reviewed. The current ten seed cases do not satisfy the 200-case activation
threshold.

Phase A/B identity diagnostic (2026-10-05): the ten seed labels were independently
reviewed and sent to the provider. Candidate recall was 6/7 (85.71%) because the
intentional `PG` alias miss never reached JEV. JEV scored 8/9 offered judgments
(88.89%); it incorrectly reused Project Delta for ordinary lowercase `delta`.
The frozen active gate accepted four matches, all correct, and abstained on six,
for 100% acceptance precision and zero wrong reuse in this small diagnostic.
Provider-reported cost was $0.00022277. A1/I4 remain open for the full 200-case
held-out activation set.

Phase B review (2026-10-07): four gaps were reproduced without live provider
calls. Observe mode with `identity-positive-v1` can produce an accepted record
and fail the non-active provenance validator. The fuzzy name search's 50-alias
limit can omit competing identities without setting candidate truncation. A
second entity pass with an exhausted shared budget can discard an accepted
first-pass identity, allocate a new ID, and fail durable acceptance consistency.
The evaluator also treats a missing observation as a candidate-discovery miss
even when observation limits or sampling explain its absence. These need fixes;
Phase B is not fully verified. Validation: 155 existing identity, ingestion,
restart/publication, and PostgreSQL tests passed, plus four temporary reproductions.
One model-dependent smoke test skipped. Representative identity readiness/I4
remains open; the reviewed synthetic stress packet does not replace it.

Phase B review fixes (2026-10-07): observe mode cannot apply the positive identity
gate. JEV eligibility now groups all qualifying fuzzy names into identities before
bounding options; the historical baseline search is unchanged. An accepted
first-pass decision can survive the second pass only after a fresh locked
snapshot produces identical evidence, options, domain, and policy inputs. This
uses the existing window decision, consumes no additional call/observation slot,
and stores/logs it once. Changed or removed decisions lose their acceptance
status before the final commit. Missing-observation reporting now separates
explicit discovery misses from unknown recall and reports supplied skip reasons;
the pilot marks its deliberate discovery-miss cases explicitly.
Validation: 258 identity, ingestion, restart/publication, classification,
provider/policy, and live PostgreSQL tests passed; one local-model smoke test
skipped. The PostgreSQL regression confirms accepted carry-forward commits once
after shared-budget exhaustion. Representative identity readiness/I4 remains open.
No live provider calls or production setting changes were made.

### Phase C — Bounded extraction observe pilot

- [x] E1: Refactor gap detection into per-block reasons and literal candidate
  preparation without altering baseline extraction.
- [x] E2: Discover candidates from unknown endpoints and known alias gaps.
  Do not silently lower GLiNER thresholds or add broad candidate discovery.
- [x] E3: Build type Choice/Noul requests for candidates without a usable type.
  Observe results while running existing fallback unchanged.
- [x] E4: Add explicit `jev_fallback` origin and validated offset/support semantics.
  Supporting-only occurrences may retain nullable Context offsets where validated.
- [x] E5: Define and evaluate positive-recovery coverage rules. Recovering Delta
  must not be treated as proof that PostgreSQL and Maya in the same block were found.
- [x] E6: Add active positive recovery and recompute residual gaps. Unresolved
  blocks still reach generative fallback when permitted; negative/uncertain JEV
  answers initially do not suppress fallback.
- [x] E7: Preserve independent `llm_ner_mode` semantics and the existing bounded
  relationship/entity second pass. Report call avoidance separately from token savings.

Phase C implementation (2026-10-03): literal candidates now come only from
validated unknown relationship endpoints and known-alias gaps. Observe mode stores
the judgment and retains the baseline fallback. Active mode accepts only a literal,
domain-valid positive result above the versioned Choice/Noul gates, then recomputes
residual gaps. A block that was originally uncovered still reaches generative NER,
because recovering one supplied name cannot prove exhaustive coverage. JEV and
`llm_ner_mode` remain independent, and both entity passes share the existing call
and elapsed-time budget. Private extraction decisions commit atomically with the
semantic window. Trace counters separate avoided calls, avoided blocks, prompt
character reduction, and actual LLM fallback work; provider token savings still
require live measurement. Focused unit and live PostgreSQL semantic-commit tests
pass. Live JEV quality and broader restart/concurrency checks remain Phase E
readiness gates.

Phase C review (2026-10-03): active type selection now records whether its
bounded type list was truncated. A truncated list may be observed but cannot be
accepted, and the atomic writer rejects any forged accepted result carrying that
flag. Observe/fallback behavior, literal validation, independent JEV/LLM modes,
and durable storage contracts otherwise matched the plan. Live quality, recall,
latency, and savings measurement remain Phase E readiness work rather than a
claim made by Phase C.

Phase C extraction diagnostic (2026-10-05): twelve independently reviewed
candidate cases were evaluated through the real request builder and frozen
`positive-v1` acceptance gate. Raw decisions were correct on 11/12 (91.67%).
The gate accepted four candidates, all correct, for 100% precision and 57.14%
positive recall; no provider calls were unavailable. Provider-reported cost was
$0.00027569. This small diagnostic supports the conservative gate but is not
large enough to claim production extraction quality or fallback savings.

Phase C review (2026-10-07): three integration gaps were reproduced without live
provider calls. A second entity build with an exhausted shared budget can drop an
accepted first-pass extraction while leaving its record accepted, causing commit
validation to fail. Supporting-only occurrences use identical null-offset
deduplication keys across different blocks, so one mention disappears while both
decisions can claim acceptance. Finally, represented-name sets are window-wide;
recovering an alias in one block can clear another block's unresolved alias gap
and incorrectly suppress generative fallback. Required fixes are extraction
carry-forward/revalidation, block-scoped occurrence deduplication, and block-scoped
residual coverage. Validation: 104 existing extraction, relationship, ingestion,
restart/publication, and PostgreSQL tests passed, plus three temporary reproductions.
Phase C is not fully verified; representative quality and actual fallback-savings
evidence remain separate open readiness work.

Phase C review fixes (2026-10-07): occurrence deduplication, candidate discovery,
and residual alias coverage now include the source block ID. Supporting-only
mentions in separate blocks remain distinct, and a recovery in one block cannot
clear another block's unresolved gap. Accepted extraction decisions survive a
second build only when freshly validated literal evidence, offsets, type options,
domain, and policy match their original fingerprint. Carry-forward consumes no
extra call or observation slot and retains one durable decision record; stale or
unapplied decisions lose their acceptance status. Regression coverage includes
active/observe/disabled behavior, residual fallback under partial exhaustion,
changed evidence/policy, removed gaps, and supporting-only occurrences.
Validation: 228 combined extraction, relationship, ingestion, identity,
classification, provider/policy, and live PostgreSQL tests passed. The new SQL
contract confirms both supporting-only recoveries persist once after an exhausted
second-pass rebuild and remain owner scoped. Ruff and diff checks passed. No live
provider calls or production mode changes were made. Representative quality and
actual fallback savings remain Phase E readiness work.

### Phase D — Project classification and topic selection

- [x] C1: Extend and validate domain configuration with allowed topics per type
  and a default; update compilation, serialization, and activation/reclassification
  assumptions. Existing configurations retain their fixed mapping.
- [x] C2: Implement topic Choice/Noul in observe mode when alternatives exist.
  Reuse known valid types; select a topic for first-entry project classification.
- [x] C3: Aggregate proposals after identity resolution, preserve existing project
  classification, and handle new-entity conflicts deterministically with diagnostics.
- [x] C4: Commit classification decision provenance atomically, including an
  accepted-topic slot for later active mode. Expose bounded
  decision/evidence records for later review without creating an adjudication loop.
- [x] C5: Evaluate topic accuracy and conflict behavior before enabling active topic
  selection independently of identity and bounded extraction.

Phase D start (2026-10-03): entity types retain `topic` as their default and may
now declare `allowed_topics`. Omitting the new field produces the old one-topic
mapping. Configuration parsing canonicalizes and validates every allowed topic,
requires the default to be included, filters inactive choices in the compiled
snapshot, and preserves older admitted snapshots during replay. Domain previews
report allowed-topic changes as future-only; existing classifications still
change only through the reclassification path.

Phase D observe classification (2026-10-04): first-entry classifications whose
known type has multiple active allowed topics now receive an occurrence-specific
Choice and independent evidence Noul when `classification_mode` is `observe`.
`classification-v2` withholds the configured default from the request and tells
JEV to abstain on generic, indirect, future, multi-topic, and name-only evidence.
Single-topic types are derived without a call. Existing project classifications
remain authoritative and skip topic observation. The bounded record contains the
default topic, allowed options, suggestion, response, and truncation status, but
the staged classification and new-entity write continue using the configured
default. Truncated sets are indeterminate. Provider failure keeps the default and
cancellation propagates. The bounded ingestion trace/log path feeds the atomic
durable classification decision store described in C4. Active topic selection
remains disabled pending C5 evaluation.

Phase D proposal aggregation (2026-10-04): occurrence-level topic proposals are
grouped by the entity ID produced by identity resolution. Aggregates distinguish
one consistent proposal, conflicting proposals, and no usable proposal. Existing
project classifications never enter this path and remain authoritative. For a
first-entry entity, agreement is still observational; the configured default is
the operational topic. When occurrences propose different allowed topics, their
records are marked `conflicting`, the aggregate retains every proposed topic in
domain order, and the default remains the deterministic result. Conflicting types
or a missing valid default fail through the existing classification validation
path rather than selecting arbitrarily. C5 quality evaluation remains open.

Phase D durable provenance (2026-10-04): each occurrence-level topic decision is
stored with its entity aggregate in `project_jev_classification_decisions` during
the same transaction that publishes Knowledge. The writer revalidates the frozen
domain/model/question policy, evidence ownership, allowed options, JEV response,
aggregate, and first-entry classification before inserting anything. Observe mode
requires `accepted_topic` to remain null and leaves the configured default as the
operational topic. The scoped reader is paginated, replay is idempotent, and
deleting the semantic window cascades to its records. Live PostgreSQL contracts
cover successful storage, owner scoping, rollback on unowned evidence, and cascade
cleanup. Active topic selection remains gated on C5.

Phase D classification evaluation tooling (2026-10-04): the offline scorer,
review packet, and optional live pilot are implemented. They keep occurrence and
decisive accuracy, abstention accuracy, non-default override precision/recall,
aggregate accuracy, unavailable results, and false conflicts separate. The scorer
refuses labels that are not explicitly human reviewed. Readiness requires at least
50 reviewed occurrences, 10 non-default occurrences, 10 ambiguous occurrences,
and 10 repeated-entity aggregates; 95% for every accuracy/precision/recall gate;
at most 10% unscorable occurrence or aggregate results; and at most a 2%
false-conflict rate. The packet contains 50 occurrences, 24 non-default, 10
ambiguous, and 10 repeated-entity aggregates. Its labels were human-approved on
2026-10-04.

Phase D classification-v2 diagnostic (2026-10-05): the original live run scored
78% raw and 77.5% aggregate accuracy. Ten of its eleven occurrence errors selected
the exposed default instead of abstaining. V2 removes that anchor, strengthens the
general abstention rule, and treats only non-default answers as override proposals.
On the same packet it scored 96% raw accuracy, 95% decisive accuracy, 100%
abstention accuracy, 95.83% override precision and recall, and 95% aggregate
accuracy with no unavailable results or false conflicts. OpenRouter reported
$0.00131855 for the 50 calls. This packet informed v2, so the result is an A/B
diagnostic rather than held-out evidence and did not complete C5 by itself.

Phase D held-out and active completion (2026-10-05): an independently reviewed
50-occurrence packet passed every frozen gate: 96% raw accuracy, 95% decisive
accuracy, 100% abstention, override precision/recall, and aggregate accuracy, with
no unavailable results or false conflicts. The run used 31,572 input tokens,
4,048 output tokens, and $0.00132602 of provider-reported cost. Active selection
is now separately gated by `classification_mode: active` and
`override-positive-v1`. It accepts only a unanimous non-default aggregate whose
contributing proposals meet the frozen Choice confidence/probability/margin and
evidence-Noul thresholds. Default, weak, unavailable, truncated, and conflicting
results retain the configured default. Observe mode remains the default.

Phase D review (2026-10-07): four gaps were reproduced without live provider
calls. The second entity build appends classification decisions and aggregates
from both passes without reconciling final entity IDs, causing commit failures
and loss of first-pass active overrides under budget exhaustion. Classification
commit validation checks acceptance thresholds but does not validate the response
model or full option distribution. Compiled snapshot hydration accepts allowed
topics outside the active topic set; a malformed admitted snapshot can therefore
stage an unconfigured topic. Finally, reclassification still forces the default
topic even when an existing alternative remains allowed. Required fixes are
proposal carry-forward/remapping and final aggregation, response revalidation,
compiled allowed-topic validation, and reclassification semantics that preserve
valid alternatives unless a reset is explicitly requested.
Validation: 145 existing configuration, classification, reclassification,
ingestion, runtime, and PostgreSQL tests passed, plus five temporary reproductions
covering the four gaps. Phase D is not fully verified; its earlier quality
measurements do not cover these integration failures.

Phase D review fixes (2026-10-07): unchanged available topic responses now carry
across in-memory rebuilds using a fingerprint of the window, occurrence, full
evidence, options, and frozen policy. They are remapped to the final entity IDs,
then aggregated again without another call or observation-budget charge. The
final trace replaces stale classification decisions and aggregates and records
`reused_from_pass`. Changed evidence or policy prevents reuse. The semantic
writer now revalidates available response models and complete distributions,
rolling back malformed decisions atomically. Compiled domain replay validates
active allowed topics and defaults while retaining legacy inactive mappings.
Reclassification preserves an existing allowed alternative and uses the default
when that alternative is no longer allowed.

Validation: 271 tests passed across classification, configuration and activation,
reclassification, identity, extraction, provider validation, and live local
PostgreSQL contracts. New regressions cover both rebuild modes, evidence/policy
invalidation, stale-ID removal, malformed model/distribution/missing-response
rollback, invalid compiled topic lists, and preservation of valid alternatives.
The end-to-end PostgreSQL test preserves an active override after exhausting both
call and observation budgets, commits only its final ID, and replays once without
duplicates. Ruff and `git diff --check` passed. These four reproduced integration
gaps are resolved; no live JEV calls or production mode changes were made.

### Phase E — Integration and active-mode readiness

PostgreSQL verification (2026-10-06): after starting the existing local Docker
PostgreSQL service, all 20 semantic commit contracts passed in 38.72 seconds.
The suite used a temporary database and covers scoped JEV decision persistence,
atomic rollback, replay, and semantic publication. These existing classification
contracts exercise observe records; dedicated active-override persistence coverage
remains part of V3.

- [x] V1: Bound requests across both ingestion passes; measure resolver lock
  occupancy. Do not release its lock around HTTP without snapshot revalidation.
- [ ] V2 (deferred by agreement, 2026-10-07): Reconsider caching after measuring
  repeated requests and expected cache-hit rate. Completed semantic-window replay
  already skips provider work; no additional response cache is implemented.
  Any future cache must contain only immutable semantic responses keyed by
  occurrence, evidence, options, domain/model/question/policy versions, and must
  never contain private allocated entity IDs.

V1 progress (2026-10-07): extraction and resolution use the same window-owned
JEV budget across both passes; retries consume its call limit and its elapsed
deadline is not reset by a second pass. The ingestion trace now records each
resolver pass's lock wait time, lock hold time, cumulative JEV calls, and remaining
JEV time. Lock timing is measured with a monotonic clock and the lock remains held
through provider work. A focused resolver/client suite passed 78 tests, including
timing on failure. The two-pass ingestion test was strengthened to prove that an
exhausted first-pass budget stays exhausted, but its execution is currently blocked
by Windows Application Control preventing a spaCy DLL from loading. V1 remains
open until that integration check and representative lock-occupancy measurements
are completed.

V1 completion (2026-10-07): the spaCy DLL loaded successfully on retry, and the
full ingestion/resolver/client regression passed 123 tests, including the
two-pass exhaustion check. The approved classification held-out pilot now saves
per-case resolver lock timings. A fresh 50-request live run across 40 cases
measured lock hold mean 0.322 seconds, p50 0.250 seconds, p95 0.500 seconds,
and maximum 1.765 seconds. This includes candidate loading, resolution, and one
or two JEV calls per case. Lock wait was zero in this sequential fixture-backed
pilot; these measurements do not establish contention or production-database
performance. The run retained 96% raw quality and 100% override precision/recall
and aggregate accuracy, with $0.00132602 provider-reported cost. The lock remains
held during provider calls; releasing it still requires snapshot revalidation.
- [x] V3: Verify cancellation, provider failure, budget exhaustion, admitted-policy
  restart, atomic commit, and publication only after commit.

V3 verification (2026-10-07): 93 client/policy/semantic-stage tests passed,
covering cancellation and accounting recovery, provider failure, shared budget
exhaustion, frozen policy snapshots, restart checkpoints, and publication after
durable Knowledge commit. All 22 PostgreSQL semantic commit contracts passed,
including accepted classification override persistence, owner-scoped reads,
idempotent replay, and rejection of a forged weak-evidence override. Commit
validation now recomputes acceptance for the entire proposal aggregate and checks
that the staged operational topic follows that gate. Active conflict/truncation
statuses are validated in the same priority order as resolution. Ruff and diff
checks passed. V1's separate spaCy-blocked two-pass test and representative timing
measurements remain open.
- [ ] V4: Record held-out quality, latency, and total spend. Enable capabilities
  independently only when their quality gates pass.

V4 measurement progress (2026-10-07): all three live runners now report request
mean/p50/p95/max latency, provider attempts, tokens, and provider-reported cost.
Unknown cost remains null rather than being counted as zero. Latency covers the
client request including retries and accounting; it excludes resolver lock wait,
candidate discovery, and end-to-end ingestion. Percentiles use nearest rank.
Saved 2026-10-05 observations produced the following measurements without new
provider calls:

| Capability | Attempted requests | p50 seconds | p95 seconds | Total USD | Evidence |
| --- | ---: | ---: | ---: | ---: | --- |
| Classification | 50 | 0.172 | 0.265 | 0.00132602 | Reviewed held-out packet |
| Identity | 9 | 0.187 | 0.438 | 0.00022277 | Ten-case diagnostic; one discovery miss |
| Extraction | 12 | 0.157 | 0.375 | 0.00027569 | Small candidate diagnostic |

Classification passed its packet quality gates and has a separately opt-in active
policy. Identity and extraction reports explicitly remain diagnostic-only and
not active-ready. No production mode settings were changed. V4 remains open for
the 200-case identity held-out set, broader extraction held-out quality and actual
fallback savings, and representative end-to-end latency/lock measurements.

V4 packet preparation (2026-10-07): new proposed review files contain 200
identity cases (140 explicit matches, 40 ambiguous, 15 different identities,
and five discovery misses) and 60 extraction cases (40 explicit known/unknown
type positives and 20 ambiguous names). Both are marked `pending_human_review`
and `synthetic_stress`. Their template variants are correlated; meeting a numeric
sample threshold is not sufficient to claim representative held-out readiness.
No live calls have been made on these packets. Human label review is next,
followed by frozen-policy stress scoring and separate representative project
ingestion evidence for extraction discovery recall, fallback savings, and rollout.

V4 reviewed stress results (2026-10-07): the user approved both packets unchanged.
Identity produced 195/195 correct offered judgments, candidate recall 140/145
(96.55%), and 140 correct active-gate accepts with zero wrong reuse. All 55
ambiguous/no-match cases abstained under the active gate; the five deliberately
missing aliases produced no provider request. Request p50/p95 were 0.203/0.312
seconds and total provider cost was $0.00522220.

Extraction produced 54/60 correct diagnostic answers (90%), accepted 32/40
positives (80% positive recall), and made zero false accepts. Eight unknown-type
Person positives had correct Choice answers but evidence Noul below the frozen
0.80 threshold. Six ambiguous first names were incorrectly typed Person, but
their evidence Noul also remained below that threshold, so they were not accepted.
Request p50/p95 were 0.203/0.297 seconds and cost was $0.00138373. No thresholds
were changed after seeing these results. The correlated synthetic packets meet
their numerical composition targets but do not establish production readiness.
V4 remains open for representative ingestion evidence and actual fallback savings.
The user deferred these real-ingestion runs on 2026-10-07. Existing diagnostic
and stress results are recorded; production mode settings remain unchanged.

Phase E review (2026-10-07): two evaluation-report defects were reproduced
offline, plus a Windows integration-fixture failure. For proposed-type extraction
candidates, the pilot writes `raw_choice`
from the thresholded accepted type instead of retaining the raw evidence Noul.
An identical 0.79 response is reported as incorrect at a 0.80 gate and correct
at a 0.70 gate, so raw quality and acceptance quality are not independent.
Known-type Noul results need their own explicitly defined quality metric and
must not invent a provider Choice answer. Identity scorers and the extraction
scorer also accept duplicate labels/case IDs: one identity observation counted
as 200 reviewed correct judgments, and one extraction observation counted as
60 reviewed correct answers. Reject duplicate labels and observation keys before
scoring or reporting sample composition. Classification already rejects duplicate
label and observation keys.

The ingestion PostgreSQL fixture also lacks the Windows Selector event-loop
policy used by the storage fixtures. Its default Proactor loop makes psycopg's
async pool initialization fail after 30 seconds before the test body runs.
Selecting the compatible loop for the test process made all seven ingestion
integration tests pass. Initial synchronous fixture connections also have no
connection timeout; the review used a two-second timeout and explicit local IPv4
to keep database setup bounded.

Validation: 229 JEV unit/storage checks, five durable spending-budget checks,
and seven ingestion PostgreSQL integration checks passed (241 total). Two
temporary offline reproductions confirmed the scoring defects and were removed.
The four shipped identity/extraction packets currently have no duplicate case
IDs. `git diff --check` passed. This review changed only the plan; the three
findings remain to be fixed, and no live JEV calls were made.

These findings concern evaluation integrity; they do not demonstrate a broken
operational acceptance gate. V2 response caching and V4 representative ingestion,
fallback savings, and production latency evidence remain deferred by agreement.
The synthetic packets remain diagnostic, and production settings were unchanged.

Phase E review fixes (2026-10-07): extraction observations now retain the raw
entity-evidence Noul and never invent a Choice answer for known-type requests.
Raw Choice accuracy covers only unknown-type cases, with a scored-case count and
null accuracy when none were scored. Known-type evidence has a separate Brier
score (mean squared error against reviewed entity/non-entity labels); this
measures entity support, not type correctness, and is independent of acceptance
thresholds. Acceptance precision and positive recall remain separate. Earlier
combined extraction raw-accuracy figures are historical and must not be treated
as independent raw quality under the corrected metric.

Identity quality and acceptance scorers now reject duplicate label and
observation keys. Extraction rejects duplicate case and observation IDs, and
both pilot loaders/runners reject duplicate case IDs before provider work. The
ingestion PostgreSQL fixture now selects the Windows-compatible event loop,
defaults to local IPv4 with a bounded connection timeout, and cleans up its
isolated database even if setup fails after creation.

Validation: 253 tests passed in a normal pytest run, including all seven
ingestion PostgreSQL integration tests without the earlier event-loop workaround.
Twelve new regressions cover threshold-independent raw evidence, mixed Choice
and Noul reports, duplicate scoring keys, packet loaders, and rejection before
client creation. Ruff and `git diff --check` passed. The three review findings
are resolved. V2 and V4's agreed deferrals remain open; no live JEV calls or
production settings changes were made.

Coverage follow-up (2026-10-07): added regression cases for corrupt frozen build
inputs, invalid staging outputs, bounded owner-scoped decision reads, topic
aggregation and evidence gates, malformed private provenance, valid provenance
inserts, provider failures, and admitted work-budget enforcement. Invalid writer
inputs are checked to fail before SQL inserts. The existing 90% gate and coverage
configuration are unchanged; production code was not altered for coverage.

Validation: all 121 focused checks passed, including 98 new regression cases.
The CI diff-cover command against `origin/main`, including branch coverage,
reports 91% from the fast report alone (827 changed executable lines, 71 missing).
The full fast lane passed 2,010 tests and encountered nine local failures outside
JEV in benchmarking, document extraction, filesystem/symlink behavior, and
project-file cleanup. The broader service lane also encountered failures
and was interrupted; affected JEV service contracts are checked
separately. Ruff passed. No live provider calls were made.

Final coverage verification: all 52 affected PostgreSQL/ingestion service checks
passed. The fast and affected-service reports together pass the unchanged
`diff-cover --compare-branch origin/main --branch-coverage --fail-under 90` gate
at 92% (827 changed executable lines, 59 missing). The allowed-topic regression
now uses the canonical compiled `project` key, so it tests malformed topic values
rather than merely rejecting an extra mapping key. Diff and lint checks passed.

## 5. Main file owners

Paths below are relative to `server/src/`; new filenames are proposed.

| Files | Responsibility |
| --- | --- |
| New `infrastructure/jev_client.py` | Provider boundary and validation |
| `infrastructure/llm_client.py` | Shared spending/accounting seam |
| `common/schema/settings.py`, `common/conf/manager.py` | Configuration and publication |
| `runtime/resources.py`, `runtime/project_factory.py` | Lifecycle and injection |
| `core/ingestion/policy.py`, `semantic_window_admission.py`, `common/schema/semantic_window.py` | Frozen policy/snapshot contracts as required |
| `common/conf/domain_config.py`, `core/project/domain_config_operations.py`, `domain_config_store.py` | Allowed-topic configuration and activation; inspect storage assumptions |
| `core/knowledge/entity/resolver.py`, `candidates.py`, `profile.py` | Identity decisions and classification contracts |
| `core/knowledge/entity/reclassification.py` | Classification change rules |
| `core/ingestion/text_processor.py`, `batch.py`, `common/schema/ingestion/contracts.py` | Candidates, gaps, origins, and decision state |
| `core/ingestion/context_entity_build.py`, `project_semantic_processor.py` | Assembly, conflicts, two-pass coordination, and commit sequencing |
| `core/knowledge/db/writers/semantic_commit_writer.py` and relevant readers/schema | Durable provenance if required; preserve atomic storage contracts |

`vp01.py` needs changes only if preserving GLiNER confidence for analysis becomes
necessary. Relationship extraction remains generative; inspect its diagnostic
contract without replacing its implementation. No migration of a populated user
knowledge base is needed, but development schema changes still require validation.

## 6. Verification and completion

Use fake providers for contracts and opt-in live JEV runs for actual model quality.
Human-reviewed examples are ground truth; baseline LLM disagreement is not enough.

Required cases include invalid/invisible handles, incompatible types, foreign
classification neutrality, uncertain results, malformed responses, multiple same-name
occurrences, supporting-only/cross-block spans, residual gaps, conflicting proposals,
all JEV/LLM mode combinations, second-pass rebuild, cancellation, and restart.

Existing test anchors:

- `server/tests/unit/core/knowledge/test_entity_resolution_semantic_smoke.py`
- `server/tests/unit/core/knowledge/test_entity_manager_candidates_contract.py`
- `server/tests/unit/core/knowledge/test_entity_manager_cache_contract.py`
- `server/tests/unit/core/knowledge/test_entity_identity_models.py`
- `server/tests/unit/core/ingestion/test_context_entity_build.py`
- `server/tests/unit/core/ingestion/test_vp01_benchmark.py`
- `server/tests/contract/storage/test_semantic_commit_contract.py`
- `server/tests/integration/ingestion/test_project_semantic_postgres_flow.py`

Add provider, domain configuration, topic selection, and provenance tests alongside
their actual owners. Verify storage/reclassification contracts affected by the changes.

Identity completion requires improved reuse on held-out uncertain cases without
unacceptable wrong reuse. Extraction completion requires measurable generative work
savings without unacceptable entity or relationship recall loss. Topic completion
requires valid, consistent project classifications and correct provenance.

Record numerical acceptance gates before active rollout, including wrong-reuse rate,
entity recall/precision, topic accuracy, conflicts, abstentions, p50/p95 latency,
lock occupancy, and total spend including JEV. Limits and thresholds are intentionally
not invented in this plan; choose them from reviewed evaluation and provider limits
verified during implementation. Disabled/unavailable behavior must remain usable.
