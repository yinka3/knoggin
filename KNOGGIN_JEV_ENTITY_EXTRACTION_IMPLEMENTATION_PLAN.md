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

- [ ] A1: Capture human-reviewed identity examples and baseline outputs. Include
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

### Phase E — Integration and active-mode readiness

- [ ] V1: Bound requests across both ingestion passes; measure resolver lock
  occupancy. Do not release its lock around HTTP without snapshot revalidation.
- [ ] V2: Cache only immutable semantic responses keyed by occurrence, evidence,
  options, domain/model/question/policy versions. Never cache private allocated IDs.
- [ ] V3: Verify cancellation, provider failure, budget exhaustion, admitted-policy
  restart, atomic commit, and publication only after commit.
- [ ] V4: Record held-out quality, latency, and total spend. Enable capabilities
  independently only when their quality gates pass.

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
