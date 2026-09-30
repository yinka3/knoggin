# JEV entity identity and extraction implementation plan

Date: 2026-09-28
Status: Phase A foundation implemented; consumer pilots have not started.

This plan captures the subsequent discussion and extends the original
[identity journal](KNOGGIN_JEV_ENTITY_IDENTITY_JOURNAL.md) and
[extraction journal](KNOGGIN_JEV_EXTRACTION_JOURNAL.md).
Use the [shared implementation journal](KNOGGIN_JEV_IMPLEMENTATION_JOURNAL.md)
for provider ownership, spending, and lifecycle requirements. Where the older
journals differ on the initial question shape or topic selection, use this plan.

## 1. Agreed behavior

- Start identity and extraction in observe mode: call JEV and record judgments,
  but retain baseline decisions and fallback calls. Observe costs time and money.
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

- [ ] A1: Capture human-reviewed identity, extraction, and classification examples
  and baseline outputs. Include same names, aliases, foreign classifications,
  multiple missed entities, and second-pass endpoint recovery.
- [x] A2: Add a small async provider adapter with typed Choice/Noul requests,
  response validation, timeout/retry bounds, cancellation, and fake-backed tests.
- [x] A3: Add independent capability modes, model/question versions, work limits,
  and separate acceptance policies. Use one shared external-model spending budget.
- [x] A4: Wire application-owned client lifecycle and dependency injection.
- [x] A5: Extend frozen ingestion policy and snapshot validation. Credentials stay
  runtime-only. Committed windows resume without new judgments.
- [ ] A6: Implement decision records and their retention/commit ownership.

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

- [ ] I1: Separate visibility/scope/type eligibility from deterministic acceptance
  heuristics; verify disabled behavior remains unchanged.
- [ ] I2: Build bounded candidate handles from the durable candidate snapshot.
  Keep per-occurrence evidence even when searches share a normalized name.
- [ ] I3: Run Choice/Noul for eligible uncertain cases in observe mode. Record the
  baseline result and hypothetical semantic result without changing resolution.
- [ ] I4: Measure candidate recall separately from judgment quality. Optionally
  sample deterministic matches to estimate false reuse.
- [ ] I5: Establish held-out acceptance criteria before adding active acceptance.
  Do not compare JEV probabilities with the existing score capped at 1.5.
- [ ] I6: Add active decisions behind mode configuration. Preserve hard boundaries,
  pending reuse/new-ID fallback, existing classifications, and postcommit publication.

### Phase C — Bounded extraction observe pilot

- [ ] E1: Refactor gap detection into per-block reasons and literal candidate
  preparation without altering baseline extraction.
- [ ] E2: Discover candidates from unknown endpoints and known alias gaps.
  Do not silently lower GLiNER thresholds or add broad candidate discovery.
- [ ] E3: Build type Choice/Noul requests for candidates without a usable type.
  Observe results while running existing fallback unchanged.
- [ ] E4: Add explicit `jev_fallback` origin and validated offset/support semantics.
  Supporting-only occurrences may retain nullable Context offsets where validated.
- [ ] E5: Define and evaluate positive-recovery coverage rules. Recovering Delta
  must not be treated as proof that PostgreSQL and Maya in the same block were found.
- [ ] E6: Add active positive recovery and recompute residual gaps. Unresolved
  blocks still reach generative fallback when permitted; negative/uncertain JEV
  answers initially do not suppress fallback.
- [ ] E7: Preserve independent `llm_ner_mode` semantics and the existing bounded
  relationship/entity second pass. Report call avoidance separately from token savings.

### Phase D — Project classification and topic selection

- [ ] C1: Extend and validate domain configuration with allowed topics per type
  and a default; update compilation, serialization, and activation/reclassification
  assumptions. Existing configurations retain their fixed mapping.
- [ ] C2: Implement topic Choice/Noul in observe mode when alternatives exist.
  Reuse known valid types; select a topic for first-entry project classification.
- [ ] C3: Aggregate proposals after identity resolution, preserve existing project
  classification, and handle new-entity conflicts deterministically with diagnostics.
- [ ] C4: Commit accepted classification provenance atomically. Expose bounded
  decision/evidence records for later review without creating an adjudication loop.
- [ ] C5: Evaluate topic accuracy and conflict behavior before enabling active topic
  selection independently of identity and bounded extraction.

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
