# Knowledge and ingestion simplification plan

Status: Work groups 1–5 implemented. Completion records and validation are below.

Reviewed against completed Stage 6 on 2026-09-14: HEAD `4b769b7`, including
the current uncommitted Knowledge/ingestion changes. Episode simplification
was subsequently completed in work group 4. Stage 6 introduces no reason to retain cross-window episode
consolidation; it already renders separate episodes and qualified chronology.

## Agreed episode behavior

Accumulate complete exchanges until the semantic message window closes at its
configured token threshold, five-minute idle flush, or explicit flush. The
threshold belongs to the message window, not an individual episode.

Use one normal episode-generation call to organize that window's frozen messages
into coherent episodes. Combine related developments within that window; create
multiple episodes when the evidence covers distinct topics. Preserve the existing
ability to skip material that does not warrant an episode.

Persist the result through the existing durable checkpoint. Do not load older
episode candidates or make a second cross-window consolidation call. Do not
rewrite previously finalized narratives during ordinary ingestion. Current
Context continues to represent evolving understanding; episodes preserve history.

Keep validation and bounded repair for invalid or oversized model output. “One
pass” describes the successful normal path, not removal of failure recovery.
Keep the existing Episode → Context → Knowledge → completed sequence; this change
does not introduce overlapping active windows or a continuously updated draft.

## Episode implementation scope

| Files | Planned change |
| --- | --- |
| `src/core/knowledge/episodes/generator.py` | Generate from the frozen window only; remove prior-episode loading and consolidation regeneration. |
| `src/core/knowledge/episodes/build.py` | Remove consolidation targets, combined historical evidence, and fallback branches; retain window reference validation and narrative limits. |
| `src/core/knowledge/episodes/prompts.py`, `src/common/schema/episode/generation.py` | Support create/skip decisions only; remove consolidation response models and prompts. |
| `src/core/knowledge/episodes/policy.py`, `src/common/schema/settings.py` | Remove settings and snapshot fields used exclusively by consolidation. |
| `src/core/knowledge/db/writers/episode_writer.py` | Remove consolidation-specific write paths; require source membership in the claimed window and preserve atomic retry behavior. |
| `src/core/knowledge/store.py`, `src/core/knowledge/db/readers/episode_reader.py` | Remove prior-candidate APIs only after verifying they have no remaining consumers. Preserve agent retrieval. |
| Episode unit, writer, and semantic-window integration tests | Replace retired consolidation contracts with the validation gates below. |

Validation gates:

- One successful generation call per eligible closed window, with no historical
  candidate lookup or consolidation call.
- Related developments within a window can form one episode; distinct topics
  can form multiple episodes; skip-only output remains supported.
- References cannot include messages outside the claimed window.
- A later window does not rewrite an earlier episode's narrative or membership.
- Retry after a durable Episode commit does not regenerate or duplicate results.
- Narrative validation/repair, embeddings, chronology, source links, and later
  entity/relationship enrichment remain correct.
- Threshold, idle-flush, and explicit-flush behavior remain intact.

## Other reviewed opportunities

These remain proposals; review and scope each before implementation.

1. Conflict discovery: select contiguous observation-ID prefixes so date filtering
   cannot skip evidence; budget provenance with observations and trim optional
   neighborhood context before advancing the cursor.
2. Entity candidates: use an authoritative batch snapshot instead of filling an
   additive cache and repeatedly querying/revalidating each name. Obsolete aliases
   must disappear; visibility and ambiguity checks must remain correct.
3. Admission: replace full selection in readiness checks with a cheap check; keep
   authoritative selection at claim time and exact final token accounting.
4. Unused automation: remove future-classifier conflict-resolution entry points
   and trusted-action configuration if no production caller is planned.
5. Advisory actions: consider merging accept/merge only after confirming callers
   do not require a meaningful distinction.

Suggested order: conflict packet/cursor correctness → entity snapshots → admission
cleanup → episode consolidation removal → unused automation/advisory cleanup.

## Work group 1 — Conflict discovery correctness and packet simplification

Status: complete (2026-09-14). This first implementation group is independent
of episode generation and entity-cache changes.

### Required behavior

1. Read seeds in increasing observation-ID order. Apply the configured date span
   as a stopping boundary: stop at the first out-of-span row, rather than filtering
   it out and accepting later IDs. The next run starts with that unreviewed row
   as its new date anchor. Contiguous means a prefix of eligible query results;
   database sequence gaps are normal.
2. Construct a bounded candidate pool from that seed prefix and direct endpoint
   neighbors. Keep seed membership separate from optional neighborhood membership.
   A newer neighbor shown as context never authorizes a cursor jump.
3. Load bounded provenance through the existing batch evidence API. Respect its
   maximum observation count as well as the final prompt token ceiling; chunk
   bounded loads if necessary and reuse loaded bundles within this build.
4. Render and measure the complete model input, including header, observation
   records, and provenance. Prefer complete individual records over the current
   separate edge-collapse representation. Remove compaction-only fields and
   metadata if no remaining consumer needs them; do not retain a compatibility
   branch for this unreleased format.
5. When over budget, remove optional neighbors in deterministic order before
   removing seeds from the end of the prefix. Retain all required bounded
   provenance for every included observation, including truncation/status labels.
   Match the package's allowed evidence IDs exactly to observations actually
   represented in the model input.
6. If even the first seed with its required bounded provenance cannot fit, fail
   explicitly with its ID and size information. Leave the cursor unchanged.
   Do not silently drop evidence, exceed the budget, or invent a continuation
   subsystem. Normal oversized multi-record packets must shrink successfully;
   an irreducible record is a configuration/data limit, not a successful review.
7. Preserve existing atomic review writes and cursor advancement in
   `ProjectMaintenanceService.complete_conflict_discovery()`. This transaction
   already exists: verify it rather than replacing it. Model, provenance, or
   persistence failures must not advance the cursor. A successful no-candidate
   response may advance across the reviewed seed prefix.

### File boundaries

| File | Work |
| --- | --- |
| `src/core/knowledge/db/readers/conflict_discovery_reader.py` | Return a contiguous seed prefix; preserve project scope and bounded neighborhood reads. |
| `src/core/knowledge/conflict/conflict_discovery.py` | Own complete-packet budgeting, deterministic trimming, seed/context distinction, and build-local provenance reuse. |
| `src/core/knowledge/conflict/conflicts.py` | Adjust packet metadata only where the simpler representation requires it. |
| `src/core/knowledge/jobs/conflict_discovery_job.py` | Keep model invocation and validation aligned with the packet's actual allowed IDs; preserve explicit failure behavior. |
| `src/core/project/maintenance_service.py` | Adapt evidence-loader batching or retired packet metadata only as needed; preserve atomic completion and evidence-state checks. |
| Existing conflict discovery unit/persistence tests and storage contracts | Add behavioral regressions below; retire compaction-specific expectations only when that contract is removed. |

Shared EvidenceService, its readers, and Agent tools are contract dependencies,
not rewrite targets. No new scheduler, retries, leases, trust modes, or schema
tables are planned. Keep automated-resolution deletion and advisory cleanup in
separate work groups.

### Implementation sequence and validation gates

1. Reproduce ID/date mismatch in the reader test, fix prefix selection, and
   verify repeated reads with cursor advancement eventually visit every eligible
   seed. Cover exact span boundaries and nonconsecutive IDs.
2. Replace packet compaction/budgeting with complete-input measurement and
   deterministic trimming. Cover provenance pushing an otherwise fitting packet
   over budget, oversized neighborhoods, observation-count limits, a single
   irreducible seed, and deterministic output for identical inputs.
3. Verify packet IDs and model validation agree. A removed neighbor cannot be
   cited; retained neighbors do not move the seed checkpoint. No-candidate
   responses and provider failures have distinct completion behavior.
4. Exercise real PostgreSQL completion rollback by failing a review write and
   by failing cursor advancement. Both review state and cursor must roll back;
   retry must not duplicate equivalent reviews. Preserve scoped visibility.
5. Run focused conflict discovery/maintenance tests plus Stage 6 evidence-service
   and graph-retrieval contracts. Run touched-path Ruff, compile checks,
   architecture checks if imports change, and `git diff --check`.

Record measured query/model-call counts for the test fixtures, without claiming
production latency improvements. Update this plan with exact results and any
remaining limits before declaring the group complete.

### Completion record — 2026-09-14

- Seed selection now stops at the first observation outside the date span. The
  next cursor run anchors on that ID, so an out-of-order timestamp cannot skip
  it or a later ID. Exact span boundaries and nonconsecutive IDs are covered.
- Packet selection keeps a seed prefix separate from optional direct-neighbor
  context. It measures records and bounded provenance together, drops the
  lowest-ID optional neighbor first, and then stops before the next seed if it
  cannot fit. An irreducible first seed raises an ID- and size-specific error.
- Provenance is required for packet construction, loaded in chunks of the
  existing `EvidenceTraversalLimits.max_observations` limit, and cached for all
  render attempts in one build. The packet and model validation both use only
  the represented observation IDs.
- Removed the edge-collapse packet format and `packet_compacted` review
  metadata. The existing atomic completion transaction remains unchanged.
- Fixture measurements: the deterministic-trim fixture performs one seed read,
  one neighborhood read, and one three-ID provenance batch across two complete
  renders. The six-record fixture performs one seed read, one neighborhood
  read, and three two-ID provenance batches. Packet construction makes no model
  request; the job invokes its structured provider only after packet building.
- Validation passed: 22 focused conflict reader/packet/persistence/storage
  tests; 17 adjacent conflict and maintenance tests; and 19 Stage 6
  evidence-service, graph-retrieval, and episode-retrieval tests. Touched-path
  Ruff, compile, and architecture-import checks also passed.
- Remaining intentional limits: direct-neighborhood reads retain their existing
  128-row bound, and a complete packet is still limited by the configured model
  token ceiling. No continuation, retry, lease, or scheduler behavior was added.

### Post-completion audit — 2026-09-14

- Re-read the reader, packet builder, job, maintenance caller, and review writer
  against the required behavior. The sole production packet-construction path
  supplies scoped provenance; packet IDs remain the job's candidate boundary;
  and the existing transaction remains the only cursor-advance path.
- No follow-up change was required. The current focused conflict reader, packet,
  persistence, storage, writer, and review suite passed 29 tests, with Ruff,
  compile, architecture-import, and diff checks clean.

## Work group 2 — Authoritative entity candidates per resolution batch

Status: complete 2026-09-14. Candidate resolution now owns one private,
authoritative catalog snapshot per nonempty batch. Identity decision rules remain
unchanged.

### Required behavior

1. Load the active, visible entity catalog once per nonempty resolution batch.
   Include canonical names, all aliases, and classifications needed by matching.
   Read a coherent database snapshot: use one scoped query, or one read transaction
   with an appropriate snapshot if multiple queries are required. “One load” does
   not mean an ID query followed by per-entity hydration.
2. Build a private, non-evicting candidate snapshot with profiles by ID and
   normalized names mapped to every owner. Preserve local-project classification
   preference, foreign visibility, and the reserved identity's existing policy.
   Do not populate the shared runtime cache as a side effect of candidate loading.
3. Search that snapshot once per distinct normalized mention. Preserve exact,
   alias, and fuzzy scores, cutoffs, short-name scorer, ambiguity handling, and
   deterministic ordering. Keep all owners of shared aliases. Preserve the rule
   that a unique exact owner is not made ambiguous by another fuzzy shared alias.
4. Use the same snapshot for candidate acceptance: profiles, aliases, acronym
   checks, and contextual/name evidence must not fall back to shared cached data.
   Keep type/topic compatibility, contextual acceptance, and pending same-name
   disambiguation unchanged. Deduplicate searches, not typed mention decisions.
5. Keep pending identities and alias writes private until durable commit. Refresh
   runtime indexes through the existing committed-publication boundary and retain
   alias-version invalidation for TextProcessor's phrase matcher.
6. Keep standalone candidate lookup working by creating one snapshot for that
   call and using the same matching function. Remove `catalog_hydrated` and
   per-name durable exact/fuzzy revalidation plumbing once no callers need them.
   Storage failure during ingestion must propagate, never become an empty catalog
   that causes duplicate identity creation. Review standalone failure contracts
   explicitly; do not preserve a stale-cache fallback merely for compatibility.
7. Snapshot consistency does not authorize stale canonical writes. Verify existing
   maintenance exclusion and commit-time identity/scope/classification validation
   before removing revalidation reads. If an identity changes between snapshot
   and commit, reject/retry safely through existing machinery. Add only the
   specific missing check if a reproducible race demonstrates one.

### File boundaries

| File | Work |
| --- | --- |
| `src/core/knowledge/entity/resolver.py` | Load once, perform pure snapshot searches, and use snapshot profiles/aliases throughout acceptance. |
| `src/core/knowledge/entity/index.py` or a small `entity/candidates.py` module | Define the minimal batch snapshot and matching helpers; keep runtime cache ownership explicit. Choose one home during implementation, not parallel abstractions. |
| `src/core/knowledge/db/readers/entity_reader.py`, `src/core/knowledge/store.py` | Supply one coherent scoped catalog load without vectors or per-name reads; retain APIs used by other flows. |
| `src/core/ingestion/context_entity_build.py` | Adjust ownership wiring only if required; retain extraction → resolution → result assembly. |
| `src/core/ingestion/text_processor.py` | Verify committed alias invalidation; do not rewrite extraction or add a second catalog load. |
| Entity candidate/cache/alias tests, Context entity-build tests, scoped storage tests | Cover snapshot parity, stale aliases, failures, and publication boundaries. |

Keep Agent briefing, entity-maintenance product behavior, matching thresholds,
embeddings, and semantic-window admission outside this group. Do not replace the
runtime cache wholesale, add catalog versioning, or introduce cross-batch caching.
The snapshot covers candidate resolution; phrase-matcher extraction coverage is a
separate concern and must not be claimed solved by this change.

### Implementation sequence and validation gates

1. Lock matching behavior with fixtures covering unique exact names, aliases,
   ambiguous owners, fuzzy/short names, cross-project classifications, and typed
   same-name mentions. Record baseline query counts and expected decisions.
2. Implement coherent catalog loading and the private snapshot. Verify inactive,
   redirected, retired, and invisible identities are excluded according to the
   existing policy. Test authoritative empty catalogs and database failures.
3. Route search and acceptance through the snapshot. With identical durable data,
   cold, warm, and deliberately stale runtime caches must yield identical results.
   Removing or transferring an alias between batches must affect the next batch;
   the old alias must not remain as fuzzy evidence for its former owner.
4. Verify multiple mentions reuse one load, distinct typed mentions still receive
   separate decisions, and pending writes do not leak into runtime indexes.
   Standalone lookup and batch lookup must agree for the same snapshot/settings.
5. Verify committed publication refreshes affected runtime aliases and invalidates
   the phrase matcher only when appropriate. Test mutation between snapshot and
   commit against real PostgreSQL, preserving existing rejection/retry guarantees.
6. Run entity candidate/cache/alias and Context entity-build tests, relevant
   semantic commit and scoped reader contracts, plus Stage 6 graph/episode
   retrieval regressions. Run touched-path lint, compile/import checks, and
   `git diff --check`.

Acceptance: catalog query count stays independent of distinct mention count;
matching/acceptance makes no per-name catalog queries; durable commit validation
remains intact. Record fixture query counts and representative snapshot size/time
without treating them as a production performance benchmark. Document intentional
stale-cache corrections separately from unchanged matching-policy behavior.

### Completion record — 2026-09-14

- `EntityReader.get_visible_entities_for_resolution` now returns active scoped
  identities, aliases, and visible project classifications in one SQL statement.
  It no longer reads IDs and then hydrates each entity through follow-up queries.
- Added a batch-local, non-evicting `EntityCandidateSnapshot`. It selects the
  target project's classification when present, otherwise retains the first
  visible foreign classification, and maps every normalized canonical name and
  alias to all active visible owners.
- Context resolution loads one snapshot for a nonempty batch and uses it for
  exact, alias, fuzzy, ambiguity, acronym, profile, and alias-admission checks.
  Candidate loading no longer mutates the runtime `EntityIndex`, its alias
  version, or TextProcessor's phrase-matcher input.
- Standalone candidate lookup uses the same snapshot matcher. Removed the
  `catalog_hydrated` path and per-name exact/fuzzy revalidation reads. Storage
  errors propagate instead of treating a failed catalog load as an empty catalog.
- The existing committed-publication path still refreshes the runtime index and
  increments the alias version only when durable aliases change. The semantic
  commit writer already locks the current read scope and verifies active,
  readable identities and local classifications. Its real PostgreSQL merge race
  regression rejects a stale commit without partial state, so no new race path
  was needed.
- Fixture measurements: each nonempty batch made one catalog call regardless of
  whether it contained repeated typed mentions or two distinct names; it made no
  per-name or profile lookup while resolving candidates. The scoped reader mock
  and real PostgreSQL contract each exercised one catalog statement. No latency
  claim was made.
- Validation passed: focused entity candidate/cache/alias, Context entity-build,
  and scoped reader contracts; a real PostgreSQL catalog test; the real semantic
  commit classification/scope/merge-race cases; and Stage 6 episode, graph, and
  evidence retrieval regressions. Touched-path Ruff, compile, architecture, and
  diff checks passed.
- Intentional boundary: this snapshot covers candidate resolution only. The
  runtime cache still serves committed phrase matching and ordinary lookup flows;
  it is not a cross-batch candidate cache.

### Post-completion audit — 2026-09-14

- The current implementation still has one scoped durable catalog query per
  nonempty candidate-resolution batch, and no candidate-path cache hydration or
  per-name catalog lookup. Committed publication remains the only path that
  refreshes the runtime alias index.
- Re-ran the focused candidate/cache/alias, Context build, and scoped reader
  suite (76 passed), then the real semantic-commit classification/scope/race
  cases and Stage 6 retrieval/evidence plus real ingestion flow regression (23
  passed). No behavioral correction was required; removed one duplicated test
  marker found during the audit.

## Work group 3 — Semantic-window admission simplification

Status: complete — 2026-09-14. Follow entity candidate ownership with this
independent cleanup of readiness, selection, and claiming.

### Required behavior

1. Readiness must not assemble/tokenize a complete admission proposal or allocate
   a window ID. Let claim-time selection own message membership, exact token
   accounting, and the policy snapshot used by the resulting durable window.
2. Keep due-stage handling for active windows, including retry backoff and
   exhaustion. Review the existing scheduler contract first: it runs a job when
   either `should_run()` or cadence is due. Prefer using the existing semantic
   job cadence for new-window checks over adding a second readiness subsystem.
   If that changes threshold-trigger latency, document the bounded delay and
   decide explicitly whether a lightweight eligibility query is needed. Do not
   claim identical scheduling behavior without testing it.
3. Perform authoritative selection once per claim attempt. Freeze the actual
   ingestion/domain and episode policy for that attempt; preserve the writer's
   project lock, active-window exclusion, source validation, and atomic membership
   insert. A stale proposal must be rejected/retried through existing machinery.
   Never hold a database write lock while doing model work or expensive tokenizing.
4. Preserve the first whole exchange that crosses the target, per-session FIFO
   barriers, cross-session ordering, five-minute idle flush, explicit flush,
   oversized-exchange behavior, and exact final token count/overfill metadata.
   Do not increase the threshold or introduce another configurable size cap.
5. Remove obvious duplicate computation first: reuse the last exact prefix count
   as the selected window's final count, and avoid repeatedly rendering unchanged
   text where practical. Token counts of separate exchanges are not necessarily
   additive across separators. Do not substitute character estimates, summed
   fragment counts, or binary-search assumptions for exact boundary behavior.
   More aggressive tokenization changes require tokenizer-level parity evidence;
   retaining exact prefix counting is acceptable after duplicate selection is gone.
6. No persistent token cache, scheduler rewrite, extra claim owner, or cross-turn
   proposal cache. Preserve the existing Episode → Context → Knowledge → completed
   sequence and Context-file synchronization, including human-edit windows.

### File boundaries

| File | Work |
| --- | --- |
| `src/core/ingestion/semantic_window_admission.py` | Keep one authoritative selection path; reuse exact counts and preserve eligibility/boundary semantics. |
| `src/core/ingestion/project_semantic_job.py` | Remove full selection from readiness; retain active-stage due checks and claim-time policy capture. |
| `src/core/knowledge/db/readers/semantic_window_reader.py`, `src/core/knowledge/store.py` | Add a minimal eligibility read only if existing cadence is insufficient; do not duplicate selection rules in SQL and Python. |
| `src/core/knowledge/db/writers/semantic_window_writer.py` | Verify existing claim validation and transaction boundaries; change only a demonstrated gap. |
| `src/infrastructure/job/scheduler.py` | Contract dependency to inspect/test, not a planned rewrite. |
| Admission, semantic-job, scheduler, and PostgreSQL claim tests | Prove scheduling, membership, frozen policy, and race behavior. |

### Implementation sequence and validation gates

1. Record current readiness/execute queries, policy captures, and token-counter
   calls for no-work, below-threshold, threshold-crossing, idle, and active-retry
   cases. Include scheduler cadence and first-check behavior.
2. Remove duplicate selection from readiness. Test that cadence does not cause
   retries before backoff or restart exhausted work, and that ready new windows
   are discovered within the documented latency bound. Preserve synchronization
   of human Context edits and pending file projection repairs.
3. Simplify selection bookkeeping. Compare old/new membership, close reason,
   source count, and overfill on fixtures with multiple sessions, blocked heads,
   claimed exchanges, exact thresholds, oversized exchanges, Unicode, whitespace,
   and empty message contents. Keep exact final validation with the configured
   tokenizer; do not rely solely on additive fake token counters.
4. Verify force flush and settings changes: readiness does not freeze stale
   policy, claim records its actual policy, and existing windows retain theirs.
5. Run real PostgreSQL races for competing claims and source/session changes
   between selection and claim. Assert at most one active window, no duplicate
   membership, no skipped eligible exchanges, and safe rejection/retry.
6. Run focused admission/job/scheduler tests and PostgreSQL semantic-window
   contracts, then touched-path Ruff, compile/import checks, and `git diff --check`.

Acceptance: readiness performs no full selection; one claim attempt performs one
selection; final exact count is reused rather than recomputed. Record any remaining
prefix-tokenization cost honestly and leave further optimization to measurements.
Episode consolidation removal remains the next separate work group.

### Completion record — 2026-09-14

- `ProjectSemanticJob.should_run` now checks only an active window's due stages.
  With no active window it returns false without capturing policy, building a
  proposal, allocating a window ID, reading unclaimed exchanges, or tokenizing.
  The existing scheduler runs the semantic job on its first check and then at
  its 30-second cadence; a newly eligible window therefore waits at most one
  cadence interval after the prior run, plus any currently running job.
- `execute` remains the owner of a new-window claim. It captures the policy for
  that attempt and passes the same policy/domain to `claim_next`, whose single
  selection freezes membership and policy before the existing writer acquires
  its project lock and revalidates durable membership. Context-file sync keeps
  its independent current-policy capture because it is a separate projection
  operation.
- Selection still evaluates exact rendered prefixes. It now appends the next
  whole exchange, records that exact prefix count, and reuses the final count
  for durable window metadata. It does not use additive per-exchange estimates,
  a persistent token cache, or a second full selection path.
- No reader, schema, writer, or scheduler rewrite was needed. Existing real
  PostgreSQL claim-race and stale participation/source validation contracts
  remain the consistency boundary; active-window backoff and exhaustion still
  prevent cadence executions from rerunning a stage or claiming a replacement.
- Validation passed: focused admission, semantic job, scheduler, participation,
  and integration tests; real PostgreSQL competing-claim and stale-membership
  contracts; Stage 6 agent/retrieval/evidence regressions; touched-path Ruff,
  compile, architecture, and whitespace checks. Full-file format checks still
  report pre-existing formatting drift outside this work group's hunks.

## Work group 4 — Finalize episodes from their own semantic window

Status: complete — 2026-09-15. The implementation removes cross-window episode
consolidation and makes finalization a window-local, create-only operation.

### Intended behavior

One normal generation call organizes a claimed conversation window into zero,
one, or multiple episodes. Related developments may combine within that window.
Finalized historical narratives and their source membership remain immutable
during later ingestion. Context continues to represent current understanding.
Keep bounded narrative repair, durable zero-result checkpoints, disabled-generation
behavior, human-edit windows, and the existing stage sequence.

### Additional findings from the planning review

- The generation instructions also live in
  `src/common/templates/prompts/episode.md`; updating only the Python prompt
  helpers would leave the model instructed to consolidate.
- `KnowledgeRetrieval` calls `get_project_episode_source_messages`. Keep this
  reader/store API for retrieval even after the generator stops using it.
  `get_nearby_project_episodes` is the candidate API to remove after checking all
  callers.
- The semantic writer currently permits historical source IDs when metadata says
  `decision_action=consolidate`. Its internal write path also reads existing
  episodes, clears links, and uses an upsert. Removing the metadata exception
  alone cannot establish that later windows never rewrite an existing episode.
- `ProjectEpisodeBuild.create_episodes` currently skips proposals exceeding
  source-message or source-token limits. These limits are separate from the
  semantic-window threshold and must remain effective, but an oversized create
  proposal must produce an explicit validation failure/retry rather than silently
  becoming a successful empty result. Include source constraints in the generation
  instructions; do not add another automatic model pass for source splitting.
- Once historical reads disappear, review whether the generator's store protocol
  and constructor dependency can disappear too. Update runtime construction and
  fakes together; keep any helper only if its remaining callers justify it.

### File boundaries

| Files | Planned work |
| --- | --- |
| `src/core/knowledge/episodes/generator.py`, `build.py` | Remove prior candidates, target references, historical evidence assembly, regeneration and fallback branches. Retain empty-window skip, stable source-based IDs, chronology, validation, bounded narrative repair and batched embeddings. |
| `src/common/schema/episode/generation.py`, `src/core/knowledge/episodes/prompts.py`, `src/common/templates/prompts/episode.md` | Remove consolidate response/action/target contracts and instructions; keep local message handles and all narrative fields. Explain per-episode source limits. |
| `src/core/knowledge/episodes/policy.py`, `src/common/schema/settings.py` | Remove `prior_episode_candidate_count` from settings, capture, metadata and frozen snapshot validation. Update current fixtures/config consumers without legacy compatibility branches. |
| `src/core/knowledge/db/writers/episode_writer.py` | Require every source to belong to the claimed window regardless of metadata. Make semantic episode creation reject pre-existing episode identities instead of replacing their narratives/links; retain atomic checkpoint and same-window replay handling. Audit shared write helpers before changing other consumers. |
| `src/core/knowledge/db/readers/episode_reader.py`, `src/core/knowledge/store.py` | Remove unused nearby-candidate reads; retain source reads used by retrieval. |
| `src/runtime/project_factory.py` and relevant test fixtures | Remove generator dependencies made unnecessary by the changed flow. Search the repository for retired settings, schema exports, prompt references and constructor arguments. |
| Episode tests, storage contracts and semantic ingestion integration tests | Replace retired behavior with current window ownership, failure/retry, source bounds and producer-to-retrieval coverage. |

### Implementation sequence and validation gates

1. Establish the baseline from episode build/decision/embedding tests, writer and
   scope contracts, and the real semantic ingestion flow. Identify shared writer
   consumers before removing helpers. Record failures as source defects or retired
   contracts rather than changing expectations just to pass.
2. Implement create-only proposals, with empty output as the skip result, with
   matching schema and prompt changes as one slice. Verify zero/one/multiple
   episodes, one normal generation call, no
   historical reads, correct local references, and one embedding batch for output.
   Preserve bounded narrative repair; provider, invalid-reference and source-limit
   failures must not checkpoint success. Test exact source limits and over-limit
   proposals, distinct topics, Unicode and empty/skip output.
3. Tighten semantic persistence. Real PostgreSQL tests must reject foreign-window
   sources even with forged consolidation metadata and reject attempts to reuse
   an earlier episode ID with otherwise valid current sources. Verify rollback
   leaves the old narrative, embedding, chronology, membership and links intact.
   Verify duplicate/restarted execution of the same recorded window returns its
   durable result without generation or duplicate episode membership.
4. Remove dead candidate APIs, constructor dependencies and settings. Update
   frozen-policy fixtures and references together; existing source-based IDs,
   episode embeddings/fingerprints and retrieval result contracts remain covered.
   A development reset is available if removed snapshot fields require it;
   do not add migration compatibility solely for unreleased snapshots.
5. Add a real two-window producer integration case: first a decision, then its
   reversal. Persist distinct episodes, retrieve both, and pass them through
   notebook admission and synthesis rendering alongside current Context. Assert
   chronology, source handles and unresolved qualifications survive; scripted
   output establishes data flow, not real-model reasoning quality.
6. Run the episode subsystem and PostgreSQL contracts, semantic job/recovery
   tests, and the Stage 6 regression gates below. Run touched-path lint, compile,
   architecture and diff checks; inspect the final diff and record results here.

Acceptance: ordinary generation uses only its frozen window; prior episodes
cannot be rewritten by this path; invalid output cannot become a silent successful
skip; durable retries and retrieval provenance remain intact. Cross-window
consolidation code and exclusive configuration have no remaining consumers.
Unused conflict automation/advisory cleanup remains a later work group.

### Completion record — 2026-09-15

- A normal generation call now sees only a frozen semantic window and emits new
  episodes from local message references. An empty proposal list records the
  intentional skip; bounded narrative repair remains the only exceptional
  follow-up call.
- Removed historical candidate reads, consolidation schema/prompt branches,
  regeneration logic, `prior_episode_candidate_count`, and the generator's
  store dependency. The retrieval source-reader remains because Stage 6 uses it
  to render episode provenance.
- Semantic persistence now requires every source `(message_id, session_id)` to
  belong to the claimed window and rejects a reused episode identity. It inserts
  final episodes and memberships without clearing or upserting historical data;
  the existing durable same-window replay path remains intact.
- Added source-limit failure coverage, one-call/one-embedding-batch coverage,
  forged historical-metadata and reused-ID PostgreSQL contracts, and a two-window
  decision/reversal producer-to-retrieval-to-notebook/synthesis integration test.
- Validation passed: 101 focused episode/ingestion/storage/retrieval tests, 7
  real semantic-ingestion integration tests, 286 Stage 6 agent/context/evidence
  tests, 33 additional Stage 6 storage/context/reference tests, prompt-library
  validation, touched-path Ruff, compile, architecture, and whitespace checks.
  The Stage 6 plan needs no change: its source handles, chronology, notebook,
  Context, and synthesis contracts remain unchanged and passed their regressions.
- Re-checked immediately before Work group 5: the complete Work group 4
  acceptance suite passed again (101 tests).

## Work group 5 — Remove unused automation and simplify advisory decisions

Status: complete — 2026-09-15. The implementation removes unused automatic
authority and makes conflict discovery and advisory decisions explicit.

### Intended behavior and review findings

Scheduled conflict discovery produces evidence-backed proposals. Users explicitly
classify conflicts and decide relationship advisories. Remove the unused future
classifier entry point and its exclusive authority configuration.

The current `automatically_resolve_conflict_group` method has no production caller
and explicitly describes a future classifier. However, `MaintenanceTrustPolicy`
also controls the live discovery job's scheduler eligibility and health output.
Remove the unused authority rules without accidentally disabling discovery or
dropping its settings-update behavior.

Advisory `accept` and `merge` currently share required inputs, allowed starting
state, and resulting `accepted` disposition. They differ in persisted
`last_action`, so audit history and downstream consumers must be inspected before
removing `merge`. The intended simplification is one `accept` action if no caller
uses that distinction. If an actual product behavior depends on it, document that
behavior and retain it rather than declaring both actions redundant.

### File boundaries

| Files | Planned work |
| --- | --- |
| `src/core/project/maintenance_service.py`, `src/core/project/project_manager.py` | Remove unused automatic resolution, policy state/imports, and the now-unneeded application-level policy settings subscription. Keep explicit review/application operations and their scope/evidence checks. |
| `src/core/knowledge/maintenance/maintenance_policy.py` | Delete the automatic-resolution policy module. The discovery job owns the remaining scheduler and health behavior directly. |
| `src/common/schema/settings.py`, `src/core/knowledge/jobs/conflict_discovery_job.py` | Remove trusted actions and trusted mode. Retain manual/assisted discovery behavior and enabled control; verify hot settings updates and LLM availability still control scheduling correctly. |
| `src/core/knowledge/relationship_advisories.py`, `src/core/knowledge/maintenance/maintenance_reviews.py`, `src/core/knowledge/db/writers/relationship_advisory_writer.py` | Trace both direct advisory decisions and reviewed-plan application. If redundant, remove `merge` from validation, transitions and consumers; keep explicit decision metadata, revisions and review consistency. |
| Health/settings consumers, API/tool/UI contracts, schema/config files and fixtures located by repository search | Remove retired fields/actions wherever actually exposed, including `trusted_action_count`. Do not assume a UI consumer exists; record the real surface. Change database constraints only if they encode the retired action. |
| Policy, discovery, advisory, maintenance application and health tests | Replace retired contracts with coverage of the remaining behavior and meaningful failure boundaries. |

### Implementation sequence and validation gates

1. Trace production callers and public surfaces for automatic resolution,
   trusted mode/actions, advisory actions and health policy snapshots. Inspect
   direct writer calls and reviewed maintenance plans, including how they record
   `last_action`. Establish baseline focused tests and classify existing failures.
2. Remove automatic resolution plus exclusive settings, actor and policy helpers
   in one slice. Verify assisted discovery still schedules, manual/disabled
   discovery does not schedule, settings changes propagate, and discovery results
   remain open proposals rather than applying decisions. Preserve explicit
   conflict classification and all used evidence/status checks.
3. Consolidate advisory actions only after the caller audit supports it. Verify
   accept persists the chosen relationship type, actor, note and revision; edit,
   dismiss, suppress and reopen retain their transitions. Reject removed `merge`
   input at the owning validation boundary. Advisory acceptance must not activate
   DomainConfig or rewrite canonical relationship evidence.
4. Verify real PostgreSQL maintenance application: project scope, current evidence,
   stale revision/evidence rejection, atomic decision/review updates, replay and
   failure rollback. Keep authorization and explicit entity-merge/rollback paths;
   the advisory action named `merge` is unrelated to canonical entity merging.
5. Update settings and health fixtures, API/tool schemas, docs and persisted
   development configuration as needed. No aliases or legacy compatibility for
   retired unreleased fields. If old local rows/config prevent validation, record
   the reset/recreation requirement rather than silently translating decisions.
6. Run focused policy/discovery/advisory/maintenance/health suites, relevant real
   PostgreSQL contracts, and Work group 1 conflict packet/cursor regressions.
   Run touched-path lint, compile, architecture and diff checks. Search for stale
   production references and record the resulting supported action set here.

Acceptance: there is no unused automatic classifier authority surface; discovery
still produces reviewable proposals; advisory actions each have a distinct used
meaning; scope, evidence, audit and transaction boundaries remain enforced.
Stage 6 evidence retrieval remains covered where shared maintenance code changes.
This closes the listed simplification groups, not a claim that all server
complexity or unrelated maintenance opportunities have been reviewed.

### Completion record — 2026-09-15

- Removed `automatically_resolve_conflict_group`, `MaintenanceTrustPolicy`, its
  automated actor, `trusted` mode, `trusted_actions`, and
  `trusted_action_count`. `ConflictDiscoveryJob` now directly owns the remaining
  `manual`/`assisted`, enabled, LLM-availability, cadence, and health behavior.
  Its runtime-level hot-settings subscription remains; the obsolete
  `ProjectManager` policy subscription was removed.
- Caller inventory found no production caller for automatic resolution and no
  API/tool caller for direct advisory actions. Scheduled discovery still builds
  bounded packets and persists reviewable proposals; explicit conflict
  classification remains available through `resolve_conflict_group`.
- Removed advisory `merge`: it had the same required inputs, starting state, and
  accepted result as `accept`, differing only in a meaningless action label.
  The remaining actions have distinct transitions. Both action application and
  the persisted `RelationshipAdvisoryPlan` reject `merge`.
- Kept actor ownership in the durable maintenance-review event. Removed the
  transient `decided_by` field from the reconstructed advisory decision because
  it was never rehydrated from storage. Acceptance retains relationship type,
  note, revision, and audit actor without activating DomainConfig or rewriting
  relationship evidence.
- No live configuration or database constraint contained the retired fields.
  A development database containing an old advisory plan with `action: merge`
  must be reset rather than translated; no compatibility path was added.
- Validation passed: the group 4 acceptance re-check (101 tests), 134 focused
  conflict/discovery/advisory/maintenance/health/PostgreSQL tests, touched-path
  Ruff, compile, architecture, and whitespace checks. Stage 6's shared
  EvidenceService regression is included; its plan needs no contract update.

## Stage 6 coordination

Stage 6 is complete. Preserve its model-facing provenance, chronology
interpretation, and Context briefing contracts. This plan changes episode
production, not those responsibilities.
Preserve episode IDs, chronology, source handles, and retrieval result contracts.
Stage 6's old-decision/later-reversal scenario should consume separate finalized
episodes and current Context without relying on a merged historical narrative.

### Post-Stage-6 review additions

- Preserve notebook admission before source encounter recording. A retrieved
  historical support locator is not newly consulted evidence. Episode generation
  cleanup must retain summary, developments, updates, unresolved qualifications,
  source chronology, and stable IDs consumed by the notebook renderer and
  `read_episode`.
- Add an integration gate that generates two separate windows (decision, then
  reversal), retrieves their persisted episodes, and passes them through notebook
  admission and synthesis rendering. Stage 6's scripted reversal test establishes
  model-input behavior; it does not by itself prove the changed producer/writer
  path. Assert both histories and qualifications reach the prompt, not that a
  scripted provider proves real-model reasoning quality.
- EvidenceService now serves `read_observation_evidence` through its visible-
  project traversal as well as maintenance. Reuse its bounded batch traversal
  when budgeting conflict packets; preserve project scoping, truncation/status
  reporting, and maintenance evidence-state checks. Do not remove provenance to
  make packets fit, or replace the service with a conflict-only helper.
- Keep Stage 6's Brief, qualified Context, and document manifest cache run-local.
  Do not turn the authoritative entity-candidate batch snapshot into a shared
  Agent briefing cache. Preserve alias-version invalidation for ingestion's
  phrase matcher when changing entity index ownership.
- Separate optional-feature deletion from correctness fixes. Automated conflict
  resolution still has no production caller; scheduled proposal discovery and
  explicit maintenance decisions remain used. Removing trusted automation must
  preserve the latter and their authorization/evidence checks.
- No additional Agent architecture rewrite is justified by this review. Further
  retrieval/ranking work stays in the separate retrieval plan; Stage 6's prompt
  size measurements are not evidence that consolidation improves retrieval.

Regression gates after relevant implementation slices:

- `tests/unit/core/agent/test_executor_loop_contract.py` (accumulation, reversal,
  admission and adaptive briefing).
- `tests/unit/core/agent/test_agent_runtime_context_contract.py`,
  `test_episode_retrieval_contract.py`, and `test_graph_retrieval_contract.py`.
- `tests/unit/core/knowledge/test_evidence_service.py` and `test_context_render.py`.
- Real PostgreSQL episode writer/reader/scope and source-reference contracts,
  plus the new two-window producer-to-synthesis integration gate above.

Keep durable checkpoints, frozen policy/evidence, provenance validation, and
projection recovery. Do not add legacy compatibility for removed unreleased
contracts. Run focused tests, touched-path lint, architecture checks when imports
change, and `git diff --check` for each implementation slice.
