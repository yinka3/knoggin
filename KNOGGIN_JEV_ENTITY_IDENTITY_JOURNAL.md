# JEV entity identity resolution journal

Status: detailed implementation proposal; no identity behavior changed.
Baseline: working tree inspected on 2026-09-27 at HEAD `efc75a3`.
Shared foundation: [implementation journal](KNOGGIN_JEV_IMPLEMENTATION_JOURNAL.md).

## 1. Goal and example

Improve decisions about whether a typed Context mention refers to an existing global identity. This is not NER and not a merge of two already-persisted entities.

Example: a new block says "Alex Morgan will review the API changes" and two visible people share the alias "Alex". Current rules look for literal corroborating names and compatible types. JEV could judge a bounded comparison when the current evidence is ambiguous. If the input only contains "Alex" and both candidates are described identically, it must abstain; the current profile data does not magically supply employers or biographies.

An alias such as "Postgres" versus "PostgreSQL" is only actionable if candidate discovery supplies that identity. Semantic verification cannot recover an existing entity excluded by the fuzzy cutoff.

## 2. Current subsystem map

| Source | Current role | Planned effect |
| --- | --- | --- |
| `core/ingestion/context_entity_build.py`, `ContextEntityBuildService.build` | Extract mentions, resolve them, assemble pending writes and literal message refs | Retain pipeline; add identity provider dependency to resolver |
| `core/knowledge/entity/resolver.py`, `resolve_context_block_mentions` | Loads a visible durable snapshot, ranks candidates, reuses or creates IDs | Primary semantic identity decision seam |
| Same file, `_identity_decision_signals`, `should_accept_candidate`, `schema_compatibility` | Deterministic name/context scores and conservative acceptance | Separate hard boundaries from heuristics JEV may supplement |
| Same file, `_should_reuse_pending_entity` | Reuses a private pending ID only for matching surface/type/support | Keep deterministic initial behavior; benchmark future semantic extension separately |
| `core/knowledge/entity/candidates.py`, `EntityCandidateSnapshot` | Non-evicting names, profiles, and name-owner map for one build | Authoritative candidate catalog for JEV request |
| `core/knowledge/entity/profile.py` | Global identity versus project classification; lightweight EntityProfile | Use real fields; preserve foreign-project neutrality |
| `core/knowledge/db/writers/semantic_commit_writer.py` | Atomic Context-grounded entity/alias/association/relationship reconciliation | Remains sole commit boundary |
| `core/ingestion/project_semantic_processor.py` | Builds, reconciles unknown endpoints, commits, then publishes resolver state | Preserve staging and retry semantics |

## 3. Current decision behavior

The resolver holds its resolution lock, loads durable visible entities, and constructs candidate searches. Searches use exact/alias/fuzzy evidence, with a default fuzzy cutoff of 85. Searches are deduplicated by normalized surface, but each mention keeps its own support text for decisions.

Candidates get comparison scores from name similarity, unique names/aliases, compatible classification, and literal name support in Context. The comparison score is capped at 1.5: it is not a calibrated probability. Acceptance separately checks the candidate name score against `resolution_threshold`, conservative name/context policy, and a runner-up margin unless direct name evidence is present.

Failed acceptance records an abstention, then checks same-batch pending entities and may allocate a new ID. There is no durable "unresolved mention queue" here today. JEV abstention initially returns to that baseline behavior; it must not be documented as creating a queue that does not exist.

Visible foreign-project identities may be candidates, but their classification is neutral for the active project. Reuse produces the current project's own classification from the extracted mention. Do not copy a foreign project's vocabulary into the active domain.

## 4. Proposed JEV boundary

Keep clear deterministic matches cheap. Evaluate only cases with plausible candidates where baseline heuristics abstain or competition is genuinely ambiguous. Observe mode can sample deterministic acceptances to estimate false reuse without adding a call to every active match.

Request state contains the occurrence name/type/topic, its eligible Context support text, and a bounded set of visible candidate IDs represented by local handles. Candidate descriptions use canonical name, aliases, and project classification status. Do not promise relationship or narrative context that EntityCandidateSnapshot does not currently provide. Any later enrichment must be a separate bounded scoped read.

Suggested Choice outcomes are candidate handles plus `none_of_these` and `insufficient_evidence`. Candidate descriptions explicitly distinguish same identity from related entities. A companion Noul may test whether the selected pair is supported as the same identity; evaluate its usefulness rather than assuming extra questions always improve decisions.

### Which rules stay hard?

- Visibility, active identity eligibility, valid returned handle, and project scope.
- Explicit incompatible active-project type classifications.
- Valid current-project classification and no contradictory classifications within the build.
- Block/version provenance and literal canonical-message checks during assembly.
- No model-issued IDs, aliases, or writes outside the supplied candidate set.

### Which rules can JEV supplement?

Name-strength requirements, literal corroboration for ambiguous aliases, and runner-up comparison are the heuristics the experiment targets. Requiring every existing heuristic to pass after JEV would prevent it from improving the cases it was added for. Preserve hard constraints, then use a separately evaluated semantic acceptance policy. Do not compare JEV probabilities to the existing score capped at 1.5.

Low confidence, none, malformed output, or provider failure returns to baseline creation/reuse behavior. This can leave duplicates; that is safer than forced identity reuse. Existing maintenance remains responsible for merges of two stored identities.

## 5. Concurrency, publication, and policy

Initial implementation can await bounded provider work inside the current resolution lock to preserve ordering, with strict request and per-build limits. This increases lock occupancy, so measure contention with concurrent semantic/maintenance work. Do not simply release the lock around HTTP: snapshot validity and state publication need explicit revalidation before any such optimization.

JEV selection changes pending resolution only. It does not publish aliases or entities to the live cache before `commit_project_semantic_knowledge()`. Finalization continues to call `publish_committed_entity_ids()` after the durable commit.

The semantic processor may build entities twice when relationship extraction reports unknown endpoints. Cache semantic decisions only by frozen occurrence, candidate content, model, and question version. Do not cache allocated pending IDs or assume the second build uses identical pending state. Record pass identity in diagnostics.

Extend the frozen ingestion snapshot with identity mode, acceptance policy version, pinned JEV model, criteria version, and candidate/work limits. Restart must use admitted policy, not newly edited settings. A committed window resumes publication without another identity judgment. Cancellation must unwind without publishing private state.

## 6. Implementation units

- [ ] I1: label identity cases for same name/different people, abbreviations, common words, renamed projects, foreign classifications, and repeated occurrences.
- [ ] I2: explicitly separate hard eligibility from deterministic acceptance heuristics while verifying disabled behavior.
- [ ] I3: add a bounded local-handle candidate representation and an occurrence-specific decision contract.
- [ ] I4: implement observe mode, compare decisions with baseline, and extend `identity_decisions` with provider/version/outcome metadata.
- [ ] I5: add active semantic acceptance for eligible uncertain cases; abstention uses current pending reuse/new-ID path.
- [ ] I6: wire frozen policy, bounded retries/calls, both reconciliation passes, and postcommit publication.
- [ ] I7: evaluate candidate recall separately. Broader semantic candidate discovery is a follow-up if the current fuzzy search excludes necessary matches.
- [ ] I8: run transaction/restart and concurrency checks, record acceptance thresholds and held-out results.

## 7. Tests and evaluation

Existing anchors:

- `server/tests/unit/core/knowledge/test_entity_resolution_semantic_smoke.py`
- `server/tests/unit/core/knowledge/test_entity_manager_candidates_contract.py`
- `server/tests/unit/core/knowledge/test_entity_manager_cache_contract.py`
- `server/tests/unit/core/knowledge/test_entity_identity_models.py`
- `server/tests/unit/core/ingestion/test_context_entity_build.py`
- `server/tests/contract/storage/test_semantic_commit_contract.py`
- `server/tests/integration/ingestion/test_project_semantic_postgres_flow.py`

New fake-backed cases must verify wrong/invisible candidate rejection, hard type incompatibility, foreign-type neutrality, JEV resolving a heuristic abstention, insufficient evidence, malformed handles, failure/cancellation, occurrence-specific decisions for the same surface, and no cache mutation before commit. Check that a second build does not reuse stale private IDs, and committed restart does not re-evaluate identities.

Measure candidate recall, incorrect reuse, unnecessary duplicate creation, abstention rate, alias correctness, classification correctness, latency, provider spend, and lock occupancy. Incorrect reuse is the primary quality gate because later facts would otherwise attach to the wrong identity. The unreleased state removes migration constraints, not the need to verify this behavior before real use.

## 8. Decisions and completion

Resolve candidate/work caps, Choice versus pairwise judgments, semantic evidence floor, threshold/margin, observe sampling, and bounded lock occupancy. If profiles are insufficient to distinguish a case, record that limitation rather than adding invented context.

Complete when reuse quality improves on held-out uncertain cases without unacceptable false reuse, all classification/provenance/commit contracts pass, and baseline remains usable with JEV disabled or unavailable. No migration of a populated user knowledge base is required.

## 9. Progress log

- 2026-09-27: durable candidate source, current score/acceptance rules, pending-ID behavior, second-pass rebuild, and postcommit publication reviewed. Implementation pending.
- 2026-09-30: implemented the first observe pilot for deterministic abstentions.
  Choice uses bounded local candidate handles plus none/insufficient outcomes;
  independent Noul asks whether the evidence distinguishes one identity.
  The private trace records the scoped option mapping, candidate counts, JEV
  result, and baseline ID for each occurrence. Hard-incompatible and invisible
  identities are excluded; foreign-project classifications are neutral. The
  shared call budget spans both in-memory build passes. No JEV answer changes
  an entity ID, classification, alias, or commit. Human-reviewed acceptance
  gates, candidate recall, active policy, and durable provenance remain pending.
