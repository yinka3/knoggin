# Knoggin JEV implementation journal

Status: shared Phase A foundation implemented; consumer pilots remain pending.
Reviewed: 2026-09-27. Git HEAD: `efc75a3ec5104d613f82b30fb01dd053f27c52f8`.
The working tree contains user changes; findings describe the inspected working tree, not only committed HEAD.

Knoggin is unreleased and has no populated knowledge base to migrate. Persisted knowledge below means the knowledge these paths will create once used. Development schema and configuration changes are allowed; preserve the actual transaction, recovery, reference, and scope requirements.

## 1. Selected choices and their journals

| Choice | Main owner | Intended improvement | Detailed plan |
| --- | --- | --- | --- |
| Retrieval reranking | KnowledgeRetrieval and DocumentService | Return better evidence from existing searches | [Retrieval journal](KNOGGIN_JEV_RETRIEVAL_JOURNAL.md) |
| Entity identity resolution | EntityResolver | Reuse the right identity instead of producing duplicates or wrong matches | [Identity journal](KNOGGIN_JEV_ENTITY_IDENTITY_JOURNAL.md) |
| Notebook retention and representation | AgentExecutor, RunNotebook, notebook renderer | Preserve decisive evidence and spend context space usefully | [Notebook journal](KNOGGIN_JEV_NOTEBOOK_JOURNAL.md) |
| Bounded extraction fallback | TextProcessor and ContextEntityBuildService | Avoid generative NER for recoverable candidates | [Extraction journal](KNOGGIN_JEV_EXTRACTION_JOURNAL.md) |

These are selected areas to develop. Detailed policies below are proposals until their benchmark and design checkpoints are resolved. Clarification, model routing, final-answer verification, and replacing relationship extraction are outside these four plans.

## 2. Corrections established by source review

- The actual retrieval class is `KnowledgeRetrieval`, not `RetrievalPipeline`.
- Entities are extracted from eligible versions of Context blocks in a semantic-window impact closure. They are not extracted directly from every incoming message.
- GLiNER and alias matching already avoid a general LLM for normal NER. The generative model is a gap fallback.
- Entity identity resolution is deterministic today; adding JEV there buys quality rather than eliminating a current LLM call.
- Message and document retrieval already have a local cross-encoder. JEV must beat that baseline, not just keyword search.
- Episode retrieval uses lexical/semantic rank fusion and does not share the message reranking call.
- Notebook admission and rollover are synchronous and atomic over copied state. Remote decisions belong in the async execution layer.
- EntityProfile contains names, type, topic, and project context, not a rich narrative biography. Initial identity judgments must use the evidence actually available.
- Sources consulted are tracked separately from the notebook. They do not by themselves provide claim-level answer grounding.

## 3. Shared provider foundation

Implement once, then inject into the actual callers. Avoid parallel JEV subsystem implementations and a broad generic decision framework.

### Source owners

- `server/src/infrastructure/llm_client.py`: OpenAI-compatible text/structured generation, token use, lifecycle, and a private `_LLMSpendingLedger`.
- `server/src/runtime/resources.py`: application-owned shared services, settings subscriptions, startup, and teardown.
- `server/src/runtime/project_factory.py`: project retrieval, resolver, processor, and document-service construction.
- `server/src/common/schema/settings.py` and `common/conf/manager.py`: validated settings and publication.
- `server/src/core/ingestion/policy.py` and `semantic_window_admission.py`: frozen semantic-window decision policy.
- `server/src/infrastructure/model_work.py`: local blocking inference coordination; do not route asynchronous HTTP through its worker pool merely because it is model work.

### Proposed contract and ownership

One small async JEV provider adapter accepts text/structured state and bounded typed questions. It returns validated answers plus provider model version, usage, timing, and outcome. Subsystem code constructs questions and applies policy; the adapter owns HTTP, authentication, limits, and response validation.

Construct one application-owned client when configured, with no JEV request on disabled startup. Inject it into project services and the agent executor. Close it through RuntimeResources teardown, including failed startup. Settings replacement must safely drain or retire clients already serving requests, following the existing lifecycle requirement without copying all LLMService code.

Use a validated `jev` settings block for provider key, endpoint, pinned model, request timeout, bounded retries, and per-capability modes. Recommended modes are `disabled`, `observe`, and `active`. Observe means run the judgment but return baseline behavior; it still incurs provider cost and latency. Do not imply that observe is free.

Keep policies specific: retrieval relevance is not identity acceptance, and Choice confidence is not a Noul value. A Choice can select an option relatively even when every option is poor; include explicit none/uncertain outcomes where appropriate. Do not treat independent probabilities as complementary or multiply them under an unproven independence assumption.

### Spending and packaging

The current spending ledger is private to LLMService. A second independently capped ledger would break the stated server-wide external-model ceiling. Extract only the reservation/accounting seam needed by both providers, keeping one durable balance and clear usage attribution. Budget exhaustion skips optional JEV work and uses baseline where baseline is allowed; it must not authorize an otherwise prohibited LLM call or conceal a failed required fallback.

JEV has a dedicated `/v1/systemone` endpoint. It is not another chat model name. Prefer existing async HTTP support if that makes a small correct adapter; add an optional server dependency only if the SDK materially reduces implementation work. `knoggin[jev]` is not currently the server package name: packaging must respect the existing workspace with `server` and SDK distributions. Do not add an extra just for the name.

### Frozen policy and replay

Identity and extraction modes, thresholds, model version, question/criteria version, and candidate limits must be captured in the ingestion policy admitted with a semantic window. Credentials remain runtime-only. `IngestionPolicy.from_semantic_window_snapshot()` currently requires an exact set of keys, so extending settings without extending snapshot serialization and validation is incomplete.

Pin the provider model and retain versioned question builders needed by outstanding development windows. A retry of an uncommitted stage can still get a different probabilistic answer; frozen settings are not a promise of bit-for-bit replay. Committed stages must resume without repeating JEV or rewriting their result. Document failure/fallback outcomes so a retry using baseline can be explained. Decide whether exact precommit decision replay is necessary before adding durable decision storage; it is not part of the initial foundation by default.

### Diagnostics

Record capability, mode, scoped run/window ID, pinned and reported model versions, question version, bounded item count, input usage, cost, elapsed time, accepted/abstained/error outcome, and fallback reason. Do not dump full private content or API keys into ordinary logs. Existing `ExtractionTrace` is an in-memory trace container; verify durable trace ownership before claiming these records survive restart.

Response checks cover missing/extra answer IDs, invalid labels, non-finite or out-of-range numbers, and wrong distribution shape. Propagate task cancellation. Bounded timeouts and rate-limit retries must fit the owning foreground/background deadline. Failure returns a typed unavailable result for optional consumers, not fabricated probabilities.

## 4. Shared implementation units

- [ ] F0: capture human-reviewed evaluation examples and current baseline outputs before changing decisions.
- [x] F1: add validated configuration, the minimal provider contract, and fake-backed provider tests.
- [x] F2: add runtime ownership, settings subscriptions, cancellation/teardown, and shared spending accounting.
- [x] F3: extend admission snapshots for identity/extraction and test frozen configuration across restart.
- [ ] F4: add bounded diagnostics and observe mode; verify disabled behavior remains baseline.
- [ ] F5: implement retrieval or identity as the first real consumer; use the subsystem journals for completion.

Recommended order: foundation, message retrieval pilot and identity pilot, document reranking, notebook selection, targeted extraction fallback. Retrieval and identity are independent candidate pilots after the common foundation. No sub-agents or code changes are authorized by this document itself.

## 5. Evaluation and release decisions

Use fixed human-reviewed inputs for quality evaluation, fake responses for contract tests, and separate opt-in live JEV runs. Current tests verify behavior and contracts; they are not evidence of actual JEV quality. The synthetic conversation material under `benchmark_authoring/` can supply scenarios after review, but should not be treated as a fully wired executable benchmark.

Record p50/p95 latency, provider spend, candidate count, fallback rate, and each capability's quality measures. Evaluate capabilities independently before testing their combined effects. Do not use the current LLM's output as the sole ground truth. Set numerical thresholds and acceptable regressions from held-out examples before enabling active decisions broadly.

Run relevant unit/runtime checks after implementation. Run storage contracts for identity/extraction transaction changes and PostgreSQL integration for ingestion replay; report unavailable checks accurately. No runtime tests were executed during this documentation-only review.

## 6. Primary references

Official documentation researched during this discussion:

- [JEV introduction and independent typed questions](https://docs.typesafe.ai/introduction)
- [Confidence versus probability](https://docs.typesafe.ai/confidence)
- [Known limitations](https://docs.typesafe.ai/model-jaggedness/jev-1.13)
- [Model limits and version pinning](https://docs.typesafe.ai/models)
- [API shape](https://docs.typesafe.ai/api)
- [Reranking example](https://docs.typesafe.ai/cookbooks/rerank_typesafe)
- [Entity alignment example](https://docs.typesafe.ai/cookbooks/entity_alignment)

Vendor examples demonstrate a possible approach. They do not establish that JEV beats Knoggin's existing models and heuristics. Recheck provider limits/prices when implementing rather than baking today's values into code.

## 7. Progress log

- 2026-09-27: selected four capabilities, inspected their source owners/callers/contracts, and created linked journals. Implementation and live evaluation remain pending.
- 2026-09-28: implemented the entity/extraction Phase A provider foundation using
  existing httpx, shared spending, lifecycle/injection, and frozen policy. Added
  serializable decision contracts and synthetic baseline capture. Human review,
  durable provenance storage, consumer observe calls, and live evaluation remain
  pending. See [foundation notes](server/JEV.md) and the
  [current plan](KNOGGIN_JEV_ENTITY_EXTRACTION_IMPLEMENTATION_PLAN.md).
- 2026-09-29: fixed three Phase A review issues: accounting cannot hold foreground
  requests or shutdown beyond their deadlines; invalid compressed HTTP responses
  return typed unavailable results; budget updates cannot redirect in-flight
  settlement. Unfinished accounting is retained for retry, settlement is
  idempotent, and PostgreSQL accounting now also covers uncapped calls. Expired
  unsettled reservations charge their estimate once using existing tables.
  Validation: 183 focused regression tests passed; five PostgreSQL tests skipped
  because the test service was unavailable. Consumer pilots, human-reviewed
  examples, durable decision provenance, and live provider evaluation remain
  pending.
