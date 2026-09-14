# Provenance and Agent review — 2026-09-09

Reviewed current checkout `aadedewe/refactor`, HEAD `acc0cac33b74759d07880a8b6fffeeee0cdee282`. Production scope: `server/`. Read `CODE_REVIEW_GUIDE.md`, the SQLite/agent retrieval plan, and the locked Agent review as intent and indexes. Excluded README, SDK, and frontend. This extends, rather than replaces, `CORE_LAYER_REVIEW_2026-09-09.md`.

## Verdict

**The provenance foundation is useful and substantially implemented. The agent does not reliably receive all the knowledge and provenance that the backend already preserves.** The biggest problems are evidence delivery and contract mismatches, before retrieval quality or model intelligence enters the picture.

The existing ownership boundaries are worth keeping: durable Agent configuration, orchestrator policy resolution, one run-owned notebook, scoped tools, and atomic answer/source persistence. A broad Agent rewrite is unnecessary. However, the locked review's conclusion that the remaining work is primarily execution-policy enforcement is too optimistic: there are also reproducible failures between retrieved knowledge, notebook state, and model prompts.

“Sources consulted” is an appropriate contract. It records material encountered during an answer; it does not prove that every answer claim follows from that material or that the model interpreted it correctly. This review does not propose a claim-verification platform.

## Confirmed findings

### PA1 — P1 / Finish: episode discoveries lose their content and identifiers before reaching the model

`RunNotebook.model_view()` emits each stored episode directly under `results`. `format_episode_results()` expects each result to contain an `episodes` collection. `build_user_message()` shows only the discovery count, directing the model to the incompatible accumulated-context rendering.

Reproduction: return one episode containing an ID and “The durable launch phrase is violet.” The notebook retains it. The actual model prompt says both “Found 1 episode(s)” and “No contextual episodes recorded”; neither the episode ID nor the launch phrase appears. The model cannot reliably choose `read_episode` from an ID it was never shown.

Locations: [notebook.py](/home/yinka/dev/knoggin/server/src/core/agent/notebook.py:632), [formatters.py](/home/yinka/dev/knoggin/server/src/core/agent/formatters.py:351), [prompt_context.py](/home/yinka/dev/knoggin/server/src/core/agent/prompt_context.py:64).

Fix the notebook-to-prompt episode contract and verify the final model input, including episode handle, summary, chronology, and follow-up availability. Checking only the retrieval return value or notebook contents misses this defect.

### PA2 — P1 / Finish: earlier evidence disappears from subsequent reasoning and final synthesis

The prompt renders previously retrieved messages as a count, entities as names, and connections as an edge count. Older web reads retain compact metadata but lose their passages. Each `_step()` sends a fresh system/user prompt, without the preceding model/tool transcript. A model's earlier reasoning therefore cannot compensate for this omission.

Reproduction through the real executor with a scripted provider: retrieve a launch fact, retrieve a different ownership fact, then synthesize. Both facts remain in the notebook, but the synthesis prompt contains only the ownership fact. The earlier launch fact is absent. No capacity pressure or rollover is required.

Locations: [prompt_context.py](/home/yinka/dev/knoggin/server/src/core/agent/prompt_context.py:217), [executor.py](/home/yinka/dev/knoggin/server/src/core/agent/executor.py:447).

Retain a bounded, informative representation of earlier evidence in every reasoning step, especially synthesis. A summary can replace details only after it actually preserves them. The same lossy projection currently feeds evidence summarization and fallback, so those paths do not automatically recover what disappeared.

### PA3 — P2 / Finish: graph-path provenance is silently discarded

`GraphReader._relationship_observation_refs()` emits `observation_id`, `semantic_window_id`, project, and user. `KnowledgeRetrieval.find_path()` passes these to message hydration, whose normalizer accepts only `message_id` or `id`. It rejects every observation reference, then removes the original `evidence_refs` collection.

The returned path can therefore describe a relationship while carrying empty evidence even though the durable observation has support. This is separate from the retired-observation filtering issue in the earlier core review.

Locations: [graph_reader.py](/home/yinka/dev/knoggin/server/src/core/knowledge/db/readers/graph_reader.py:106), [retrieval.py](/home/yinka/dev/knoggin/server/src/core/knowledge/retrieval.py:340), [retrieval.py](/home/yinka/dev/knoggin/server/src/core/knowledge/retrieval.py:502), [retrieval.py](/home/yinka/dev/knoggin/server/src/core/knowledge/retrieval.py:573).

Use the existing bounded observation-evidence traversal, or deliberately translate observations through their Context supports. Preserve observation identity; do not reinterpret an observation ID as a message ID. The reproduction uses the exact current reader reference shape and the real hydration code.

### PA4 — P2 / Finish: a document change after capture can prevent saving the answer

The source insert requires the document's *current* hash to equal the captured hash and its current status to be non-deleted. A version change between reading and finalization rejects a valid historical encounter. This can also occur within one run that reads a document and then updates it through workspace tools.

Answer, source references, and artifact share a transaction. Consequently, rejecting this reference also prevents the answer from committing; retries do not repair a permanently changed hash. Existing tests cover changing/deleting a document *after* provenance was saved, which the reader handles correctly.

Locations: [source_reference_writer.py](/home/yinka/dev/knoggin/server/src/core/knowledge/db/writers/source_reference_writer.py:180), [store.py](/home/yinka/dev/knoggin/server/src/core/knowledge/store.py:604), [session_runtime.py](/home/yinka/dev/knoggin/server/src/runtime/session_runtime.py:444).

Real PostgreSQL reproduction confirms that a candidate captured at version A is rejected after the document changes to version B. The effect on answer persistence follows the shared transaction in `finalize_assistant_exchange`; this probe does not simulate a whole model-driven editing run.

Separate authorization to capture a source from whether that version remains current at answer finalization. Preserve the authorized encounter's snapshot and expose its historical/unavailable status. Keep scope validation and the atomic answer/source transaction; do not simply suppress every source-writing error.

### PA5 — P2 / Finish: notebook rejection is reported as successful retrieval

`RunNotebook.apply()` can return `accepted=False, reason='capacity'`. `AgentRun.accumulate_tool_result()` reduces this to a boolean change flag, and the executor ignores that flag. It records tool success and supplies a result count. Source candidates were already appended before admission.

Reproduction: a two-passage result against a one-document notebook limit. The notebook rejects the whole result, the tool emits `tool_end`, the prompt claims “Found 2 items,” neither passage reaches the model, and both source candidates remain attached to the run. A single oversized result can also encounter the rendered-size boundary.

Locations: [notebook.py](/home/yinka/dev/knoggin/server/src/core/agent/notebook.py:872), [run.py](/home/yinka/dev/knoggin/server/src/core/agent/run.py:418), [executor.py](/home/yinka/dev/knoggin/server/src/core/agent/executor.py:687).

Propagate admission status. Either admit a bounded portion with an explicit truncation result, or tell the executor/model that a smaller read is required. Decide source-list inclusion consistently with what was actually exposed to the model; a successful backend fetch is not sufficient here.

### PA6 — P2 / Finish: synthesis-only tool policy is enforced in schemas, not dispatch

During SYNTHESIZE the advertised tools are only executor protocols, but a returned ordinary tool call still reaches `_execute_tools()`. Runtime authorization remains the full run allowlist. It enforces user-granted capabilities, but has no execution-phase restriction.

A scripted provider returning `search_messages` during synthesis causes actual dispatch even though the tool is absent from that step's schemas. An otherwise enabled write tool follows the same dispatch path. This does not bypass the run's user authorization; it violates the executor's declared synthesis boundary.

Locations: [executor.py](/home/yinka/dev/knoggin/server/src/core/agent/executor.py:349), [executor.py](/home/yinka/dev/knoggin/server/src/core/agent/executor.py:398), [tool_runtime.py](/home/yinka/dev/knoggin/server/src/core/agent/tool_runtime.py:118).

Validate returned calls against the current phase before execution. Handle mixed terminal/investigative batches at that same boundary, as the locked review already proposes.

### PA7 — P2 / Finish, already documented: research modes still lack execution guarantees

Deep research can accept `submit_answer` on the first step, perform zero investigative calls, and produce a `research_report` artifact. The current profile changes budgets, prompting, and output defaults, but does not enforce grounded investigation or a distinct gap-review checkpoint.

Locations: [executor.py](/home/yinka/dev/knoggin/server/src/core/agent/executor.py:277), [research.py](/home/yinka/dev/knoggin/server/src/common/schema/agent/research.py:15).

Confirmed with a scripted execution. Follow the locked decision: remove unused policy fields, centralize the grounded-evidence predicate, and add the deep-research review checkpoint. Keep normal-mode direct answers and recognize legitimate supplied evidence; do not require arbitrary source counts or a retrieval call for every response.

## How provenance is actually used

| Boundary | Current behavior | Assessment |
| --- | --- | --- |
| Tool/source capture | Document search/read, web/news discovery, web/PDF reads, selected document passages, and explicit/delimited pasted text produce typed candidates. Tool capture occurs before source metadata localization. | Keep. Exact excerpts, hashes, and locators are valuable independently of model output. |
| Answer persistence | References carry answer/run/tool identity and deterministic encounter idempotency. Final answer/source/artifact persistence is atomic. | Keep; Stage 4 repaired PA4 without weakening scope checks. |
| Historical presentation | Stored excerpts remain queryable. Deleted or replaced document versions are marked unavailable; search results are labeled snippets. Web availability here does not establish a fresh successful URL fetch. | Useful historical context, with appropriately narrow semantics. |
| Episode context | Source lists are derived through episode-message attachments, avoiding a duplicate source ledger on episodes. Retrieval includes `sources_consulted` in serialized cards. | Good storage model, incomplete model-facing use: the episode formatter does not render that field, and PA1 additionally hides the card itself. |
| Context maintenance | Frozen messages and assistant source references get revision-local handles. Validation requires a user message for `user_asserted` and a source reference for `source_grounded`; unknown/out-of-window references fail. | Provenance actively constrains admissible updates. It verifies attachment/scope, not semantic entailment. |
| Relationship investigation | `EvidenceService` assembles bounded observation → Context block → message/source bundles with excerpts, hashes, statuses, and state tokens. | Real functionality, now used by ordinary graph hydration after Stage 4. Prompt rendering of structured support remains separate work. |
| Answer generation | The agent receives current Context, recent conversation, and formatted notebook projections. | Currently the weakest boundary: PA1, PA2, and PA5 prevent reliable use of stored evidence. |

Supporting implementations: [source capture](/home/yinka/dev/knoggin/server/src/core/agent/sources/tool_results.py:24), [historical presentation](/home/yinka/dev/knoggin/server/src/core/knowledge/db/readers/source_reference_reader.py:338), [episode serialization](/home/yinka/dev/knoggin/server/src/core/knowledge/retrieval.py:595), [Context evidence validation](/home/yinka/dev/knoggin/server/src/core/knowledge/context/updater.py:292), [EvidenceService](/home/yinka/dev/knoggin/server/src/core/knowledge/evidence_service.py:40), [maintenance consumption](/home/yinka/dev/knoggin/server/src/core/project/maintenance_service.py:244).

## Scoped improvements and simplification

1. **Simplify the competing prompt renderers.** `notebook_renderer.py` already renders a structured notebook, but production model prompts use `prompt_context.py` and its older formatters; notebook capacity uses `notebook.render()`. These are different views with different retained detail. Establish one bounded evidence projection for reasoning, synthesis, fallback, and token measurement. Keep the canonical notebook and local-reference mapping; consolidate rendering rather than replacing the Agent architecture.
2. **Expose provenance on demand through existing reads.** Preserve a usable observation/block reference and let an existing detailed read expand its support. The bounded traversal machinery exists. Include an episode's consulted-source status/locator when relevant. `read_episode` currently expands conversation messages, not those messages' underlying external source excerpts. Do not automatically label old sources as newly consulted without actually exposing them in the new run.
3. **Decide the ordinary `read_file` provenance contract.** This tool exposes local file content but is outside the source-capture map. A run can inspect an ordinary document through workspace tools and finish without that document in its source list. Either route registered evidence-document reads through the existing document-source capture path or clearly keep this limitation. User-owned instructions such as `PROJECT.md` should remain distinguishable from evidence documents.
4. **Preserve important qualifications in Context briefing.** Canonical Context briefing currently renders block Markdown without assertion kind or support pointers. Unless the prose includes the distinction, the model cannot tell a user assertion from source-grounded or agent-derived understanding. Compact labels plus an optional evidence reference would make existing provenance useful without dumping the full evidence graph into every prompt.
5. **Finish the known manager races, not a new manager abstraction.** `update_agent()` still performs full Brain replacement without expected-revision CAS. `delete_agent()` checks default status before a later unconditional delete; `set_default_agent()` checks existence before its transaction. The locked fixes remain applicable to overlapping local sessions/settings operations. These were source-traced here, not newly reproduced as concurrency tests.
6. **Treat adaptive briefing as a later efficiency improvement.** The executor currently loads Brief and canonical Context once per run and includes them in each model step. Ordinary greetings may already skip tools. Conditional briefing can reduce cost, but preserving evidence during substantive work matters more immediately. No separate classifier service is necessary to repair these findings.

## Assessment of the SQLite and agent retrieval plan

The plan's separation of storage migration from retrieval behavior changes is sound. Hybrid message/episode candidate union, broader entity discovery, and more conservative identity decisions address distinct current limitations. They should not be bundled into one undiagnosable backend replacement.

**Add model-input and provenance parity to the plan's existing storage/retrieval parity gates.** Matching IDs, candidate sets, rankings, and graph paths is insufficient if the final prompt loses the result. Preserve these cases across migration:

- Episode discovery shows a usable ID/handle, summary, chronology, and source context where requested.
- Two independent retrievals both contribute relevant content to final synthesis.
- An observation-backed path exposes inspectable support rather than an empty evidence list.
- A document captured before an authorized edit remains representable at answer commit.
- Capacity overflow is explicit and does not invent model consultation.
- Deleted/replaced historical sources, idempotency, and atomic answer/source persistence keep their intended semantics.

Repair PA1, PA2, and PA5 independently of backend migration. They are Python contract/projection problems that SQLite will not solve. Carry the Stage 4 PA3/PA4 contracts and these remaining fixes as invariant tests into the SQLite branch. Retain transaction/CAS requirements for local concurrent work, but implement them with the plan's SQLite ownership model rather than mechanically preserving PostgreSQL locking syntax.

I would prioritize: evidence rendering and hydration → encounter persistence/admission → executor research/phase enforcement → manager concurrency fixes → retrieval quality changes, with backend migration separately staged. Evaluate retrieval improvements on paraphrases, exact names/identifiers, chronology, ambiguous entities, and conflicting evidence; this review did not run a live-model quality benchmark.

## Verification and limits

- **277 existing tests passed:** 237 Agent/source-contract unit tests; 19 evidence-service, evidence-contract, source-admission, and Context-updater tests; 21 source storage/transaction tests, including real local PostgreSQL cases.
- **At review time, seven review probes passed by asserting current defective behavior.** They are reproductions, not evidence that the defects are fixed. PA3 and PA4 are now skipped historical records in the [probe file](/home/yinka/dev/knoggin/server/reviews/provenance_agent_probes_2026_09_09.py).
- The PostgreSQL fixture creates and removes temporary databases. No application database contents were intentionally changed.
- Scripted-provider probes exercise executor/prompt behavior deterministically. They do not measure real-model adherence, answer quality, latency, or frequency of malformed calls.
- Ruff formatting/checks passed for the added probe file. No production fixes or existing-test edits were made. The pre-existing Requests dependency warning was unchanged.
- The earlier report's document indexing/hash mismatch, retired-observation retrieval, and ingestion/identity findings remain separate concerns; they are not counted again here.

Coverage was concentrated on source capture and schemas, answer/source readers and writers, Context evidence admission, bounded evidence traversal and its maintenance callers, knowledge-to-agent hydration, Agent policy resolution and lifecycle, notebook admission/rendering, execution phases, tool dispatch/authorization/auditing, research profiles, and relevant locked decisions. This was a cross-boundary review with reproductions, not a claim that every server line or every runtime interleaving was exhaustively verified.

## Stage 4 implementation status — 2026-09-12

The original PA3 and PA4 rationale remains historical. Both storage/retrieval
boundaries are resolved:

- **PA3:** `8ea0136` makes graph paths emit typed relationship-observation
  references. Retrieval dispatches them to the existing bounded,
  project-scoped evidence facade, retaining observation identity, support, and
  explicit missing status. Stage 6 prompt/notebook rendering remains separate.
- **PA4:** `a6396d5` separates captured-encounter validity from the catalog's
  current version. Finalization still verifies assistant/session/project scope,
  captured readable scope, and document ownership, while valid version A
  persists atomically after replacement/deletion and reads as historical or
  unavailable.

The Stage 4 document, provenance, graph, Episode, and retained semantic gate
passed **307 tests** with the existing Requests dependency warning. PA3 and
PA4 are retained as skipped historical probes; the normal graph and
source-reference/finalization contracts are the acceptance evidence. PA1,
PA2, PA5, PA6, and PA7 remain open.

## Stage 6 implementation closeout — 2026-09-14

PA1, PA2, PA5, PA6, and PA7 are now resolved. The original descriptions above
remain the historical review evidence, not the current behavior.

- **PA1/PA2:** `3601bf3` made the bounded notebook renderer the model-facing
  projection. `716953e` adds a scripted multi-step synthesis contract that
  retains an Episode handle/chronology, message, graph path, document passage,
  and web passage together.
- **PA5:** `347d660` records source encounters only after notebook admission;
  an oversized result reports a narrow-retry instruction and creates no
  model-visible evidence or final source reference.
- **PA6:** `037de22` rejects phase-forbidden calls and mixed terminal batches
  before dispatch.
- **PA7:** `72b30e1` requires admitted grounded evidence for research and one
  executor-owned deep-research gap-review pass.

`fab2b4a` also completed the related default-Agent lifecycle lock and full-Brain
CAS contracts. `7c0c21c` introduced the adaptive Brief/Context policy, and the
closeout correction in `716953e` places the indexed-document manifest behind the
same one-load policy. An adaptive greeting now has no persistent Project read;
substantive signals and a nonterminal fast-path tool transition load the cached
material once.

The closeout gate passed **379** Agent/notebook/prompt/provenance unit tests and
**54** targeted storage contracts against local PostgreSQL, plus Ruff, compile,
architecture, and diff checks. A fixed greeting payload measured 943 local
word-token proxy words in adaptive mode versus 1,072 in always mode, with one
scripted model step in both cases. This remains a deterministic prompt-shape
measure; no live-provider quality or latency claim was made.
