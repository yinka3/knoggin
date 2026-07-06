# Knoggin Core Simplification Notes

This note captures the current direction for simplifying Knoggin without
flattening the parts that are actually core to the product. Treat it as a
working design memo, not a final implementation plan.

Working assumption: the system has not been released yet. Prefer direct
schema, module, and contract changes over compatibility shims or legacy
migration paths unless local development data needs an explicit one-time
cleanup.

## Product Core

Knoggin is a durable personal memory engine with an agent interface.

The core loop is:

- capture conversations as durable episodic records;
- extract entities, facts, relationships, timestamps, and provenance;
- resolve entities conservatively;
- refine profiles and facts over time;
- retrieve memory with source evidence;
- let an agent reason over that memory through controlled tools.

The product should optimize for trustworthy long-term memory. A missed entity
merge is usually repairable. A wrong entity merge can poison the memory graph
and is harder to unwind.

## Core Infrastructure

These pieces belong in the core system:

- Postgres as the durable source of truth;
- Redis as bounded runtime coordination, not durable authority;
- user, project, session, and visible-project scoping;
- message storage, provenance, and source references;
- entity, fact, relationship, and search/projection storage;
- ingestion workers and write paths;
- background profile refinement;
- the agent layer, excluding community-agent features;
- agent orchestration, prompt assembly, bounded tool loops, tool registry,
  tool authorization, and write-tool auditing;
- graph/search retrieval tools that return evidence;
- Agent Brain persistence, revisioning, snapshots, and guarded edit/restore
  tools. The Brain is core to Knoggin's agent identity model, even though its
  implementation should remain modular and auditable;
- merge proposal, audit, confirmation, and rollback boundaries;
- scheduler/job infrastructure required by ingestion and refinement.

Background profile refinement is core because raw extraction is not sufficient
long-term memory. It consolidates noisy episodic observations into usable
profile and fact state.

## Feature Layer

These are valuable features, but they should not define the core memory kernel:

- document RAG and folder upload;
- web/news search;
- community-agent discussions;
- specialist spawning;
- topic auto-management;
- maintenance UX and inspection tools;
- DLQ inspection UI;
- import/export and convenience workflows.

Document RAG should be framed as a useful way to submit project context, not as
the center of the memory system. It can help answer questions, but it should not
hide weak conversation-memory behavior.

Agent tools are now classified by layer in the registry:
`core_memory`, `core_brain`, `feature_external`, `feature_project_admin`,
`feature_maintenance`, `feature_community`, and `runtime_special`. This is
groundwork for later decoupling and does not change tool availability.

Tool module metadata now owns default per-tool limits and post-tool
bookkeeping hooks for topic updates and maintenance candidates. The executor
still dispatches the same tools with the same schemas; this only moves
ownership of feature-specific bookkeeping out of the core loop.

Agent-facing schema selection now runs through the tool module registry. The
schema definitions still live in the shared schema files, but modules own which
schema names belong to their layer. Low-level tag and capability filtering
remain behavior-compatible.

## Ingestion Boundary

Ingestion should record observations and suggest candidates. It should avoid
silent semantic writes unless the evidence is deterministic and boring.

Safe ingestion responsibilities:

- extract observations;
- attach provenance;
- reuse entities on exact canonical or alias matches;
- reuse entities on very strong, schema-compatible matches only when the risk
  is low;
- create or preserve separate entities when identity is ambiguous;
- store candidate suggestions with reasons for later review.

Risky ingestion responsibilities:

- using one mention's evidence to resolve another mention;
- allowing graph-neighbor overlap to push an ambiguous match over the reuse
  threshold;
- treating LLM fact relevance as identity evidence;
- resolving overloaded names only by lowercased mention text;
- making merge-like decisions from batch-level aggregates.

The useful reframing is: boosts become evidence, not authority.

Candidate reasons can include:

- exact canonical match;
- exact alias match;
- semantic similarity;
- compatible type or topic;
- shared message context;
- overlapping facts;
- graph-neighbor context;
- sparse-context risk;
- common-word risk;
- high-degree candidate risk.

The agent, profile refinement, maintenance workflows, or the user can later
promote strong candidates into aliases, merges, or corrected facts.

## Safe Batching

Batching should improve throughput without changing identity authority.

Safe batching targets:

- embedding computation;
- candidate lookup, as long as results remain tied to the exact mention
  instance;
- profile, alias, fact, and neighbor reads;
- search-index writes keyed by durable IDs;
- relationship and fact writes after building an explicit mutation plan;
- profile refinement over bounded groups of stale entities.

Avoid batching that authorizes semantic writes:

- do not decide identity at batch level;
- do not let one mention's evidence boost another mention's entity reuse;
- do not merge aliases or entities because a batch aggregate looks strong;
- do not ask one LLM call to decide many unrelated identity matches unless the
  outputs are advisory candidate metadata.

A safer ingestion shape is:

- extract mentions in batch;
- assign each mention a stable work-item identity;
- embed mentions in batch;
- fetch candidates and supporting context in batch;
- evaluate each mention independently;
- batch-write accepted observations and advisory candidate records.

## Document Service Decomposition

`DocumentService` should stay a user-facing workflow boundary, but the current
single-file shape mixes upload scanning, filesystem lifecycle, text extraction,
chunk preparation, SQL reads, SQL writes, and public orchestration.

Keep the service package small:

```text
src/core/knowledge/services/documents/
  __init__.py
  service.py
  scan.py
  storage.py
```

Keep document SQL in the same database layer as the rest of the knowledge
system:

```text
src/core/knowledge/db/readers/document_reader.py
src/core/knowledge/db/writers/document_writer.py
```

Recommended responsibilities:

- `services/documents/service.py`: public `DocumentService` API and workflow
  orchestration;
- `services/documents/scan.py`: folder preview, upload eligibility,
  gitignore/default/custom filters, and content-type safety checks;
- `services/documents/storage.py`: managed storage paths, atomic writes,
  quarantine/restore/purge, text extraction, splitting, and prepared chunk
  helpers;
- `db/readers/document_reader.py`: scoped document, folder, scan-setting, and
  search reads;
- `db/writers/document_writer.py`: scoped scan-setting writes, document/folder
  metadata writes, chunk persistence, index-failure updates, and deletes.

Because the system has not been released, the old
`src/core/knowledge/services/document_service.py` module does not
need to remain as a compatibility wrapper. Move call sites directly to the new
package boundary when the split happens.

The desired boundary is:

- service coordinates use cases;
- reader performs scoped SQL reads;
- writer performs scoped SQL mutations;
- storage owns filesystem bytes and parsing;
- scan owns upload acceptance decisions.

For destructive or cross-boundary flows, such as document deletion and folder
acceptance, keep orchestration explicit in the service while delegating the
actual SQL and filesystem operations to the lower-level collaborators.
