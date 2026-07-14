# Episodic Memory Implementation Blueprint

## 1. Decision

Knoggin will remove the Fact layer and replace it with an Episodic
Memory layer.

Facts are not retained as a parallel source of truth. This is an
unreleased system, so the implementation must remove Fact storage and
runtime behavior directly: it must not generate, resolve, invalidate,
embed, query, or present `Fact` records.

The canonical evidence remains the original message. Episodes are a
maintained, source-linked interpretation of meaningful conversation
windows. Entities and relationships remain the graph/navigation layer
created during normal message ingestion.

The intended hierarchy is:

```text
Messages (canonical evidence)
  -> entity and relationship extraction (graph/navigation)
  -> episodic memory (meaningful, mutable summaries over message windows)
  -> agent retrieval and answer synthesis
```

An episode is not a permanent atomic statement. It is a compact,
updatable description of an evolving part of a conversation, with every
claim traceable to the messages that produced it.

## 2. Goals

1. Preserve complete provenance. An agent can always retrieve the
   messages behind an episode.
2. Avoid fact resolution, contradiction detection, fact invalidation,
   and fact-specific audit machinery.
3. Avoid creating a new memory for every unimportant message window.
4. Preserve full graph context for an episode: all entities and all
   relationships observed in its source messages.
5. Support fast reverse lookup from an entity to relevant episodes.
6. Let the agent retrieve a concise episode first, then expand into the
   highest-influence source messages when it needs evidence or detail.
7. Keep the design understandable and rebuildable. If an episode is
   wrong, the system can regenerate it from canonical messages.

## 3. Non-Goals

The first implementation does not need the following:

- Fact records, fact IDs, or a `FactRecord` compatibility layer.
- Per-fact contradiction classification, invalidation, or audit rows.
- Version history for episode summaries.
- An LLM that creates entities or relationships during episode
  generation.
- A separate permanent "current truth" profile for every entity.
- A separate episode for every topic inside one source window.

There is no legacy-data migration, compatibility period, dual-write, or
dual-read path. Replace the initial schema and runtime code directly.

## 4. Definitions

### 4.1 Ingestion batch

`B` is the number of messages handled by one normal ingestion batch.
Existing ingestion resolves entities and relationships for these
messages before episode generation is considered.

Examples:

- If `B = 8`, an episode window can contain 24, 32, 48, or 64 messages.
- If `B = 10`, an episode window can contain 30, 40, 50, or 60 messages.

Episode windows must be a whole-number multiple of `B`. This guarantees
that every message in an episode has already passed entity and
relationship extraction.

### 4.2 Episode window

An episode candidate is a contiguous set of `N = B * K` fully ingested
messages, where `K` is configured per project or deployment.

`N` is not a hard requirement that a new episode be created. It is the
point at which the system evaluates whether the window changed memory in
a meaningful way.

### 4.3 Episode

An episode contains:

- A structured summary of the meaningful development.
- All messages that support the episode.
- A per-message influence score and optional short reason.
- All entities observed in the attached messages.
- All relationships observed in the attached messages.
- A smaller set of LLM-selected focus entities and, optionally, central
  relationships for high-quality retrieval.

### 4.4 Focus entity

A focus entity is one of the one or two entities that most define what
an episode is about. Focus entities are selected by the episode LLM
from the set of already resolved entities attached to the candidate
window.

Focus does not replace full membership. Every observed entity receives
an episode-to-entity link; focus entities are a ranked subset of those
links.

### 4.5 Consolidation

Consolidation updates a recent relevant episode rather than creating a
new one. The target episode gains the new source messages and graph
context, and its structured summary and influence scores are regenerated
against the complete source set.

There is no episode-summary version history in this design. The raw
messages remain the audit trail.

## 5. Required Invariants

These rules are non-negotiable implementation constraints:

1. Messages are canonical. No episode may cite a message that does not
   exist in the project and conversation scope.
2. Episode generation only uses messages whose entity/relationship
   ingestion has completed successfully.
3. An episode must contain links to every source message used to make
   its summary.
4. Every entity or relationship attached to an episode must be derived
   from one or more of its source messages. The episode LLM may rank
   links, but it must not invent IDs.
5. Every entity observed in the source messages is linked to the
   episode. `is_focus_entity` only changes ranking and retrieval, not
   inclusion.
6. Focus entity IDs returned by the LLM must be a subset of the
   resolved entity IDs present in the source window.
7. Episode writes are idempotent. Retrying a job must not duplicate
   message, entity, or relationship links.
8. Episode updates are bounded. A repeatedly consolidated episode must
   not grow forever; see Section 9.5.
9. The final agent surface must not expose `fact_check`, fact IDs, or
   facts formatted as evidence.

## 6. Target Data Model

The exact SQL naming can follow local conventions, but the following
logical tables and fields are required. Because the system has not
launched, edit the initial schema directly rather than writing a data
migration from Fact rows.

### 6.1 `episodes`

One row per active episodic memory.

| Field | Purpose |
| --- | --- |
| `id` | Stable episode identifier. |
| `project_id` | Project ownership and retrieval scope. |
| `conversation_id` | Conversation ownership. Keep this nullable only if project-wide episodes are explicitly supported. |
| `summary` | Concise natural-language episode summary. |
| `new_developments` | Structured JSON/text list of meaningful introductions or decisions. |
| `updates` | Structured JSON/text list describing how prior understanding changed. |
| `unresolved` | Structured JSON/text list of open questions, uncertainty, or follow-up. |
| `importance` | LLM-scored overall retrieval importance. |
| `source_message_count` | Denormalized count for diagnostics and limits. |
| `first_message_at` | Earliest attached message time. |
| `last_message_at` | Latest attached message time. |
| `created_at` | Episode creation time. |
| `updated_at` | Last consolidation/regeneration time. |
| `embedding` | Vector embedding of the current episode summary for semantic retrieval, if the database supports vector search. |
| `generator_metadata` | Optional model/prompt/schema version and job metadata for operations. This is not summary versioning. |

`summary`, `new_developments`, `updates`, and `unresolved` are replaced
in place on consolidation. Do not add `previous_summary`, an episode
revision table, or a Fact-style change audit unless a future product
need specifically requires history.

### 6.2 `episode_messages`

This is the provenance table. It makes all episode claims inspectable.

| Field | Purpose |
| --- | --- |
| `episode_id` | Episode owner. |
| `message_id` | Canonical source message. |
| `influence_weight` | Normalized LLM score representing the message's contribution to the current episode. |
| `influence_reason` | Optional short explanation such as "introduced decision" or "provided correction". |
| `message_position` | Stable ordering within the episode's source set. |
| `attached_at` | Operational timestamp. |

Use a unique constraint on `(episode_id, message_id)`.

When an episode is consolidated, regenerate weights for its complete
source-message set, not only the newly appended messages. Otherwise
weights from two different LLM runs are not comparable.

### 6.3 `episode_entities`

This is both complete context and the reverse entity-to-episode index.

| Field | Purpose |
| --- | --- |
| `episode_id` | Episode owner. |
| `entity_id` | Existing resolved entity. |
| `prominence_weight` | Relative contribution to the episode. May be LLM scored or deterministically derived. |
| `role` | Optional role such as `subject`, `participant`, `counterparty`, or `context`. |
| `is_focus_entity` | True for at most the top one or two episode-defining entities. |
| `source_message_count` | Number of attached episode messages where the entity occurs. |
| `first_seen_at` | First attached episode-message time containing the entity. |
| `last_seen_at` | Last attached episode-message time containing the entity. |

Use a unique constraint on `(episode_id, entity_id)` and indexes on
`(entity_id, is_focus_entity, episode_id)` and `(entity_id, episode_id)`.

Do not only store focus entities. A question about a secondary entity
must still be able to find episodes where it participated.

### 6.4 `episode_relationships`

This gives an episode its complete graph context.

| Field | Purpose |
| --- | --- |
| `episode_id` | Episode owner. |
| `relationship_id` | Existing relationship produced by message ingestion. |
| `prominence_weight` | Relative importance to the episode. |
| `is_central_relationship` | Optional LLM-selected marker for the relationships defining the development. |
| `source_message_count` | Number of attached episode messages evidencing this relationship. |

Use a unique constraint on `(episode_id, relationship_id)` and an index
on `relationship_id` for graph-oriented retrieval.

### 6.5 Episode search index

Replace `fact_search` with an episode-oriented search path. Depending on
the current database design, this can be a dedicated `episode_search`
table or indexes directly on `episodes`.

The index searches the current summary and structured fields. It returns
episode IDs, after which the application loads attached entities,
relationships, and highest-influence messages.

Do not create individual embeddings for replacement Fact rows. Episode
summary embeddings are the semantic memory index.

### 6.6 Processing checkpoint

Add a durable per-project/per-conversation episode-processing checkpoint
or job-state record. It must record the last fully evaluated ingestion
window so a skipped low-signal window is not evaluated forever.

The checkpoint needs enough information to safely resume after a worker
restart. It is operational state, not an episode version history.

## 7. Episode Generation Contract

### 7.1 Inputs supplied to the LLM

The generator receives:

1. The candidate message window of `N` messages, including stable
   message IDs, timestamps, authors/roles, and text.
2. The resolved entity catalog for those messages: entity IDs, names,
   types, and useful aliases.
3. The resolved relationships for those messages: relationship IDs,
   endpoint IDs, type, and evidence message IDs where available.
4. The summaries and focus entities of the two or three most relevant
   prior episodes. Candidate selection should prefer conversation
   proximity and entity overlap, not merely the globally newest rows.
5. Explicit constraints that message/entity/relationship IDs are closed
   sets and no IDs may be invented.

The LLM does not perform entity resolution. Normal message ingestion has
already resolved or created every entity and relationship available to
the episode generator.

### 7.2 Output contract

Use strict structured output. A representative contract is:

```json
{
  "action": "create | consolidate | skip",
  "target_episode_id": "required only when action is consolidate",
  "summary": "required for create or consolidate",
  "new_developments": ["..."],
  "updates": ["..."],
  "unresolved": ["..."],
  "importance": 0.0,
  "message_influence": [
    {
      "message_id": "existing-message-id",
      "weight": 0.0,
      "reason": "short explanation"
    }
  ],
  "focus_entities": [
    {
      "entity_id": "resolved-entity-id",
      "weight": 0.0,
      "role": "subject"
    }
  ],
  "central_relationship_ids": ["resolved-relationship-id"]
}
```

Validation rules:

- `target_episode_id` must be one of the supplied recent-episode IDs.
- `message_influence.message_id` must be an attached source message.
- All source messages must receive a weight. Missing weights can be
  deterministically filled with `0`, but the prompt should request all
  messages explicitly.
- `focus_entities` must contain zero, one, or two IDs and each must be
  from the resolved entity set.
- `central_relationship_ids` must be a subset of the resolved
  relationship set.
- `skip` must not write an episode or episode links.

The application, not the LLM, attaches the complete entity and
relationship membership sets. The LLM only supplies the ranking signals
for focus entities and central relationships.

### 7.3 Meaning of actions

`create`

Create a new episode containing all candidate-window messages and their
complete entity/relationship memberships.

`consolidate`

Update exactly one supplied recent episode in the first implementation.
Attach the new window's messages, entities, and relationships to that
episode; regenerate its summary and structured fields; regenerate
message influence across all source messages; update entity and
relationship prominence; and update the episode embedding/search index.

Limiting an incoming window to one target episode prevents accidental
duplication of the same raw messages into multiple memories. Supporting
multi-episode updates can be added later only when product behavior
proves it necessary.

`skip`

Do not create or update memory. Advance the processing checkpoint. The
raw messages and their entity/relationship graph records remain fully
available to normal message retrieval.

## 8. Attaching Full Context

The episode writer must construct attachment sets from the database, not
from the LLM response:

```text
source message IDs
  -> all entity IDs evidenced by those messages
  -> all relationship IDs evidenced by those messages
  -> upsert episode_messages, episode_entities, episode_relationships
```

This is what makes the episode self-contained for agent retrieval.

For each attached entity, calculate a deterministic baseline prominence,
such as the sum of influence weights for source messages that mention
the entity. Apply the LLM focus-entity weight/role when the entity is
selected as a focus entity. The same pattern applies to relationships.

This produces useful rankings without trusting the LLM to enumerate a
long list of identifiers correctly.

## 9. Generation and Consolidation Flow

### 9.1 Trigger

After normal graph ingestion completes a batch of `B` messages, record
that batch as eligible for episodic processing. Do not invoke episode
generation while message extraction or entity resolution is still
running.

When a conversation has at least `N = B * K` newly eligible contiguous
messages since its episode checkpoint, enqueue an `EpisodeGenerationJob`.

### 9.2 Candidate selection

The job loads:

1. The next `N` eligible messages in chronological order.
2. Full entity and relationship memberships for those messages.
3. The last two or three relevant episodes in the same conversation.

Prior episodes should be chosen by a simple deterministic rank before
calling the LLM:

- entity overlap with the candidate window;
- recent temporal proximity;
- optionally semantic similarity of the candidate-window provisional
  text to episode summaries.

Keep the candidate list small and always include the immediately
previous episode when one exists. The LLM should never browse an
unbounded episode history for this decision.

### 9.3 Write transaction

For `create` or `consolidate`, perform one database transaction that:

1. Locks the processing checkpoint for the conversation.
2. Revalidates that all candidate messages are still eligible.
3. Creates or updates the episode row.
4. Upserts all message links and current influence data.
5. Upserts all entity links and focus/prominence data.
6. Upserts all relationship links and central/prominence data.
7. Updates the summary search index/embedding.
8. Advances the checkpoint.

For `skip`, only advance the checkpoint after recording enough job-level
diagnostic data to explain why no write occurred.

### 9.4 No versioning, with operational observability

No historical episode summary needs to be stored. Keep only current
episode content plus `created_at`, `updated_at`, generator metadata, and
standard job logs/errors. If a summary needs investigation, inspect its
current source messages and rerun generation.

This is deliberately different from fact auditing. The system is not
trying to prove that a sequence of atomic claims was valid over time.
It is maintaining a current useful synopsis of a traceable conversation
segment.

### 9.5 Bounded episode growth

Consolidation must have limits. Configure at least:

- `episode_target_message_count`: normal initial window size `N`.
- `episode_max_message_count`: maximum source messages allowed after
  consolidation, for example two or three target windows.
- `episode_max_age`: optional maximum time span for one episode.

If a candidate would push a target episode beyond either limit, instruct
the generator to create a new episode instead. This keeps agent context
predictable and prevents a long-running topic from turning into one huge
memory object.

## 10. Agent Retrieval Design

Replace `fact_check` with an episode-oriented tool, for example
`episode_check` or `memory_check`.

The tool should accept an entity name/ID and/or a natural-language
question. It should return episodes, not atomic facts.

### 10.1 Entity-first retrieval

For an entity query:

1. Resolve the requested entity through the existing entity resolver.
2. Query `episode_entities` for that entity.
3. Rank focus links above non-focus links, then use prominence, recency,
   episode importance, and optional semantic similarity.
4. Load the top episodes and their structured summaries.
5. Return the top source messages ordered by `influence_weight` as
   evidence, with the option to expand to all messages.

This directly answers the missing reverse lookup problem: an entity can
find every episode where it occurred, while focus status highlights the
episodes principally about that entity.

### 10.2 Question-first retrieval

For a general question:

1. Semantic-search episode summaries/structured fields.
2. Optionally resolve named entities from the question and boost episodes
   linked to them.
3. Load complete episode graph context: all linked entities and
   relationships, not only focus items.
4. Give the agent the summary plus the highest-influence messages.
5. The agent may call a message-expansion tool when it must quote,
   verify, or reconcile detail.

### 10.3 Answering behavior

The agent must phrase episode content as contextual memory, not as an
unqualified database fact. It should cite or expose message evidence
when the question is sensitive, disputed, or asks for exact detail.

There is no need to recreate a hidden Fact layer inside the agent prompt.
The agent synthesizes the answer from episode summaries and source
messages.

## 11. Repository Changes

This section maps the target design to the existing codebase. Exact
module names can evolve, but these current Fact dependencies must be
removed as part of this implementation.

### 11.1 Schema and shared contracts

Replace Fact contracts in:

- `server/src/common/schema/primitives.py`
  - Remove `Fact`, `FactRecord`, and `ProfileUpdate.facts`.
  - Add episode, episode-message, episode-entity, episode-relationship,
    and structured LLM-output models.
- `server/src/infrastructure/schema.sql`
  - Remove `facts`, `fact_change_audits`, and `fact_search`.
  - Add the tables and indexes from Section 6.
  - Remove Fact graph projection definitions.

Suggested shared models:

```text
EpisodeRecord
EpisodeMessageRef
EpisodeEntityRef
EpisodeRelationshipRef
EpisodeGenerationDecision
EpisodeGenerationOutput
EpisodeSearchResult
```

Keep LLM output models separate from persisted records. The output is
validated before the writer derives and attaches full graph context.

### 11.2 Ingestion and scheduling

Replace profile refinement in:

- `server/src/core/ingestion/jobs/profile_job.py`
  - Replace `ProfileRefinementJob` with `EpisodeGenerationJob`.
- `server/src/core/ingestion/prompts.py`
  - Remove Fact extraction/contradiction prompt helpers.
  - Add the structured episode-generation prompt helper.
- `server/src/common/prompt_loader.py`
  - Register the episode prompt.
- `server/src/core/knowledge/write_graph_db.py`
  - Stop marking entities dirty solely to trigger Fact/profile refinement.
  - Mark completed ingestion batches eligible for episode processing.
- `server/src/core/project_manager.py`
  - Register and schedule the new job rather than the profile job.
- `server/src/common/settings.py`
  - Replace profile/fact refinement settings with episode batch multiple,
    target/max window size, candidate count, and retrieval limits.
- `server/src/infrastructure/redis_client.py`
  - Replace profile-specific progress keys with episode job/checkpoint
    keys, or move durable checkpoint state into the database.

### 11.3 Fact generation and resolution removal

Remove rather than port:

- `server/src/core/knowledge/services/fact_resolution.py`
- `server/src/core/knowledge/services/fact_change_service.py`
- Fact-specific logic in `server/src/common/utils/data_utils.py`,
  including `process_extracted_facts`.
- Fact extraction and contradiction prompts.

Do not translate contradiction logic into episode invalidation. New
information is represented by consolidation or a later episode summary
that explains the changed understanding, with the supporting messages
attached.

### 11.4 Persistence APIs

Remove Fact methods from `server/src/infrastructure/knowledge_store.py`
and add episode-oriented APIs such as:

```text
create_episode(...)
update_episode(...)
upsert_episode_messages(...)
upsert_episode_entities(...)
upsert_episode_relationships(...)
get_episode(...)
get_episodes_for_entity(...)
search_episodes(...)
get_episode_source_messages(...)
get_episode_graph_context(...)
advance_episode_checkpoint(...)
```

Replace these Fact-specific reader/writer areas with episode readers and
writers:

- `server/src/core/knowledge/db/readers/fact_reader.py`
- `server/src/core/knowledge/db/writers/fact_writer.py`
- `server/src/core/knowledge/db/readers/fact_audit_reader.py`
- `server/src/core/knowledge/db/writers/fact_audit_writer.py`

Update or remove Fact handling in:

- `server/src/core/knowledge/db/writers/age_projection_writer.py`
- `server/src/core/knowledge/db/projection_rebuilder.py`
- `server/src/core/knowledge/db/search_index_rebuilder.py`

The AGE graph should continue projecting entities and relationships. It
must stop projecting `Fact` nodes and Fact edges.

### 11.5 Agent tools and prompts

Replace the Fact retrieval contract in:

- `server/src/core/agent/tools/graph.py`
  - Replace `fact_check` implementation with `episode_check` or
    `memory_check`.
- `server/src/common/schema/tool_schema.py`
  - Replace the tool schema and arguments.
- `server/src/core/agent/tools/registry.py`
  - Register the new tool and remove `fact_check`.
- `server/src/core/agent/system_prompt.py`
  - Tell the agent to use episode memory and inspect source messages for
    verification, not to request facts.
- `server/src/core/agent/types.py`
  - Replace fact evidence/accumulation types with episode evidence.
- `server/src/core/agent/internals.py`
  - Accumulate episode IDs, summaries, and source-message references.
- `server/src/core/agent/formatters.py`
  - Format summaries, focus entities, complete graph context, and
    influence-ranked messages.

The visible tool result should make the provenance clear:

```text
Episode summary
  -> focus entities
  -> complete linked graph context
  -> highest-influence source messages
```

### 11.6 Entity resolution, merge, graph, and community code

The existing entity merge flow uses facts as support and contradiction
evidence. Replace that evidence model with episodes and source messages
in:

- `server/src/core/knowledge/entity/resolver.py`
- `server/src/core/knowledge/entity/merge_service.py`
- merge audit readers/writers that snapshot, restore, or delete facts
- entity and graph readers that display fact snippets/counts
- `server/src/core/knowledge/tool_queries.py`
- `server/src/core/knowledge/community_manager.py`

For entity merge decisions, use:

1. Existing direct message evidence.
2. Overlapping episode memberships and focus status.
3. Episode source messages when an LLM/NLI decision needs natural
   language context.

Do not invent `evidence_fact_ids` replacements that hide the source.
Use `episode_id` and `message_id` evidence explicitly.

## 12. Implementation Order

Build in this order. Because no production data or released clients need
support, remove Fact storage and callers as soon as their episodic
replacement is implemented and tested.

### Phase 1: Contracts and storage

1. Replace the Fact schema definitions with episode schema definitions,
   then add episode models, readers, writers, and store methods.
2. Add full-context attachment queries for messages, entities, and
   relationships.
3. Add checkpointing and idempotent upserts.
4. Add episode search indexing/embedding support.

Acceptance: a test can create an episode, attach all source context,
look it up from any linked entity, and retrieve messages in influence
order.

### Phase 2: Episode generation job

1. Define the strict structured-output schema and prompt.
2. Add candidate selection for the next eligible `N` messages and last
   2-3 relevant episodes.
3. Implement `create`, `consolidate`, and `skip` validation and writes.
4. Enforce source/window limits and retry-safe checkpoint updates.

Acceptance: mocked LLM outputs exercise all three actions, invalid IDs
are rejected, and retrying the same window creates no duplicate links.

### Phase 3: Retrieval

1. Add entity-first episode retrieval with focus-aware ranking.
2. Add semantic question-first episode retrieval.
3. Add source-message expansion and graph-context loading.
4. Update the agent tool, prompt, evidence types, and formatting.

Acceptance: an entity query returns focus episodes first but can still
find episodes where the entity was only contextual; a question query can
retrieve an episode and inspect its evidence messages.

### Phase 4: Remove Fact consumers

1. Replace Fact-dependent entity merge evidence.
2. Replace Fact usage in community, graph, projection, and rebuild jobs.
3. Remove Fact extraction/refinement jobs, prompts, APIs, models, and
   tests.
4. Remove Fact database tables, indexes, graph projections, audit
   tables, and the associated schema definitions.

Acceptance: `rg -n "Fact|fact_check|fact_search|fact_change" server`
returns no live runtime dependencies. Remaining documentation references
must describe the removal only, not a compatibility path.

## 13. Test Plan

Add focused tests before deleting the Fact suite.

### Storage and validation

- A created episode has every candidate message, entity, and
  relationship attached.
- Focus IDs outside the candidate entity set are rejected.
- Relationship IDs outside the candidate relationship set are rejected.
- Duplicate job execution does not duplicate rows.
- Consolidation regenerates influence scores for all attached messages.
- An entity can locate episodes where it is focus and where it is only
  contextual.

### Generation behavior

- `create` creates a bounded episode with complete context.
- `consolidate` updates one recent eligible episode and unions source
  context correctly.
- `skip` advances the checkpoint without creating an episode.
- A low-signal window is not repeatedly processed after retry/restart.
- A too-large consolidation becomes a new episode.

### Agent behavior

- The agent no longer has a `fact_check` tool.
- Entity questions use episode-to-entity links.
- Episode results include enough source evidence for verification.
- The agent can expand a retrieved episode into raw messages.
- No response path formats a Fact record or Fact ID.

### Regression tests

- Normal entity and relationship extraction still works unchanged.
- Entity merge behavior uses message/episode evidence correctly.
- Search/index rebuilds produce episode indexes and no Fact indexes.
- Project job registration works with the episode generator.

## 14. Operational Metrics

Track these from day one:

- windows evaluated, created, consolidated, and skipped;
- average source messages per episode;
- consolidation rate and episodes hitting maximum size;
- number of entity and relationship links per episode;
- focus-entity retrieval hit rate;
- episode retrieval latency and source-message expansion latency;
- LLM validation failures and invalid-ID rates;
- agent use of episode evidence versus direct raw-message fallback.

These metrics will show whether the system is producing useful durable
memory or simply moving raw conversation into another table.

## 15. Design Decisions to Preserve

1. No Fact layer remains in the final architecture.
2. No fact-resolution or contradiction-invalidation subsystem is
   recreated under another name.
3. No episode version history is required. Raw messages are the
   canonical inspectable record.
4. An episode includes all messages, entities, and relationships from
   its chunk or consolidated source set.
5. Every observed entity is reverse-linked to the episode.
6. The LLM-selected top one or two entities are focus markers used for
   ranking, not a lossy filter.
7. The application mechanically attaches complete graph context; the
   LLM ranks and summarizes it.
8. New windows may create, consolidate, or skip memory. A window is not
   automatically a new episode.
9. Episode size is tied to ingestion batch size through `N = B * K`.
10. The agent retrieves episodes for context and source messages for
    proof and exact detail.

## 16. First Implementation Ticket

Start with the smallest vertical slice:

1. Add the four episode tables plus a checkpoint.
2. Implement a writer that receives a fixed set of already-ingested
   message IDs and mechanically attaches all related entities and
   relationships.
3. Add a test proving entity-to-episode reverse lookup, including a
   non-focus entity.
4. Add the structured LLM output model and mocked `create` path.
5. Only then add scheduling, consolidation, semantic search, and agent
   integration.

This sequence proves the central data contract before changing the
agent or deleting Fact consumers.
