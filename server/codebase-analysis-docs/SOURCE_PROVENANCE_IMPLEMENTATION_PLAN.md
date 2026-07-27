# Sources Consulted for Agent Responses

**Status:** Proposed implementation design — not implemented
**Purpose:** Let a user see the uploaded-document passages that were supplied
to an agent while it produced an answer.  The same references should be
available when the answer later appears in an episode.

---

## 1. Product Objective

For an agent response, Knoggin should be able to answer:

> Which uploaded document passages did the agent consult while producing this
> response?

This is answer context, not a truth system.  It gives a user useful context
for a good answer, a way to follow the underlying material, and a way to
investigate a wrong answer if they choose.

Example:

```text
Assistant response
  → "Acme Q2 Report.pdf", content version SHA-256 8e4…
      → page 14
          → exact excerpt returned by search_documents
```

The user-facing label should be **Sources consulted** (or **Context used**).
Do not call these references proof that a particular claim is true.  They
reliably record what retrieval supplied to the model; they do not prove which
words the model relied on or whether its reasoning was correct.

### 1.1 Relationship to episodes

Episodes remain the current durable memory model.  This feature does not
replace them and must not introduce facts or a competing memory pipeline.

The existing relationship already provides the episode bridge:

```text
episode
  → episode_messages
    → assistant message
      → sources consulted for that response
```

When an episode is read, Knoggin should derive a deduplicated source list from
its attached assistant messages.  Do not copy the list into the `episodes`
row: message-level references are the canonical source and avoid stale,
duplicated data when an episode is consolidated or regenerated.

---

## 2. First Release Scope

### Included

1. **Uploaded PDFs and text documents:** TXT, Markdown, CSV, and supported
   source-code files.
2. Page-aware PDF indexing and line/section-aware text-document indexing so a
   retrieved chunk has a reliable locator.
3. Capture successful `search_documents` and `read_document` results for those
   document types.
4. Capture explicit user-pasted text and successful `web_search` / `news_search`
   result snippets as answer context.
5. Persist those results against the final assistant message for the agent
   run that received them.
6. Return source references with an individual answer and aggregate them when
   inspecting an episode.
7. Preserve the exact returned excerpt, source version/hash, locator,
   agent run ID, and tool-call ID.
8. Let a user explicitly select a visible uploaded document with a
   `/relative/path` message command.

### Explicitly excluded

- DOCX structure, headings, paragraph locators, and sentence-level citations.
- Attached/fetched websites, web-page reading, URLs, and external-source
  retention.
- Images and OCR-derived source locations.
- User references without supplied text, connector/webhook ingestion, and
  generic third-party sources.
- Automatic source extraction, active facts, profile changes, or episode
  generation changes.
- A general source/revision/segment/encounter platform.
- A new UI requirement.  The core read result is sufficient for a future UI or
  SDK to render a source drawer.

The excluded work is useful only when Knoggin needs a cross-source provenance
platform.  It is not required to show the document context supplied to an
answer.

### 2.1 Source-selection command

`/relative/path` is an explicit request to use an already uploaded, visible
document as context.  For example:

```text
Summarize /reports/Acme_Q2_Report.pdf
```

The command resolver must match the path to a document visible in the current
project/session, resolve it to its canonical `document_id`, and attach that ID
as the request's document focus.  It must reject a missing or ambiguous path
rather than selecting an arbitrary document.

The command does **not** put the full document in the LLM prompt.  It gives
the agent a focused document target; the agent still uses bounded document
search/read calls and only their returned passages become sources consulted.

Reserve `/` for project document paths.  A pasted URL is not a fetched source;
it remains ordinary user text until a later explicit URL-attachment flow
retrieves it safely.

### 2.2 Clean target state

Knoggin is unreleased, so this implementation should replace the old flattened
PDF indexing/read path rather than preserve it for backwards compatibility.
Existing documents may be reindexed as part of deployment; no legacy
page-less PDF representation needs to remain supported.

---

## 3. What This Design Keeps From the Earlier Plan

| Essential behavior | Kept in this release |
|---|---|
| Identify the consulted source | document/message ID or search-result URL |
| Distinguish an old upload or text snapshot from a replacement | `content_hash` |
| Give the user a location | PDF page, document line/section, message span, or search rank |
| Preserve what the agent actually received | exact returned `excerpt` |
| Connect retrieval to a response | assistant `message_id`, `agent_run_id`, and `tool_call_id` |
| Make it available in episode context | derive through `episode_messages` |
| Keep chat/episode memory independent | no changes to the episode generation or consolidation model |

The earlier plan's sentence segments, generic source revisions, and encounter
ledger are deliberately deferred.  Page-level provenance plus the returned
excerpt is enough for the first product value.

---

## 4. Data Model

### 4.1 Existing data reused

- `project_documents.document_id` identifies the uploaded PDF.
- `project_documents.content_hash` identifies the indexed content version.
- `document_content` stores the source bytes and extracted text.
- `document_chunks` stores retrieval chunks.
- `messages` stores the final assistant response.
- `episode_messages` already attaches response messages to an episode.

### 4.2 Page-aware chunks

Add a `page_number INTEGER` to `document_chunks` for PDF chunks.  For text
documents, preserve their existing or new line/section span on the chunk.

For PDFs in scope, indexing must extract pages independently, split each page
independently, and write each resulting chunk with that page number.  A PDF
retrieval chunk must never cross a page boundary.

This is the smallest reliable mapping from a returned chunk to a page.  It
avoids a chunk-to-sentence bridge table and avoids trying to recover locations
later with text matching.

For non-PDF document types, `page_number` remains `NULL`; their locator is a
line range, CSV row range, code symbol/range, or Markdown section/range.

### 4.3 `message_source_refs`

Add one narrow table for answer context supplied to an assistant response:

```text
message_source_refs
  source_ref_id UUID primary key
  project_id TEXT not null
  session_id TEXT not null
  message_id BIGINT not null                 → messages
  source_kind TEXT not null                  # pdf_document | text_document | user_pasted_text | web_search_result | news_search_result
  document_id UUID nullable                  → project_documents
  canonical_url TEXT nullable
  source_message_id BIGINT nullable          → messages
  content_hash TEXT not null
  locator JSONB not null                     # {"page": 14}, {"start_line": 8, "end_line": 15}, {"start_char": 0, "end_char": 200}, or search metadata
  excerpt TEXT not null
  metadata JSONB not null default {}         # document display data; provider, query, rank, title, URL
  encounter_kind TEXT not null               # document_search | document_read | user_pasted_text | web_search | news_search
  agent_run_id TEXT not null
  tool_call_id TEXT nullable
  result_position INTEGER not null
  idempotency_key TEXT not null unique
  created_at TIMESTAMPTZ not null
```

Required constraints and indexes:

- scope the message foreign key by `message_id`, `project_id`, and `session_id`;
- use `ON DELETE CASCADE` from the assistant message;
- require `result_position >= 0`;
- for document kinds, require `document_id` and a matching page/line/section
  locator; for `user_pasted_text`, require `source_message_id` and a character
  span; for search/news results, require a normalized `canonical_url`, provider,
  query, and rank in the locator;
- scope `source_message_id` to the same project and session when it is present;
- require `tool_call_id` for every tool-derived kind and leave it `NULL` only
  for `user_pasted_text`;
- derive `idempotency_key` from the run plus the candidate origin and position
  (tool call/result position or user message/span) so retries cannot duplicate
  a reference;
- index `(message_id, project_id, session_id)` for answer and episode reads.

`document_id` should use the project's document-deletion policy.  The default
for private, deleted source material is to delete source references and their
stored excerpts with the document.  If product policy instead needs historical
answer audit after document deletion, retain only a redacted reference; do not
retain an accessible excerpt without an explicit retention decision.

This is a deliberately small answer-context table, not the earlier general
source ledger.  Versioned web retrieval, generic source records, and
cross-source segment storage remain deferred.

For a document, `content_hash` is the existing document version hash.  For
user-pasted text and a search/news result, it is a hash of the exact stored
source payload.  This makes it possible to distinguish a changed message or
provider snippet without pretending either is a fetched web-page revision.

### 4.4 Reference payload

The model-facing and UI-facing form should be compact:

```json
{
  "source_ref_id": "…",
  "kind": "pdf_document",
  "document_id": "…",
  "document_name": "Acme Q2 Report.pdf",
  "content_hash": "8e4…",
  "locator": {"page": 14},
  "excerpt": "Revenue increased 18% year over year…",
  "encounter_kind": "document_search"
}
```

The initial implementation records **sources consulted**, not citations chosen
by the model.  The list may contain a retrieved passage that the final prose
does not use.  Exact claim-to-source citations are a separate future feature
that would require the agent to deliberately return `source_ref_id` values
with its claims.

---

## 5. Runtime Flow

```text
PDF upload/index
  → extract text page by page
  → make chunks within one page only
  → persist chunk.page_number

Text-document upload/index
  → make chunks with line, section, row, or code-range locators

Agent calls search_documents or read_document
  → result contains document ID, hash, locator, and excerpt
  → executor retains successful document result + run/call identifiers in memory

User sends explicit pasted text
  → retain its user-message ID and character span as a source candidate

Agent calls web_search or news_search
  → retain the exact provider snippet, URL, query, provider, and rank
  → label it as a discovery snippet, not fetched page content

Agent submits final answer
  → response event carries its consulted-source candidates
  → SessionContext.add_assistant_turn persists the assistant message
  → persist message_source_refs for that message in the same durable workflow

Answer or episode inspection
  → return message source refs directly
  → for an episode, join episode_messages → messages → message_source_refs
```

### 5.1 Explicit document selection

Before agent execution, parse one eligible `/relative/path` command from
the user's message.  Resolve it through the document service's scoped
visibility rules and give the resolved document ID to the request as document
focus.  The focused document must be an explicit constraint/instruction, not
an unbounded text injection.

The initial command supports one document path per request.  Multiple-source
selection, folder selection, and inline autocomplete can follow once the
single-document flow is stable.

### 5.2 Capture rule

Capture only successful, non-empty results returned to the agent from:

- `search_documents`; and
- `read_document`;
- `web_search`; and
- `news_search`.

Do not create a reference for errors, authorization failures, empty results,
metadata-only document listings, documents without a reliable locator, or
search-provider placeholder/error results.

For user-pasted text, capture only text explicitly marked or detected as a
paste (for example, a code block, quote block, or attachment-style pasted
content).  Ordinary conversational statements remain normal messages and are
not relabeled as external sources.

The agent executor already has the `ToolCall` while it executes the tool.  The
implementation must preserve the call's `call_id` along with `ctx.run_id` for
each capture candidate.  It must not create a durable row before an assistant
message exists; a retrieved passage that belongs to an abandoned/failed run is
not an answer source reference in this simplified design.

### 5.3 Final-answer handoff

`AgentExecutor._wrap_final_response()` must include all accumulated source
reference candidates in its response event.  The response consumer then passes
them as metadata or an explicit argument to `SessionContext.add_assistant_turn`.

After `SessionContext` allocates and persists the assistant `message_id`, its
message persistence path writes `message_source_refs`.  The message and refs
should be saved atomically where practical; a failed reference write must not
silently claim that the response has source context.

This keeps the executor focused on collection during a run and keeps durable
message ownership in `SessionContext`/the knowledge-store boundary.

---

## 6. Read Behavior

### Individual response

An answer read/API response should include:

```json
{
  "content": "…",
  "sources_consulted": [
    {
      "document_name": "Acme Q2 Report.pdf",
      "page_number": 14,
      "excerpt": "…",
      "document_id": "…"
    }
  ]
}
```

Order references by their tool-call/result position.  Deduplicate only exact
duplicates from retries; do not collapse different excerpts on the same page.

### Episode

When an episode is read, return a separate `sources_consulted` collection that
is derived from the episode's assistant messages.  Each source should identify
the response message it informed, so a user can navigate from episode context
back to the particular answer.

Do not add `sources_consulted` to the durable `episodes` row or to episode
generation prompts.  It is response context, not new episode content.

---

## 7. Implementation Map

| Area | Change |
|---|---|
| `src/infrastructure/schema.sql` | Add PDF page and text-document locator columns as needed, plus `message_source_refs` with scoped foreign keys, source-kind constraints, and indexes. |
| `src/core/knowledge/documents/storage.py` | Replace flattened PDF extraction/indexing with page-preserving extraction and produce locators for supported text documents. |
| `src/core/knowledge/documents/service.py` | Index documents with type-appropriate locators and return source-reference payloads from reads and searches. |
| document reader/writer boundary | Read/write document locators and add scoped source-reference persistence and reads. |
| `src/infrastructure/knowledge_store.py` | Expose narrow source-reference read/write methods. |
| `src/core/agent/executor.py` | Retain successful document/search result candidates with `ToolCall.call_id`; include them in the final response event. |
| `src/core/agent/tools/search.py` | Preserve locators and exact excerpts in document, web-search, and news-search results. |
| `src/core/session/context.py` | Persist source references after the final assistant message receives its canonical ID. |
| message-command/request setup | Parse `/relative/path`, resolve it through document visibility, set one document focus without injecting full file text, and identify explicit pasted-text source spans. |
| episode reader/tool response | Aggregate source references through existing `episode_messages`; do not alter episode persistence. |
| tests | Add page-indexing, capture, message persistence, scope, deletion, retry, response, and episode-aggregation contracts. |

---

## 8. Delivery Sequence

Implement this in the following small, independently testable chunks.  Each
chunk should be reviewable and mergeable on its own; do not combine the entire
source-context feature into one change.

### Chunk 0 — Lock the source-reference contract

**Goal:** Make the supported source kinds and their required locator fields
unambiguous before storage or agent changes begin.

- Define the five source kinds: `pdf_document`, `text_document`,
  `user_pasted_text`, `web_search_result`, and `news_search_result`.
- Define required locator/metadata fields for each kind.
- Define what counts as explicit pasted text and what must remain an ordinary
  conversation message.
- Define document-deletion behavior for saved response references.
- Add fixtures for a two-page PDF, Markdown/text/code/CSV documents, a pasted
  text span, and web/news provider results.

**Implementation detail:**

- Add a typed `SourceReferenceCandidate` contract for data collected before an
  assistant message exists, and a `SourceReference` contract for persisted
  rows/read responses.  Do not use unvalidated dictionaries between tools,
  executor, session persistence, and readers.
- Model `locator` as a discriminated shape: PDF `{page}`, text
  `{start_line, end_line, section_path?}`, CSV `{start_row, end_row}`, code
  `{start_line, end_line, symbol_name?}`, pasted text `{start_char, end_char}`,
  and provider result `{provider, query, rank}`.
- The UI/request layer should send explicit `pasted_text_spans` metadata when
  it knows content was pasted.  As a server-only fallback, recognize fenced
  code blocks and quote blocks; do not guess based only on message length.
- State the deletion rule in one place: deleting a private document removes its
  stored references/excerpts; deleting a source user message removes references
  that point to its pasted span.

**Exit condition:** Tests and typed contracts can construct and reject every
source-reference shape without calling an agent or database.

### Chunk 1 — Add source-reference storage and the storage boundary

**Goal:** Make answer-context references durable and project/session scoped.

- Add `message_source_refs` plus its scoped foreign keys, discriminated source
  constraints, idempotency key, and indexes.
- Add the reader/writer methods behind `knowledge_store`.
- Add the minimal chunk locator columns needed by the agreed source shapes.
- Add storage contracts for scope isolation, idempotency, message deletion, and
  the selected document-deletion policy.

**Implementation detail:**

- In `schema.sql`, add `message_source_refs` with its composite assistant
  message scope foreign key and, when present, a composite source-user-message
  scope foreign key.  Keep `document_id` as a real foreign key rather than a
  polymorphic string.
- Add `page_number` for PDF chunks and retain/extend the existing
  `start_line`, `end_line`, `symbol_name`, and any needed `section_path` fields
  for text-document locators.  Do not add a generic source ledger table.
- Add a dedicated `SourceReferenceWriter`/`SourceReferenceReader` beside the
  existing document reader/writer boundary, then expose only narrow methods
  from `KnowledgeStore`: write refs for one assistant message, read refs for
  one message, and read refs for one episode.
- Use `idempotency_key`, not a nullable tool-call unique key.  Derive it from
  `agent_run_id + origin + position`, where origin is either a tool call or a
  user-message character span.
- Write real-Postgres tests before agent integration; fake SQL matchers alone
  are not sufficient for composite foreign-key and JSON constraint coverage.

**Exit condition:** A test can persist and read each source-reference kind for
an assistant message without touching document indexing or agent execution.

### Chunk 2 — Produce reliable document locators during indexing

**Goal:** Every supported uploaded-document result has an exact, displayable
location before it reaches the agent.

- Replace flattened PDF indexing with page-by-page extraction and chunks that
  never cross a page.
- Preserve line/section ranges for TXT, Markdown, CSV, and source-code chunks.
- Carry locator fields through `DocumentChunk`, document writer, reader, and
  search projections.
- Reindex existing documents as a clean unreleased-system migration.

**Implementation detail:**

- Replace PDF's current joined-page `extract_text()` path for indexing with a
  structured page result.  Each page has one-based `page_number`, extracted
  text, and page-local chunks; the splitter is invoked separately for each
  page so a chunk cannot span pages.
- Extend `DocumentChunk` with the locator fields needed by its source type and
  make the document writer copy them into `document_chunks`.  Readers must
  select those fields in both exact reads and vector-search results.
- For plain text and Markdown, calculate stable one-based line ranges.  For
  Markdown, include the active heading path when available.  For CSV, use
  one-based data-row ranges.  For source code, retain the existing symbol and
  line metadata and ensure fallback chunks also have line ranges.
- Change `read_document` so it returns a source locator that matches the text
  it actually returned.  Do not label a cross-page/cross-section line range
  with one misleading location; split the result or return multiple source
  candidates.
- Because the system is unreleased, replace the old flattened representation
  and reindex rather than maintain a dual path or compatibility adapter.

**Exit condition:** Search results for fixtures return the correct PDF page or
text-document line/section/row/symbol locator and the exact chunk text.

### Chunk 3 — Add explicit document selection

**Goal:** Let a user target one uploaded document without placing all of its
text in the prompt.

- Parse one `/relative/path` command from a user request.
- Resolve it through existing project/session visibility rules.
- Reject missing and ambiguous paths.
- Pass the resolved document ID as the request's document focus and preserve
  the request's remaining natural-language text.

**Implementation detail:**

- Parse the command at request ingress, before agent orchestration, and carry
  a typed `document_focus` in request/agent context.  Do not make the model
  parse a path from prose or trust a client-supplied document ID.
- Match normalized `relative_path` values only within the current project and
  session visibility scope.  Preserve the service's existing rule that an
  ambiguous path requires a document ID/selection rather than guessing.
- Remove only the command token from the user query.  Preserve surrounding
  punctuation and the remaining natural-language request, then add a compact
  instruction that the selected document is the active document focus.
- Keep the first version intentionally narrow: one path, no glob patterns, no
  parent-directory traversal, no folder selection, and no URL syntax.
- Add parser tests for quoted paths with spaces, paths at the beginning/middle/
  end of a request, duplicate paths, invisible documents, and malformed
  commands.

**Exit condition:** A focused request can only search/read its selected visible
document; a malformed path cannot silently select another document.

### Chunk 4 — Make document tool results source-ready

**Goal:** Give the executor a consistent source-candidate payload for uploaded
documents.

- Have `search_documents` and `read_document` return document ID, content hash,
  source kind, locator, excerpt, and display metadata.
- Exclude metadata-only, empty, error, and locator-less document results.
- Add tests that the returned payload exactly matches the stored chunk/read
  range rather than reconstructed text.

**Implementation detail:**

- Add a compact machine-readable `source_context` payload to successful tool
  result items.  It carries `source_kind`, document identity/version, locator,
  excerpt, and display metadata; it is not a persisted source-reference ID.
- For `search_documents`, use the stored chunk text and its stored locator.
  Do not re-extract the document or search for the chunk text a second time.
- For `read_document`, emit one candidate per returned location if a bounded
  read spans multiple pages/sections.  The user-facing content may be joined,
  but source candidates must stay location-correct.
- Keep source-context payloads small and bounded by the returned tool text.
  They should be available to the executor for capture and may be available to
  the model for citation-aware wording, but must not cause a second full copy
  of every document chunk to accumulate in prompt state.
- Add negative contracts for unsupported document types and for an old chunk
  that lacks the new locator fields; the clean reindex must fix those before a
  source candidate is emitted.

**Exit condition:** A successful supported-document tool result can be turned
directly into a `message_source_refs` candidate.

### Chunk 5 — Add pasted-text and search-result source adapters

**Goal:** Cover the remaining selected source families without fetching web
pages or treating ordinary conversation as a source attachment.

- Identify explicit pasted text in the current user message and create a
  candidate with the canonical user `message_id`, character span, excerpt, and
  exact-payload hash.
- Normalize successful `web_search` and `news_search` results into candidates
  containing provider, query, rank, title, URL, exact snippet, and hash.
- Label those candidates as discovery snippets; they must never claim that
  Knoggin read the linked page.

**Implementation detail:**

- Obtain the canonical user `message_id` from the persisted incoming turn;
  candidate spans must point into that stored content, not a rewritten prompt.
  Store only the selected span as `excerpt` and its offsets as the locator.
- Prefer a structured `pasted_text_spans` input from the client.  The fallback
  parser may recognize only clearly delimited blocks and must not classify an
  ordinary sentence such as “the report says…” as pasted source text.
- Normalize each provider result at the search-tool boundary into the same
  fields regardless of provider: provider name, original query, one-based
  rank, title, URL, exact provider snippet, and a hash of that payload.
- Mark web/news candidates in metadata as `discovery_snippet`.  Never fetch,
  canonicalize, summarize, or call the linked URL in this chunk.
- Treat provider `No Results`, configuration notices, blank URLs, and error
  objects as non-sources even when they are formatted like a normal result.

**Exit condition:** Pasted text and successful provider results create valid
candidates, while ordinary messages, no-results, and provider errors do not.

### Chunk 6 — Carry candidates through agent execution into the final message

**Goal:** Persist only the sources consulted by a completed response.

- Accumulate eligible candidates in the agent context while preserving
  `run_id`, `ToolCall.call_id`, and result position.
- Include candidates in the final response event.
- Pass them to `SessionContext.add_assistant_turn` and write
  `message_source_refs` after the canonical assistant `message_id` exists.
- Make message and reference persistence atomic, or fail the response
  persistence visibly rather than returning a false “sources consulted” state.

**Implementation detail:**

- Add a dedicated source-candidate collection to `AgentContext`/`AgentState`;
  do not overload `RetrievedEvidence`, whose current role is prompt context and
  may trim or rank items differently.
- Change the post-tool-result seam to receive the full `ToolCall`, not only a
  tool name and result.  Candidate creation needs the immutable `call_id`,
  arguments such as the original web query, and result position.
- Record candidates only after a successful tool result.  Preserve tool order
  and result position so the final list is reproducible and its idempotency
  key is stable.
- Seed the candidate collection with explicit pasted-text candidates before
  tool execution.  A final response may therefore have pasted-text context
  even when no search tool was called.
- Use a separate `sources_consulted` field in the final response event rather
  than reusing the executor's existing web-search `sources` field.  That field
  is not a complete answer-context audit trail.
- At assistant-message persistence, write the message and its references in a
  single knowledge-store transaction.  If a retry repeats the handoff, the
  idempotency key must make the write safe.

**Exit condition:** One completed agent response persists exactly its eligible
source candidates once; an abandoned/failed run persists none.

### Chunk 7 — Expose answer and episode source context

**Goal:** Make the feature useful to callers without changing the episode
model.

- Add a core read method for an assistant message and its ordered
  `sources_consulted` list.
- Extend episode reads/tool output to derive and deduplicate source references
  through `episode_messages`.
- Include the contributing assistant `message_id` in every episode reference
  so callers can return to the answer that consulted it.

**Implementation detail:**

- Return a stable presentation shape from the reader: source kind, display
  label, locator, excerpt, document ID or URL, source status, and contributing
  assistant message ID.  Do not expose raw storage JSON as the public shape.
- Define deduplication as matching source kind, stable identity/version,
  locator, and excerpt hash.  Preserve two references to the same document if
  they are different passages or came from different assistant messages.
- Add `get_message_source_refs(...)` and `get_episode_source_refs(...)` to the
  reader boundary.  The episode query must join through scoped
  `episode_messages`, not infer membership from timestamps or session history.
- Keep the existing `Episode` persistence contract unchanged.  Source context
  is an additional read projection and must not be sent to episode-generation
  prompts, embedding input, or consolidation decisions.
- Return an empty list for an answer/episode without source context.  Do not
  fabricate a citation from document focus or the user query.

**Exit condition:** An answer shows its sources, and an episode shows the union
of source references from its attached assistant messages without storing a
duplicate list on `episodes`.

### Chunk 8 — End-to-end hardening and rollout

**Goal:** Prove the entire source-context chain under real storage conditions.

- Run real-Postgres contracts for all source kinds, scope checks, retries,
  document/message deletion, and episode consolidation.
- Add one end-to-end test per source family from input/tool result to assistant
  message to episode read.
- Reindex the development database and verify a manual PDF, Markdown, pasted
  text, web-search, and news-search scenario.
- Confirm no source reference enters episode-generation prompts or changes
  episode contents.

**Implementation detail:**

- Run the storage contracts against real Postgres with the actual schema,
  especially the source-kind checks, scoped foreign keys, cascade behavior,
  nullable `tool_call_id`, and idempotency-key uniqueness.
- Add an agent-loop contract that makes two document calls, one web search, and
  one failed call, then asserts the final assistant message has only the three
  eligible source candidates in tool/result order.
- Add a retry contract for final-message persistence and an abandoned-run
  contract that proves no orphan references are written.
- Validate the UI/API handoff manually with a response containing all selected
  source kinds, ensuring web/news entries visibly say “search-result snippet”
  rather than implying page retrieval.
- Treat the reindex as a required deployment step in this unreleased system;
  do not ship a runtime fallback for locator-less legacy chunks.

**Exit condition:** The selected sources appear only as response/episode
context, with correct locators and labels, across the full persistence path.

---

## 9. Verification Contracts

### Document contracts

- A two-page PDF produces chunks whose `page_number` is one or two.
- No PDF chunk spans pages.
- A `search_documents` result returns the correct page, document ID, hash, and
  exact chunk excerpt.
- A `read_document` PDF result returns page-aware content; a line range must
  not be labeled with a page unless the service can prove the mapping.
- Text-document search/read returns its correct line, row, symbol, or section
  locator with the exact excerpt.
- `/relative/path` resolves only a visible document, preserves the user's
  remaining request text, and rejects missing or ambiguous paths.

### Response-source contracts

- A successful supported-document search/read in a completed run creates
  references for the final assistant message.
- Explicit pasted text creates a reference to its user message and exact
  character span.
- A successful web/news search result creates a discovery-snippet reference
  with its provider, query, rank, URL, title, and exact returned snippet.
- Error, empty, and metadata-only results create none.
- The same run/tool call/result position cannot create a duplicate reference.
- A response that does not consult a supported source has an empty source list,
  not a fake
  citation.
- A source reference cannot cross project or session boundaries.

### Episode contracts

- Reading an episode returns the references of its attached assistant messages.
- References remain linked to the response message that used them.
- Rebuilding or consolidating an episode does not duplicate source references.
- Deleting a message or document follows the selected privacy/retention policy.

---

## 10. Deliberate Follow-ups

Add these only when the selected source-context flow is useful in production:

1. DOCX paragraph/heading references. **Implemented as an add-on:** DOCX body
   chunks retain one-based paragraph ranges and built-in Word heading paths;
   tables, headers/footers, comments, and text boxes remain out of scope.
2. Explicit URL attachment: validate, safely fetch, and retain a bounded web
   snapshot before allowing it to become a `web_page` source reference.
3. Images and OCR-derived source locations.
4. Explicit model-selected citations for individual claims.
5. A generic cross-source provenance ledger if several source types need common
   revision, segment, and encounter semantics.

None of these is a prerequisite for the first release.

---

## 11. Success Condition

The release is successful when a user can open an agent answer or an episode,
see the consulted document passages, explicit pasted text, and clearly labeled
web/news discovery snippets for each response, and open enough context to
understand where that information came from—without changing episodic memory
or pretending the references prove the answer is correct.
