# Source Integration Design

**Status**: Design Proposal  
**Date**: 2026-06-23  
**Updated**: 2026-07-02
**Purpose**: Extend ingestion beyond conversational messages so Knoggin can record and process messages, documents, and webhook-driven inputs through one provenance-aware source model.

---

## Executive Summary

Knoggin currently treats conversational messages as the primary ingestion unit.
This proposal generalizes that boundary without turning the core memory system
into a large event-sourcing framework.

The main shift is:

```text
message-first ingestion
```

to:

```text
source-first ingestion
```

Supported source types should start small:

```text
message
doc
webhook
```

These three are enough to cover the current product direction:

- `message`: normal chat/conversation input.
- `doc`: uploaded PDFs, DOCX files, and extracted document chunks.
- `webhook`: external automation, API callbacks, polling jobs, custom app events, game moves, and provider-specific integrations.

The core principle is:

```text
Sources audit what Knoggin saw.
Extraction creates observations and memory candidates.
Facts represent current active memory.
Audit explains how memory changed.
```

This keeps the ingestion layer simple. The generic source record should not
carry heavy semantic responsibility. It should preserve provenance, scope,
content, timing, and metadata so later extraction, correction, and audit flows
can explain where memory came from.

---

## Current Architecture

### Message Flow

```text
User Message -> Buffer -> BatchConsumer -> TextProcessor -> EntityManager -> Graph
                                |
                         extract_mentions()
                         (msg_id, name, type, topic)
```

### Current Message-Specific Identifier

`message_id` is currently used for several jobs:

- Primary message identity: `(user_name, session_id, message_id)` in the `messages` table.
- Source attribution: `EntityRecord.msg_id`, `ConnectionRecord.msg_id`, and `FactRecord.source_msg_id`.
- Batch tracking: `ExtractionTrace.message_ids`.
- Evidence grounding: relationship evidence references link relationships to source messages.

The extraction code mostly treats message IDs as opaque identifiers. That makes
generalizing the identifier practical.

---

## Proposed Architecture

### Core Model

Introduce a small source/event abstraction used by all ingestion paths.

```python
source_event_id: str
source_id: str  # extraction-facing alias for source_event_id during migration
source_type: Literal["message", "doc", "webhook"]
event_type: str
```

Examples:

```text
source_type = "message"
event_type = "message.created"

source_type = "doc"
event_type = "doc.chunk.processed"

source_type = "webhook"
event_type = "stripe.invoice.paid"

source_type = "webhook"
event_type = "github.issue.closed"

source_type = "webhook"
event_type = "game.move.applied"
```

The broad shape:

```text
Message / Doc / Webhook
        |
        v
Source record
        |
        v
Extraction pipeline
        |
        v
Entities / Relationships / Facts / Audit
```

Use `source_event_id` as the durable database reference. `source_id` can remain
as an extraction-facing name while the pipeline is being renamed from
message-oriented inputs to source-oriented inputs.

### Session As Process Context

Sessions should remain important. A session can represent a user conversation,
a document ingestion run, or an automated webhook process.

```text
Project
  Session A: user chat with Knoggin
  Session B: webhook automation run
  Session C: document ingestion run
```

This keeps automation and interaction cleanly separated while still allowing
them to share project-visible memory when permitted.

Minimum scope for source records:

```text
user_name
project_id
session_id
source_type
visibility / visible_project scope where applicable
```

---

## Source Record

The generic source record should be small and provenance-focused.

```sql
CREATE TABLE source_events (
    id UUID PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    event_type TEXT NOT NULL,
    external_id TEXT,
    idempotency_key TEXT,
    content TEXT,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    occurred_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    content_hash TEXT,
    CONSTRAINT source_events_source_type_check CHECK (
        source_type IN ('message', 'doc', 'webhook')
    )
);
```

Recommended indexes:

```sql
CREATE INDEX source_events_scope_idx
ON source_events(user_name, project_id, session_id, observed_at DESC);

CREATE INDEX source_events_type_idx
ON source_events(user_name, project_id, source_type, event_type);

CREATE UNIQUE INDEX source_events_idempotency_idx
ON source_events(user_name, project_id, source_type, idempotency_key)
WHERE idempotency_key IS NOT NULL;
```

Notes:

- `observed_at` is when Knoggin saw the input.
- `occurred_at` is when the source claims the event happened, if known.
- `external_id` is the provider's ID, such as a Stripe event ID or GitHub delivery ID.
- `idempotency_key` prevents duplicate webhook/polling ingestion.
- `metadata` carries source-specific details without widening the core schema.

---

## Source Types

### `message`

Messages are the current baseline. Existing messages can be represented as
source records with:

```text
source_type = "message"
event_type = "message.created"
content = message content
metadata.role = user | assistant | system
metadata.message_id = existing message id
```

The existing `messages` table can remain the durable conversation table. The
source event gives the extraction layer a generic provenance pointer.

### `doc`

Documents need more provenance than a single `source_id` string can express.
The document path should preserve the hierarchy:

```text
document file
  -> document chunk
    -> source event
      -> extracted observation/fact evidence
```

Recommended durable records:

```sql
CREATE TABLE documents (
    id UUID PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    file_name TEXT NOT NULL,
    file_type TEXT NOT NULL,
    storage_uri TEXT,
    content_hash TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE document_chunks (
    id UUID PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES documents(id),
    chunk_index INT NOT NULL,
    content TEXT NOT NULL,
    page_start INT,
    page_end INT,
    char_start INT,
    char_end INT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(document_id, chunk_index)
);
```

For each chunk processed for memory extraction, create a source event:

```text
source_type = "doc"
event_type = "doc.chunk.processed"
content = chunk content
metadata.document_id = document id
metadata.chunk_id = chunk id
metadata.chunk_index = chunk index
metadata.page_start = page start if available
metadata.page_end = page end if available
```

This avoids making `doc_abc_chunk_0` the entire provenance story. The chunk is
the extraction unit, but the original document remains the durable source.

### `webhook`

Webhook is intentionally broad. It should cover:

- Provider webhooks: Stripe, GitHub, Slack, Linear, etc.
- Custom automation calls.
- Polling jobs that normalize external state into events.
- Game or simulation actions.
- Internal app integrations.

Examples:

```json
{
  "source_type": "webhook",
  "event_type": "stripe.invoice.paid",
  "external_id": "evt_123",
  "idempotency_key": "stripe:evt_123",
  "content": "Invoice in_456 was paid.",
  "metadata": {
    "provider": "stripe",
    "invoice_id": "in_456",
    "raw_payload_ref": "..."
  }
}
```

```json
{
  "source_type": "webhook",
  "event_type": "game.move.applied",
  "external_id": "move_89",
  "idempotency_key": "game:session_7:move_89",
  "content": "Player A captured the east tower.",
  "metadata": {
    "provider": "custom",
    "game_id": "session_7",
    "turn": 12
  }
}
```

Webhook ingestion should require duplicate protection from the start because
providers retry deliveries and polling jobs may observe the same external
object repeatedly.

---

## Extraction Integration

### Updated Flow

```text
Source event -> BatchConsumer -> TextProcessor -> EntityManager -> Graph
                                |
                         extract_mentions()
                         (source_id, name, type, topic)
```

### Prompt and Processor Terminology

Update prompt and processor terminology from messages to sources where the code
is operating on extraction inputs.

```diff
- msg_id MUST be one of the message IDs shown as [MSG <id>] in the input.
+ source_id MUST be one of the source IDs shown as [SOURCE <id>] in the input.
```

```diff
- Messages section
+ Sources section
```

The extraction input formatter can render source-specific labels:

```python
def format_source_input(sources: list[dict]) -> str:
    lines = []

    for src in sources:
        source_id = src["source_id"]
        source_type = src["source_type"]
        content = src["content"]

        if source_type == "message":
            role = src.get("metadata", {}).get("role", "user").upper()
            lines.append(f"[SOURCE {source_id}] [MESSAGE:{role}]: {content}")
        elif source_type == "doc":
            lines.append(f"[SOURCE {source_id}] [DOC]: {content}")
        else:
            event_type = src.get("event_type", "webhook")
            lines.append(f"[SOURCE {source_id}] [WEBHOOK:{event_type}]: {content}")

    return "\n".join(lines)
```

### Records Produced By Extraction

Entity, relationship, and fact records should use generic source attribution:

```python
class EntityRecord(Entity):
    source_event_id: str
    source_type: str
    msg_id: int | None = None

class ConnectionRecord(Connection):
    source_event_id: str
    source_type: str
    msg_id: int | None = None

class FactRecord(Fact):
    source_event_id: str
    source_type: str
    source_msg_id: int | None = None
```

For a transitional implementation, `source_id` can be used instead of
`source_event_id`, but the durable target should be an actual source event
identifier rather than an encoded string.

---

## Document Chunking

Documents should be chunked for extraction separately from RAG retrieval.

Recommended baseline:

```python
from llama_index.core.node_parser import SentenceSplitter


def chunk_document_for_extraction(
    text: str,
    document_id: str,
    chunk_size: int = 200,
    overlap: int = 30,
) -> list[dict]:
    splitter = SentenceSplitter(
        chunk_size=chunk_size,
        chunk_overlap=overlap,
        paragraph_separator="\n\n",
        secondary_chunking_regex="[.!?]\\s+",
    )

    chunks = splitter.split_text(text)

    return [
        {
            "document_id": document_id,
            "chunk_index": i,
            "content": chunk,
            "metadata": {},
        }
        for i, chunk in enumerate(chunks)
    ]
```

Recommended parameters:

```python
# RAG / semantic document search
SentenceSplitter(chunk_size=512, chunk_overlap=50)

# Entity/fact extraction
SentenceSplitter(chunk_size=200, chunk_overlap=30)
```

The smaller extraction chunks reduce unrelated context and make source
attribution cleaner. Overlap helps preserve entities or facts that cross chunk
boundaries.

Document metadata can be represented as a synthetic chunk:

```text
event_type = "doc.metadata.processed"
metadata.chunk_kind = "metadata"
```

---

## Document Ingestion Flow

```python
async def ingest_document_for_extraction(
    file_path: str,
    file_name: str,
    user_name: str,
    session_id: str,
    project_id: str,
) -> BatchResult:
    """
    Ingest a document and process its chunks through the normal extraction
    pipeline with document-level provenance preserved.
    """
    from markitdown import MarkItDown

    md = MarkItDown()
    text = md.convert(file_path).text_content

    document = await document_writer.create_document(
        user_name=user_name,
        project_id=project_id,
        session_id=session_id,
        file_name=file_name,
        file_type="pdf",
        content_hash=hash_text(text),
    )

    chunks = chunk_document_for_extraction(
        text=text,
        document_id=document["id"],
        chunk_size=200,
        overlap=30,
    )

    chunk_rows = await document_writer.create_chunks(document["id"], chunks)

    sources = []
    for chunk in chunk_rows:
        source_event = await source_event_writer.create_source_event(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            source_type="doc",
            event_type="doc.chunk.processed",
            content=chunk["content"],
            metadata={
                "document_id": document["id"],
                "chunk_id": chunk["id"],
                "chunk_index": chunk["chunk_index"],
                "file_name": file_name,
                "page_start": chunk.get("page_start"),
                "page_end": chunk.get("page_end"),
            },
        )

        sources.append(
            {
                "source_id": source_event["id"],
                "source_event_id": source_event["id"],
                "source_type": "doc",
                "event_type": "doc.chunk.processed",
                "content": chunk["content"],
                "metadata": source_event["metadata"],
            }
        )

    return await processor.run(
        sources=sources,
        session_text="",
        session_id=session_id,
    )
```

Implementation can keep the current `messages=` parameter temporarily, but the
target API should accept `sources=`.

---

## Webhook Ingestion Flow

Webhook ingestion should normalize arbitrary external payloads into source
events, then optionally send a text representation through extraction.

```python
async def ingest_webhook_event(
    *,
    user_name: str,
    project_id: str,
    session_id: str,
    provider: str,
    event_type: str,
    external_id: str,
    content: str,
    payload: dict,
) -> dict:
    source_event = await source_event_writer.create_source_event(
        user_name=user_name,
        project_id=project_id,
        session_id=session_id,
        source_type="webhook",
        event_type=event_type,
        external_id=external_id,
        idempotency_key=f"{provider}:{external_id}",
        content=content,
        metadata={
            "provider": provider,
            "payload": payload,
        },
    )

    return await processor.run(
        sources=[
            {
                "source_id": source_event["id"],
                "source_event_id": source_event["id"],
                "source_type": "webhook",
                "event_type": event_type,
                "content": content,
                "metadata": source_event["metadata"],
            }
        ],
        session_text="",
        session_id=session_id,
    )
```

For v1, webhook extraction can be conservative. Knoggin does not need a complex
source ranking system. Contradictions can be corrected through normal user/admin
memory correction flows.

---

## What Works With Minimal Changes

### Extraction Prompts Are Mostly Source-Agnostic

The entity and relationship prompts mostly need an ID for evidence grounding.
They can use `source_id` instead of `msg_id`.

### Processing Logic Is ID-Based

`TextProcessor.extract_mentions()` currently returns tuples shaped like:

```python
(msg_id, name, type, topic)
```

The target shape is:

```python
(source_id, name, type, topic)
```

The current processing uses IDs for:

- validation against the set of provided IDs;
- grouping extracted spans by input;
- tracking which source produced an entity or relationship;
- linking evidence back to the input.

Those operations work with UUID/string source IDs.

### Message-Specific Assumptions Are Limited

The current pipeline does not appear to rely on:

- arithmetic on IDs;
- sequential message ordering for identity;
- message-only metadata during core extraction.

That makes a gradual migration practical.

---

## Important Boundaries

### Source Events Are Not Semantic Truth

A source event should not claim too much.

It means:

```text
Knoggin observed this input from this source in this scope.
```

It does not mean:

```text
Everything in this input is true.
```

Facts still need the existing correction, invalidation, and audit paths.

### Do Not Collapse Document Provenance

Avoid making this the whole provenance story:

```text
source_id = "doc_abc_chunk_0"
```

Prefer:

```text
source_event_id -> document_chunk_id -> document_id
```

That allows later citation UI to show:

```text
Contract.pdf, chunk 12, page 4 if available.
```

### Webhook Needs Idempotency

Webhook providers retry. Polling jobs can re-observe the same external object.
Duplicate protection is part of the minimum viable design, not an optimization.

---

## Potential Issues And Solutions

### Session Context For Non-Message Inputs

Problem: Documents and webhooks do not have conversational context.

Solution: Use the session as process context and pass empty `session_text` for
standalone ingestion runs.

### Mixed Source Batches

Problem: Can messages, docs, and webhooks be processed in the same batch?

Solution: The pipeline can support this if `source_id` values are unique.
Start with separate batches for operational simplicity.

### Entity Resolution Across Sources

Problem: The same entity can appear in messages, documents, and webhooks.

Solution: Existing entity resolution should handle this, but provenance should
preserve which source introduced or supported each observation.

### Fact Source Attribution

Problem: Facts need to reference sources beyond messages.

Solution: Add `source_event_id` or transitional `source_id` to facts,
relationships, and entity evidence records. Keep message-specific fields only
for migration.

### Document Updates

Problem: A document can be replaced or uploaded in a newer version.

Solution: Treat a changed document as a new document record with its own chunks.
Later, add explicit supersession metadata if needed.

### Webhook Payload Size

Problem: Raw webhook payloads can be large or sensitive.

Solution: Store a normalized text `content` for extraction and keep raw payloads
behind a reference or filtered metadata policy.

---

## Testing Strategy

### Unit Tests

```python
def test_source_event_accepts_supported_types():
    assert SourceType.MESSAGE == "message"
    assert SourceType.DOC == "doc"
    assert SourceType.WEBHOOK == "webhook"


def test_chunk_document_preserves_document_reference():
    text = "Sentence one. Sentence two. Sentence three."
    chunks = chunk_document_for_extraction(text, "doc-123", chunk_size=50)

    assert len(chunks) > 0
    assert all(c["document_id"] == "doc-123" for c in chunks)
    assert all("chunk_index" in c for c in chunks)


async def test_extract_mentions_with_doc_source():
    sources = [
        {
            "source_id": "source-1",
            "source_type": "doc",
            "event_type": "doc.chunk.processed",
            "content": "Alice works at Apple.",
            "metadata": {"document_id": "doc-123", "chunk_index": 0},
        }
    ]

    mentions = await processor.extract_mentions(user_name, sources, session_id)

    assert ("source-1", "Alice", "person", "General") in mentions
    assert ("source-1", "Apple", "organization", "General") in mentions


async def test_webhook_idempotency():
    first = await source_event_writer.create_source_event(
        user_name=user_name,
        project_id=project_id,
        session_id=session_id,
        source_type="webhook",
        event_type="stripe.invoice.paid",
        idempotency_key="stripe:evt_123",
        content="Invoice in_456 was paid.",
    )

    second = await source_event_writer.create_source_event(
        user_name=user_name,
        project_id=project_id,
        session_id=session_id,
        source_type="webhook",
        event_type="stripe.invoice.paid",
        idempotency_key="stripe:evt_123",
        content="Invoice in_456 was paid.",
    )

    assert second["id"] == first["id"]
```

### Integration Tests

```python
async def test_document_ingestion_flow_preserves_provenance():
    test_pdf = create_test_pdf("Alice met Bob at the Louvre.")

    result = await ingest_document_for_extraction(
        test_pdf,
        "test.pdf",
        user_name,
        session_id,
        project_id,
    )

    assert result.success

    facts = await knowledge_store.list_facts_for_project(
        user_name=user_name,
        project_id=project_id,
    )

    assert all(f["source_event_id"] for f in facts)


async def test_webhook_ingestion_flow_extracts_from_normalized_content():
    result = await ingest_webhook_event(
        user_name=user_name,
        project_id=project_id,
        session_id=session_id,
        provider="custom",
        event_type="game.move.applied",
        external_id="move-1",
        content="Player A captured the east tower.",
        payload={"move_id": "move-1"},
    )

    assert result.success
```

---

## Migration Path

### Step 1: Add Source Events

Add `source_events` and start writing message source events for new messages.

### Step 2: Add Generic Source Attribution

Add nullable generic source columns:

```sql
ALTER TABLE entities ADD COLUMN source_event_id UUID;
ALTER TABLE relationships ADD COLUMN source_event_id UUID;
ALTER TABLE facts ADD COLUMN source_event_id UUID;
```

Keep current message-specific fields during the transition.

### Step 3: Backfill Existing Messages

For existing messages, create `source_events` rows with:

```text
source_type = "message"
event_type = "message.created"
metadata.message_id = old message id
```

Then backfill entity, relationship, and fact source references where possible.

### Step 4: Rename Pipeline Concepts

Gradually rename internal extraction inputs:

```text
messages -> sources
msg_id -> source_id / source_event_id
message_ids -> source_ids
```

Avoid broad compatibility shims if the system remains unreleased, but keep the
changes staged enough to verify behavior.

### Step 5: Add Document And Webhook Adapters

Add adapters that normalize each input type into source events:

```text
message adapter
doc adapter
webhook adapter
```

Each adapter should be thin. The extraction and memory layers should not need
provider-specific logic.

---

## Performance Considerations

### Document Chunking

- LlamaIndex `SentenceSplitter` is already available and appropriate for v1.
- Cache chunked documents if reprocessing becomes common.
- Keep extraction chunk size smaller than RAG chunk size.

### Webhook Volume

- Use idempotency keys to avoid duplicate extraction.
- Consider queueing webhook extraction when provider volume is high.
- Store normalized content separately from raw payloads.

### Storage

- `source_events` will grow faster than active facts.
- Index by scope, source type, event type, and idempotency key.
- Keep raw source/event history separate from active memory retrieval paths so
  agents do not drown in low-level event noise.

---

## Open Questions

1. **Should source events replace message IDs everywhere or only extraction evidence?**
   Recommendation: start with extraction evidence and fact attribution, then
   expand if the model holds.

2. **Should document chunks be used for both RAG and extraction?**
   Recommendation: keep separate chunking profiles. RAG chunks can be larger;
   extraction chunks should be smaller and more provenance-focused.

3. **How should citation UI render document evidence?**
   Recommendation: render file name and chunk/page metadata from
   `document_chunks`, not from encoded source IDs.

4. **Should webhook sources automatically create facts?**
   Recommendation: only for conservative/simple cases. Otherwise store source
   events and allow extraction/correction flows to manage active memory.

5. **Should contradictions trigger ranking or conflict logic now?**
   Recommendation: no. Keep v1 simple. Let users/admins correct active memory
   through existing fact correction and audit flows.

---

## Timeline Estimate

| Phase | Tasks | Duration |
|-------|-------|----------|
| Phase 1 | Add `source_events`, model types, and message source writes | 1-2 days |
| Phase 2 | Add document/document_chunk provenance tables and chunking | 2-3 days |
| Phase 3 | Rename extraction inputs from messages to sources | 2-3 days |
| Phase 4 | Add document ingestion adapter and tests | 3-5 days |
| Phase 5 | Add webhook ingestion adapter, idempotency, and tests | 2-4 days |
| Testing | Integration tests and extraction quality checks | 2-3 days |

**Total: 12-20 days** for a production-ready first pass.

---

## Conclusion

The integration should be generalized beyond documents, but still kept small.

The target model is:

```text
Session = process/context
Source event = observed input
Source types = message | doc | webhook
Document = durable file
Document chunk = extraction/citation unit
Fact = active memory
Audit = why memory changed
```

This preserves the low-complexity advantage of the original proposal while
fixing the biggest gap: document chunks should not be the whole provenance
story. They should be extraction units that point back to durable document
records, and extracted memory should point back to source events.
