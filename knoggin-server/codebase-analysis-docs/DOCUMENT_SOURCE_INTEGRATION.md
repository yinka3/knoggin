# Document Source Integration Design

**Status**: Design Proposal  
**Date**: 2026-06-23  
**Purpose**: Extend the ingestion pipeline to support document extraction (PDF, DOCX) alongside conversational messages

---

## Executive Summary

The system currently ingests conversational messages and extracts entities/relationships from them. This design extends the pipeline to support **document sources** (PDFs, DOCX files) by:

1. **Replacing `message_id` with `source_id`** - A unified identifier for both messages and document chunks
2. **Using LlamaIndex SentenceSplitter** - Leverage existing dependency for document chunking
3. **Minimal core changes** - The extraction pipeline is already source-agnostic

**Integration Complexity**: ✅ **LOW** - Mostly additive changes, no major refactoring needed

---

## Current Architecture

### Message Flow
```
User Message → Buffer → BatchConsumer → TextProcessor → EntityManager → Graph
                                ↓
                         extract_mentions()
                         (msg_id, name, type, topic)
```

### Key Identifier: `message_id`
- **Primary Key**: `(user_name, session_id, message_id)` in `messages` table
- **Source Attribution**: Used in `EntityRecord.msg_id`, `ConnectionRecord.msg_id`, `FactRecord.source_msg_id`
- **Batch Tracking**: `ExtractionTrace.message_ids` tracks which messages were processed
- **Evidence Grounding**: `relationship_evidence_refs` links relationships to source messages

---

## Proposed Architecture

### Unified Source Model

```python
# New abstraction
source_id: str  # "msg_123" or "doc_abc_chunk_5"
source_type: Literal["message", "document"]

# Examples:
"msg_42"                    # Message with ID 42
"doc_abc123_chunk_0"        # Document chunk 0 from file abc123
"doc_xyz789_sent_12-15"     # Sentences 12-15 from file xyz789
```

### Extended Flow
```
Message OR Document → Buffer → BatchConsumer → TextProcessor → EntityManager → Graph
                                        ↓
                                 extract_mentions()
                                 (source_id, name, type, topic)
```

---

## Integration Analysis

### ✅ What Works Without Changes

#### 1. **Extraction Prompts Are Source-Agnostic**
- [`extract_entities.j2`](../src/common/templates/pipeline/extract_entities.j2) references `msg_id` only as an identifier
- [`extract_relationships.j2`](../src/common/templates/pipeline/extract_relationships.j2) same - just needs an ID for evidence grounding
- **Change needed**: Terminology only (`msg_id` → `source_id`)

#### 2. **Processing Logic Is ID-Based**
- [`TextProcessor.extract_mentions()`](../src/knoggin_server/ingestion/services/processor.py#L163-L500) returns tuples: `(msg_id, name, type, topic)`
- No message-specific calculations (no `msg_id + 1`, no ordering assumptions)
- Uses `msg_id` only for:
  - **Validation**: `if entity.msg_id not in valid_msg_ids` (set membership)
  - **Grouping**: `covered_texts[msg_id]` (dict keys)
  - **Tracking**: `entity_msg_map[ent_id].append(msg_id)` (list append)
- **All of these work identically with string `source_id`**

#### 3. **No Message-Specific Assumptions**
Checked for problematic patterns:
- ❌ No arithmetic on IDs
- ❌ No sequential ordering requirements
- ❌ No message-specific metadata extraction
- ✅ IDs are treated as opaque identifiers

---

## Implementation Plan

### Phase 1: Schema Extension (1-2 days)

#### Database Changes
```sql
-- Add source_id to existing tables
ALTER TABLE entities ADD COLUMN source_id TEXT;
ALTER TABLE relationships ADD COLUMN source_id TEXT;
ALTER TABLE facts ADD COLUMN source_id TEXT;

-- New table for document sources
CREATE TABLE document_sources (
    source_id TEXT PRIMARY KEY,
    file_id TEXT NOT NULL,
    chunk_index INT NOT NULL,
    content TEXT NOT NULL,
    user_name TEXT NOT NULL,
    session_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    timestamp_ms BIGINT,
    metadata JSONB
);

-- Backfill existing data
UPDATE entities SET source_id = 'msg_' || source_msg_id WHERE source_msg_id IS NOT NULL;
UPDATE relationships SET source_id = 'msg_' || message_id WHERE message_id IS NOT NULL;
UPDATE facts SET source_id = 'msg_' || source_msg_id WHERE source_msg_id IS NOT NULL;
```

#### Pydantic Model Changes
```python
# primitives.py
class EntityRecord(Entity):
    source_id: str  # New unified field
    msg_id: Optional[int] = None  # Deprecated, keep for migration
    
    @property
    def source_type(self) -> str:
        return "message" if self.source_id.startswith("msg_") else "document"

# Same for ConnectionRecord, FactRecord
```

---

### Phase 2: Document Chunking (2-3 days)

#### Simple Function Using LlamaIndex

```python
from llama_index.core.node_parser import SentenceSplitter
from typing import List, Dict

def chunk_document_for_extraction(
    text: str,
    file_id: str,
    chunk_size: int = 200,  # Tokens per chunk (smaller than RAG chunks)
    overlap: int = 30,      # Token overlap
) -> List[Dict]:
    """
    Chunk document into extraction-ready sources using LlamaIndex.
    
    Returns:
        List of source dicts compatible with existing pipeline:
        [{
            'source_id': 'doc_abc_chunk_0',
            'content': 'chunk text...',
            'file_id': 'abc',
            'type': 'document',
        }]
    """
    # Use LlamaIndex's sentence-aware splitter
    splitter = SentenceSplitter(
        chunk_size=chunk_size,
        chunk_overlap=overlap,
        paragraph_separator="\n\n",
        secondary_chunking_regex="[.!?]\\s+"  # Sentence boundaries
    )
    
    chunks = splitter.split_text(text)
    
    return [
        {
            'source_id': f"doc_{file_id}_chunk_{i}",
            'content': chunk,
            'file_id': file_id,
            'chunk_index': i,
            'type': 'document',
        }
        for i, chunk in enumerate(chunks)
    ]
```

#### Why LlamaIndex SentenceSplitter?
- ✅ Already in dependencies (used in FileRAG)
- ✅ Token-aware (respects actual token counts)
- ✅ Sentence-boundary aware (won't split mid-sentence)
- ✅ Handles edge cases (abbreviations, quotes, etc.)
- ✅ Battle-tested by thousands of projects

#### Recommended Parameters

```python
# For RAG (current FileRAG usage):
SentenceSplitter(chunk_size=512, chunk_overlap=50)
# Larger chunks for semantic search

# For Entity Extraction (new usage):
SentenceSplitter(chunk_size=200, chunk_overlap=30)
# Smaller chunks for focused extraction
# More overlap to catch cross-chunk entities
```

---

### Phase 3: Pipeline Integration (2-3 days)

#### Update TextProcessor

```python
# processor.py - extract_mentions()
async def extract_mentions(
    self,
    user_name: str,
    sources: List[Dict],  # Changed from 'messages'
    session_id: str,
    trace: Optional[ExtractionTrace] = None,
    issues: Optional[List[ValidationIssue]] = None,
) -> List[Tuple[str, str, str, str]]:  # Changed from (int, ...) to (str, ...)
    """
    Extracts entities from sources (messages or documents).
    Returns: List[(source_id, name, type, topic)]
    """
    # ... existing logic works with minimal changes
    # Just replace msg_id with source_id throughout
```

#### Update Prompt Input Formatting

```python
# core_utils.py - format_vp01_input()
def format_vp01_input(
    sources: List[Dict],  # Changed from 'messages'
    known_ents: List[Tuple[str, int]],
    gliner_ents: List[Tuple[str, str, str]],  # (source_id, span, label)
    ambiguous: List[Tuple[str, str, str, List[str]]],
    covered_texts: Dict[str, set],
    label_block: str,
) -> str:
    """Format sources for VP-01 extraction."""
    lines = []
    
    for src in sources:
        source_id = src.get('source_id') or f"msg_{src['id']}"
        source_type = src.get('type', 'message')
        
        if source_type == 'message':
            role = src.get('role', 'user').upper()
            lines.append(f"[SOURCE {source_id}] [{role}]: {src['content']}")
        else:  # document
            lines.append(f"[SOURCE {source_id}] [DOCUMENT]: {src['content']}")
    
    # ... rest of formatting logic
```

#### Update Prompt Templates

```diff
# extract_entities.j2
- msg_id MUST be one of the message IDs shown as [MSG <id>] in the input.
+ source_id MUST be one of the source IDs shown as [SOURCE <id>] in the input.

# extract_relationships.j2
- msg_id MUST be one of the message IDs shown in the Messages section.
+ source_id MUST be one of the source IDs shown in the Sources section.
```

---

### Phase 4: Document Ingestion Flow (3-5 days)

#### New Document Consumer

```python
async def ingest_document_for_extraction(
    file_path: str,
    file_id: str,
    user_name: str,
    session_id: str,
    project_id: str,
) -> BatchResult:
    """
    Ingest document and extract entities/relationships.
    Integrates with existing pipeline.
    """
    # 1. Extract text (using existing MarkItDown)
    from markitdown import MarkItDown
    md = MarkItDown()
    text = md.convert(file_path).text_content
    
    # 2. Chunk for extraction
    chunks = chunk_document_for_extraction(
        text=text,
        file_id=file_id,
        chunk_size=200,
        overlap=30
    )
    
    # 3. Format as sources (compatible with existing pipeline)
    sources = [
        {
            'id': chunk['source_id'],  # For backward compat
            'source_id': chunk['source_id'],
            'message': chunk['content'],  # Alias for 'content'
            'content': chunk['content'],
            'role': 'document',
            'type': 'document',
            'file_id': chunk['file_id'],
        }
        for chunk in chunks
    ]
    
    # 4. Process through existing BatchProcessor
    result = await processor.run(
        messages=sources,  # Existing parameter name
        session_text="",   # Documents don't have conversation context
        session_id=session_id
    )
    
    return result
```

---

## Potential Issues & Solutions

### Issue 1: Session Context for Documents
**Problem**: Documents don't have conversational context like messages do  
**Solution**: Pass empty `session_text=""` for document batches

### Issue 2: Mixed Batches (Messages + Documents)
**Problem**: Can we process messages and documents in the same batch?  
**Solution**: Yes! The pipeline doesn't care - just ensure `source_id` is unique across both

### Issue 3: Entity Resolution Across Sources
**Problem**: Same entity mentioned in message and document  
**Solution**: Existing `EntityManager` already handles this via semantic similarity

### Issue 4: Fact Source Attribution
**Problem**: Facts need to reference document chunks, not just messages  
**Solution**: `FactRecord.source_id` replaces `source_msg_id` - works for both

### Issue 5: Document Metadata Preservation
**Problem**: Need to track which file a chunk came from  
**Solution**: Store `file_id` in chunk metadata, link via `document_sources` table

---

## Testing Strategy

### Unit Tests
```python
def test_chunk_document():
    """Test document chunking produces valid sources."""
    text = "Sentence one. Sentence two. Sentence three."
    chunks = chunk_document_for_extraction(text, "test_file", chunk_size=50)
    
    assert len(chunks) > 0
    assert all('source_id' in c for c in chunks)
    assert all(c['source_id'].startswith('doc_test_file') for c in chunks)

def test_extract_mentions_with_documents():
    """Test entity extraction from document chunks."""
    sources = [
        {'source_id': 'doc_abc_chunk_0', 'content': 'Alice works at Apple.', 'type': 'document'}
    ]
    mentions = await processor.extract_mentions(user_name, sources, session_id)
    
    assert ('doc_abc_chunk_0', 'Alice', 'person', 'General') in mentions
    assert ('doc_abc_chunk_0', 'Apple', 'organization', 'General') in mentions
```

### Integration Tests
```python
async def test_document_ingestion_flow():
    """Test full document ingestion pipeline."""
    # Create test PDF
    test_pdf = create_test_pdf("Alice met Bob at the Louvre.")
    
    # Ingest
    result = await ingest_document_for_extraction(
        test_pdf, "test_doc", user_name, session_id, project_id
    )
    
    # Verify entities extracted
    assert result.success
    assert len(result.entity_ids) >= 3  # Alice, Bob, Louvre
    
    # Verify source attribution
    entities = await graph.get_entities(result.entity_ids)
    assert all(e['source_id'].startswith('doc_test_doc') for e in entities)
```

---

## Migration Path

### Step 1: Add `source_id` columns (nullable)
```sql
ALTER TABLE entities ADD COLUMN source_id TEXT;
-- Keep msg_id for backward compatibility
```

### Step 2: Backfill existing data
```sql
UPDATE entities SET source_id = 'msg_' || msg_id WHERE msg_id IS NOT NULL;
```

### Step 3: Update code to populate `source_id`
```python
# New records get source_id
entity = EntityRecord(
    source_id=f"msg_{msg_id}",  # or f"doc_{file_id}_chunk_{i}"
    msg_id=msg_id,  # Still populate for backward compat
    ...
)
```

### Step 4: Gradual deprecation of `msg_id`
- Phase 1: Both fields populated
- Phase 2: Code uses `source_id`, `msg_id` optional
- Phase 3: Remove `msg_id` (future)

---

## Performance Considerations

### Document Chunking
- **Cost**: Minimal - LlamaIndex is fast
- **Memory**: Chunks processed in batches (same as messages)
- **Optimization**: Cache chunked documents if re-processing

### Extraction
- **No change**: Same LLM calls as messages
- **Batch size**: Keep same limits (8 sources per batch)

### Storage
- **Minimal increase**: `source_id` is just a string
- **Index**: Add index on `source_id` for lookups

---

## Open Questions

1. **Should we support mixed message+document batches?**
   - **Recommendation**: Yes, but start with separate batches for simplicity

2. **How to handle document updates?**
   - **Recommendation**: Treat as new document (new `file_id`), mark old entities as stale

3. **Citation UI: How to show document sources to users?**
   - **Recommendation**: Store `file_id` + `chunk_index`, link back to original file

4. **Should we extract from document metadata (title, author)?**
   - **Recommendation**: Yes, treat metadata as a special chunk with `source_id = "doc_{file_id}_metadata"`

---

## Timeline Estimate

| Phase | Tasks | Duration |
|-------|-------|----------|
| **Phase 1** | Schema changes, model updates, backfill | 1-2 days |
| **Phase 2** | Document chunking function, testing | 2-3 days |
| **Phase 3** | Pipeline integration, prompt updates | 2-3 days |
| **Phase 4** | Document ingestion flow, end-to-end testing | 3-5 days |
| **Testing** | Integration tests, quality validation | 2-3 days |

**Total: 10-16 days** for full production-ready implementation

---

## Conclusion

The integration is **straightforward** because:
1. ✅ Extraction pipeline treats IDs as opaque identifiers
2. ✅ No message-specific logic in core processing
3. ✅ LlamaIndex already in dependencies
4. ✅ Prompts only need terminology changes

The hardest part is **document chunking strategy**, but using LlamaIndex's `SentenceSplitter` with appropriate parameters (200 tokens, 30 overlap) provides a solid starting point that can be tuned based on extraction quality.

**Recommendation**: Start with simple token-based chunking, monitor extraction quality, and iterate on chunk size if needed.