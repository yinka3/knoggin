// Behavioral port of server/src/core/knowledge/documents/indexer.py.
// Traits keep durable claims, extraction, embedding, and publication separate.
type IndexResult<T> = Result<T, String>;

#[derive(Clone, PartialEq, Eq)]
pub enum Status { Queued, Indexing, Indexed, Failed, Deleted }

#[derive(Clone)]
pub struct Document {
    pub id: String,
    pub project_id: String,
    pub relative_path: String,
    pub extension: String,
    pub content_hash: [u8; 32],
    pub status: Status,
    pub attempts: u32,
}

pub struct Chunk {
    pub content: String,
    pub language: String,
    pub symbol_name: Option<String>,
    pub start_line: usize,
    pub end_line: usize,
}

pub struct Extraction {
    pub chunks: Vec<Chunk>,
    pub snapshot: String,
}

pub trait Reader {
    fn find_document(&self, id: &str) -> IndexResult<Option<Document>>;
    fn find_by_path(&self, path: &str) -> IndexResult<Option<Document>>;
    fn list_unfinished(&self, project_id: &str) -> IndexResult<Vec<Document>>;
    fn list_queued(&self, project_id: &str, limit: usize) -> IndexResult<Vec<Document>>;
}

pub trait Writer {
    fn claim(&mut self, id: &str, force: bool, reset_attempts: bool) -> IndexResult<Option<Document>>;
    fn publish(&mut self, id: &str, chunks: &[Chunk], vectors: &[Vec<f32>], snapshot: &str, hash: [u8; 32]) -> IndexResult<Option<Document>>;
    fn requeue(&mut self, id: &str) -> IndexResult<()>;
    fn mark_failed(&mut self, id: &str, message: &str, retry_at: i64) -> IndexResult<()>;
}

pub trait SourceStore {
    fn read_bytes(&self, path: &str) -> IndexResult<Vec<u8>>;
    fn sha256(&self, source: &[u8]) -> [u8; 32];
    fn extract(&self, source: &[u8], extension: &str) -> IndexResult<Extraction>;
    fn reconcile(&mut self) -> IndexResult<()>;
}

pub trait Embedding { fn encode_batch(&mut self, inputs: &[String]) -> IndexResult<Vec<Vec<f32>>>; }
pub trait Policy { fn retry_time(&self, attempts: u32) -> i64; }

pub struct DocumentIndexerRealistic<R, W, S, E, P> {
    project_id: String,
    reader: R,
    writer: W,
    store: S,
    embedding: E,
    policy: P,
    stopping: bool,
}

impl<R: Reader, W: Writer, S: SourceStore, E: Embedding, P: Policy>
    DocumentIndexerRealistic<R, W, S, E, P>
{
    pub fn new(project_id: String, reader: R, writer: W, store: S, embedding: E, policy: P) -> Self {
        Self { project_id, reader, writer, store, embedding, policy, stopping: false }
    }

    pub fn index_document(&mut self, id: &str, force: bool, reset_attempts: bool) -> IndexResult<Document> {
        let metadata = self.reader.find_document(id)?.ok_or("Document not found")?;
        if metadata.project_id != self.project_id { return Err("Document not found".into()); }
        if metadata.status == Status::Indexed && !force { return Ok(metadata); }
        let claimed = match self.writer.claim(id, force, reset_attempts)? {
            Some(document) => document,
            None => return self.reader.find_document(id)?.ok_or("Document disappeared after claim".into()),
        };

        let source = self.store.read_bytes(&claimed.relative_path)?;
        if source.is_empty() { return Err("Document content is missing".into()); }
        let read_hash = self.store.sha256(&source);
        if read_hash != claimed.content_hash { return self.reconcile_stale_claim(claimed); }

        let extraction = self.store.extract(&source, &claimed.extension)?;
        Self::validate_chunk_boundaries(&extraction.chunks)?;
        let inputs = extraction.chunks.iter()
            .map(|chunk| Self::embedding_text(chunk, &claimed.relative_path))
            .collect::<Vec<_>>();
        let vectors = self.encode_embeddings(&inputs)?;
        Self::validate_embeddings(&vectors, &extraction.chunks)?;
        match self.writer.publish(id, &extraction.chunks, &vectors, &extraction.snapshot, read_hash)? {
            Some(published) => Ok(published),
            None => self.reconcile_stale_claim(claimed),
        }
    }

    pub fn reindex_document(&mut self, id: &str) -> IndexResult<Document> {
        self.index_document(id, true, true)
    }

    fn reconcile_stale_claim(&mut self, claimed: Document) -> IndexResult<Document> {
        self.store.reconcile()?;
        self.writer.requeue(&claimed.id)?;
        Ok(self.reader.find_by_path(&claimed.relative_path)?.unwrap_or(Document {
            status: Status::Deleted,
            ..claimed
        }))
    }

    fn encode_embeddings(&mut self, inputs: &[String]) -> IndexResult<Vec<Vec<f32>>> {
        let mut vectors = Vec::with_capacity(inputs.len());
        for batch in inputs.chunks(16) {
            let encoded = self.embedding.encode_batch(batch)?;
            if encoded.len() != batch.len() { return Err("Embedding service returned an incomplete batch".into()); }
            vectors.extend(encoded);
        }
        Ok(vectors)
    }

    fn validate_embeddings(vectors: &[Vec<f32>], chunks: &[Chunk]) -> IndexResult<()> {
        if vectors.len() != chunks.len() { return Err("Embedding count does not match chunk count".into()); }
        for vector in vectors {
            if vector.len() != 768 || vector.iter().any(|value| !value.is_finite()) {
                return Err("Document chunk embedding is invalid".into());
            }
        }
        Ok(())
    }

    fn validate_chunk_boundaries(chunks: &[Chunk]) -> IndexResult<()> {
        let mut previous_end = 0;
        for chunk in chunks {
            if chunk.start_line == 0 || chunk.end_line < chunk.start_line || chunk.start_line <= previous_end {
                return Err("Document chunk line ranges overlap or are invalid".into());
            }
            previous_end = chunk.end_line;
        }
        Ok(())
    }

    pub fn recover_pending_indexes(&mut self) -> IndexResult<usize> {
        let unfinished = self.reader.list_unfinished(&self.project_id)?;
        let mut requeued = 0;
        for document in unfinished {
            if document.status != Status::Indexing { continue; }
            let current = self.reader.find_document(&document.id)?;
            if !matches!(current, Some(ref item) if item.status == Status::Indexing) { continue; }
            self.writer.requeue(&document.id)?;
            requeued += 1;
        }
        Ok(requeued)
    }

    pub fn drain_pending_indexes(&mut self) -> IndexResult<()> {
        while !self.stopping {
            let queued = self.reader.list_queued(&self.project_id, 16)?;
            if queued.is_empty() { return Ok(()); }
            for document in queued {
                if self.stopping { return Ok(()); }
                if let Err(error) = self.index_document(&document.id, false, false) {
                    self.record_index_failure(&document, &error)?;
                }
            }
        }
        Ok(())
    }

    fn record_index_failure(&mut self, document: &Document, error: &str) -> IndexResult<()> {
        let message = error.chars().take(1000).collect::<String>();
        self.writer.mark_failed(&document.id, &message, self.policy.retry_time(document.attempts))
    }

    fn embedding_text(chunk: &Chunk, relative_path: &str) -> String {
        let symbol = chunk.symbol_name.as_deref().map(|name| format!("\nSymbol: {name}")).unwrap_or_default();
        format!("File: {relative_path}\nLanguage: {}{symbol}\n\n{}", chunk.language, chunk.content)
    }
}
