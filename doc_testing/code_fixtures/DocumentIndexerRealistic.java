import java.util.ArrayList;
import java.util.List;

// Behavioral port of server/src/core/knowledge/documents/indexer.py.
// Collaborators are interfaces so the indexing lifecycle remains visible.
public final class DocumentIndexerRealistic {
    public interface Reader {
        Document findDocument(String documentId);
        Document findByPath(String relativePath);
        List<Document> listUnfinished(String projectId);
        List<Document> listQueued(String projectId, int limit);
    }

    public interface Writer {
        Document claim(String documentId, List<String> allowedStatuses, boolean resetAttempts);
        Document publish(String documentId, List<Chunk> chunks, List<List<Double>> vectors, String hash);
        void requeue(String documentId);
        void markFailed(String documentId, String message, long retryAt);
    }

    public interface Embedding {
        List<List<Double>> encodeBatch(List<String> texts);
    }

    public interface SourceStore {
        byte[] readBytes(String relativePath);
        String sha256(byte[] content);
        Extraction extract(byte[] content, String extension);
    }

    public interface Policy {
        long retryTime(int attempts);
    }

    public static final class Document {
        private final String documentId, projectId, relativePath, extension, contentHash, status;
        private final int attempts;
        public Document(String documentId, String projectId, String relativePath,
                        String extension, String contentHash, String status, int attempts) {
            this.documentId = documentId;
            this.projectId = projectId;
            this.relativePath = relativePath;
            this.extension = extension;
            this.contentHash = contentHash;
            this.status = status;
            this.attempts = attempts;
        }
        public String documentId() { return documentId; }
        public String projectId() { return projectId; }
        public String relativePath() { return relativePath; }
        public String extension() { return extension; }
        public String contentHash() { return contentHash; }
        public String status() { return status; }
        public int attempts() { return attempts; }
    }

    public static final class Chunk {
        private final String content, language, symbolName;
        public Chunk(String content, String language, String symbolName) {
            this.content = content;
            this.language = language;
            this.symbolName = symbolName;
        }
        public String content() { return content; }
        public String language() { return language; }
        public String symbolName() { return symbolName; }
    }

    public static final class Extraction {
        private final List<Chunk> chunks;
        public Extraction(List<Chunk> chunks) { this.chunks = chunks; }
        public List<Chunk> chunks() { return chunks; }
    }

    private final String projectId;
    private final Reader reader;
    private final Writer writer;
    private final Embedding embedding;
    private final SourceStore sourceStore;
    private final Policy policy;
    private Runnable reconciliationCallback;

    public DocumentIndexerRealistic(String projectId, Reader reader, Writer writer,
                                    Embedding embedding, SourceStore sourceStore, Policy policy) {
        this.projectId = projectId;
        this.reader = reader;
        this.writer = writer;
        this.embedding = embedding;
        this.sourceStore = sourceStore;
        this.policy = policy;
    }

    public void setReconciliationCallback(Runnable callback, int intervalSeconds) {
        if (intervalSeconds < 10) throw new IllegalArgumentException("interval too short");
        this.reconciliationCallback = callback;
    }

    public Document indexDocument(String documentId, boolean force, boolean resetAttempts) {
        Document metadata = reader.findDocument(documentId);
        if (metadata == null || !metadata.projectId().equals(projectId)) {
            throw new IllegalArgumentException("Document not found");
        }
        if (metadata.status().equals("indexed") && !force) return metadata;

        List<String> allowed = force
            ? List.of("queued", "failed", "indexed") : List.of("queued", "failed");
        Document claimed = writer.claim(documentId, allowed, resetAttempts);
        if (claimed == null) return reader.findDocument(documentId);

        byte[] source = sourceStore.readBytes(claimed.relativePath());
        if (source == null) throw new IllegalStateException("Document content is missing");
        String readHash = sourceStore.sha256(source);
        if (!readHash.equals(claimed.contentHash())) {
            return reconcileStaleClaim(claimed);
        }
        Extraction extraction = sourceStore.extract(source, claimed.extension());
        List<String> inputs = new ArrayList<>();
        for (Chunk chunk : extraction.chunks()) {
            inputs.add(embeddingText(chunk, claimed.relativePath()));
        }
        List<List<Double>> vectors = encodeEmbeddings(inputs);
        validateEmbeddings(vectors, extraction.chunks());
        Document published = writer.publish(documentId, extraction.chunks(), vectors, readHash);
        return published == null ? reconcileStaleClaim(claimed) : published;
    }

    public Document reindexDocument(String documentId) {
        return indexDocument(documentId, true, true);
    }

    private Document reconcileStaleClaim(Document claimed) {
        if (reconciliationCallback == null) {
            throw new IllegalStateException("Document source changed without reconciliation");
        }
        reconciliationCallback.run();
        writer.requeue(claimed.documentId());
        Document refreshed = reader.findByPath(claimed.relativePath());
        return refreshed == null
            ? new Document(claimed.documentId(), projectId, claimed.relativePath(),
                           claimed.extension(), claimed.contentHash(), "deleted", claimed.attempts())
            : refreshed;
    }

    private List<List<Double>> encodeEmbeddings(List<String> inputs) {
        List<List<Double>> result = new ArrayList<>();
        for (int offset = 0; offset < inputs.size(); offset += 16) {
            List<String> batch = inputs.subList(offset, Math.min(offset + 16, inputs.size()));
            List<List<Double>> encoded = embedding.encodeBatch(batch);
            if (encoded.size() != batch.size()) {
                throw new IllegalStateException("Embedding service returned an incomplete batch");
            }
            result.addAll(encoded);
        }
        return result;
    }

    private void validateEmbeddings(List<List<Double>> vectors, List<Chunk> chunks) {
        if (vectors.size() != chunks.size()) {
            throw new IllegalStateException("Embedding count does not match chunk count");
        }
        for (List<Double> vector : vectors) {
            if (vector == null || vector.size() != 768) {
                throw new IllegalStateException("Document chunk embedding must have 768 dimensions");
            }
            for (Double value : vector) {
                if (value == null || !Double.isFinite(value)) {
                    throw new IllegalStateException("Document embedding contains a non-finite value");
                }
            }
        }
    }

    public int recoverPendingIndexes() {
        int recovered = 0;
        for (Document document : reader.listUnfinished(projectId)) {
            if (!document.status().equals("indexing")) continue;
            Document current = reader.findDocument(document.documentId());
            if (current == null || !current.status().equals("indexing")) continue;
            writer.requeue(document.documentId());
            recovered++;
        }
        return recovered;
    }

    public void drainPendingIndexes(boolean stopping) {
        while (!stopping) {
            List<Document> queued = reader.listQueued(projectId, 16);
            if (queued.isEmpty()) return;
            for (Document document : queued) {
                try {
                    indexDocument(document.documentId(), false, false);
                } catch (RuntimeException error) {
                    recordIndexFailure(document, error);
                }
            }
        }
    }

    private void recordIndexFailure(Document document, RuntimeException error) {
        long retryAt = policy.retryTime(document.attempts());
        String message = error.getMessage() == null ? error.getClass().getSimpleName()
                                                   : error.getMessage();
        writer.markFailed(document.documentId(), message.substring(0, Math.min(1000, message.length())), retryAt);
    }

    private String embeddingText(Chunk chunk, String relativePath) {
        String symbol = chunk.symbolName() == null ? "" : "\nSymbol: " + chunk.symbolName();
        return "File: " + relativePath + "\nLanguage: " + chunk.language() + symbol
             + "\n\n" + chunk.content();
    }
}
