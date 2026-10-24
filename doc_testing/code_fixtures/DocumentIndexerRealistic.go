package fixture

// Behavioral port of server/src/core/knowledge/documents/indexer.py.
// Interfaces keep source admission, claims, embedding, and publication distinct.
import (
    "crypto/sha256"
    "errors"
    "fmt"
    "math"
)

type Status string

const (
    Queued Status = "queued"
    Indexing Status = "indexing"
    Indexed Status = "indexed"
    Failed Status = "failed"
    Deleted Status = "deleted"
)

type Document struct {
    ID, ProjectID, RelativePath, Extension string
    ContentHash [32]byte
    Status Status
    AttemptCount int
}

type Chunk struct {
    Content, Language, SymbolName string
    StartLine, EndLine int
}

type Extraction struct {
    Chunks []Chunk
    Snapshot any
}

type Reader interface {
    FindDocument(id string) (*Document, error)
    FindByPath(path string) (*Document, error)
    ListUnfinished(projectID string) ([]Document, error)
    ListQueued(projectID string, limit int) ([]Document, error)
}

type Writer interface {
    Claim(id string, force, resetAttempts bool) (*Document, error)
    Publish(id string, chunks []Chunk, vectors [][]float32, snapshot any, hash [32]byte) (*Document, error)
    Requeue(id string) error
    MarkFailed(id, message string, retryAt int64) error
}

type SourceStore interface {
    ReadBytes(path string) ([]byte, error)
    Extract(source []byte, extension string) (Extraction, error)
    Reconcile() error
}

type Embedding interface { EncodeBatch(inputs []string) ([][]float32, error) }
type Policy interface { RetryTime(attempts int) int64 }

type DocumentIndexerRealistic struct {
    ProjectID string
    Reader Reader
    Writer Writer
    SourceStore SourceStore
    Embedding Embedding
    Policy Policy
    Stopping bool
}

func (i *DocumentIndexerRealistic) indexDocument(id string, force, resetAttempts bool) (Document, error) {
    metadata, err := i.Reader.FindDocument(id)
    if err != nil { return Document{}, err }
    if metadata == nil || metadata.ProjectID != i.ProjectID { return Document{}, errors.New("document not found") }
    if metadata.Status == Indexed && !force { return *metadata, nil }
    claimed, err := i.Writer.Claim(id, force, resetAttempts)
    if err != nil { return Document{}, err }
    if claimed == nil {
        current, lookupErr := i.Reader.FindDocument(id)
        if lookupErr != nil { return Document{}, lookupErr }
        if current == nil { return Document{}, errors.New("document disappeared after claim") }
        return *current, nil
    }

    source, err := i.SourceStore.ReadBytes(claimed.RelativePath)
    if err != nil { return Document{}, err }
    if source == nil { return Document{}, errors.New("document content is missing") }
    readHash := sha256.Sum256(source)
    if readHash != claimed.ContentHash { return i.reconcileStaleClaim(*claimed) }

    extraction, err := i.SourceStore.Extract(source, claimed.Extension)
    if err != nil { return Document{}, err }
    if err = i.validateChunkBoundaries(extraction.Chunks); err != nil { return Document{}, err }
    inputs := make([]string, 0, len(extraction.Chunks))
    for _, chunk := range extraction.Chunks { inputs = append(inputs, i.embeddingText(chunk, claimed.RelativePath)) }
    vectors, err := i.encodeEmbeddings(inputs)
    if err != nil { return Document{}, err }
    if err = i.validateEmbeddings(vectors, extraction.Chunks); err != nil { return Document{}, err }
    published, err := i.Writer.Publish(id, extraction.Chunks, vectors, extraction.Snapshot, readHash)
    if err != nil { return Document{}, err }
    if published == nil { return i.reconcileStaleClaim(*claimed) }
    return *published, nil
}

func (i *DocumentIndexerRealistic) reindexDocument(id string) (Document, error) {
    return i.indexDocument(id, true, true)
}

func (i *DocumentIndexerRealistic) reconcileStaleClaim(claimed Document) (Document, error) {
    if err := i.SourceStore.Reconcile(); err != nil { return Document{}, err }
    if err := i.Writer.Requeue(claimed.ID); err != nil { return Document{}, err }
    current, err := i.Reader.FindByPath(claimed.RelativePath)
    if err != nil { return Document{}, err }
    if current != nil { return *current, nil }
    claimed.Status = Deleted
    return claimed, nil
}

func (i *DocumentIndexerRealistic) encodeEmbeddings(inputs []string) ([][]float32, error) {
    vectors := make([][]float32, 0, len(inputs))
    for offset := 0; offset < len(inputs); offset += 16 {
        end := offset + 16
        if end > len(inputs) { end = len(inputs) }
        batch := inputs[offset:end]
        encoded, err := i.Embedding.EncodeBatch(batch)
        if err != nil { return nil, err }
        if len(encoded) != len(batch) { return nil, errors.New("embedding service returned an incomplete batch") }
        vectors = append(vectors, encoded...)
    }
    return vectors, nil
}

func (i *DocumentIndexerRealistic) validateEmbeddings(vectors [][]float32, chunks []Chunk) error {
    if len(vectors) != len(chunks) { return errors.New("embedding count does not match chunk count") }
    for _, vector := range vectors {
        if len(vector) != 768 { return errors.New("document chunk embedding has wrong dimensions") }
        for _, value := range vector {
            if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) { return errors.New("non-finite embedding") }
        }
    }
    return nil
}

func (i *DocumentIndexerRealistic) validateChunkBoundaries(chunks []Chunk) error {
    previousEnd := 0
    for _, chunk := range chunks {
        if chunk.StartLine < 1 || chunk.EndLine < chunk.StartLine || chunk.StartLine <= previousEnd {
            return errors.New("document chunk line ranges overlap or are invalid")
        }
        previousEnd = chunk.EndLine
    }
    return nil
}

func (i *DocumentIndexerRealistic) recoverPendingIndexes() (int, error) {
    unfinished, err := i.Reader.ListUnfinished(i.ProjectID)
    if err != nil { return 0, err }
    requeued := 0
    for _, document := range unfinished {
        if document.Status != Indexing { continue }
        current, err := i.Reader.FindDocument(document.ID)
        if err != nil { return requeued, err }
        if current == nil || current.Status != Indexing { continue }
        if err := i.Writer.Requeue(document.ID); err != nil { return requeued, err }
        requeued++
    }
    return requeued, nil
}

func (i *DocumentIndexerRealistic) drainPendingIndexes() error {
    for !i.Stopping {
        queued, err := i.Reader.ListQueued(i.ProjectID, 16)
        if err != nil { return err }
        if len(queued) == 0 { return nil }
        for _, document := range queued {
            if i.Stopping { return nil }
            if _, err := i.indexDocument(document.ID, false, false); err != nil {
                if failure := i.recordIndexFailure(document, err); failure != nil { return failure }
            }
        }
    }
    return nil
}

func (i *DocumentIndexerRealistic) recordIndexFailure(document Document, cause error) error {
    message := cause.Error()
    if len(message) > 1000 { message = message[:1000] }
    return i.Writer.MarkFailed(document.ID, message, i.Policy.RetryTime(document.AttemptCount))
}

func (i *DocumentIndexerRealistic) embeddingText(chunk Chunk, relativePath string) string {
    header := fmt.Sprintf("File: %s\nLanguage: %s", relativePath, chunk.Language)
    if chunk.SymbolName != "" { header += "\nSymbol: " + chunk.SymbolName }
    return header + "\n\n" + chunk.Content
}
