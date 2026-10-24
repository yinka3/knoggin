using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;

// Behavioral port of server/src/core/knowledge/documents/indexer.py.
namespace Fixtures;

public enum IndexStatus { Queued, Indexing, Indexed, Failed, Deleted }

public sealed record Document(
    string Id, string ProjectId, string RelativePath, string Extension,
    byte[] ContentHash, IndexStatus Status, int AttemptCount);

public sealed record Chunk(
    string Content, string Language, string? SymbolName, int StartLine, int EndLine);

public sealed record Extraction(IReadOnlyList<Chunk> Chunks, object Snapshot);

public interface IReader {
    Task<Document?> FindDocumentAsync(string id);
    Task<Document?> FindByPathAsync(string path);
    Task<IReadOnlyList<Document>> ListUnfinishedAsync(string projectId);
    Task<IReadOnlyList<Document>> ListQueuedAsync(string projectId, int limit);
}

public interface IWriter {
    Task<Document?> ClaimAsync(string id, bool force, bool resetAttempts);
    Task<Document?> PublishAsync(string id, IReadOnlyList<Chunk> chunks, IReadOnlyList<float[]> vectors, object snapshot, byte[] hash);
    Task RequeueAsync(string id);
    Task MarkFailedAsync(string id, string message, long retryAt);
}

public interface ISourceStore {
    Task<byte[]?> ReadBytesAsync(string path);
    Task<Extraction> ExtractAsync(byte[] source, string extension);
    Task ReconcileAsync();
}

public interface IEmbedding { Task<IReadOnlyList<float[]>> EncodeBatchAsync(IReadOnlyList<string> inputs); }
public interface IPolicy { long RetryTime(int attempts); }

public sealed class DocumentIndexerRealistic {
    private readonly string projectId;
    private readonly IReader reader;
    private readonly IWriter writer;
    private readonly ISourceStore store;
    private readonly IEmbedding embedding;
    private readonly IPolicy policy;
    private bool stopping;

    public DocumentIndexerRealistic(string projectId, IReader reader, IWriter writer,
                                    ISourceStore store, IEmbedding embedding, IPolicy policy) {
        this.projectId = projectId;
        this.reader = reader;
        this.writer = writer;
        this.store = store;
        this.embedding = embedding;
        this.policy = policy;
    }

    public async Task<Document> IndexDocumentAsync(string id, bool force = false, bool resetAttempts = false) {
        var metadata = await reader.FindDocumentAsync(id);
        if (metadata is null || metadata.ProjectId != projectId) throw new InvalidOperationException("Document not found");
        if (metadata.Status == IndexStatus.Indexed && !force) return metadata;
        var claimed = await writer.ClaimAsync(id, force, resetAttempts);
        if (claimed is null) return await reader.FindDocumentAsync(id) ?? metadata;

        var source = await store.ReadBytesAsync(claimed.RelativePath);
        if (source is null) throw new InvalidOperationException("Document content is missing");
        var readHash = SHA256.HashData(source);
        if (!readHash.SequenceEqual(claimed.ContentHash)) return await ReconcileStaleClaimAsync(claimed);

        var extraction = await store.ExtractAsync(source, claimed.Extension);
        ValidateChunkBoundaries(extraction.Chunks);
        var inputs = extraction.Chunks.Select(chunk => EmbeddingText(chunk, claimed.RelativePath)).ToList();
        var vectors = await EncodeEmbeddingsAsync(inputs);
        ValidateEmbeddings(vectors, extraction.Chunks);
        var published = await writer.PublishAsync(id, extraction.Chunks, vectors, extraction.Snapshot, readHash);
        return published ?? await ReconcileStaleClaimAsync(claimed);
    }

    public Task<Document> ReindexDocumentAsync(string id) => IndexDocumentAsync(id, true, true);

    private async Task<Document> ReconcileStaleClaimAsync(Document claimed) {
        await store.ReconcileAsync();
        await writer.RequeueAsync(claimed.Id);
        return await reader.FindByPathAsync(claimed.RelativePath)
            ?? claimed with { Status = IndexStatus.Deleted };
    }

    private async Task<IReadOnlyList<float[]>> EncodeEmbeddingsAsync(IReadOnlyList<string> inputs) {
        var vectors = new List<float[]>();
        for (var offset = 0; offset < inputs.Count; offset += 16) {
            var batch = inputs.Skip(offset).Take(16).ToList();
            var encoded = await embedding.EncodeBatchAsync(batch);
            if (encoded.Count != batch.Count) throw new InvalidOperationException("Embedding service returned an incomplete batch");
            vectors.AddRange(encoded);
        }
        return vectors;
    }

    private static void ValidateEmbeddings(IReadOnlyList<float[]> vectors, IReadOnlyList<Chunk> chunks) {
        if (vectors.Count != chunks.Count) throw new InvalidOperationException("Embedding count does not match chunk count");
        foreach (var vector in vectors) {
            if (vector.Length != 768 || vector.Any(value => !float.IsFinite(value))) {
                throw new InvalidOperationException("Document chunk embedding is invalid");
            }
        }
    }

    private static void ValidateChunkBoundaries(IReadOnlyList<Chunk> chunks) {
        var previousEnd = 0;
        foreach (var chunk in chunks) {
            if (chunk.StartLine < 1 || chunk.EndLine < chunk.StartLine || chunk.StartLine <= previousEnd) {
                throw new InvalidOperationException("Document chunk line ranges overlap or are invalid");
            }
            previousEnd = chunk.EndLine;
        }
    }

    public async Task<int> RecoverPendingIndexesAsync() {
        var unfinished = await reader.ListUnfinishedAsync(projectId);
        var requeued = 0;
        foreach (var document in unfinished) {
            if (document.Status != IndexStatus.Indexing) continue;
            var current = await reader.FindDocumentAsync(document.Id);
            if (current?.Status != IndexStatus.Indexing) continue;
            await writer.RequeueAsync(document.Id);
            requeued++;
        }
        return requeued;
    }

    public async Task DrainPendingIndexesAsync() {
        while (!stopping) {
            var queued = await reader.ListQueuedAsync(projectId, 16);
            if (queued.Count == 0) return;
            foreach (var document in queued) {
                if (stopping) return;
                try { await IndexDocumentAsync(document.Id); }
                catch (Exception error) { await RecordIndexFailureAsync(document, error); }
            }
        }
    }

    private Task RecordIndexFailureAsync(Document document, Exception error) {
        var message = error.Message[..Math.Min(error.Message.Length, 1000)];
        return writer.MarkFailedAsync(document.Id, message, policy.RetryTime(document.AttemptCount));
    }

    private static string EmbeddingText(Chunk chunk, string path) {
        var symbol = chunk.SymbolName is null ? "" : "\nSymbol: " + chunk.SymbolName;
        return $"File: {path}\nLanguage: {chunk.Language}{symbol}\n\n{chunk.Content}";
    }
}
