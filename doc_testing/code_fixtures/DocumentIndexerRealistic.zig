const std = @import("std");

// Behavioral port of server/src/core/knowledge/documents/indexer.py.
// Dependency methods are placeholders; this file exercises realistic Zig retrieval.
pub const Status = enum { queued, indexing, indexed, failed, deleted };
pub const Vector = [768]f32;

pub const Document = struct {
    id: []const u8,
    project_id: []const u8,
    relative_path: []const u8,
    extension: []const u8,
    content_hash: [32]u8,
    status: Status,
    attempts: u32,
};

pub const Chunk = struct {
    content: []const u8,
    language: []const u8,
    symbol_name: ?[]const u8,
    start_line: usize,
    end_line: usize,
};

pub const Reader = struct {
    pub fn findDocument(_: *Reader, _: []const u8) !?Document { return error.NotImplemented; }
    pub fn findByPath(_: *Reader, _: []const u8) !?Document { return error.NotImplemented; }
    pub fn listUnfinished(_: *Reader, _: []const u8) ![]const Document { return error.NotImplemented; }
    pub fn listQueued(_: *Reader, _: []const u8, _: usize) ![]const Document { return error.NotImplemented; }
};

pub const Writer = struct {
    pub fn claim(_: *Writer, _: []const u8, _: bool, _: bool) !?Document { return error.NotImplemented; }
    pub fn publish(_: *Writer, _: Document, _: []const Chunk, _: []const Vector, _: [32]u8) !?Document { return error.NotImplemented; }
    pub fn requeue(_: *Writer, _: []const u8) !void { return error.NotImplemented; }
    pub fn markFailed(_: *Writer, _: []const u8, _: []const u8, _: i64) !void { return error.NotImplemented; }
};

pub const SourceStore = struct {
    pub fn readBytes(_: *SourceStore, _: []const u8) ![]const u8 { return error.NotImplemented; }
    pub fn extract(_: *SourceStore, _: []const u8, _: []const u8) ![]const Chunk { return error.NotImplemented; }
    pub fn reconcile(_: *SourceStore) !void { return error.NotImplemented; }
};

pub const Embedding = struct {
    pub fn encodeBatch(_: *Embedding, _: []const Chunk) ![]Vector { return error.NotImplemented; }
};

pub const Policy = struct {
    pub fn retryTime(_: *Policy, _: u32) i64 { return 0; }
};

pub const DocumentIndexer = struct {
    allocator: std.mem.Allocator,
    project_id: []const u8,
    reader: *Reader,
    writer: *Writer,
    store: *SourceStore,
    embedding: *Embedding,
    policy: *Policy,
    stopping: bool = false,

    pub fn indexDocument(self: *DocumentIndexer, document_id: []const u8, force: bool, reset_attempts: bool) !Document {
        const metadata = (try self.reader.findDocument(document_id)) orelse return error.DocumentNotFound;
        if (!std.mem.eql(u8, metadata.project_id, self.project_id)) return error.DocumentNotFound;
        if (metadata.status == .indexed and !force) return metadata;
        const claimed = (try self.writer.claim(document_id, force, reset_attempts)) orelse
            return (try self.reader.findDocument(document_id)) orelse error.DocumentNotFound;

        const source = try self.store.readBytes(claimed.relative_path);
        var read_hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(source, &read_hash, .{});
        if (!std.mem.eql(u8, &read_hash, &claimed.content_hash)) {
            return self.reconcileStaleClaim(claimed);
        }

        const chunks = try self.store.extract(source, claimed.extension);
        try self.validateChunkBoundaries(chunks);
        const vectors = try self.encodeEmbeddings(chunks);
        defer self.allocator.free(vectors);
        try self.validateEmbeddings(vectors, chunks);
        return (try self.writer.publish(claimed, chunks, vectors, read_hash)) orelse
            self.reconcileStaleClaim(claimed);
    }

    pub fn reindexDocument(self: *DocumentIndexer, document_id: []const u8) !Document {
        return self.indexDocument(document_id, true, true);
    }

    fn reconcileStaleClaim(self: *DocumentIndexer, claimed: Document) !Document {
        try self.store.reconcile();
        try self.writer.requeue(claimed.id);
        return (try self.reader.findByPath(claimed.relative_path)) orelse blk: {
            var deleted = claimed;
            deleted.status = .deleted;
            break :blk deleted;
        };
    }

    fn encodeEmbeddings(self: *DocumentIndexer, chunks: []const Chunk) ![]Vector {
        const vectors = try self.allocator.alloc(Vector, chunks.len);
        errdefer self.allocator.free(vectors);
        var offset: usize = 0;
        while (offset < chunks.len) : (offset += 16) {
            const end = @min(offset + 16, chunks.len);
            const batch = try self.embedding.encodeBatch(chunks[offset..end]);
            defer self.allocator.free(batch);
            if (batch.len != end - offset) return error.IncompleteEmbeddingBatch;
            @memcpy(vectors[offset..end], batch);
        }
        return vectors;
    }

    fn validateEmbeddings(_: *DocumentIndexer, vectors: []const Vector, chunks: []const Chunk) !void {
        if (vectors.len != chunks.len) return error.EmbeddingCountMismatch;
        for (vectors) |vector| {
            for (vector) |value| {
                if (!std.math.isFinite(value)) return error.NonFiniteEmbedding;
            }
        }
    }

    fn validateChunkBoundaries(_: *DocumentIndexer, chunks: []const Chunk) !void {
        var previous_end: usize = 0;
        for (chunks) |chunk| {
            if (chunk.start_line == 0 or chunk.end_line < chunk.start_line) return error.InvalidLineRange;
            if (chunk.start_line <= previous_end) return error.OverlappingChunks;
            previous_end = chunk.end_line;
        }
    }

    pub fn recoverPendingIndexes(self: *DocumentIndexer) !usize {
        const unfinished = try self.reader.listUnfinished(self.project_id);
        var requeued: usize = 0;
        for (unfinished) |document| {
            if (document.status != .indexing) continue;
            const current = (try self.reader.findDocument(document.id)) orelse continue;
            if (current.status != .indexing) continue;
            try self.writer.requeue(document.id);
            requeued += 1;
        }
        return requeued;
    }

    pub fn drainPendingIndexes(self: *DocumentIndexer) !void {
        while (!self.stopping) {
            const queued = try self.reader.listQueued(self.project_id, 16);
            if (queued.len == 0) return;
            for (queued) |document| {
                _ = self.indexDocument(document.id, false, false) catch |err| {
                    try self.recordIndexFailure(document, err);
                    continue;
                };
            }
        }
    }

    fn recordIndexFailure(self: *DocumentIndexer, document: Document, err: anyerror) !void {
        const retry_at = self.policy.retryTime(document.attempts);
        try self.writer.markFailed(document.id, @errorName(err), retry_at);
    }
};
