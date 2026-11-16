// Behavioral port of server/src/core/knowledge/documents/indexer.py.
// This is a retrieval fixture, not a production implementation.
export class DocumentIndexer {
  constructor(projectId, reader, writer, embedding, filesystem, policy) {
    this.projectId = projectId;
    this.reader = reader;
    this.writer = writer;
    this.embedding = embedding;
    this.filesystem = filesystem;
    this.policy = policy;
    this.pending = new Map();
    this.stopping = false;
    this.reconciliationCallback = null;
  }

  setReconciliationCallback(callback, intervalSeconds) {
    if (!Number.isInteger(intervalSeconds) || intervalSeconds < 10) {
      throw new Error("reconciliation interval must be at least 10 seconds");
    }
    this.reconciliationCallback = callback;
    this.reconciliationIntervalSeconds = intervalSeconds;
  }

  async indexDocument(documentId, { force = false, resetAttempts = false } = {}) {
    const metadata = await this.reader.findDocument(documentId);
    if (!metadata || metadata.projectId !== this.projectId) {
      throw new Error("Document not found");
    }
    if (metadata.status === "indexed" && !force) return metadata;

    const allowedStatuses = force
      ? ["queued", "failed", "indexed"]
      : ["queued", "failed"];
    const claimed = await this.writer.transitionIndexStatus(documentId, "indexing", {
      allowedStatuses,
      resetAttempts,
    });
    if (!claimed) return this.reader.findDocument(documentId);

    try {
      const source = await this.readSourceBytes(claimed);
      if (!source) throw new Error("Document content is missing");
      const contentHash = await this.hashBytes(source);
      if (contentHash !== claimed.contentHash) {
        return await this.reconcileStaleClaim(claimed);
      }

      const extraction = await this.extractDocument(source, claimed.extension);
      const embeddingInputs = extraction.chunks.map((chunk) =>
        this.embeddingText(chunk, claimed.relativePath),
      );
      const vectors = await this.encodeEmbeddings(embeddingInputs);
      this.validateEmbeddings(vectors, extraction.chunks);
      const published = await this.writer.persistIndexedChunks({
        documentId,
        chunks: extraction.chunks,
        embeddings: vectors,
        parseSnapshot: extraction.snapshot,
        readContentHash: contentHash,
      });
      return published ?? this.reconcileStaleClaim(claimed);
    } catch (error) {
      if (error.name === "AbortError") throw error;
      if (error.message === "Document not found") throw error;
      throw new Error(`Failed to index document: ${String(error.message).slice(0, 1000)}`);
    }
  }

  async reindexDocument(documentId) {
    return this.indexDocument(documentId, {
      force: true,
      resetAttempts: true,
    });
  }

  async readSourceBytes(document) {
    if (!this.filesystem) {
      throw new Error("Document source filesystem is not configured");
    }
    return this.filesystem.readBytes(document.relativePath);
  }

  async reconcileStaleClaim(claimed) {
    if (!this.reconciliationCallback) {
      throw new Error("Document source changed but reconciliation is unavailable");
    }
    await this.reconciliationCallback();
    await this.writer.requeueIndexClaims([claimed.documentId]);
    const refreshed = await this.reader.findByPath(claimed.relativePath);
    return refreshed ?? {
      ...claimed,
      status: "deleted",
      indexedAt: null,
      errorMessage: null,
      chunkCount: 0,
    };
  }

  validateEmbeddings(vectors, chunks) {
    if (!Array.isArray(vectors) || vectors.length !== chunks.length) {
      throw new Error("Embedding count does not match chunk count");
    }
    for (const vector of vectors) {
      if (!Array.isArray(vector) || vector.length !== 768) {
        throw new Error("Document chunk embeddings must have 768 dimensions");
      }
      if (vector.some((value) => !Number.isFinite(value))) {
        throw new Error("Document chunk embedding contains a non-finite value");
      }
    }
  }

  async encodeEmbeddings(inputs) {
    const vectors = [];
    for (let offset = 0; offset < inputs.length; offset += 16) {
      const batch = inputs.slice(offset, offset + 16);
      const encoded = await this.embedding.encodeBatch(batch);
      if (encoded.length !== batch.length) {
        throw new Error("Embedding service returned an incomplete batch");
      }
      vectors.push(...encoded);
    }
    return vectors;
  }

  async recoverPendingIndexes() {
    const unfinished = await this.reader.listUnfinished(this.projectId);
    let requeued = 0;
    for (const document of unfinished) {
      if (document.status !== "indexing") continue;
      const current = await this.reader.findDocument(document.documentId);
      if (!current || current.status !== "indexing") continue;
      await this.writer.requeueIndexClaims([document.documentId]);
      requeued += 1;
    }
    return requeued;
  }

  async drainPendingIndexes(signal) {
    while (!signal.aborted && !this.stopping) {
      const queued = await this.reader.listQueued(this.projectId, 16);
      if (queued.length === 0) {
        await this.waitForWakeup(signal);
        continue;
      }
      for (const document of queued) {
        if (signal.aborted || this.stopping) return;
        try {
          await this.indexDocument(document.documentId);
        } catch (error) {
          await this.recordIndexFailure(document, error);
        }
      }
    }
  }

  async recordIndexFailure(document, error) {
    const retryAt = this.policy.retryTime(document.attemptCount);
    await this.writer.transitionIndexStatus(document.documentId, "failed", {
      errorMessage: String(error.message).slice(0, 1000),
      retryAt,
    });
  }

  async waitForWakeup(signal) {
    const delay = new Promise((resolve) => setTimeout(resolve, 100));
    await Promise.race([delay, this.pendingWakeup(signal)]);
  }

  pendingWakeup(signal) {
    return new Promise((resolve) => {
      if (signal.aborted) resolve();
      else signal.addEventListener("abort", resolve, { once: true });
    });
  }

  embeddingText(chunk, relativePath) {
    const header = [`File: ${relativePath}`, `Language: ${chunk.language}`];
    if (chunk.symbolName) header.push(`Symbol: ${chunk.symbolName}`);
    return `${header.join("\n")}\n\n${chunk.content}`;
  }
}
