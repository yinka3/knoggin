// Behavioral port of server/src/core/knowledge/documents/indexer.py.
// Interfaces stand in for the durable store and model during retrieval tests.
type Status = "queued" | "indexing" | "indexed" | "failed" | "deleted";

interface Document {
  documentId: string;
  projectId: string;
  relativePath: string;
  extension: string;
  contentHash: string;
  status: Status;
  attemptCount: number;
}

interface Chunk {
  content: string;
  language: string;
  symbolName?: string;
  startLine: number;
  endLine: number;
}

interface Extraction { chunks: Chunk[]; snapshot: unknown }
interface Reader {
  findDocument(id: string): Promise<Document | null>;
  findByPath(path: string): Promise<Document | null>;
  listUnfinished(projectId: string): Promise<Document[]>;
  listQueued(projectId: string, limit: number): Promise<Document[]>;
}
interface Writer {
  claim(id: string, force: boolean, resetAttempts: boolean): Promise<Document | null>;
  publish(id: string, chunks: Chunk[], vectors: number[][], snapshot: unknown, hash: string): Promise<Document | null>;
  requeue(id: string): Promise<void>;
  markFailed(id: string, message: string, retryAt: number): Promise<void>;
}
interface SourceStore {
  readBytes(path: string): Promise<Uint8Array | null>;
  sha256(content: Uint8Array): Promise<string>;
  extract(content: Uint8Array, extension: string): Promise<Extraction>;
  reconcile(): Promise<void>;
}
interface Embedding { encodeBatch(texts: string[]): Promise<number[][]> }
interface Policy { retryTime(attempts: number): number }

export class DocumentIndexerRealistic {
  private stopping = false;

  constructor(
    private readonly projectId: string,
    private readonly reader: Reader,
    private readonly writer: Writer,
    private readonly store: SourceStore,
    private readonly embedding: Embedding,
    private readonly policy: Policy,
  ) {}

  async indexDocument(id: string, force = false, resetAttempts = false): Promise<Document> {
    const metadata = await this.reader.findDocument(id);
    if (!metadata || metadata.projectId !== this.projectId) throw new Error("Document not found");
    if (metadata.status === "indexed" && !force) return metadata;
    const claimed = await this.writer.claim(id, force, resetAttempts);
    if (!claimed) return (await this.reader.findDocument(id)) ?? metadata;

    const source = await this.store.readBytes(claimed.relativePath);
    if (!source) throw new Error("Document content is missing");
    const readHash = await this.store.sha256(source);
    if (readHash !== claimed.contentHash) return this.reconcileStaleClaim(claimed);

    const extraction = await this.store.extract(source, claimed.extension);
    this.validateChunkBoundaries(extraction.chunks);
    const inputs = extraction.chunks.map((chunk) => this.embeddingText(chunk, claimed.relativePath));
    const vectors = await this.encodeEmbeddings(inputs);
    this.validateEmbeddings(vectors, extraction.chunks);
    const published = await this.writer.publish(id, extraction.chunks, vectors, extraction.snapshot, readHash);
    return published ?? this.reconcileStaleClaim(claimed);
  }

  async reindexDocument(id: string): Promise<Document> {
    return this.indexDocument(id, true, true);
  }

  private async reconcileStaleClaim(claimed: Document): Promise<Document> {
    await this.store.reconcile();
    await this.writer.requeue(claimed.documentId);
    return (await this.reader.findByPath(claimed.relativePath)) ?? {
      ...claimed,
      status: "deleted",
    };
  }

  private async encodeEmbeddings(inputs: string[]): Promise<number[][]> {
    const vectors: number[][] = [];
    for (let offset = 0; offset < inputs.length; offset += 16) {
      const batch = inputs.slice(offset, offset + 16);
      const encoded = await this.embedding.encodeBatch(batch);
      if (encoded.length !== batch.length) throw new Error("Embedding service returned an incomplete batch");
      vectors.push(...encoded);
    }
    return vectors;
  }

  private validateEmbeddings(vectors: number[][], chunks: Chunk[]): void {
    if (vectors.length !== chunks.length) throw new Error("Embedding count does not match chunk count");
    for (const vector of vectors) {
      if (vector.length !== 768 || vector.some((value) => !Number.isFinite(value))) {
        throw new Error("Document chunk embedding is invalid");
      }
    }
  }

  private validateChunkBoundaries(chunks: Chunk[]): void {
    let previousEnd = 0;
    for (const chunk of chunks) {
      if (chunk.startLine < 1 || chunk.endLine < chunk.startLine || chunk.startLine <= previousEnd) {
        throw new Error("Document chunk line ranges overlap or are invalid");
      }
      previousEnd = chunk.endLine;
    }
  }

  async recoverPendingIndexes(): Promise<number> {
    const unfinished = await this.reader.listUnfinished(this.projectId);
    let recovered = 0;
    for (const document of unfinished) {
      if (document.status !== "indexing") continue;
      const current = await this.reader.findDocument(document.documentId);
      if (!current || current.status !== "indexing") continue;
      await this.writer.requeue(document.documentId);
      recovered++;
    }
    return recovered;
  }

  async drainPendingIndexes(): Promise<void> {
    while (!this.stopping) {
      const queued = await this.reader.listQueued(this.projectId, 16);
      if (queued.length === 0) return;
      for (const document of queued) {
        if (this.stopping) return;
        try {
          await this.indexDocument(document.documentId);
        } catch (error) {
          await this.recordIndexFailure(document, error);
        }
      }
    }
  }

  private async recordIndexFailure(document: Document, error: unknown): Promise<void> {
    const message = error instanceof Error ? error.message : String(error);
    await this.writer.markFailed(document.documentId, message.slice(0, 1000), this.policy.retryTime(document.attemptCount));
  }

  private embeddingText(chunk: Chunk, relativePath: string): string {
    const symbol = chunk.symbolName ? `\nSymbol: ${chunk.symbolName}` : "";
    return `File: ${relativePath}\nLanguage: ${chunk.language}${symbol}\n\n${chunk.content}`;
  }
}
