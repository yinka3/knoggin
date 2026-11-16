#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

// Behavioral port of server/src/core/knowledge/documents/indexer.py.
namespace fixture {

enum class Status { Queued, Indexing, Indexed, Failed, Deleted };

struct Document {
    std::string id, projectId, relativePath, extension;
    std::array<std::uint8_t, 32> contentHash;
    Status status;
    int attemptCount;
};

struct Chunk {
    std::string content, language, symbolName;
    int startLine, endLine;
};

struct Extraction {
    std::vector<Chunk> chunks;
    std::string snapshot;
};

struct Reader {
    virtual ~Reader() = default;
    virtual std::optional<Document> findDocument(const std::string& id) = 0;
    virtual std::optional<Document> findByPath(const std::string& path) = 0;
    virtual std::vector<Document> listUnfinished(const std::string& projectId) = 0;
    virtual std::vector<Document> listQueued(const std::string& projectId, int limit) = 0;
};

struct Writer {
    virtual ~Writer() = default;
    virtual std::optional<Document> claim(const std::string& id, bool force, bool resetAttempts) = 0;
    virtual std::optional<Document> publish(const std::string& id, const std::vector<Chunk>& chunks,
                                            const std::vector<std::vector<float>>& vectors,
                                            const std::string& snapshot,
                                            const std::array<std::uint8_t, 32>& hash) = 0;
    virtual void requeue(const std::string& id) = 0;
    virtual void markFailed(const std::string& id, const std::string& message, std::int64_t retryAt) = 0;
};

struct SourceStore {
    virtual ~SourceStore() = default;
    virtual std::optional<std::vector<std::uint8_t>> readBytes(const std::string& path) = 0;
    virtual std::array<std::uint8_t, 32> sha256(const std::vector<std::uint8_t>& source) = 0;
    virtual Extraction extract(const std::vector<std::uint8_t>& source, const std::string& extension) = 0;
    virtual void reconcile() = 0;
};

struct Embedding {
    virtual ~Embedding() = default;
    virtual std::vector<std::vector<float>> encodeBatch(const std::vector<std::string>& inputs) = 0;
};

struct Policy {
    virtual ~Policy() = default;
    virtual std::int64_t retryTime(int attempts) = 0;
};

class DocumentIndexerRealistic {
public:
    DocumentIndexerRealistic(std::string projectId, Reader& reader, Writer& writer,
                            SourceStore& store, Embedding& embedding, Policy& policy)
        : projectId_(std::move(projectId)), reader_(reader), writer_(writer),
          store_(store), embedding_(embedding), policy_(policy) {}

    Document indexDocument(const std::string& id, bool force = false, bool resetAttempts = false) {
        auto metadata = reader_.findDocument(id);
        if (!metadata || metadata->projectId != projectId_) throw std::runtime_error("Document not found");
        if (metadata->status == Status::Indexed && !force) return *metadata;
        auto claimed = writer_.claim(id, force, resetAttempts);
        if (!claimed) return reader_.findDocument(id).value_or(*metadata);

        auto source = store_.readBytes(claimed->relativePath);
        if (!source) throw std::runtime_error("Document content is missing");
        auto readHash = store_.sha256(*source);
        if (readHash != claimed->contentHash) return reconcileStaleClaim(*claimed);

        auto extraction = store_.extract(*source, claimed->extension);
        validateChunkBoundaries(extraction.chunks);
        std::vector<std::string> inputs;
        for (const auto& chunk : extraction.chunks) inputs.push_back(embeddingText(chunk, claimed->relativePath));
        auto vectors = encodeEmbeddings(inputs);
        validateEmbeddings(vectors, extraction.chunks);
        auto published = writer_.publish(id, extraction.chunks, vectors, extraction.snapshot, readHash);
        return published ? *published : reconcileStaleClaim(*claimed);
    }

    Document reindexDocument(const std::string& id) { return indexDocument(id, true, true); }

    std::size_t recoverPendingIndexes() {
        std::size_t requeued = 0;
        for (const auto& document : reader_.listUnfinished(projectId_)) {
            if (document.status != Status::Indexing) continue;
            auto current = reader_.findDocument(document.id);
            if (!current || current->status != Status::Indexing) continue;
            writer_.requeue(document.id);
            ++requeued;
        }
        return requeued;
    }

    void drainPendingIndexes() {
        while (!stopping_) {
            auto queued = reader_.listQueued(projectId_, 16);
            if (queued.empty()) return;
            for (const auto& document : queued) {
                if (stopping_) return;
                try { indexDocument(document.id); }
                catch (const std::exception& error) { recordIndexFailure(document, error); }
            }
        }
    }

private:
    Document reconcileStaleClaim(const Document& claimed) {
        store_.reconcile();
        writer_.requeue(claimed.id);
        auto refreshed = reader_.findByPath(claimed.relativePath);
        if (refreshed) return *refreshed;
        auto deleted = claimed;
        deleted.status = Status::Deleted;
        return deleted;
    }

    std::vector<std::vector<float>> encodeEmbeddings(const std::vector<std::string>& inputs) {
        std::vector<std::vector<float>> vectors;
        for (std::size_t offset = 0; offset < inputs.size(); offset += 16) {
            auto end = std::min(offset + 16, inputs.size());
            std::vector<std::string> batch(inputs.begin() + offset, inputs.begin() + end);
            auto encoded = embedding_.encodeBatch(batch);
            if (encoded.size() != batch.size()) throw std::runtime_error("Embedding service returned an incomplete batch");
            vectors.insert(vectors.end(), encoded.begin(), encoded.end());
        }
        return vectors;
    }

    static void validateEmbeddings(const std::vector<std::vector<float>>& vectors,
                                   const std::vector<Chunk>& chunks) {
        if (vectors.size() != chunks.size()) throw std::runtime_error("Embedding count does not match chunk count");
        for (const auto& vector : vectors) {
            if (vector.size() != 768 || std::any_of(vector.begin(), vector.end(),
                                                    [](float value) { return !std::isfinite(value); })) {
                throw std::runtime_error("Document chunk embedding is invalid");
            }
        }
    }

    static void validateChunkBoundaries(const std::vector<Chunk>& chunks) {
        int previousEnd = 0;
        for (const auto& chunk : chunks) {
            if (chunk.startLine < 1 || chunk.endLine < chunk.startLine || chunk.startLine <= previousEnd) {
                throw std::runtime_error("Document chunk line ranges overlap or are invalid");
            }
            previousEnd = chunk.endLine;
        }
    }

    void recordIndexFailure(const Document& document, const std::exception& error) {
        auto message = std::string(error.what()).substr(0, 1000);
        writer_.markFailed(document.id, message, policy_.retryTime(document.attemptCount));
    }

    static std::string embeddingText(const Chunk& chunk, const std::string& path) {
        auto symbol = chunk.symbolName.empty() ? "" : "\nSymbol: " + chunk.symbolName;
        return "File: " + path + "\nLanguage: " + chunk.language + symbol + "\n\n" + chunk.content;
    }

    std::string projectId_;
    Reader& reader_;
    Writer& writer_;
    SourceStore& store_;
    Embedding& embedding_;
    Policy& policy_;
    bool stopping_ = false;
};

} // namespace fixture
