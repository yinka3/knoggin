import asyncio
import math
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.schema import Document
from llama_index.core.vector_stores import VectorStoreQuery
from llama_index.vector_stores.postgres import PGVectorStore
from loguru import logger
from sqlalchemy import text as sql_text

try:
    from markitdown import MarkItDown
except ImportError:
    MarkItDown = None

try:
    from rank_bm25 import BM25Okapi
except ImportError:
    BM25Okapi = None

from knoggin.knowledge.services.embedding_service import EmbeddingService

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALLOWED_EXTENSIONS = {
    ".txt",
    ".csv",
    ".json",
    ".md",
    ".css",
    ".py",
    ".js",
    ".ts",
    ".jsx",
    ".tsx",
    ".java",
    ".go",
    ".rs",
    ".c",
    ".cpp",
    ".h",
    ".html",
    ".pdf",
    ".docx",
}

MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB
MAX_FILES_PER_SESSION = 100


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class FileRAGService:
    """Session-scoped file ingestion and retrieval via LlamaIndex + pgvector."""

    def __init__(
        self,
        session_id: str,
        embedding_service: EmbeddingService,
    ):
        self.session_id = session_id
        self._embedding = embedding_service

        # Build SQLAlchemy DSN from DATABASE_URL env var.
        # psycopg uses postgresql://, SQLAlchemy+asyncpg uses postgresql+asyncpg://
        raw_dsn = os.environ.get("DATABASE_URL", "")
        if not raw_dsn:
            raise RuntimeError("DATABASE_URL env var is required for FileRAGService")
        if raw_dsn.startswith("postgresql://"):
            self._pg_conn = raw_dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
        else:
            self._pg_conn = raw_dsn

        # File-level metadata (in-memory, rebuilt on restart from DB if needed)
        self._manifest: Dict[str, Dict] = {}

        # BM25 for hybrid keyword search — remains in-memory, independent of pgvector
        self._bm25 = None
        self._bm25_corpus: List[str] = []
        self._bm25_metadata: List[Dict] = []
        self._bm25_dirty = True

        # LlamaIndex components (lazy-initialised)
        self._vector_store: Optional[PGVectorStore] = None

    # ── Internal helpers ──────────────────────────────────────────────────

    def _get_vector_store(self) -> PGVectorStore:
        if self._vector_store is None:
            # Each session gets its own table in Postgres.
            # LlamaIndex creates it automatically on first use.
            table_name = f"file_chunks_{self.session_id[:50].replace('-', '_')}"
            self._vector_store = PGVectorStore.from_params(
                connection_string=self._pg_conn,
                table_name=table_name,
                embed_dim=self._embedding.embedding_dim,
            )
        return self._vector_store

    def _read_file(self, file_path: str) -> str:
        path = Path(file_path)
        ext = path.suffix.lower()

        if ext in {".pdf", ".docx"}:
            if MarkItDown is None:
                raise ValueError("markitdown not installed. Run: uv add markitdown")
            try:
                md = MarkItDown()
                return md.convert(str(path)).text_content
            except Exception as e:
                raise ValueError(f"Failed to parse document: {e}")

        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()

    def _rebuild_bm25(self):
        """Rebuild the in-memory BM25 index from current session chunks."""
        if BM25Okapi is None:
            logger.warning("rank_bm25 not installed. BM25 hybrid search disabled.")
            return

        if not self._bm25_corpus:
            self._bm25 = None
            return

        tokenized = [doc.lower().split() for doc in self._bm25_corpus]
        self._bm25 = BM25Okapi(tokenized)

    # ── Public Interface ──────────────────────────────────────────────────

    async def ingest_file(self, file_path: str, original_name: str) -> Dict:
        """Chunk, embed, and store a file for RAG retrieval."""
        path = Path(file_path)
        ext = path.suffix.lower()

        if ext not in ALLOWED_EXTENSIONS:
            raise ValueError(
                f"Unsupported file type: {ext}. Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}"
            )

        file_size = await asyncio.to_thread(lambda p: p.stat().st_size, path)
        if file_size > MAX_FILE_SIZE:
            raise ValueError(
                f"File too large: {file_size / 1024 / 1024:.1f}MB. Max: {MAX_FILE_SIZE / 1024 / 1024:.0f}MB"
            )

        if len(self._manifest) >= MAX_FILES_PER_SESSION:
            raise ValueError(
                f"Session file limit reached ({MAX_FILES_PER_SESSION}). Remove a file first."
            )

        content = self._read_file(file_path)
        if not content.strip():
            raise ValueError("File is empty or could not be read")

        file_id = f"file_{uuid.uuid4().hex[:8]}"

        # Use LlamaIndex's built-in SimpleFileNodeParser via Document ingestion.
        # We construct nodes manually so we can attach our file_id metadata.
        splitter = SentenceSplitter(chunk_size=512, chunk_overlap=50)
        doc = Document(
            text=content,
            metadata={
                "file_id": file_id,
                "file_name": original_name,
                "session_id": self.session_id,
            },
        )
        nodes = splitter.get_nodes_from_documents([doc])

        # Embed all nodes async directly via our service
        texts = [n.get_content() for n in nodes]
        embeddings = await self._embedding.encode(texts)
        for node, emb in zip(nodes, embeddings):
            node.embedding = emb

        # Insert directly into pgvector via PGVectorStore
        store = self._get_vector_store()
        await asyncio.get_running_loop().run_in_executor(None, lambda: store.add(nodes))

        # Update BM25 corpus
        for node in nodes:
            self._bm25_corpus.append(node.get_content())
            self._bm25_metadata.append({"file_id": file_id, "file_name": original_name})
        self._bm25_dirty = True

        file_meta = {
            "file_id": file_id,
            "original_name": original_name,
            "extension": ext,
            "size_bytes": file_size,
            "chunk_count": len(nodes),
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }
        self._manifest[file_id] = file_meta

        logger.info(
            f"Ingested '{original_name}' → {len(nodes)} chunks (session: {self.session_id})"
        )
        return file_meta

    async def search(
        self,
        query: str,
        n_results: int = 5,
        file_filter: str = None,
    ) -> List[Dict]:
        """Hybrid search: pgvector similarity + BM25 keyword, with reranking."""
        if not self._manifest:
            return []

        if self._bm25_dirty:
            self._rebuild_bm25()
            self._bm25_dirty = False

        query_embedding = await self._embedding.encode_single(query)

        # --- Vector search directly via PGVectorStore ---
        store = self._get_vector_store()
        vs_query = VectorStoreQuery(
            query_embedding=query_embedding, similarity_top_k=35
        )
        loop = asyncio.get_running_loop()
        vector_result = await loop.run_in_executor(None, store.query, vs_query)

        candidate_texts: Dict[str, Dict] = {}  # text -> meta
        if vector_result.nodes:
            for node in vector_result.nodes:
                meta = node.metadata
                if file_filter and meta.get("file_id") != file_filter:
                    continue
                text = node.get_content()
                if text not in candidate_texts:
                    candidate_texts[text] = meta

        # --- BM25 augmentation ---
        if self._bm25 and self._bm25_corpus:
            tokenized_query = query.lower().split()
            bm25_scores = self._bm25.get_scores(tokenized_query)
            top_indices = sorted(
                range(len(bm25_scores)), key=lambda i: bm25_scores[i], reverse=True
            )[:35]
            for idx in top_indices:
                if bm25_scores[idx] <= 0:
                    continue
                meta = self._bm25_metadata[idx]
                if file_filter and meta.get("file_id") != file_filter:
                    continue
                text = self._bm25_corpus[idx]
                if text not in candidate_texts:
                    candidate_texts[text] = meta

        if not candidate_texts:
            return []

        parent_texts = list(candidate_texts.keys())
        parent_metas = list(candidate_texts.values())

        # --- Reranking ---
        try:
            rerank_scores = await self._embedding.rerank(query, parent_texts)
        except Exception as e:
            logger.warning(f"Reranking failed, using default ordering: {e}")
            rerank_scores = [0.0] * len(parent_texts)

        scored = sorted(
            zip(parent_texts, parent_metas, rerank_scores),
            key=lambda x: x[2],
            reverse=True,
        )

        output = []
        for text, meta, score in scored[:n_results]:
            clamped = max(min(float(score), 500.0), -500.0)
            norm_score = 1.0 / (1.0 + math.exp(-clamped))
            output.append(
                {
                    "content": text,
                    "file_name": meta.get("file_name", ""),
                    "file_id": meta.get("file_id", ""),
                    "score": round(norm_score, 4),
                    "raw_score": round(float(score), 4),
                }
            )
        return output

    def list_files(self) -> List[Dict]:
        return list(self._manifest.values())

    async def delete_file(self, file_id: str) -> bool:
        if file_id not in self._manifest:
            return False

        # Delete nodes from pgvector store by metadata filter
        store = self._get_vector_store()
        try:
            store.delete(ref_doc_id=file_id)
        except Exception as e:
            logger.warning(f"Failed to delete chunks from pgvector for {file_id}: {e}")

        # Prune from BM25 corpus
        keep_texts = []
        keep_metas = []
        for text, meta in zip(self._bm25_corpus, self._bm25_metadata):
            if meta.get("file_id") != file_id:
                keep_texts.append(text)
                keep_metas.append(meta)
        self._bm25_corpus = keep_texts
        self._bm25_metadata = keep_metas
        self._bm25_dirty = True

        original_name = self._manifest[file_id].get("original_name", file_id)
        del self._manifest[file_id]
        logger.info(f"Deleted file '{original_name}' from session {self.session_id}")
        return True

    def cleanup_session(self):
        """Drop the session's pgvector table and clear in-memory state."""
        store = self._get_vector_store()
        try:
            # PGVectorStore exposes the underlying engine — drop the session table directly
            table_name = f"file_chunks_{self.session_id[:50].replace('-', '_')}"
            with store._engine.connect() as conn:
                conn.execute(sql_text(f'DROP TABLE IF EXISTS "data_{table_name}"'))
                conn.commit()
        except Exception as e:
            logger.warning(f"Failed to drop session table: {e}")

        self._manifest.clear()
        self._bm25 = None
        self._bm25_corpus = []
        self._bm25_metadata = []
        self._bm25_dirty = True
        self._vector_store = None

        logger.info(f"Cleaned up file RAG data for session {self.session_id}")
