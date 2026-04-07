import asyncio
import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
import chromadb
import math
from loguru import logger
from langchain_text_splitters import RecursiveCharacterTextSplitter, Language
from common.rag.embedding import EmbeddingService

LANGUAGE_MAP = {
    ".py": Language.PYTHON,
    ".js": Language.JS,
    ".ts": Language.TS,
    ".jsx": Language.JS,
    ".tsx": Language.TS,
    ".java": Language.JAVA,
    ".go": Language.GO,
    ".rs": Language.RUST,
    ".c": Language.C,
    ".cpp": Language.CPP,
    ".h": Language.C,
    ".html": Language.HTML,
    ".css": None,
    ".md": Language.MARKDOWN,
}

TEXT_EXTENSIONS = {".txt", ".csv", ".json", ".md", ".css"}
CODE_EXTENSIONS = set(LANGUAGE_MAP.keys()) - TEXT_EXTENSIONS
BINARY_EXTENSIONS = {".pdf", ".docx"}

ALLOWED_EXTENSIONS = TEXT_EXTENSIONS | CODE_EXTENSIONS | BINARY_EXTENSIONS

MAX_FILE_SIZE = 50 * 1024 * 1024
MAX_FILES_PER_SESSION = 100
DEFAULT_CHUNK_SIZE = 512
DEFAULT_CHUNK_OVERLAP = 50


class FileRAGService:
    """Session-scoped file ingestion and retrieval via ChromaDB and BM25."""

    def __init__(
        self,
        session_id: str,
        chroma_client: chromadb.ClientAPI,
        embedding_service: EmbeddingService,
        upload_dir: str,
        child_chunk_size: int = DEFAULT_CHUNK_SIZE,
        child_chunk_overlap: int = DEFAULT_CHUNK_OVERLAP,
        parent_chunk_size: int = 2000,
        parent_chunk_overlap: int = 200,
    ):
        self.session_id = session_id
        self.chroma = chroma_client
        self.embedding = embedding_service
        self.child_chunk_size = child_chunk_size
        self.child_chunk_overlap = child_chunk_overlap
        self.parent_chunk_size = parent_chunk_size
        self.parent_chunk_overlap = parent_chunk_overlap
        self._parent_cache: Optional[Dict[str, str]] = None
        self._manifest_cache: Optional[Dict[str, Dict]] = None

        self.upload_dir = Path(upload_dir) / session_id
        self.files_dir = self.upload_dir / "files"
        self.manifest_path = self.upload_dir / "manifest.json"
        self.parents_path = self.upload_dir / "parent_chunks.json"

        self._collection_name = f"session-{session_id[:58]}"  # ChromaDB 63 char limit
        self._collection = None

        self._default_parent_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.parent_chunk_size,
            chunk_overlap=self.parent_chunk_overlap,
        )
        self._default_child_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.child_chunk_size,
            chunk_overlap=self.child_chunk_overlap,
        )

        self._bm25 = None
        self._bm25_dirty = True
        self._bm25_corpus = []  # List of child chunk texts for BM25
        self._bm25_metadata = [] # List of metadata dicts corresponding to _bm25_corpus
        self._load_bm25_from_chroma()
        
    def _load_bm25_from_chroma(self):
        """Initialize the in-memory BM25 index from ChromaDB."""
        try:
            from rank_bm25 import BM25Okapi
        except ImportError:
            logger.warning("rank_bm25 not installed. BM25 hybrid search disabled.")
            return

        collection = self._get_collection()
        if collection is None or collection.count() == 0:
            self._bm25 = None
            self._bm25_corpus = []
            self._bm25_metadata = []
            return

        results = collection.get(include=["documents", "metadatas"])
        if results and results["documents"]:
            self._bm25_corpus = results["documents"]
            self._bm25_metadata = results["metadatas"]
            tokenized_corpus = [doc.lower().split(" ") for doc in self._bm25_corpus]
            self._bm25 = BM25Okapi(tokenized_corpus)

    def _get_collection(self):
        if self._collection is None:
            self._collection = self.chroma.get_or_create_collection(
                name=self._collection_name,
                metadata={"hnsw:space": "cosine"},
            )
        return self._collection


    def _load_manifest(self) -> Dict[str, Dict]:
        if self._manifest_cache is not None:
            return self._manifest_cache
        if self.manifest_path.exists():
            with open(self.manifest_path, "r") as f:
                self._manifest_cache = json.load(f)
                return self._manifest_cache
        self._manifest_cache = {}
        return self._manifest_cache

    def _save_manifest(self, manifest: Dict[str, Dict]):
        self._manifest_cache = manifest
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        with open(self.manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

    def _load_parents(self) -> Dict[str, str]:
        if self._parent_cache is not None:
            return self._parent_cache
        if self.parents_path.exists():
            with open(self.parents_path, "r") as f:
                self._parent_cache = json.load(f)
                return self._parent_cache
        self._parent_cache = {}
        return self._parent_cache

    def _save_parents(self, parents: Dict[str, str]):
        self._parent_cache = parents
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        with open(self.parents_path, "w") as f:
            json.dump(parents, f, indent=2)


    def _get_splitters(self, ext: str) -> tuple[RecursiveCharacterTextSplitter, RecursiveCharacterTextSplitter]:
        lang = LANGUAGE_MAP.get(ext)
        if lang:
            parent_splitter = RecursiveCharacterTextSplitter.from_language(
                language=lang,
                chunk_size=self.parent_chunk_size,
                chunk_overlap=self.parent_chunk_overlap,
            )
            child_splitter = RecursiveCharacterTextSplitter.from_language(
                language=lang,
                chunk_size=self.child_chunk_size,
                chunk_overlap=self.child_chunk_overlap,
            )
            return parent_splitter, child_splitter
        return self._default_parent_splitter, self._default_child_splitter

    def _read_file(self, file_path: Path) -> str:
        ext = file_path.suffix.lower()

        if ext not in TEXT_EXTENSIONS and ext not in CODE_EXTENSIONS:
            try:
                from markitdown import MarkItDown
                md = MarkItDown()
                result = md.convert(str(file_path))
                return result.text_content
            except ImportError:
                raise ValueError("markitdown not installed. Run: uv add markitdown")
            except Exception as e:
                 raise ValueError(f"Failed to parse document with markitdown: {e}")

        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()

    async def ingest_file(self, file_path: str, original_name: str) -> Dict:
        """
        Process and index a file for RAG retrieval using Parent Document Retrieval.
        """
        path = Path(file_path)
        ext = path.suffix.lower()

        if ext not in ALLOWED_EXTENSIONS:
            raise ValueError(f"Unsupported file type: {ext}. Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}")

        file_size = path.stat().st_size
        if file_size > MAX_FILE_SIZE:
            raise ValueError(f"File too large: {file_size / 1024 / 1024:.1f}MB. Max: {MAX_FILE_SIZE / 1024 / 1024:.0f}MB")

        manifest = self._load_manifest()
        if len(manifest) >= MAX_FILES_PER_SESSION:
            raise ValueError(f"Session file limit reached ({MAX_FILES_PER_SESSION}). Remove a file first.")

        content = self._read_file(path)
        if not content.strip():
            raise ValueError("File is empty or could not be read")

        parent_splitter, child_splitter = self._get_splitters(ext)
        
        parent_chunks = parent_splitter.split_text(content)
        if not parent_chunks:
            raise ValueError("File produced no chunks after splitting")

        self.files_dir.mkdir(parents=True, exist_ok=True)
        file_id = f"file_{uuid.uuid4().hex[:8]}"
        stored_name = f"{file_id}{ext}"
        dest = self.files_dir / stored_name
        shutil.copy2(str(path), str(dest))

        parent_store = self._load_parents()
        all_child_chunks = []
        metadatas = []
        chunk_ids = []

        for p_idx, p_text in enumerate(parent_chunks):
            p_id = f"{file_id}_parent_{p_idx}"
            parent_store[p_id] = p_text

            children = child_splitter.split_text(p_text)
            for c_idx, c_text in enumerate(children):
                c_id = f"{file_id}_child_{p_idx}_{c_idx}"
                all_child_chunks.append(c_text)
                chunk_ids.append(c_id)
                metadatas.append({
                    "file_id": file_id,
                    "file_name": original_name,
                    "file_type": ext,
                    "parent_id": p_id,
                    "chunk_index": p_idx,
                    "total_chunks": len(parent_chunks),
                })

        collection = self._get_collection()

        embed_batch_size = self.embedding.BATCH_SIZE
        all_embeddings = []
        for i in range(0, len(all_child_chunks), embed_batch_size):
            chunk = all_child_chunks[i:i + embed_batch_size]
            batch_embeddings = await self.embedding.encode(chunk)
            all_embeddings.extend(batch_embeddings)
            await asyncio.sleep(0)

        chroma_batch_size = 500
        for i in range(0, len(all_child_chunks), chroma_batch_size):
            end = min(i + chroma_batch_size, len(all_child_chunks))
            collection.add(
                ids=chunk_ids[i:end],
                embeddings=all_embeddings[i:end],
                documents=all_child_chunks[i:end],
                metadatas=metadatas[i:end],
            )
        self._save_parents(parent_store)

        file_meta = {
            "file_id": file_id,
            "original_name": original_name,
            "stored_name": stored_name,
            "extension": ext,
            "size_bytes": file_size,
            "chunk_count": len(parent_chunks),
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }
        manifest[file_id] = file_meta
        self._save_manifest(manifest)
        
        self._bm25_dirty = True

        logger.info(f"Ingested file '{original_name}' -> {len(parent_chunks)} parents, {len(all_child_chunks)} children (session: {self.session_id})")
        return file_meta


    async def search(
        self,
        query: str,
        n_results: int = 5,
        fetch_k: int = 35,
        file_filter: str = None,
    ) -> List[Dict]:
        """
        Search indexed files using Hybrid Search (Vector + BM25) and Reranking.
        Returns the Parent chunks associated with the best matching Child chunks.
        """
        
        collection = self._get_collection()

        if collection.count() == 0:
            return []
        
        if self._bm25_dirty:
            self._load_bm25_from_chroma()
            self._bm25_dirty = False

        query_embedding = await self.embedding.encode_single(query)

        where_filter = None
        if file_filter:
            where_filter = {"file_id": file_filter}

        vector_results = collection.query(
            query_embeddings=[query_embedding],
            n_results=min(fetch_k, collection.count()),
            where=where_filter,
            include=["documents", "metadatas", "distances"],
        )

        candidate_parents = {}
        
        if vector_results and vector_results["ids"] and vector_results["ids"][0]:
            for i, _ in enumerate(vector_results["ids"][0]):
                meta = vector_results["metadatas"][0][i]
                parent_id = meta.get("parent_id")
                if parent_id and parent_id not in candidate_parents:
                    candidate_parents[parent_id] = meta

        if self._bm25 and len(self._bm25_corpus) > 0:
            tokenized_query = query.lower().split(" ")
            bm25_scores = self._bm25.get_scores(tokenized_query)
            
            top_bm25_indices = sorted(range(len(bm25_scores)), key=lambda i: bm25_scores[i], reverse=True)[:fetch_k]
            for idx in top_bm25_indices:
                if bm25_scores[idx] <= 0:
                    continue
                meta = self._bm25_metadata[idx]
                
                if file_filter and meta.get("file_id") != file_filter:
                    continue
                    
                parent_id = meta.get("parent_id")
                if parent_id and parent_id not in candidate_parents:
                    candidate_parents[parent_id] = meta

        if not candidate_parents:
            return []

        parent_store = self._load_parents()
        parent_texts = []
        parent_metas = []
        
        for parent_id, meta in list(candidate_parents.items()):
            parent_text = parent_store.get(parent_id)
            if parent_text:
                parent_texts.append(parent_text)
                parent_metas.append(meta)
            else:
                logger.warning(f"Parent chunk not found: {parent_id}")

        if not parent_texts:
            return []

        try:
            rerank_scores = await self.embedding.rerank(query, parent_texts)
        except Exception as e:
            logger.error(f"Reranking failed: {e}. Falling back to default ordering.")
            rerank_scores = [0.0] * len(parent_texts)

        scored_parents = sorted(zip(parent_texts, parent_metas, rerank_scores), key=lambda x: x[2], reverse=True)

        output = []
        for text, meta, score in scored_parents[:n_results]:
            norm_score = 0.5
            if isinstance(score, (int, float)):
                clamped_score = max(min(-score, 500.0), -500.0)
                norm_score = 1.0 / (1.0 + math.exp(clamped_score))
            
            output.append({
                "content": text, # Returning the LARGE Parent chunk
                "file_name": meta.get("file_name", ""),
                "file_id": meta.get("file_id", ""),
                "chunk_index": meta.get("chunk_index", 0),
                "total_chunks": meta.get("total_chunks", 0),
                "score": round(norm_score, 4),
                "raw_score": round(score, 4)
            })

        return output


    def list_files(self) -> List[Dict]:
        manifest = self._load_manifest()
        return list(manifest.values())

    def delete_file(self, file_id: str) -> bool:
        manifest = self._load_manifest()

        if file_id not in manifest:
            return False

        file_meta = manifest[file_id]

        collection = self._get_collection()
        where_filter = {"file_id": file_id}

        try:
            collection.delete(where=where_filter)
        except Exception as e:
            logger.warning(f"Failed to delete chunks from ChromaDB: {e}")
            
        parent_store = self._load_parents()
        parent_keys_to_delete = [p_id for p_id in parent_store.keys() if p_id.startswith(f"{file_id}_parent_")]
        for key in parent_keys_to_delete:
            del parent_store[key]
        self._save_parents(parent_store)

        stored_path = self.files_dir / file_meta["stored_name"]
        stored_path.unlink(missing_ok=True)

        del manifest[file_id]
        self._save_manifest(manifest)
        
        self._bm25_dirty = True

        logger.info(f"Deleted file '{file_meta['original_name']}' from session {self.session_id}")
        return True

    def cleanup_session(self):
        """Remove all files and ChromaDB collection for this session."""
        try:
            self.chroma.delete_collection(self._collection_name)
        except Exception as e:
            logger.warning(f"Failed to delete ChromaDB collection: {e}")

        if self.upload_dir.exists():
            shutil.rmtree(str(self.upload_dir), ignore_errors=True)

        self._parent_cache = None
        self._manifest_cache = None
        self._bm25 = None
        self._bm25_corpus = []
        self._bm25_metadata = []
        self._bm25_dirty = False
        self._collection = None

        logger.info(f"Cleaned up file RAG data for session {self.session_id}")