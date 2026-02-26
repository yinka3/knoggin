import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
import docx2txt
import chromadb
from loguru import logger
from langchain_text_splitters import RecursiveCharacterTextSplitter, Language
from pypdf import PdfReader
from shared.embedding import EmbeddingService

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
MAX_FILES_PER_SESSION = 20
DEFAULT_CHUNK_SIZE = 512
DEFAULT_CHUNK_OVERLAP = 50


class FileRAGService:
    """Session-scoped file ingestion and retrieval via ChromaDB."""

    def __init__(
        self,
        session_id: str,
        chroma_client: chromadb.ClientAPI,
        embedding_service: EmbeddingService,
        upload_dir: str,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        chunk_overlap: int = DEFAULT_CHUNK_OVERLAP,
    ):
        self.session_id = session_id
        self.chroma = chroma_client
        self.embedding = embedding_service
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

        self.upload_dir = Path(upload_dir) / session_id
        self.files_dir = self.upload_dir / "files"
        self.manifest_path = self.upload_dir / "manifest.json"

        self._collection_name = f"session-{session_id[:58]}"  # ChromaDB 63 char limit
        self._collection = None

        self._default_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
        )

    def _get_collection(self):
        if self._collection is None:
            self._collection = self.chroma.get_or_create_collection(
                name=self._collection_name,
                metadata={"hnsw:space": "cosine"},
            )
        return self._collection

    # ==================
    # Manifest Management
    # ==================

    def _load_manifest(self) -> Dict[str, Dict]:
        if self.manifest_path.exists():
            with open(self.manifest_path, "r") as f:
                return json.load(f)
        return {}

    def _save_manifest(self, manifest: Dict[str, Dict]):
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        with open(self.manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

    # ==================
    # File Ingestion
    # ==================

    def _get_splitter(self, ext: str) -> RecursiveCharacterTextSplitter:
        lang = LANGUAGE_MAP.get(ext)
        if lang:
            return RecursiveCharacterTextSplitter.from_language(
                language=lang,
                chunk_size=self.chunk_size,
                chunk_overlap=self.chunk_overlap,
            )
        return self._default_splitter

    def _read_file(self, file_path: Path) -> str:
        ext = file_path.suffix.lower()

        if ext == ".pdf":
            try:
                

                reader = PdfReader(str(file_path))
                return "\n".join(page.extract_text() or "" for page in reader.pages)
            except ImportError:
                raise ValueError("pypdf not installed. Run: pip install pypdf")

        if ext == ".docx":
            try:
                

                return docx2txt.process(str(file_path))
            except ImportError:
                raise ValueError("docx2txt not installed. Run: pip install docx2txt")

        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()

    def ingest_file(self, file_path: str, original_name: str) -> Dict:
        """
        Process and index a file for RAG retrieval.

        Args:
            file_path: Path to the uploaded file on disk
            original_name: Original filename from the user

        Returns:
            File metadata dict with id, name, chunk count, etc.
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

        splitter = self._get_splitter(ext)
        chunks = splitter.split_text(content)

        if not chunks:
            raise ValueError("File produced no chunks after splitting")

        embeddings = self.embedding.encode(chunks)

        self.files_dir.mkdir(parents=True, exist_ok=True)
        file_id = f"file_{uuid.uuid4().hex[:8]}"
        stored_name = f"{file_id}{ext}"
        dest = self.files_dir / stored_name
        shutil.copy2(str(path), str(dest))

        collection = self._get_collection()
        chunk_ids = [f"{file_id}_chunk_{i}" for i in range(len(chunks))]
        metadatas = [
            {
                "file_id": file_id,
                "file_name": original_name,
                "file_type": ext,
                "chunk_index": i,
                "total_chunks": len(chunks),
            }
            for i in range(len(chunks))
        ]

        batch_size = 500
        for i in range(0, len(chunks), batch_size):
            end = min(i + batch_size, len(chunks))
            collection.add(
                ids=chunk_ids[i:end],
                embeddings=embeddings[i:end],
                documents=chunks[i:end],
                metadatas=metadatas[i:end],
            )

        file_meta = {
            "file_id": file_id,
            "original_name": original_name,
            "stored_name": stored_name,
            "extension": ext,
            "size_bytes": file_size,
            "chunk_count": len(chunks),
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }
        manifest[file_id] = file_meta
        self._save_manifest(manifest)

        logger.info(f"Ingested file '{original_name}' -> {len(chunks)} chunks (session: {self.session_id})")
        return file_meta

    # ==================
    # Search / Retrieval
    # ==================

    def search(
        self,
        query: str,
        n_results: int = 5,
        file_filter: str = None,
    ) -> List[Dict]:
        """
        Search indexed files for relevant chunks.

        Args:
            query: Search query text
            n_results: Number of chunks to return
            file_filter: Optional file_id to restrict search to one file

        Returns:
            List of {content, file_name, file_id, chunk_index, score}
        """
        collection = self._get_collection()

        if collection.count() == 0:
            return []

        query_embedding = self.embedding.encode_single(query)

        where_filter = None
        if file_filter:
            where_filter = {"file_id": file_filter}

        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=min(n_results, collection.count()),
            where=where_filter,
            include=["documents", "metadatas", "distances"],
        )

        if not results or not results["ids"] or not results["ids"][0]:
            return []

        output = []
        for i, doc_id in enumerate(results["ids"][0]):
            meta = results["metadatas"][0][i]
            distance = results["distances"][0][i]
            score = 1 - distance

            output.append({
                "content": results["documents"][0][i],
                "file_name": meta.get("file_name", ""),
                "file_id": meta.get("file_id", ""),
                "chunk_index": meta.get("chunk_index", 0),
                "total_chunks": meta.get("total_chunks", 0),
                "score": round(score, 4),
            })

        return output

    # ==================
    # File Management
    # ==================

    def list_files(self) -> List[Dict]:
        manifest = self._load_manifest()
        return list(manifest.values())

    def delete_file(self, file_id: str) -> bool:
        manifest = self._load_manifest()

        if file_id not in manifest:
            return False

        file_meta = manifest[file_id]

        collection = self._get_collection()
        chunk_ids = [f"{file_id}_chunk_{i}" for i in range(file_meta["chunk_count"])]

        try:
            collection.delete(ids=chunk_ids)
        except Exception as e:
            logger.warning(f"Failed to delete chunks from ChromaDB: {e}")

        stored_path = self.files_dir / file_meta["stored_name"]
        if stored_path.exists():
            stored_path.unlink()

        del manifest[file_id]
        self._save_manifest(manifest)

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

        logger.info(f"Cleaned up file RAG data for session {self.session_id}")