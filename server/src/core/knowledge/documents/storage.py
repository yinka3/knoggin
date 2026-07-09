import json
import os
import shutil
import uuid
from pathlib import Path
from typing import List, Optional

import docx2txt
from llama_index.core.node_parser import SentenceSplitter
from pypdf import PdfReader

from core.knowledge.documents.constants import CHUNK_SIZE, CHUNK_OVERLAP

def remove_tree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    try:
        path.parent.rmdir()
    except OSError:
        pass

def write_prepared_chunks(
    path: Path,
    chunks: List[str],
    embeddings: List[List[float]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as handle:
        for chunk_index, (content, embedding) in enumerate(
            zip(chunks, embeddings)
        ):
            handle.write(
                json.dumps(
                    {
                        "chunk_index": chunk_index,
                        "content": content,
                        "embedding": embedding,
                    },
                    separators=(",", ":"),
                )
                + "\n"
            )

def iter_prepared_chunks(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)

def write_file_atomically(target: Path, content: bytes) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as file_handle:
            file_handle.write(content)
            file_handle.flush()
            os.fsync(file_handle.fileno())
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)

def remove_stored_file(target: Path) -> None:
    target.unlink(missing_ok=True)
    try:
        target.parent.rmdir()
    except OSError:
        pass

def quarantine_stored_file(target: Path) -> Optional[Path]:
    directory = target.parent
    if not directory.exists():
        return None
    quarantine = directory.with_name(
        f".{directory.name}.deleting-{uuid.uuid4().hex}"
    )
    os.replace(directory, quarantine)
    return quarantine

def restore_quarantined_file(quarantine: Path, target: Path) -> None:
    if quarantine.exists():
        os.replace(quarantine, target.parent)

def purge_quarantined_file(quarantine: Path) -> None:
    shutil.rmtree(quarantine)

def looks_binary(content: bytes) -> bool:
    if b"\x00" in content:
        return True
    if not content:
        return False
    control_bytes = sum(
        byte < 32 and byte not in (8, 9, 10, 12, 13) for byte in content
    )
    return control_bytes / len(content) > 0.30

def extract_text(stored_path: Path, extension: str) -> str:
    if not stored_path.is_file():
        raise ValueError("Managed document content is missing")

    if extension == ".pdf":
        reader = PdfReader(str(stored_path))
        text = "\n\n".join(page.extract_text() or "" for page in reader.pages)
    elif extension == ".docx":
        text = docx2txt.process(str(stored_path)) or ""
    else:
        content = stored_path.read_bytes()
        if looks_binary(content):
            raise ValueError("Document appears to contain binary content")
        try:
            text = content.decode("utf-8-sig", errors="strict")
        except UnicodeDecodeError as exc:
            raise ValueError("Document is not valid UTF-8 text") from exc

    if not text.strip():
        raise ValueError("Document contains no extractable text")
    return text

def split_text(text: str) -> List[str]:
    splitter = SentenceSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
    )
    chunks = [chunk.strip() for chunk in splitter.split_text(text)]
    chunks = [chunk for chunk in chunks if chunk]
    if not chunks:
        raise ValueError("Document produced no non-empty chunks")
    return chunks
