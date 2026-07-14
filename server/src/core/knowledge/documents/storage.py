import io
from typing import List

import docx2txt
import pytesseract
from llama_index.core.node_parser import SentenceSplitter
from PIL import Image as PILImage
from pypdf import PdfReader

from core.knowledge.documents.constants import (
    ACCEPTED_EXTENSIONS,
    CHUNK_OVERLAP_TOKENS,
    CHUNK_SIZE_TOKENS,
    IMAGE_EXTENSIONS,
)

# Module-level splitter — constructing SentenceSplitter loads a tokenizer,
# so we create it once rather than on every split_text() call.
_SPLITTER = SentenceSplitter(
    chunk_size=CHUNK_SIZE_TOKENS,
    chunk_overlap=CHUNK_OVERLAP_TOKENS,
)


def looks_binary(content: bytes) -> bool:
    if not content:
        return False
    if b"\x00" in content:
        return True
    control_bytes = sum(
        byte < 32 and byte not in (8, 9, 10, 12, 13) for byte in content
    )
    return control_bytes / len(content) > 0.30


def is_accepted_extension(extension: str) -> bool:
    """Return True if the extension is supported for upload and indexing."""
    return extension.lower() in ACCEPTED_EXTENSIONS


def extract_text(content: bytes, extension: str) -> str:
    ext = extension.lower()

    if ext not in ACCEPTED_EXTENSIONS:
        raise ValueError(
            f"Unsupported file type '{ext}'. "
            f"Accepted types include PDF, DOCX, plain text, source code, and images."
        )

    if ext == ".pdf":
        reader = PdfReader(io.BytesIO(content))
        text = "\n\n".join(page.extract_text() or "" for page in reader.pages)
    elif ext == ".docx":
        text = docx2txt.process(io.BytesIO(content)) or ""
    elif ext in IMAGE_EXTENSIONS:
        image = PILImage.open(io.BytesIO(content))
        text = pytesseract.image_to_string(image)
        if not text.strip():
            raise ValueError(
                "Image contains no readable text — it may be a photo or illustration."
            )
        return text
    else:
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
    chunks = [chunk.strip() for chunk in _SPLITTER.split_text(text)]
    chunks = [chunk for chunk in chunks if chunk]
    if not chunks:
        raise ValueError("Document produced no non-empty chunks")
    return chunks
