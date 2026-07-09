"""
Backward-compatibility shim.

DocumentService has moved to core.knowledge.documents.service.
This module re-exports it so that any remaining references
(including test monkeypatching) continue to resolve.
"""

from core.knowledge.documents.service import DocumentService  # noqa: F401

# Re-export symbols that tests monkeypatch at this module level.
# These keep `monkeypatch.setattr(document_service_module, "X", ...)` working.
from core.knowledge.documents.constants import MAX_DOCUMENT_SIZE, MAX_READ_CHARACTERS  # noqa: F401
from core.knowledge.documents.storage import extract_text, split_text  # noqa: F401

__all__ = ["DocumentService"]
