"""
Documents subpackage — document storage, retrieval, scanning, and indexing.
"""

from core.knowledge.documents.policy import DocumentIndexPolicy
from core.knowledge.documents.service import DocumentService

__all__ = ["DocumentIndexPolicy", "DocumentService"]
