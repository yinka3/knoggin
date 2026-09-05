"""Read-only document access for runtimes that must not mutate documents."""

from __future__ import annotations

from typing import Any, Dict, List

from core.knowledge.documents.service import DocumentService


class DocumentReadService:
    """Narrow document-read boundary backed by the normal document service.

    The underlying service remains the single implementation of document
    visibility, extraction, and hybrid search.  This adapter deliberately
    exposes only read methods so AAC composition cannot accidentally receive a
    document writer/indexing API.
    """

    def __init__(self, service: DocumentService) -> None:
        self._service = service

    async def list_documents(self, **kwargs: Any) -> List[Dict]:
        return await self._service.list_documents(**kwargs)

    async def get_document_info(self, **kwargs: Any) -> Dict:
        return await self._service.get_document_info(**kwargs)

    async def read_document(self, **kwargs: Any) -> Dict:
        return await self._service.read_document(**kwargs)

    async def search(self, query: str, **kwargs: Any) -> List[Dict]:
        return await self._service.search(query, **kwargs)
