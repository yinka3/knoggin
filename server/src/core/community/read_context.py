"""Application-owned, user-wide read services for AAC execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple

from common.scoping import IDENTITY_SCOPE, require_scope_value
from core.knowledge.db.readers.document_reader import DocumentReader
from core.knowledge.documents import DocumentService
from core.knowledge.documents.read_service import DocumentReadService
from core.knowledge.entity.resolver import EntityResolver
from core.knowledge.retrieval import KnowledgeRetrieval


@dataclass(frozen=True, slots=True)
class AACReadContext:
    """Read-only services scoped to one user's identity and all readable projects."""

    user_name: str
    readable_project_ids: Tuple[str, ...]
    entities: EntityResolver
    knowledge_retrieval: KnowledgeRetrieval
    documents: DocumentReadService

    @classmethod
    async def create(
        cls,
        *,
        user_name: str,
        postgres: Any,
        knowledge_store: Any,
        embedding_service: Any,
        redis: Any,
        search_config: Dict | None = None,
    ) -> "AACReadContext":
        """Build independent user-wide retrieval services for AAC.

        Project rows are discovered from PostgreSQL at composition time.  The
        identity scope is the active scope, so no project domain, topic filter,
        session focus, or project workspace is inherited.
        """

        user_name = require_scope_value(user_name, "user_name", "AACReadContext")
        rows = await postgres.fetch_all(
            """
            SELECT project_id
            FROM public.projects
            WHERE user_name = %(user_name)s
              AND status IN ('active', 'archived')
            ORDER BY project_id
            """,
            {"user_name": user_name},
        )
        project_ids = tuple(
            str(row["project_id"])
            for row in rows
            if row.get("project_id")
        )
        readable_project_ids = (IDENTITY_SCOPE, *project_ids)

        entities = EntityResolver(
            project_id=IDENTITY_SCOPE,
            readable_project_ids=list(readable_project_ids),
            knowledge_store=knowledge_store,
            embedding_service=embedding_service,
        )
        retrieval = KnowledgeRetrieval(
            project_id=IDENTITY_SCOPE,
            readable_project_ids=list(readable_project_ids),
            user_name=user_name,
            entities=entities,
            embedding_service=embedding_service,
            knowledge_store=knowledge_store,
            postgres=postgres,
            redis=redis,
            search_config=search_config,
            active_topics=None,
        )
        reader = DocumentReader(
            postgres,
            IDENTITY_SCOPE,
            readable_project_ids=list(readable_project_ids),
        )
        service = DocumentService(
            project_id=IDENTITY_SCOPE,
            postgres_client=postgres,
            embedding_service=embedding_service,
            readable_project_ids=list(readable_project_ids),
            reader=reader,
        )
        return cls(
            user_name=user_name,
            readable_project_ids=readable_project_ids,
            entities=entities,
            knowledge_retrieval=retrieval,
            documents=DocumentReadService(service),
        )

