import asyncio
import json

import pytest

from core.knowledge.documents import DocumentService
from core.project.workspace_service import (
    PROJECT_FILE_PATH,
    ProjectWorkspaceService,
    build_project_markdown,
)
from tests.unit.core.knowledge.test_document_service import MemoryPostgres


class InlineBackgroundWork:
    async def submit(self, _project_id, operation, *, name, coalesce_key=None):
        del name, coalesce_key
        return await operation()


class IndexingEmbedding:
    async def encode(self, values):
        return [[0.0] * 1024 for _ in values]

    async def encode_single(self, _value):
        return [0.0] * 1024


async def run_inline(function, *args, **kwargs):
    return function(*args, **kwargs)


@pytest.mark.integration
@pytest.mark.no_network
async def test_project_workspace_context_is_readable_before_indexing_and_searchable_after():
    postgres = MemoryPostgres()
    document_service = DocumentService(
        project_id="project-a",
        postgres_client=postgres,
        embedding_service=IndexingEmbedding(),
        background_work=InlineBackgroundWork(),
        blocking_runner=run_inline,
        document_rerank_enabled=False,
    )
    workspace = ProjectWorkspaceService(document_service)
    content = build_project_markdown(
        "Research",
        "Investigate bounded workspace indexing.",
    )

    created = await workspace.create_file(PROJECT_FILE_PATH, content)
    assert created["status"] == "queued"
    assert await workspace.read_project_context() == content

    pending_tasks = list(document_service.indexer._background_tasks)
    assert pending_tasks
    await asyncio.gather(*pending_tasks)

    assert postgres.rows[0]["status"] == "indexed"
    assert await document_service.pending_index_count() == 0
    postgres.search_results = [
        {
            "document_id": created["document_id"],
            "relative_path": PROJECT_FILE_PATH,
            "original_name": PROJECT_FILE_PATH,
            "extension": ".md",
            "content": "Investigate bounded workspace indexing.",
            "score": 1.0,
        }
    ]
    results = await document_service.search(
        "bounded workspace indexing",
        session_id="session-a",
        n_results=3,
    )
    assert results[0]["relative_path"] == PROJECT_FILE_PATH
    assert "project-a" not in json.dumps(results)
