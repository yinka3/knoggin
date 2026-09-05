import json

import pytest

from core.knowledge.db.readers.document_reader import DocumentReader
from core.knowledge.db.writers.document_writer import DocumentWriter
from core.knowledge.documents import DocumentService, ProjectFilesystemFactory
from core.project.project_files import PROJECT_FILE_PATH, build_project_markdown
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
async def test_native_project_context_is_readable_before_indexing_and_searchable_after(
    tmp_path,
):
    postgres = MemoryPostgres()
    reader = DocumentReader(postgres, "project-a", ["project-a"])
    writer = DocumentWriter(postgres, "project-a")
    document_service = DocumentService(
        project_id="project-a",
        postgres_client=postgres,
        embedding_service=IndexingEmbedding(),
        background_work=InlineBackgroundWork(),
        blocking_runner=run_inline,
        document_rerank_enabled=False,
        reader=reader,
        writer=writer,
        filesystem_factory=ProjectFilesystemFactory(tmp_path / "projects"),
    )
    content = build_project_markdown(
        "Research",
        "Investigate bounded workspace indexing.",
    )

    created = await document_service.create_project_file(PROJECT_FILE_PATH, content)
    assert created["relative_path"] == PROJECT_FILE_PATH
    assert await document_service.read_project_brief() == content

    await document_service.index_document(document_id=postgres.rows[0]["document_id"])

    assert postgres.rows[0]["status"] == "indexed"
    assert await document_service.pending_index_count() == 0
    postgres.search_results = [
        {
            "document_id": postgres.rows[0]["document_id"],
            "relative_path": PROJECT_FILE_PATH,
            "original_name": PROJECT_FILE_PATH,
            "extension": ".md",
            "content": "Investigate bounded workspace indexing.",
            "score": 1.0,
        }
    ]
    results = await document_service.search(
        "bounded workspace indexing",
        n_results=3,
    )
    assert results[0]["relative_path"] == PROJECT_FILE_PATH
    assert "project-a" not in json.dumps(results)
