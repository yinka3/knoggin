from datetime import datetime, timezone
from uuid import UUID

import pytest

from core.knowledge.db.readers.document_reader import DocumentReader


@pytest.mark.unit
@pytest.mark.no_network
async def test_document_and_link_cursors_remain_inside_project_scope():
    calls = []

    class Client:
        async def fetch_all(self, query, params):
            calls.append((query, params))
            return []

    reader = DocumentReader(Client(), "project-1", readable_project_ids=["project-1"])
    before = (datetime(2026, 1, 1, tzinfo=timezone.utc), UUID(int=2))

    await reader.list_documents(path_prefix="src", limit=11, before=before)
    document_sql, document_params = calls.pop()
    assert "pd.project_id = ANY(%s)" in document_sql
    assert "(pd.created_at, pd.document_id) < (%s, %s)" in document_sql
    assert "ORDER BY pd.created_at DESC, pd.document_id DESC" in document_sql
    assert document_params == (["project-1"], "src", "src/%", *before, 11)

    await reader.list_saved_web_links(limit=11, before=before)
    link_sql, link_params = calls.pop()
    assert "WHERE project_id = %s" in link_sql
    assert "(updated_at, link_id) < (%s, %s)" in link_sql
    assert "ORDER BY updated_at DESC, link_id DESC" in link_sql
    assert link_params == ("project-1", *before, 11)
