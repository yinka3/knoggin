"""Private JEV decision reads preserve scope and enforce bounded pages."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from core.knowledge.db.readers.semantic_window_reader import SemanticWindowReader
from core.knowledge.store import KnowledgeStore

METHODS = [
    f"list_jev_{capability}_decisions"
    for capability in ("identity", "extraction", "classification")
]


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("limit,offset", [(0, 0), (501, 0), (100, -1)])
async def test_invalid_pages_do_not_query_database(method, limit, offset):
    client = SimpleNamespace(fetch_all=AsyncMock())
    with pytest.raises(ValueError, match="out of bounds"):
        await getattr(SemanticWindowReader(client), method)(
            uuid4(), user_name="ada", project_id="project-1", limit=limit, offset=offset
        )
    client.fetch_all.assert_not_awaited()


@pytest.mark.parametrize("method", METHODS)
async def test_decision_reads_forward_scope_and_pagination(method):
    window_id = uuid4()
    decision = {"private": method}
    client = SimpleNamespace(fetch_all=AsyncMock(return_value=[{"decision": decision}]))
    store = KnowledgeStore.__new__(KnowledgeStore)
    store._semantic_window_reader = SemanticWindowReader(client)
    assert await getattr(store, method)(
        str(window_id), user_name="ada", project_id="project-1", limit=500, offset=2
    ) == [decision]
    query, params = client.fetch_all.await_args.args
    assert "semantic_window.user_name = %s" in query
    assert "semantic_window.project_id = %s" in query
    assert params == (window_id, "ada", "project-1", 500, 2)
