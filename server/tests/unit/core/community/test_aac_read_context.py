from __future__ import annotations

import pytest

from common.scoping import IDENTITY_SCOPE
from core.community.read_context import AACReadContext
from tests.fixtures.fakes import FakeEmbeddingService, FakePostgresClient


@pytest.mark.no_network
async def test_aac_read_context_discovers_user_projects_and_uses_identity_scope():
    postgres = FakePostgresClient()
    postgres.upsert_project("active-project", status="active", user_name="ada")
    postgres.upsert_project("archived-project", status="archived", user_name="ada")
    postgres.upsert_project("deleted-project", status="deleted", user_name="ada")
    postgres.upsert_project("other-user-project", status="active", user_name="bob")

    context = await AACReadContext.create(
        user_name="ada",
        postgres=postgres,
        knowledge_store=object(),
        embedding_service=FakeEmbeddingService(),
    )

    assert context.readable_project_ids == (
        IDENTITY_SCOPE,
        "active-project",
        "archived-project",
    )
    assert context.entities.project_id == IDENTITY_SCOPE
    assert context.entities.readable_project_ids == list(context.readable_project_ids)
    assert context.knowledge_retrieval.project_id == IDENTITY_SCOPE
    assert not hasattr(context.knowledge_retrieval, "active_topics")
    assert not hasattr(context.documents, "delete_document")


@pytest.mark.no_network
async def test_aac_document_reader_uses_only_readable_project_ownership():
    postgres = FakePostgresClient()
    postgres.upsert_project("project-1", user_name="ada")

    context = await AACReadContext.create(
        user_name="ada",
        postgres=postgres,
        knowledge_store=object(),
        embedding_service=FakeEmbeddingService(),
    )

    await context.documents.list_documents(limit=10)
    query = postgres.calls[-1][1]
    assert "pd.project_id = ANY(%s)" in query
    assert "visibility_scope" not in query
    assert "session_id" not in query
    params = postgres.calls[-1][2]
    assert params[0] == list(context.readable_project_ids)
    assert params[1] == 10


@pytest.mark.no_network
async def test_refreshed_context_withdraws_deleted_projects_from_all_readers():
    postgres = FakePostgresClient()
    postgres.upsert_project("withdrawn", user_name="ada")
    args = dict(user_name="ada", postgres=postgres, knowledge_store=object(),
                embedding_service=FakeEmbeddingService())
    previous = await AACReadContext.create(**args)
    assert "withdrawn" in previous.readable_project_ids
    postgres.upsert_project("withdrawn", status="deleted", user_name="ada")
    postgres.upsert_project("new", user_name="ada")
    refreshed = await AACReadContext.create(**args)
    assert refreshed.readable_project_ids == (IDENTITY_SCOPE, "new")
    assert refreshed.entities.readable_project_ids == [IDENTITY_SCOPE, "new"]
    await refreshed.documents.list_documents(limit=10)
    assert postgres.calls[-1][2][0] == [IDENTITY_SCOPE, "new"]
