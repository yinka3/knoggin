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
    assert context.knowledge_retrieval.active_topics is None
    assert not hasattr(context.documents, "delete_document")


@pytest.mark.no_network
async def test_aac_document_reader_never_grants_session_private_visibility():
    postgres = FakePostgresClient()
    postgres.upsert_project("project-1", user_name="ada")

    context = await AACReadContext.create(
        user_name="ada",
        postgres=postgres,
        knowledge_store=object(),
        embedding_service=FakeEmbeddingService(),
    )

    await context.documents.list_documents(session_id=None, limit=10)
    query = postgres.calls[-1][1]
    assert "visibility_scope = 'project'" in query
    assert "visibility_scope = 'session'" in query
    assert "session_id = %s" in query
    params = postgres.calls[-1][2]
    assert params[0] == list(context.readable_project_ids)
    assert params[1] == IDENTITY_SCOPE
    assert params[2] is None
