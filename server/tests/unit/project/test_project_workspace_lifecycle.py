from contextlib import asynccontextmanager

import pytest

from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
from core.project.project_manager import ProjectManager
from core.project.workspace_service import PROJECT_FILE_PATH, build_project_markdown
from tests.fixtures.factories import make_domain_config


class RecordingPostgres:
    def __init__(self, *, fail_on_content=False):
        self.calls = []
        self.transactions = []
        self.fail_on_content = fail_on_content

    @asynccontextmanager
    async def transaction(self):
        transaction = []
        self.transactions.append(transaction)
        try:
            yield RecordingCursor(self, transaction)
        except Exception:
            transaction.append(("rollback",))
            raise
        else:
            transaction.append(("commit",))

    async def fetch_all(self, query, params=None):
        self.calls.append((query, params))
        project_id = (params or {}).get("project_id", "project-1")
        return [
            {
                "project_id": project_id,
                "user_name": "ada",
                "name": "Research",
                "description": "A project description",
                "access_mode": "open",
                "status": "active",
                "domain_config": {},
                "allowed_projects": [],
                "session_count": 0,
                "created_at": None,
                "updated_at": None,
                "archived_at": None,
                "deleted_at": None,
                "last_activity_at": None,
            }
        ]


class RecordingCursor:
    def __init__(self, postgres, transaction):
        self.postgres = postgres
        self.transaction = transaction

    async def execute(self, query, params=None):
        self.transaction.append((query, params))
        self.postgres.calls.append((query, params))
        if self.postgres.fail_on_content and "document_content" in query:
            raise RuntimeError("content insert failed")


@pytest.mark.asyncio
async def test_project_creation_seeds_queued_project_file_in_same_transaction():
    postgres = RecordingPostgres()
    manager = ProjectManager(
        resources=type("Resources", (), {"postgres": postgres})(),
        user_name="ada",
    )

    await manager.create_project(
        "Research",
        domain_config=make_domain_config(version=0),
        description="A project description",
    )

    transaction = postgres.transactions[0]
    assert transaction[-1] == ("commit",)
    queries = [entry[0] for entry in transaction if entry[0] != "rollback"]
    assert any("INSERT INTO public.projects" in query for query in queries)
    assert any("INSERT INTO public.document_workspace_sources" in query for query in queries)
    assert any("INSERT INTO public.project_documents" in query for query in queries)
    assert any("INSERT INTO public.document_content" in query for query in queries)

    document_insert = next(
        entry
        for entry in transaction
        if entry[0] != "commit"
        and entry[0] != "rollback"
        and "INSERT INTO public.project_documents" in entry[0]
    )
    assert document_insert[1][4] == PROJECT_FILE_PATH
    assert "'queued'" in " ".join(document_insert[0].split())


@pytest.mark.asyncio
async def test_project_creation_rolls_back_when_project_file_insert_fails():
    postgres = RecordingPostgres(fail_on_content=True)
    manager = ProjectManager(
        resources=type("Resources", (), {"postgres": postgres})(),
        user_name="ada",
    )

    with pytest.raises(RuntimeError, match="content insert failed"):
        await manager.create_project(
            "Research",
            domain_config=make_domain_config(version=0),
        )

    assert postgres.transactions[0][-1] == ("rollback",)


def test_project_markdown_seed_contains_trusted_metadata_and_is_utf8():
    content = build_project_markdown("Research", "A project description")
    assert content.startswith("# Research\n")
    assert "A project description" in content
    assert content.endswith("\n")
    content.encode("utf-8")


def test_project_deletion_includes_managed_workspace_tables():
    assert "document_content" in ProjectDeletionWriter._PROJECT_TABLES
    assert "project_documents" in ProjectDeletionWriter._PROJECT_TABLES
    assert "document_workspace_sources" in ProjectDeletionWriter._PROJECT_TABLES
