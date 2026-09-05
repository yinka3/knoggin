from contextlib import asynccontextmanager

import pytest

from core.knowledge.documents.filesystem import ProjectFilesystemFactory
from core.project.project_files import PROJECT_FILE_PATH, build_project_markdown
from core.project.project_manager import ProjectManager
from tests.fixtures.factories import make_domain_config


class RecordingPostgres:
    def __init__(self, *, fail_on_project_insert=False):
        self.calls = []
        self.transactions = []
        self.fail_on_project_insert = fail_on_project_insert

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
        if self.postgres.fail_on_project_insert and "INSERT INTO public.projects" in query:
            raise RuntimeError("project insert failed")


@pytest.mark.asyncio
async def test_project_creation_seeds_native_project_file(tmp_path):
    postgres = RecordingPostgres()
    filesystem_factory = ProjectFilesystemFactory(tmp_path / "projects")
    manager = ProjectManager(
        resources=type("Resources", (), {"postgres": postgres})(),
        user_name="ada",
        filesystem_factory=filesystem_factory,
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
    assert not any("INSERT INTO public.project_documents" in query for query in queries)
    created_project_id = transaction[0][1]["project_id"]
    assert (
        filesystem_factory.for_project(created_project_id).read_bytes(PROJECT_FILE_PATH)
        == build_project_markdown("Research", "A project description").encode("utf-8")
    )


@pytest.mark.asyncio
async def test_project_creation_removes_native_project_file_when_database_insert_fails(
    tmp_path,
):
    postgres = RecordingPostgres(fail_on_project_insert=True)
    filesystem_factory = ProjectFilesystemFactory(tmp_path / "projects")
    manager = ProjectManager(
        resources=type("Resources", (), {"postgres": postgres})(),
        user_name="ada",
        filesystem_factory=filesystem_factory,
    )

    with pytest.raises(RuntimeError, match="project insert failed"):
        await manager.create_project(
            "Research",
            domain_config=make_domain_config(version=0),
        )

    assert postgres.transactions[0][-1] == ("rollback",)
    project_id = postgres.transactions[0][0][1]["project_id"]
    assert not filesystem_factory.for_project(project_id).root.joinpath(
        PROJECT_FILE_PATH
    ).exists()


def test_project_markdown_seed_contains_trusted_metadata_and_is_utf8():
    content = build_project_markdown("Research", "A project description")
    assert content.startswith("# Research\n")
    assert "A project description" in content
    assert content.endswith("\n")
    content.encode("utf-8")
