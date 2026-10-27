from types import SimpleNamespace
from uuid import uuid4

import pytest

from common.exceptions import WorkspaceConflictError
from common.schema.settings import RootConfig
from core.knowledge.documents import ProjectFilesystemFactory
from core.project.project_manager import ProjectManager
from tests.fixtures.factories import make_domain_config


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_a_project_holds_up_to_its_limit_of_workspaces(real_postgres_client, tmp_path):
    async def get_vp01(_language):
        return SimpleNamespace(extract_entities=lambda *_args, **_kwargs: ())

    manager = ProjectManager(
        SimpleNamespace(postgres=real_postgres_client, get_vp01=get_vp01),
        user_name="ada",
        filesystem_factory=ProjectFilesystemFactory(tmp_path / "projects"),
        config_manager=SimpleNamespace(config=RootConfig()),
    )
    project = await manager.create_project("Repos", domain_config=make_domain_config(version=0))
    project_id = project["id"]

    for name in ("backend", "frontend", "infra"):
        await manager.add_workspace(project_id, name)
    with pytest.raises(ValueError):
        await manager.add_workspace(project_id, "docs")  # over the default of 3
    with pytest.raises(ValueError):
        await manager.add_workspace(project_id, "a/b")
    assert (await manager.get_project(project_id))["workspaces"] == [
        "backend", "frontend", "infra",
    ]

    document_id = uuid4()
    await real_postgres_client.execute(
        """INSERT INTO public.project_documents (
               document_id, project_id, original_name, relative_path, size_bytes, content_hash
           ) VALUES (%s, %s, 'main.py', 'backend/main.py', 1, 'hash')""",
        (document_id, project_id),
    )
    with pytest.raises(WorkspaceConflictError):
        await manager.remove_workspace(project_id, "backend")
    # A file merely starting with the name is outside the workspace.
    await real_postgres_client.execute(
        "UPDATE public.project_documents SET relative_path = 'backend-notes.md' "
        "WHERE document_id = %s",
        (document_id,),
    )
    assert await manager.remove_workspace(project_id, "backend") == ["frontend", "infra"]
    assert await manager.remove_workspace(project_id, "backend") is None
    await manager.add_workspace(project_id, "docs")
    assert (await manager.get_project(project_id))["workspaces"] == ["frontend", "infra", "docs"]
