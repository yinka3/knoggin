from types import SimpleNamespace

import psycopg
import pytest

from core.project.project_manager import ProjectManager


async def _install_scope_insert_failure(client):
    await client.execute(
        """
        CREATE OR REPLACE FUNCTION fail_project_scope_insert()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'forced project scope insert failure';
        END;
        $$;
        """
    )
    await client.execute(
        """
        CREATE TRIGGER fail_project_scope_insert_trigger
        BEFORE INSERT ON project_read_scopes
        FOR EACH ROW EXECUTE FUNCTION fail_project_scope_insert();
        """
    )


async def _remove_scope_insert_failure(client):
    await client.execute(
        "DROP TRIGGER IF EXISTS fail_project_scope_insert_trigger "
        "ON project_read_scopes"
    )
    await client.execute("DROP FUNCTION IF EXISTS fail_project_scope_insert()")


def _manager(client) -> ProjectManager:
    return ProjectManager(SimpleNamespace(postgres=client), user_name="ada")


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_project_creation_rolls_back_when_scope_insert_fails(
    real_postgres_client,
    monkeypatch,
):
    project_id = "project-creation-atomicity"
    manager = _manager(real_postgres_client)
    monkeypatch.setattr(
        "core.project.project_manager.uuid.uuid4",
        lambda: project_id,
    )
    await _install_scope_insert_failure(real_postgres_client)

    try:
        with pytest.raises(psycopg.Error, match="forced project scope insert failure"):
            await manager.create_project(
                "Atomic project",
                allowed_projects=["project-1"],
            )
    finally:
        await _remove_scope_insert_failure(real_postgres_client)

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM projects WHERE project_id = %s",
        (project_id,),
    ) == {"count": 0}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_scope_replacement_rolls_back_when_new_scope_insert_fails(
    real_postgres_client,
):
    await real_postgres_client.execute(
        """
        INSERT INTO projects (project_id, user_name, name)
        VALUES ('project-3', 'ada', 'Project 3')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO project_read_scopes (user_name, project_id, readable_project_id)
        VALUES ('ada', 'project-1', 'project-2')
        """
    )
    await _install_scope_insert_failure(real_postgres_client)

    try:
        with pytest.raises(psycopg.Error, match="forced project scope insert failure"):
            await _manager(real_postgres_client).update_project(
                "project-1",
                allowed_projects=["project-3"],
            )
    finally:
        await _remove_scope_insert_failure(real_postgres_client)

    rows = await real_postgres_client.fetch_all(
        """
        SELECT readable_project_id
        FROM project_read_scopes
        WHERE user_name = %s AND project_id = %s
        """,
        ("ada", "project-1"),
    )
    assert rows == [{"readable_project_id": "project-2"}]
