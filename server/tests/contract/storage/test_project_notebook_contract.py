"""Durable notebook ownership and revision checks."""

from uuid import uuid4

import pytest

from common.schema.project_notebook import NotebookEntryInput, NotebookReplace, NotebookWrite
from core.project.notebook_store import (
    NotebookConflict,
    NotebookReferenceUnavailable,
    ProjectNotebookStore,
)


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_project_notebooks_are_scoped_and_revision_checked(real_postgres_client):
    client = real_postgres_client
    store = ProjectNotebookStore(client, "ada")
    await client.execute(
        """INSERT INTO public.sessions (session_id, project_id, user_name)
           VALUES ('nb-session', 'project-1', 'ada'), ('other-session', 'project-2', 'ada')"""
    )
    await client.execute(
        """INSERT INTO public.messages
           (user_name, session_id, project_id, message_id, role, content)
           VALUES ('ada', 'nb-session', 'project-1', 9901, 'user', 'A source'),
                  ('ada', 'other-session', 'project-2', 9902, 'user', 'Other project')"""
    )
    first = await store.create("project-1", NotebookWrite(
        title="Research", view="source_desk",
        entries=[NotebookEntryInput(kind="message", target_id="9901")],
    ))
    assert first["revision"] == 1
    assert first["entries"][0]["source_version"] == "1"
    assert (await store.get("project-2", first["notebook_id"])) is None
    assert len(await store.list("project-1")) == 1
    with pytest.raises(NotebookReferenceUnavailable):
        await store.create("project-1", NotebookWrite(
            title="Wrong source", view="source_desk",
            entries=[NotebookEntryInput(kind="message", target_id="9902")],
        ))
    replacement = NotebookReplace(
        title="Research updated", view="timeline", expected_revision=1,
        entries=[NotebookEntryInput(
            entry_id=first["entries"][0]["entry_id"], kind="message", target_id="9901"
        )],
    )
    second = await store.replace("project-1", first["notebook_id"], replacement)
    assert second["revision"] == 2 and second["view"] == "timeline"
    with pytest.raises(NotebookConflict):
        await store.replace("project-1", first["notebook_id"], replacement)
    with pytest.raises(NotebookConflict):
        await store.delete("project-1", first["notebook_id"], 1)
    assert await store.delete("project-1", first["notebook_id"], 2)
    assert await store.get("project-1", first["notebook_id"]) is None


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_notebook_edits_require_active_project(real_postgres_client):
    client = real_postgres_client
    store = ProjectNotebookStore(client, "ada")
    notebook = await store.create("project-1", NotebookWrite(title="Sources", view="source_desk"))
    await client.execute("UPDATE public.projects SET status = 'archived' WHERE project_id = 'project-1'")
    assert (await store.get("project-1", notebook["notebook_id"])) is not None
    with pytest.raises(NotebookConflict):
        await store.create("project-1", NotebookWrite(title="New", view="source_desk"))
    with pytest.raises(NotebookConflict):
        await store.replace("project-1", notebook["notebook_id"], NotebookReplace(
            title="Changed", view="source_desk", expected_revision=1,
        ))
    with pytest.raises(NotebookConflict):
        await store.delete("project-1", notebook["notebook_id"], 1)
    assert (await store.get("project-1", str(uuid4()))) is None
