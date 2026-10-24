"""Project-scoped notebook persistence and reference admission."""

from __future__ import annotations

import json
from uuid import UUID, uuid4

from common.artifact_retention import ARTIFACT_RETENTION_PERIOD
from common.schema.project_notebook import NotebookEntry, NotebookWrite


class NotebookConflict(ValueError):
    pass


class NotebookReferenceUnavailable(ValueError):
    pass


class ProjectNotebookStore:
    def __init__(self, client, user_name: str):
        self.client = client
        self.user_name = user_name

    async def _active_project(self, cursor, project_id: str) -> None:
        await cursor.execute(
            "SELECT status FROM public.projects WHERE user_name = %s AND project_id = %s FOR UPDATE",
            (self.user_name, project_id),
        )
        row = await cursor.fetchone()
        if row is None or row["status"] != "active":
            raise NotebookConflict("Notebook edits require an active project")

    async def _source(self, cursor, project_id: str, kind: str, target_id: str):
        if kind in {"document", "saved_link", "artifact"}:
            try:
                UUID(target_id)
            except ValueError as exc:
                raise NotebookReferenceUnavailable("Invalid source ID") from exc
        if kind == "message":
            if not target_id.isdecimal():
                raise NotebookReferenceUnavailable("Invalid message ID")
            query = """SELECT selected_revision::text AS version
                       FROM public.messages WHERE user_name = %s AND project_id = %s
                         AND message_id = %s AND lifecycle_state <> 'superseded'"""
            args = (self.user_name, project_id, int(target_id))
        elif kind == "document":
            query = """SELECT content_hash AS version FROM public.project_documents
                       WHERE project_id = %s AND document_id = %s AND status <> 'deleted'"""
            args = (project_id, target_id)
        elif kind == "saved_link":
            query = """SELECT updated_at::text AS version FROM public.saved_web_links
                       WHERE project_id = %s AND link_id = %s"""
            args = (project_id, target_id)
        elif kind == "episode":
            query = """SELECT updated_at::text AS version FROM public.episodes
                       WHERE project_id = %s AND episode_id = %s"""
            args = (project_id, target_id)
        elif kind == "artifact":
            query = """SELECT artifact.current_revision::text AS version
                       FROM public.project_artifacts AS artifact
                       JOIN public.sessions AS session
                         ON session.session_id = artifact.session_id
                        AND session.project_id = artifact.project_id
                       WHERE artifact.user_name = %s AND artifact.project_id = %s
                         AND artifact.artifact_id = %s AND session.user_name = %s
                         AND (session.status = 'open' OR
                              (session.status = 'deleted' AND
                               session.deleted_at > statement_timestamp() - %s::interval))"""
            args = (self.user_name, project_id, target_id, self.user_name, ARTIFACT_RETENTION_PERIOD)
        else:
            raise NotebookReferenceUnavailable("Unknown source kind")
        await cursor.execute(query, args)
        return await cursor.fetchone()

    async def _entries(self, cursor, project_id: str, requested, previous=()):
        old = {entry["entry_id"]: entry for entry in previous}
        result = []
        for candidate in requested:
            entry_id = str(candidate.entry_id or uuid4())
            prior = old.get(entry_id)
            if prior is not None and (
                prior["kind"] != candidate.kind or prior["target_id"] != candidate.target_id
            ):
                raise NotebookConflict("An entry ID cannot point to a different source")
            source = await self._source(cursor, project_id, candidate.kind, candidate.target_id)
            if source is None and prior is None:
                raise NotebookReferenceUnavailable("Source is unavailable in this project")
            version = prior["source_version"] if prior is not None else source["version"]
            entry = NotebookEntry.model_validate({
                **candidate.model_dump(mode="json"),
                "entry_id": entry_id,
                "source_version": version,
            })
            result.append(entry.model_dump(mode="json"))
        if len({entry["entry_id"] for entry in result}) != len(result):
            raise NotebookConflict("Duplicate entry ID")
        return result

    @staticmethod
    def _row(row):
        if row is None:
            return None
        entries = row["entries"]
        if isinstance(entries, str):
            entries = json.loads(entries)
        return {**row, "notebook_id": str(row["notebook_id"]), "entries": entries}

    async def create(self, project_id: str, body: NotebookWrite):
        async with self.client.transaction() as cursor:
            await self._active_project(cursor, project_id)
            entries = await self._entries(cursor, project_id, body.entries)
            await cursor.execute(
                """INSERT INTO public.project_notebooks
                   (notebook_id, project_id, user_name, title, view, entries)
                   VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                   RETURNING *""",
                (str(uuid4()), project_id, self.user_name, body.title, body.view, json.dumps(entries)),
            )
            return self._row(await cursor.fetchone())

    async def list(self, project_id: str):
        rows = await self.client.fetch_all(
            """SELECT notebook.* FROM public.project_notebooks AS notebook
               JOIN public.projects AS project ON project.project_id = notebook.project_id
               WHERE notebook.project_id = %s AND notebook.user_name = %s
                 AND project.user_name = %s AND project.status <> 'deleted'
               ORDER BY notebook.updated_at DESC, notebook.notebook_id DESC LIMIT 100""",
            (project_id, self.user_name, self.user_name),
        )
        return [self._row(row) for row in rows]

    async def get(self, project_id: str, notebook_id: str):
        row = await self.client.fetch_one(
            """SELECT notebook.* FROM public.project_notebooks AS notebook
               JOIN public.projects AS project ON project.project_id = notebook.project_id
               WHERE notebook.project_id = %s AND notebook.notebook_id = %s
                 AND notebook.user_name = %s AND project.user_name = %s
                 AND project.status <> 'deleted'""",
            (project_id, notebook_id, self.user_name, self.user_name),
        )
        return self._row(row)

    async def replace(self, project_id: str, notebook_id: str, body):
        async with self.client.transaction() as cursor:
            await self._active_project(cursor, project_id)
            await cursor.execute(
                """SELECT * FROM public.project_notebooks
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s FOR UPDATE""",
                (project_id, notebook_id, self.user_name),
            )
            current = self._row(await cursor.fetchone())
            if current is None:
                return None
            if current["revision"] != body.expected_revision:
                raise NotebookConflict("Notebook revision changed")
            entries = await self._entries(cursor, project_id, body.entries, current["entries"])
            await cursor.execute(
                """UPDATE public.project_notebooks
                   SET title = %s, view = %s, entries = %s::jsonb,
                       revision = revision + 1, updated_at = clock_timestamp()
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s
                   RETURNING *""",
                (body.title, body.view, json.dumps(entries), project_id, notebook_id, self.user_name),
            )
            return self._row(await cursor.fetchone())

    async def delete(self, project_id: str, notebook_id: str, expected_revision: int):
        async with self.client.transaction() as cursor:
            await self._active_project(cursor, project_id)
            await cursor.execute(
                """DELETE FROM public.project_notebooks
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s
                     AND revision = %s RETURNING notebook_id""",
                (project_id, notebook_id, self.user_name, expected_revision),
            )
            if await cursor.fetchone() is not None:
                return True
            await cursor.execute(
                """SELECT 1 FROM public.project_notebooks
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s""",
                (project_id, notebook_id, self.user_name),
            )
            if await cursor.fetchone() is not None:
                raise NotebookConflict("Notebook revision changed")
            return False
