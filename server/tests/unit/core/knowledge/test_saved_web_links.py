from contextlib import asynccontextmanager
from copy import deepcopy

import pytest

from core.knowledge.documents import DocumentService


class BookmarkCursor:
    def __init__(self, store: "BookmarkStore") -> None:
        self._store = store
        self._result = None

    async def execute(self, query, params=None) -> None:
        normalized = " ".join(query.split())
        self._store.queries.append(normalized)
        if normalized.startswith("INSERT INTO public.saved_web_links"):
            (
                link_id,
                project_id,
                url,
                title,
                summary,
                created_at,
                updated_at,
            ) = params
            if any(
                row["project_id"] == project_id and row["url"] == url
                for row in self._store.rows
            ):
                raise ValueError("duplicate saved web link")
            row = {
                "link_id": link_id,
                "project_id": project_id,
                "url": url,
                "title": title,
                "summary": summary,
                "created_at": created_at,
                "updated_at": updated_at,
            }
            self._store.rows.append(row)
            self._result = deepcopy(row)
            return
        if normalized.startswith("UPDATE public.saved_web_links"):
            title, summary, updated_at, link_id, project_id = params
            row = next(
                (
                    row
                    for row in self._store.rows
                    if row["link_id"] == link_id and row["project_id"] == project_id
                ),
                None,
            )
            if row is not None:
                row.update(title=title, summary=summary, updated_at=updated_at)
            self._result = deepcopy(row) if row is not None else None
            return
        if normalized.startswith("DELETE FROM public.saved_web_links"):
            link_id, project_id = params
            row = next(
                (
                    row
                    for row in self._store.rows
                    if row["link_id"] == link_id and row["project_id"] == project_id
                ),
                None,
            )
            if row is not None:
                self._store.rows.remove(row)
                self._result = {"link_id": link_id}
            else:
                self._result = None
            return
        raise AssertionError(f"Unexpected query: {normalized}")

    async def fetchone(self):
        return self._result


class BookmarkStore:
    def __init__(self) -> None:
        self.rows = []
        self.queries = []

    @asynccontextmanager
    async def transaction(self):
        yield BookmarkCursor(self)

    async def fetch_all(self, query, params=None):
        normalized = " ".join(query.split())
        self.queries.append(normalized)
        if "FROM public.saved_web_links" not in normalized:
            raise AssertionError(f"Unexpected query: {normalized}")
        if "WHERE link_id = %s" in normalized:
            link_id, project_id = params
            return [
                deepcopy(row)
                for row in self.rows
                if row["link_id"] == link_id and row["project_id"] == project_id
            ]
        project_id, limit = params
        return [
            deepcopy(row)
            for row in sorted(
                self.rows,
                key=lambda row: (row["updated_at"], row["link_id"]),
                reverse=True,
            )
            if row["project_id"] == project_id
        ][:limit]


class UnusedEmbedding:
    pass


def make_service(store: BookmarkStore, project_id: str = "project-1") -> DocumentService:
    return DocumentService(
        project_id=project_id,
        postgres_client=store,
        embedding_service=UnusedEmbedding(),
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_saved_web_links_are_project_owned_lightweight_bookmarks():
    store = BookmarkStore()
    service = make_service(store)

    saved = await service.save_web_link(
        url="https://example.com/research?q=knoggin",
        title="Research note",
        summary="A user-chosen reference.",
    )
    listed = await service.list_saved_web_links()

    assert listed == [saved]
    assert saved["project_id"] == "project-1"
    assert saved["url"] == "https://example.com/research?q=knoggin"
    assert saved["title"] == "Research note"
    assert saved["summary"] == "A user-chosen reference."
    assert all(
        "document_chunks" not in query
        and "project_documents" not in query
        and "document_extractions" not in query
        for query in store.queries
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_saved_web_link_summary_can_evolve_without_replacing_bookmark():
    store = BookmarkStore()
    service = make_service(store)
    saved = await service.save_web_link(url="https://example.com/article")

    updated = await service.update_saved_web_link(
        link_id=saved["link_id"],
        title="Article",
        summary="A concise evolving note.",
    )

    assert updated["link_id"] == saved["link_id"]
    assert updated["url"] == "https://example.com/article"
    assert updated["title"] == "Article"
    assert updated["summary"] == "A concise evolving note."


@pytest.mark.unit
@pytest.mark.no_network
async def test_saved_web_link_rejects_non_http_and_cross_project_mutation():
    store = BookmarkStore()
    project_one = make_service(store, "project-1")
    project_two = make_service(store, "project-2")

    with pytest.raises(ValueError, match="absolute HTTP"):
        await project_one.save_web_link(url="file:///private/notes.txt")

    saved = await project_one.save_web_link(url="https://example.com/private")
    with pytest.raises(FileNotFoundError, match="Saved web link not found"):
        await project_two.update_saved_web_link(
            link_id=saved["link_id"],
            summary="not allowed",
        )
    with pytest.raises(FileNotFoundError, match="Saved web link not found"):
        await project_two.delete_saved_web_link(link_id=saved["link_id"])

    assert await project_one.delete_saved_web_link(link_id=saved["link_id"]) == {
        "link_id": saved["link_id"],
        "deleted": True,
    }
