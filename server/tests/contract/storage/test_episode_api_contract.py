"""Real PostgreSQL round trips through HTTP, port, store, reader and writer.

ProjectManager admission/release are real; only project runtime construction and
embedding inference are faked so this remains a no-model service contract.
"""

import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from api.app import create_app
from common.exceptions import EpisodeEditConflictError
from common.schema.settings import RootConfig
from core.knowledge.db.writers.episode_writer import EpisodeWriter
from core.knowledge.store import KnowledgeStore
from core.project.project_manager import ProjectManager
from runtime.api_port import ApplicationRuntimePort

pytestmark = [
    pytest.mark.storage, pytest.mark.requires_postgres,
    pytest.mark.requires_pgvector, pytest.mark.no_network,
]
INITIAL = datetime(2026, 1, 1, tzinfo=timezone.utc)
PATH = "/v1/projects/project-1/episodes/episode-1"


async def make_port(postgres):
    await postgres.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('s1', 'ada', 'project-1'), ('s2', 'ada', 'project-2');
        INSERT INTO messages (user_name, session_id, message_id, project_id, role, content, timestamp_ms)
        VALUES ('ada', 's1', 1, 'project-1', 'user', 'SECRET canonical source', 1000),
               ('ada', 's2', 2, 'project-2', 'user', 'Other project source', 2000);
        INSERT INTO episodes (episode_id, project_id, summary, updated_at, source_message_count)
        VALUES ('episode-1', 'project-1', 'Original', TIMESTAMPTZ '2026-01-01 00:00:00+00', 1),
               ('episode-2', 'project-2', 'Other project episode', TIMESTAMPTZ '2026-01-01 00:00:00+00', 1);
        INSERT INTO episode_messages (episode_id, project_id, session_id, message_id, message_position)
        VALUES ('episode-1', 'project-1', 's1', 1, 0), ('episode-2', 'project-2', 's2', 2, 0);
        """
    )
    embedding = SimpleNamespace(encode=AsyncMock(return_value=[[0.25] * 1024]))
    store = KnowledgeStore(postgres, embedding)
    projects = object.__new__(ProjectManager)
    projects.user_name = "ada"
    projects.pg = postgres
    projects._closed = False
    projects.active_projects = {}
    projects._project_leases = {}
    projects.maintenance_service = SimpleNamespace(lock=asyncio.Lock())
    projects.project_factory = SimpleNamespace(create=AsyncMock(
        return_value=SimpleNamespace(shutdown=AsyncMock()),
    ))
    port = ApplicationRuntimePort(SimpleNamespace(
        projects=projects, sessions=SimpleNamespace(user_name="ada"),
        resources=SimpleNamespace(knowledge_store=store),
        config_manager=SimpleNamespace(config=RootConfig()),
    ))
    return port, store, embedding


def body(revision):
    return dict(summary="Edited narrative", new_developments=["New development"],
                updates=[], unresolved=[], expected_updated_at=revision)


def client(port):
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
        base_url="http://test", headers={"X-User-Name": "ada"},
    )


async def test_episode_api_round_trip_preserves_sources_and_rejects_stale_foreign_archived_edits(
    real_postgres_client,
):
    postgres = real_postgres_client
    port, _, embedding = await make_port(postgres)
    sources_before = await postgres.fetch_all("SELECT * FROM episode_messages ORDER BY message_id")
    messages_before = await postgres.fetch_all("SELECT * FROM messages ORDER BY message_id")
    async with client(port) as http:
        read = await http.get(PATH)
        assert read.status_code == 200
        assert "SECRET" not in read.text
        revision = read.json()["updated_at"]
        changed = await http.patch(PATH, json=body(revision))
        assert changed.status_code == 200
        assert changed.json()["user_modified"] is True
        assert changed.json()["updated_at"] != revision
        reread = await http.get(PATH)
        assert reread.json()["summary"] == "Edited narrative"
        assert reread.json()["updated_at"] == changed.json()["updated_at"]

        stale = await http.patch(PATH, json=body(revision))
        assert stale.status_code == 409
        assert stale.json()["error"]["code"] == "episode_conflict"
        foreign = "/v1/projects/project-1/episodes/episode-2"
        assert (await http.get(foreign)).status_code == 404
        assert (await http.patch(foreign, json=body(revision))).status_code == 404
        await postgres.execute("UPDATE projects SET status = 'archived' WHERE project_id = 'project-1'")
        assert (await http.get(PATH)).status_code == 200
        assert (await http.patch(PATH, json=body(changed.json()["updated_at"]))).status_code == 403

    assert embedding.encode.await_count == 1
    assert port.runtime.projects._project_leases == {}
    assert port.runtime.projects.active_projects == {}
    assert await postgres.fetch_all("SELECT * FROM episode_messages ORDER BY message_id") == sources_before
    assert await postgres.fetch_all("SELECT * FROM messages ORDER BY message_id") == messages_before
    row = await postgres.fetch_one(
        """SELECT summary, user_modified, embedding = %s::vector AS vector_matches
           FROM episodes WHERE episode_id = 'episode-1'""",
        (json.dumps([0.25] * 1024),),
    )
    assert row == dict(summary="Edited narrative", user_modified=True, vector_matches=True)
    # Storage also rejects an archived owner, even if the API is bypassed.
    with pytest.raises(EpisodeEditConflictError):
        await EpisodeWriter(postgres).edit_episode(
            episode_id="episode-1", user_name="ada", project_id="project-1",
            summary="Must not apply", new_developments=[], updates=[], unresolved=[],
            embedding=[0.5] * 1024,
            expected_updated_at=datetime.fromisoformat(changed.json()["updated_at"].replace("Z", "+00:00")),
        )
    unchanged = await postgres.fetch_one("SELECT summary FROM episodes WHERE episode_id = 'episode-1'")
    assert unchanged["summary"] == "Edited narrative"


async def test_http_edit_detects_a_real_storage_race_after_embedding_starts(real_postgres_client):
    postgres = real_postgres_client
    port, _, embedding = await make_port(postgres)
    started, finish = asyncio.Event(), asyncio.Event()

    async def encode(_texts):
        started.set()
        await finish.wait()
        return [[0.25] * 1024]

    embedding.encode.side_effect = encode
    async with client(port) as http:
        pending = asyncio.create_task(http.patch(PATH, json=body(INITIAL.isoformat())))
        try:
            await asyncio.wait_for(started.wait(), timeout=5)
            await EpisodeWriter(postgres).edit_episode(
                episode_id="episode-1", user_name="ada", project_id="project-1",
                summary="Concurrent winner", new_developments=[], updates=[], unresolved=[],
                embedding=[0.5] * 1024, expected_updated_at=INITIAL,
            )
        finally:
            finish.set()
        response = await asyncio.wait_for(pending, timeout=5)
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "episode_conflict"
    row = await postgres.fetch_one(
        """SELECT summary, embedding = %s::vector AS vector_matches
           FROM episodes WHERE episode_id = 'episode-1'""",
        (json.dumps([0.5] * 1024),),
    )
    assert row == dict(summary="Concurrent winner", vector_matches=True)
    assert port.runtime.projects._project_leases == {}
