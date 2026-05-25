import asyncio
import inspect
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from knoggin.core import AgentDirectory, Project, SessionFiles
from knoggin.types import AgentConfig, FileInfo, FileSearchResult


class FakeFileRAG:
    def __init__(self):
        self.ingested = None
        self.searched = None
        self.deleted = None

    async def ingest_file(self, path, original_name):
        self.ingested = (path, original_name)
        return {
            "file_id": "file_1",
            "original_name": original_name,
            "extension": ".md",
            "size_bytes": 12,
            "chunk_count": 2,
            "uploaded_at": "now",
        }

    def list_files(self):
        return [
            {
                "file_id": "file_1",
                "original_name": "notes.md",
                "extension": ".md",
            }
        ]

    async def search(self, query, n_results=5, file_filter=None):
        self.searched = (query, n_results, file_filter)
        return [
            {
                "content": "matching text",
                "file_name": "notes.md",
                "file_id": "file_1",
                "score": 0.91,
                "raw_score": 2.4,
            }
        ]

    async def delete_file(self, file_id):
        self.deleted = file_id
        return True


class FakeAgentManager:
    def __init__(self):
        self.agent = SimpleNamespace(
            id="agent_1",
            name="STELLA",
            persona="direct",
            instructions=None,
            model=None,
            temperature=0.7,
            enabled_tools=None,
            is_default=True,
            is_spawned=False,
            spawned_by=None,
            created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    async def list_agents(self):
        return [self.agent]

    async def get_agent(self, agent_id):
        return self.agent if agent_id == "agent_1" else None

    async def get_agent_by_name(self, name):
        return self.agent if name == "STELLA" else None

    async def get_default_agent_id(self):
        return "agent_1"

    async def create_agent(self, **kwargs):
        return SimpleNamespace(id="agent_2", is_default=False, is_spawned=False, spawned_by=None, created_at=None, **kwargs)

    async def update_agent(self, agent_id, **kwargs):
        if agent_id != "agent_1":
            return None
        for key, value in kwargs.items():
            if value is not None:
                setattr(self.agent, key, value)
        return self.agent

    async def delete_agent(self, agent_id):
        return agent_id == "agent_1"

    async def set_default_agent(self, agent_id):
        return agent_id == "agent_1"


class SessionFilesTests(unittest.IsolatedAsyncioTestCase):
    async def test_add_defaults_original_name_from_path(self):
        rag = FakeFileRAG()
        files = SessionFiles(SimpleNamespace(file_rag=rag))

        result = await files.add("docs/notes.md")

        self.assertIsInstance(result, FileInfo)
        self.assertEqual(rag.ingested, ("docs/notes.md", "notes.md"))
        self.assertEqual(result.original_name, "notes.md")

    async def test_list_uses_to_thread(self):
        rag = FakeFileRAG()
        files = SessionFiles(SimpleNamespace(file_rag=rag))

        async def fake_to_thread(func, *args, **kwargs):
            return func(*args, **kwargs)

        with patch("knoggin.core.asyncio.to_thread", fake_to_thread) as mocked:
            result = await files.list()

        self.assertIsInstance(result[0], FileInfo)
        self.assertEqual(result[0].file_id, "file_1")
        self.assertIsNotNone(mocked)

    async def test_search_and_delete_delegate_to_file_rag(self):
        rag = FakeFileRAG()
        files = SessionFiles(SimpleNamespace(file_rag=rag))

        result = await files.search("deadline", limit=3, file_id="file_1")
        deleted = await files.delete("file_1")

        self.assertIsInstance(result[0], FileSearchResult)
        self.assertEqual(rag.searched, ("deadline", 3, "file_1"))
        self.assertTrue(deleted)
        self.assertEqual(rag.deleted, "file_1")


class AgentDirectoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_agent_directory_maps_engine_configs(self):
        directory = AgentDirectory(FakeAgentManager())

        agents = await directory.list()
        agent = await directory.get("agent_1")
        default_id = await directory.get_default_id()

        self.assertIsInstance(agents[0], AgentConfig)
        self.assertEqual(agent.name, "STELLA")
        self.assertEqual(agent.created_at, "2026-01-01T00:00:00+00:00")
        self.assertEqual(default_id, "agent_1")

    async def test_agent_directory_crud_methods_delegate(self):
        directory = AgentDirectory(FakeAgentManager())

        created = await directory.create(name="Researcher", persona="precise")
        updated = await directory.update("agent_1", name="Updated")
        missing = await directory.update("missing", name="Nope")
        deleted = await directory.delete("agent_1")
        defaulted = await directory.set_default("agent_1")

        self.assertEqual(created.name, "Researcher")
        self.assertEqual(updated.name, "Updated")
        self.assertIsNone(missing)
        self.assertTrue(deleted)
        self.assertTrue(defaulted)


class ProjectContractTests(unittest.TestCase):
    def test_project_session_has_no_topics_parameter(self):
        params = inspect.signature(Project.session).parameters

        self.assertNotIn("topics", params)


if __name__ == "__main__":
    unittest.main()
