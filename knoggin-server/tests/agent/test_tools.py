"""Tests for agent/tools.py — the Tools class."""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.agent.tools import Tools


@pytest.fixture
def mock_store():
    store = MagicMock()
    store.search_entity.return_value = []
    store.get_related_entities.return_value = []
    store.get_recent_activity.return_value = []
    store.find_path_filtered.return_value = ([], False)
    store.get_hierarchy.return_value = []
    store.get_hot_topic_context_with_messages.return_value = {}
    return store


@pytest.fixture
def mock_resolver():
    resolver = MagicMock()
    resolver.resolve_entity_name = AsyncMock(return_value=None)
    resolver.embedding_service = MagicMock()
    return resolver


@pytest.fixture
def mock_redis():
    r = MagicMock()
    r.hmget = AsyncMock(return_value=[])
    r.hgetall = AsyncMock(return_value={})
    r.get = AsyncMock(return_value=None)
    r.hset = AsyncMock()
    r.zrank = AsyncMock(return_value=None)
    r.zrange = AsyncMock(return_value=[])

    mock_pipe = MagicMock()
    mock_pipe.hget = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    r.pipeline.return_value = mock_pipe

    return r


@pytest.fixture
def mock_topic_config():
    tc = MagicMock()
    tc.active_topics = ["General", "Identity", "Tech"]
    return tc


@pytest.fixture
def mock_memory():
    mem = AsyncMock()
    mem.save_memory_dict = AsyncMock(return_value={"success": True, "memory_id": "m1"})
    mem.forget_memory_dict = AsyncMock(return_value={"success": True})
    mem.get_memory_blocks_dict = AsyncMock(return_value={})
    return mem


@pytest.fixture
def tools(mock_store, mock_resolver, mock_redis, mock_topic_config, mock_memory):
    return Tools(
        user_name="TestUser",
        store=mock_store,
        ent_resolver=mock_resolver,
        redis_client=mock_redis,
        session_id="test-session",
        topic_config=mock_topic_config,
        search_config={},
        file_rag=None,
        mcp_manager=None,
        memory=mock_memory,
    )


class TestSearchEntity:

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_results(self, tools, mock_store):
        mock_store.search_entity.return_value = []
        result = await tools.search_entity("Alice")
        assert result == []

    @pytest.mark.asyncio
    async def test_calls_store_with_active_topics(self, tools, mock_store):
        mock_store.search_entity.return_value = []
        await tools.search_entity("Alice", limit=3)
        mock_store.search_entity.assert_called_once_with(
            "Alice", ["General", "Identity", "Tech"], 3
        )

    @pytest.mark.asyncio
    async def test_hydrates_connection_evidence(self, tools, mock_store, mock_redis):
        mock_store.search_entity.return_value = [{
            "id": 1, "canonical_name": "Alice", "type": "person",
            "topic": "Identity", "facts": [], "aliases": [],
            "top_connections": [{"canonical_name": "Bob", "evidence_ids": ["msg:1", "msg:2"], "weight": 3}]
        }]
        mock_redis.hmget.return_value = [
            json.dumps({"message": "hi", "timestamp": "2025-01-01T00:00:00Z"}),
            json.dumps({"message": "hey", "timestamp": "2025-01-01T00:01:00Z"}),
        ]
        result = await tools.search_entity("Alice")
        assert len(result) == 1
        conn = result[0]["top_connections"][0]
        assert "evidence" in conn
        assert "evidence_ids" not in conn


class TestGetConnections:

    @pytest.mark.asyncio
    async def test_entity_not_found(self, tools, mock_resolver):
        mock_resolver.resolve_entity_name.return_value = None
        result = await tools.get_connections("Nobody")
        assert len(result) == 1
        assert "error" in result[0]

    @pytest.mark.asyncio
    async def test_resolves_and_queries(self, tools, mock_resolver, mock_store):
        mock_resolver.resolve_entity_name.return_value = "Alice Smith"
        mock_store.get_related_entities.return_value = [
            {"source": "Alice Smith", "target": "Bob", "weight": 5, "evidence_ids": []}
        ]
        result = await tools.get_connections("alice")
        mock_resolver.resolve_entity_name.assert_called_with("alice")
        mock_store.get_related_entities.assert_called_once()
        assert result[0]["source"] == "Alice Smith"

    @pytest.mark.asyncio
    async def test_hidden_connections_through_inactive_topics(self, tools, mock_resolver, mock_store):
        mock_resolver.resolve_entity_name.return_value = "Alice"
        mock_store.get_related_entities.side_effect = [
            [],
            [{"source": "Alice", "target": "Secret"}],
        ]
        result = await tools.get_connections("Alice")
        assert result[0]["hidden"] is True
        assert result[0]["count"] == 1

    @pytest.mark.asyncio
    async def test_no_connections_at_all(self, tools, mock_resolver, mock_store):
        mock_resolver.resolve_entity_name.return_value = "Alice"
        mock_store.get_related_entities.side_effect = [[], []]
        result = await tools.get_connections("Alice")
        assert result == []


class TestGetRecentActivity:

    @pytest.mark.asyncio
    async def test_entity_not_found(self, tools, mock_resolver):
        mock_resolver.resolve_entity_name.return_value = None
        result = await tools.get_recent_activity("Ghost")
        assert "error" in result[0]

    @pytest.mark.asyncio
    async def test_calls_store_with_hours(self, tools, mock_resolver, mock_store):
        mock_resolver.resolve_entity_name.return_value = "Alice"
        mock_store.get_recent_activity.return_value = []
        await tools.get_recent_activity("Alice", hours=168)
        mock_store.get_recent_activity.assert_called_once_with(
            "Alice", active_topics=["General", "Identity", "Tech"], hours=168
        )


class TestFindPath:

    @pytest.mark.asyncio
    async def test_entity_a_not_found(self, tools, mock_resolver):
        mock_resolver.resolve_entity_name.return_value = None
        result = await tools.find_path("Ghost", "Alice")
        assert "error" in result[0]

    @pytest.mark.asyncio
    async def test_entity_b_not_found(self, tools, mock_resolver):
        mock_resolver.resolve_entity_name.side_effect = ["Alice", None]
        result = await tools.find_path("Alice", "Ghost")
        assert "error" in result[0]

    @pytest.mark.asyncio
    async def test_path_found(self, tools, mock_resolver, mock_store):
        mock_resolver.resolve_entity_name.side_effect = ["Alice", "Carol"]
        mock_store.find_path_filtered.return_value = (
            [{"step": 0, "entity_a": "Alice", "entity_b": "Bob", "evidence_ids": []},
             {"step": 1, "entity_a": "Bob", "entity_b": "Carol", "evidence_ids": []}],
            False
        )
        result = await tools.find_path("Alice", "Carol")
        assert len(result) == 2
        assert result[0]["entity_a"] == "Alice"


class TestMemoryTools:

    @pytest.mark.asyncio
    async def test_save_memory_delegates(self, tools, mock_memory):
        result = await tools.save_memory("User likes Python", "Tech")
        mock_memory.save_memory_dict.assert_awaited_once_with("User likes Python", "Tech")

    @pytest.mark.asyncio
    async def test_forget_memory_delegates(self, tools, mock_memory):
        result = await tools.forget_memory("m1")
        mock_memory.forget_memory_dict.assert_awaited_once_with("m1")

    @pytest.mark.asyncio
    async def test_save_memory_no_manager(self, tools):
        tools.memory = None
        result = await tools.save_memory("test", "General")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_forget_memory_no_manager(self, tools):
        tools.memory = None
        result = await tools.forget_memory("m1")
        assert "error" in result


class TestSearchFiles:

    @pytest.mark.asyncio
    async def test_no_file_rag_returns_error(self, tools):
        tools.file_rag = None
        result = await tools.search_files("test query")
        assert result[0]["error"]

    @pytest.mark.asyncio
    async def test_no_files_uploaded(self, tools):
        mock_rag = MagicMock()
        mock_rag.list_files.return_value = []
        tools.file_rag = mock_rag
        result = await tools.search_files("test query")
        assert "error" in result[0]

    @pytest.mark.asyncio
    async def test_file_name_not_found(self, tools):
        mock_rag = MagicMock()
        mock_rag.list_files.return_value = [
            {"original_name": "report.pdf", "file_id": "f1"}
        ]
        tools.file_rag = mock_rag
        result = await tools.search_files("test", file_name="missing.docx")
        assert "error" in result[0]
        assert "missing.docx" in result[0]["error"]

    @pytest.mark.asyncio
    async def test_successful_search(self, tools):
        mock_rag = MagicMock()
        mock_rag.list_files.return_value = [
            {"original_name": "report.pdf", "file_id": "f1"}
        ]
        mock_rag.search = AsyncMock(return_value=[{"content": "found it", "score": 0.9}])
        tools.file_rag = mock_rag
        result = await tools.search_files("budget numbers")
        assert len(result) == 1
        assert result[0]["content"] == "found it"


class TestWebSearch:

    @pytest.mark.asyncio
    async def test_web_search_no_provider(self, tools):
        with patch("src.agent.tools.httpx.AsyncClient") as mock_client:
            mock_response = AsyncMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"results": []}
            mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_client.return_value)
            mock_client.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value.get = AsyncMock(return_value=mock_response)

            result = await tools.web_search("python asyncio")
            assert isinstance(result, list)


class TestGetHotTopicContext:

    @pytest.mark.asyncio
    async def test_empty_topics(self, tools):
        result = await tools.get_hot_topic_context([])
        assert result == {}

    @pytest.mark.asyncio
    async def test_hydrates_messages(self, tools, mock_store, mock_redis):
        mock_store.get_hot_topic_context_with_messages.return_value = {
            "Tech": {"entities": [{"name": "Python"}], "message_ids": ["msg:1", "msg:2"]}
        }
        mock_redis.hmget.return_value = [
            json.dumps({"message": "I love Python"}),
            json.dumps({"message": "Python is great"}),
        ]
        result = await tools.get_hot_topic_context(["Tech"])
        assert "Tech" in result
        assert len(result["Tech"]["messages"]) == 2
        assert "message_ids" not in result["Tech"]


class TestResolveEntityName:

    @pytest.mark.asyncio
    async def test_delegates_to_resolver(self, tools, mock_resolver):
        mock_resolver.resolve_entity_name.return_value = "Alice Smith"
        result = await tools._resolve_entity_name("alice")
        mock_resolver.resolve_entity_name.assert_called_once_with("alice")
        assert result == "Alice Smith"

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, tools, mock_resolver):
        mock_resolver.resolve_entity_name.return_value = None
        assert await tools._resolve_entity_name("nobody") is None