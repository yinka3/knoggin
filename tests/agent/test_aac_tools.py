"""
Tests for agent/community_tools.py — CommunityTools for AAC agents.

All async with mocked redis/base_tools/community_store.
Zero cost, zero external dependencies.
"""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from agent.community_tools import CommunityTools


# ════════════════════════════════════════════════════════
#  Fixtures
# ════════════════════════════════════════════════════════

@pytest.fixture
def mock_base_tools():
    base = MagicMock()
    base.redis = AsyncMock()
    base.redis.hgetall = AsyncMock(return_value={})
    base.redis.hset = AsyncMock()
    return base


@pytest.fixture
def mock_community_store():
    store = MagicMock()
    store.add_message = MagicMock()
    store.register_agent_spawn = MagicMock()
    return store


@pytest.fixture
def ct(mock_base_tools, mock_community_store):
    return CommunityTools(
        user_name="TestUser",
        base_tools=mock_base_tools,
        community_store=mock_community_store,
        discussion_id="disc-001",
        agent_id="agent-alpha",
    )


# ════════════════════════════════════════════════════════
#  save_insight
# ════════════════════════════════════════════════════════

class TestSaveInsight:

    @pytest.mark.asyncio
    async def test_saves_with_insight_prefix(self, ct, mock_community_store):
        result = await ct.save_insight("Pattern detected between X and Y")
        assert result == {"saved": True, "type": "insight"}
        mock_community_store.add_message.assert_called_once()
        call_kwargs = mock_community_store.add_message.call_args
        # Verify INSIGHT: prefix in content
        assert "INSIGHT:" in call_kwargs.kwargs.get("content", "") or "INSIGHT:" in str(call_kwargs)

    @pytest.mark.asyncio
    async def test_uses_system_agent_id(self, ct, mock_community_store):
        await ct.save_insight("test")
        call_kwargs = mock_community_store.add_message.call_args
        assert call_kwargs.kwargs.get("agent_id") == "system" or "system" in str(call_kwargs)

    @pytest.mark.asyncio
    async def test_uses_insight_role(self, ct, mock_community_store):
        await ct.save_insight("test")
        call_kwargs = mock_community_store.add_message.call_args
        assert call_kwargs.kwargs.get("role") == "insight" or "insight" in str(call_kwargs)


# ════════════════════════════════════════════════════════
#  save_memory
# ════════════════════════════════════════════════════════

class TestSaveMemory:

    @pytest.mark.asyncio
    async def test_saves_when_under_limit(self, ct, mock_base_tools):
        mock_base_tools.redis.hgetall.return_value = {f"m{i}": "{}" for i in range(5)}
        result = await ct.save_memory("Important observation")
        assert result["saved"] is True
        assert "memory_id" in result
        assert result["memory_id"].startswith("comm_mem_")
        mock_base_tools.redis.hset.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_blocked_at_10_memories(self, ct, mock_base_tools):
        mock_base_tools.redis.hgetall.return_value = {f"m{i}": "{}" for i in range(10)}
        result = await ct.save_memory("One too many")
        assert "error" in result
        assert "10/10" in result["error"]
        mock_base_tools.redis.hset.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stores_correct_payload(self, ct, mock_base_tools):
        mock_base_tools.redis.hgetall.return_value = {}
        await ct.save_memory("Test content")
        call_args = mock_base_tools.redis.hset.call_args
        # Third positional arg is the payload JSON
        payload = json.loads(call_args[0][2])
        assert payload["content"] == "Test content"
        assert "created_at" in payload
        assert payload["discussion_id"] == "disc-001"

    @pytest.mark.asyncio
    async def test_exactly_9_memories_still_allows(self, ct, mock_base_tools):
        mock_base_tools.redis.hgetall.return_value = {f"m{i}": "{}" for i in range(9)}
        result = await ct.save_memory("Ninth is fine")
        assert result["saved"] is True


# ════════════════════════════════════════════════════════
#  spawn_specialist
# ════════════════════════════════════════════════════════

class TestSpawnSpecialist:

    @pytest.mark.asyncio
    @patch("agent.community_tools.get_config_value", return_value={"agent_model": "gemini-flash"})
    @patch("agent.community_tools.emit_community", new_callable=AsyncMock)
    async def test_spawn_success(self, mock_emit, mock_config, ct, mock_base_tools, mock_community_store):
        participants = ["agent-alpha", "agent-beta"]
        result = await ct.spawn_specialist(
            name="Risk Analyst",
            persona="Expert in financial risk",
            discussion_participants=participants,
        )
        assert "id" in result
        assert result["id"].startswith("spawned_")
        assert "Risk Analyst" in result["message"]
        # Should have been added to participants list
        assert result["id"] in participants
        assert len(participants) == 3
        # Redis agent config stored
        mock_base_tools.redis.hset.assert_awaited()
        # Community store notified
        mock_community_store.register_agent_spawn.assert_called_once()

    @pytest.mark.asyncio
    async def test_spawn_limit_at_3(self, ct):
        participants = ["agent-alpha", "spawned_aaa", "spawned_bbb", "spawned_ccc"]
        result = await ct.spawn_specialist(
            name="Extra Agent",
            persona="Too many",
            discussion_participants=participants,
        )
        assert "error" in result
        assert "Spawn limit" in result["error"]

    @pytest.mark.asyncio
    async def test_spawn_limit_counts_only_spawned_prefix(self, ct):
        """Non-spawned participants don't count toward the 3-spawn limit."""
        participants = ["agent-alpha", "agent-beta", "agent-gamma", "agent-delta", "spawned_one", "spawned_two"]
        # Only 2 spawned, so one more should be allowed
        with patch("agent.community_tools.get_config_value", return_value={}), \
             patch("agent.community_tools.emit_community", new_callable=AsyncMock):
            result = await ct.spawn_specialist(
                name="Third Spawn",
                persona="Should work",
                discussion_participants=participants,
            )
        assert "id" in result
        assert result["id"].startswith("spawned_")

    @pytest.mark.asyncio
    @patch("agent.community_tools.get_config_value", return_value={})
    @patch("agent.community_tools.emit_community", new_callable=AsyncMock)
    async def test_spawn_with_initial_working_memory(self, mock_emit, mock_config, ct, mock_base_tools):
        participants = []
        result = await ct.spawn_specialist(
            name="Analyst",
            persona="Data focused",
            discussion_participants=participants,
            initial_rules=["Always cite sources"],
            initial_preferences=["Be concise"],
            initial_icks=["No jargon"],
        )
        assert result["seeded_memory"]["rules"] == 1
        assert result["seeded_memory"]["preferences"] == 1
        assert result["seeded_memory"]["icks"] == 1
        # 1 agent config hset + 3 working memory hsets = 4 total
        assert mock_base_tools.redis.hset.await_count == 4

    @pytest.mark.asyncio
    @patch("agent.community_tools.get_config_value", return_value={})
    @patch("agent.community_tools.emit_community", new_callable=AsyncMock)
    async def test_spawn_no_initial_memory(self, mock_emit, mock_config, ct, mock_base_tools):
        participants = []
        result = await ct.spawn_specialist(
            name="Analyst",
            persona="Data focused",
            discussion_participants=participants,
        )
        assert result["seeded_memory"] == {"rules": 0, "preferences": 0, "icks": 0}
        # Only 1 hset for the agent config, none for working memory
        assert mock_base_tools.redis.hset.await_count == 1

    @pytest.mark.asyncio
    @patch("agent.community_tools.get_config_value", return_value={})
    @patch("agent.community_tools.emit_community", new_callable=AsyncMock)
    async def test_spawn_emits_community_event(self, mock_emit, mock_config, ct):
        participants = []
        result = await ct.spawn_specialist(
            name="Strategist",
            persona="Big picture",
            discussion_participants=participants,
        )
        mock_emit.assert_awaited_once()
        event_data = mock_emit.call_args[1] if mock_emit.call_args[1] else mock_emit.call_args[0][3]
        assert event_data["discussion_id"] == "disc-001"
        assert event_data["parent_agent_id"] == "agent-alpha"