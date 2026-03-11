"""
Tests for agent/internals.py — the backbone of the agent reasoning loop.

Covers: AgentRunConfig, AgentState, RetrievedEvidence, build_user_message,
        update_accumulators, summarize_result, execute_tool.

All mocked — zero cost, zero external dependencies.
"""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from agent.internals import (
    AgentRunConfig,
    AgentState,
    RetrievedEvidence,
    AgentContext,
    build_user_message,
    update_accumulators,
    summarize_result,
    execute_tool,
)
from shared.models.memory import PromptContext


# ════════════════════════════════════════════════════════
#  Helpers
# ════════════════════════════════════════════════════════

def _make_ctx(
    query="test query",
    max_calls=12,
    max_attempts=15,
    max_accumulated_messages=30,
    history=None,
):
    config = AgentRunConfig(
        max_calls=max_calls,
        max_attempts=max_attempts,
        max_accumulated_messages=max_accumulated_messages,
    )
    state = AgentState()
    evidence = RetrievedEvidence()
    return AgentContext(
        config=config,
        state=state,
        evidence=evidence,
        user_query=query,
        session_id="test-session",
        run_id="test-run",
        history=history or [],
    )


# ════════════════════════════════════════════════════════
#  AgentRunConfig
# ════════════════════════════════════════════════════════

class TestAgentRunConfig:

    def test_get_tool_limit_exact_match(self):
        cfg = AgentRunConfig()
        assert cfg.get_tool_limit("search_messages") == 6
        assert cfg.get_tool_limit("save_memory") == 4
        assert cfg.get_tool_limit("spawn_specialist") == 2

    def test_get_tool_limit_wildcard_mcp(self):
        cfg = AgentRunConfig()
        assert cfg.get_tool_limit("mcp__gmail__search") == 3
        assert cfg.get_tool_limit("mcp__drive__list_files") == 3

    def test_get_tool_limit_fallback_default(self):
        cfg = AgentRunConfig()
        # Unknown tool should fall back to the provided default
        assert cfg.get_tool_limit("nonexistent_tool", default=99) == 99

    def test_get_tool_limit_fallback_uses_6_when_no_default(self):
        cfg = AgentRunConfig()
        assert cfg.get_tool_limit("nonexistent_tool") == 6

    def test_frozen_immutability(self):
        cfg = AgentRunConfig()
        with pytest.raises(AttributeError):
            cfg.max_calls = 999


# ════════════════════════════════════════════════════════
#  AgentState
# ════════════════════════════════════════════════════════

class TestAgentState:

    def test_is_duplicate_false_on_fresh_state(self):
        state = AgentState()
        assert state.is_duplicate("search_entity", {"query": "Alice"}) is False

    def test_is_duplicate_true_after_record(self):
        state = AgentState()
        args = {"query": "Alice"}
        state.record_call("search_entity", args)
        assert state.is_duplicate("search_entity", args) is True

    def test_is_duplicate_different_args(self):
        state = AgentState()
        state.record_call("search_entity", {"query": "Alice"})
        assert state.is_duplicate("search_entity", {"query": "Bob"}) is False

    def test_is_duplicate_same_tool_different_tool_name(self):
        state = AgentState()
        state.record_call("search_entity", {"query": "Alice"})
        assert state.is_duplicate("search_messages", {"query": "Alice"}) is False

    def test_record_call_increments_counters(self):
        state = AgentState()
        state.record_call("search_entity", {"query": "Alice"})
        state.record_call("search_entity", {"query": "Bob"})
        state.record_call("find_path", {"entity_a": "A", "entity_b": "B"})

        assert state.call_count == 3
        assert state.tool_call_counts["search_entity"] == 2
        assert state.tool_call_counts["find_path"] == 1
        assert state.tools_used == ["search_entity", "search_entity", "find_path"]

    def test_tool_limit_reached(self):
        cfg = AgentRunConfig(tool_limits=(("search_entity", 2),))
        state = AgentState()
        state.record_call("search_entity", {"query": "A"})
        assert state.tool_limit_reached("search_entity", cfg) is False
        state.record_call("search_entity", {"query": "B"})
        assert state.tool_limit_reached("search_entity", cfg) is True

    def test_tool_limit_reached_unknown_tool_uses_max_calls(self):
        cfg = AgentRunConfig(max_calls=3, tool_limits=())
        state = AgentState()
        for i in range(3):
            state.record_call("mystery_tool", {"i": i})
        # tool_limit_reached passes config.max_calls as the default to get_tool_limit
        # With empty tool_limits, get_tool_limit("mystery_tool", default=3) returns 3
        # 3 calls >= 3 = True
        assert state.tool_limit_reached("mystery_tool", cfg) is True


# ════════════════════════════════════════════════════════
#  RetrievedEvidence
# ════════════════════════════════════════════════════════

class TestRetrievedEvidence:

    def test_has_any_empty(self):
        ev = RetrievedEvidence()
        assert ev.has_any() is False

    def test_has_any_with_profiles(self):
        ev = RetrievedEvidence()
        ev.profiles.append({"id": 1})
        assert ev.has_any() is True

    def test_has_any_with_messages(self):
        ev = RetrievedEvidence()
        ev.messages.append({"id": "m1"})
        assert ev.has_any() is True

    def test_has_any_with_graph(self):
        ev = RetrievedEvidence()
        ev.graph.append({"source": "A", "target": "B"})
        assert ev.has_any() is True

    def test_has_any_with_sources(self):
        ev = RetrievedEvidence()
        ev.sources.append({"url": "https://example.com"})
        assert ev.has_any() is True


# ════════════════════════════════════════════════════════
#  build_user_message
# ════════════════════════════════════════════════════════

class TestBuildUserMessage:

    def test_basic_query_present(self):
        ctx = _make_ctx(query="Who is Alice?")
        msg = build_user_message(ctx)
        assert "Who is Alice?" in msg
        assert "Calls remaining:" in msg

    def test_calls_remaining_decrements(self):
        ctx = _make_ctx(max_calls=10)
        ctx.state.call_count = 3
        msg = build_user_message(ctx)
        assert "7" in msg  # 10 - 3

    def test_history_included(self):
        history = [
            {"role": "user", "content": "Hello there"},
            {"role": "assistant", "content": "Hi!"},
        ]
        ctx = _make_ctx(history=history)
        msg = build_user_message(ctx)
        assert "Hello there" in msg
        assert "USER" in msg
        assert "AGENT" in msg

    def test_history_truncated_to_max_turns(self):
        history = [{"role": "user", "content": f"msg {i}"} for i in range(20)]
        ctx = _make_ctx(history=history)
        # Default max_history_turns = 7
        msg = build_user_message(ctx)
        # Should contain the last 7 messages, not the first ones
        assert "msg 19" in msg
        assert "msg 13" in msg
        assert "msg 0" not in msg

    def test_history_with_timestamp(self):
        history = [
            {"role": "user", "content": "timed msg", "timestamp": "2025-01-15T14:30:00Z"},
        ]
        ctx = _make_ctx(history=history)
        msg = build_user_message(ctx)
        assert "14:30" in msg

    def test_history_with_bad_timestamp_falls_back(self):
        history = [
            {"role": "user", "content": "bad ts msg", "timestamp": "not-a-date"},
        ]
        ctx = _make_ctx(history=history)
        msg = build_user_message(ctx)
        # Should still include the message content, just without formatted time
        assert "bad ts msg" in msg

    def test_last_error_included_and_cleared(self):
        ctx = _make_ctx()
        ctx.state.last_error = "Duplicate call rejected"
        msg = build_user_message(ctx)
        assert "Duplicate call rejected" in msg
        # Error should be cleared after building the message
        assert ctx.state.last_error is None

    def test_last_result_tool_with_data(self):
        ctx = _make_ctx()
        last_result = {
            "tool": "search_entity",
            "result": {"data": [{"id": 1}, {"id": 2}]}
        }
        msg = build_user_message(ctx, last_result=last_result)
        assert "search_entity" in msg
        assert "Found 2 items" in msg

    def test_last_result_tool_with_error(self):
        ctx = _make_ctx()
        # Known tools check r.get("result", {}).get("data"), not top-level "error"
        # An error from a known tool comes through as empty data -> "No results found"
        # Top-level "error" only renders for tools NOT in the known tool list
        last_result = {"tool": "custom_tool", "error": "DB timeout"}
        msg = build_user_message(ctx, last_result=last_result)
        assert "DB timeout" in msg

    def test_last_result_list_of_results(self):
        ctx = _make_ctx()
        last_result = [
            {"tool": "search_entity", "result": {"data": [{"id": 1}]}},
            {"tool": "get_connections", "result": {"data": [{"source": "A", "target": "B"}]}},
        ]
        msg = build_user_message(ctx, last_result=last_result)
        assert "search_entity" in msg
        assert "get_connections" in msg

    def test_hot_topic_context_included(self):
        ctx = _make_ctx()
        ctx.hot_topic_context = {"Tech": {"entities": [{"name": "Python"}]}}
        msg = build_user_message(ctx)
        assert "Hot topic" in msg or "Python" in msg

    def test_accumulated_evidence_included(self):
        ctx = _make_ctx()
        ctx.evidence.profiles.append({
            "id": 1, "canonical_name": "Alice", "type": "person",
            "topic": "Identity", "facts": []
        })
        msg = build_user_message(ctx)
        assert "Alice" in msg


# ════════════════════════════════════════════════════════
#  update_accumulators
# ════════════════════════════════════════════════════════

class TestUpdateAccumulators:

    def test_error_result_is_noop(self):
        ctx = _make_ctx()
        update_accumulators(ctx, "search_entity", {"error": "fail"})
        assert ctx.evidence.has_any() is False

    def test_none_result_is_noop(self):
        ctx = _make_ctx()
        update_accumulators(ctx, "search_entity", None)
        assert ctx.evidence.has_any() is False

    def test_empty_data_is_noop(self):
        ctx = _make_ctx()
        update_accumulators(ctx, "search_entity", {"data": None})
        assert ctx.evidence.has_any() is False

    # -- search_messages --

    def test_search_messages_adds_to_messages(self):
        ctx = _make_ctx()
        result = {"data": [{"id": "m1", "score": 0.9}, {"id": "m2", "score": 0.8}]}
        update_accumulators(ctx, "search_messages", result)
        assert len(ctx.evidence.messages) == 2

    def test_search_messages_deduplicates(self):
        ctx = _make_ctx()
        result = {"data": [{"id": "m1", "score": 0.9}]}
        update_accumulators(ctx, "search_messages", result)
        update_accumulators(ctx, "search_messages", result)
        assert len(ctx.evidence.messages) == 1

    def test_search_messages_overflow_trims_by_score(self):
        ctx = _make_ctx(max_accumulated_messages=3)
        batch1 = {"data": [
            {"id": "m1", "score": 0.1},
            {"id": "m2", "score": 0.5},
        ]}
        batch2 = {"data": [
            {"id": "m3", "score": 0.9},
            {"id": "m4", "score": 0.3},
        ]}
        update_accumulators(ctx, "search_messages", batch1)
        update_accumulators(ctx, "search_messages", batch2)
        # Should keep top 3 by score: m3(0.9), m2(0.5), m4(0.3)
        assert len(ctx.evidence.messages) == 3
        ids = {m["id"] for m in ctx.evidence.messages}
        assert "m1" not in ids  # lowest score dropped
        assert "m3" in ids      # highest score kept

    def test_search_messages_none_score_defaults_to_05(self):
        ctx = _make_ctx(max_accumulated_messages=2)
        result = {"data": [
            {"id": "m1", "score": None},
            {"id": "m2", "score": 0.9},
            {"id": "m3", "score": 0.1},
        ]}
        update_accumulators(ctx, "search_messages", result)
        # m1 gets 0.5 default, so top 2: m2(0.9), m1(0.5)
        assert len(ctx.evidence.messages) == 2
        ids = {m["id"] for m in ctx.evidence.messages}
        assert "m3" not in ids

    # -- search_entity --

    def test_search_entity_adds_profiles(self):
        ctx = _make_ctx()
        result = {"data": [{"id": 1, "canonical_name": "Alice"}]}
        update_accumulators(ctx, "search_entity", result)
        assert len(ctx.evidence.profiles) == 1

    def test_search_entity_deduplicates(self):
        ctx = _make_ctx()
        result = {"data": [{"id": 1, "canonical_name": "Alice"}]}
        update_accumulators(ctx, "search_entity", result)
        update_accumulators(ctx, "search_entity", result)
        assert len(ctx.evidence.profiles) == 1

    # -- get_connections / get_recent_activity --

    def test_get_connections_adds_graph(self):
        ctx = _make_ctx()
        result = {"data": [{"source": "Alice", "target": "Bob", "weight": 5}]}
        update_accumulators(ctx, "get_connections", result)
        assert len(ctx.evidence.graph) == 1

    def test_get_connections_deduplicates_on_source_target(self):
        ctx = _make_ctx()
        result = {"data": [{"source": "Alice", "target": "Bob", "weight": 5}]}
        update_accumulators(ctx, "get_connections", result)
        update_accumulators(ctx, "get_connections", result)
        assert len(ctx.evidence.graph) == 1

    def test_get_recent_activity_same_branch(self):
        ctx = _make_ctx()
        result = {"data": [{"source": "X", "target": "Y"}]}
        update_accumulators(ctx, "get_recent_activity", result)
        assert len(ctx.evidence.graph) == 1

    # -- find_path --

    def test_find_path_extends_paths(self):
        ctx = _make_ctx()
        result = {"data": [{"step": 0, "entity_a": "A", "entity_b": "B"}]}
        update_accumulators(ctx, "find_path", result)
        assert len(ctx.evidence.paths) == 1

    def test_find_path_does_not_deduplicate(self):
        """Paths are appended, not deduped — by design."""
        ctx = _make_ctx()
        result = {"data": [{"step": 0, "entity_a": "A", "entity_b": "B"}]}
        update_accumulators(ctx, "find_path", result)
        update_accumulators(ctx, "find_path", result)
        assert len(ctx.evidence.paths) == 2

    # -- get_hierarchy --

    def test_get_hierarchy_dict(self):
        ctx = _make_ctx()
        result = {"data": {"entity": "Project X", "children": []}}
        update_accumulators(ctx, "get_hierarchy", result)
        assert len(ctx.evidence.hierarchy) == 1

    def test_get_hierarchy_list(self):
        ctx = _make_ctx()
        result = {"data": [{"entity": "A"}, {"entity": "B"}]}
        update_accumulators(ctx, "get_hierarchy", result)
        assert len(ctx.evidence.hierarchy) == 2

    # -- fact_check --

    def test_fact_check_dict(self):
        ctx = _make_ctx()
        result = {"data": {"resolution": "exact", "results": [{"entity_name": "Alice"}]}}
        update_accumulators(ctx, "fact_check", result)
        assert len(ctx.evidence.facts) == 1

    def test_fact_check_list(self):
        ctx = _make_ctx()
        result = {"data": [{"resolution": "exact"}, {"resolution": "fuzzy"}]}
        update_accumulators(ctx, "fact_check", result)
        assert len(ctx.evidence.facts) == 2

    # -- search_files --

    def test_search_files_normalizes_into_messages(self):
        ctx = _make_ctx()
        result = {"data": [
            {"file_id": "f1", "chunk_index": 0, "content": "hello", "score": 0.8, "file_name": "doc.pdf"},
            {"file_id": "f1", "chunk_index": 1, "content": "world", "score": 0.7, "file_name": "doc.pdf"},
        ]}
        update_accumulators(ctx, "search_files", result)
        assert len(ctx.evidence.messages) == 2
        assert ctx.evidence.messages[0]["role"] == "file"
        assert ctx.evidence.messages[0]["source"] == "doc.pdf"

    def test_search_files_error_in_first_item_is_noop(self):
        ctx = _make_ctx()
        result = {"data": [{"error": "No files uploaded"}]}
        update_accumulators(ctx, "search_files", result)
        assert len(ctx.evidence.messages) == 0

    # -- web_search / news_search --

    def test_web_search_adds_to_sources(self):
        ctx = _make_ctx()
        result = {"data": [{"url": "https://a.com", "title": "A"}]}
        update_accumulators(ctx, "web_search", result)
        assert len(ctx.evidence.sources) == 1

    def test_web_search_deduplicates_on_url(self):
        ctx = _make_ctx()
        result = {"data": [{"url": "https://a.com", "title": "A"}]}
        update_accumulators(ctx, "web_search", result)
        update_accumulators(ctx, "web_search", result)
        assert len(ctx.evidence.sources) == 1

    def test_news_search_shares_sources_with_web(self):
        ctx = _make_ctx()
        update_accumulators(ctx, "web_search", {"data": [{"url": "https://a.com"}]})
        update_accumulators(ctx, "news_search", {"data": [{"url": "https://a.com"}]})
        assert len(ctx.evidence.sources) == 1

    def test_web_search_no_url_key_collapses(self):
        """Edge case: results missing 'url' all map to None key — only first survives."""
        ctx = _make_ctx()
        result = {"data": [{"title": "A"}, {"title": "B"}]}
        update_accumulators(ctx, "web_search", result)
        # Both have url=None, so dedup collapses to 1
        assert len(ctx.evidence.sources) == 1

    # -- mcp__ --

    def test_mcp_tool_appends_to_messages(self):
        ctx = _make_ctx()
        ctx.state.call_count = 5
        result = {"data": "some mcp output"}
        update_accumulators(ctx, "mcp__gmail__search", result)
        assert len(ctx.evidence.messages) == 1
        assert ctx.evidence.messages[0]["id"] == "mcp_5"

    def test_mcp_tool_truncates_long_content(self):
        ctx = _make_ctx()
        result = {"data": "x" * 3000}
        update_accumulators(ctx, "mcp__drive__read", result)
        content = ctx.evidence.messages[0]["context"][0]["content"]
        assert len(content) == 2000

    # -- save_memory / forget_memory --

    def test_save_memory_is_noop(self):
        ctx = _make_ctx()
        update_accumulators(ctx, "save_memory", {"data": {"saved": True}})
        assert ctx.evidence.has_any() is False

    def test_forget_memory_is_noop(self):
        ctx = _make_ctx()
        update_accumulators(ctx, "forget_memory", {"data": {"removed": True}})
        assert ctx.evidence.has_any() is False


# ════════════════════════════════════════════════════════
#  summarize_result
# ════════════════════════════════════════════════════════

class TestSummarizeResult:

    def test_error_result(self):
        summary, count = summarize_result("search_entity", {"error": "timeout"})
        assert "Error" in summary
        assert count == 0

    def test_no_data(self):
        summary, count = summarize_result("search_entity", {"data": None})
        assert summary == "No results"
        assert count == 0

    def test_search_entity_with_results(self):
        summary, count = summarize_result("search_entity", {"data": [{"id": 1}, {"id": 2}]})
        assert count == 2
        assert "Found 2 results" in summary

    def test_search_messages_empty(self):
        summary, count = summarize_result("search_messages", {"data": []})
        assert count == 0

    def test_find_path_with_hops(self):
        summary, count = summarize_result("find_path", {"data": [{"step": 0}, {"step": 1}]})
        assert count == 2
        assert "2 hops" in summary

    def test_find_path_empty(self):
        summary, count = summarize_result("find_path", {"data": []})
        assert summary == "No path"
        assert count == 0

    def test_fact_check_dict(self):
        data = {"resolution": "exact", "results": [{"entity_name": "A"}, {"entity_name": "B"}]}
        summary, count = summarize_result("fact_check", {"data": data})
        assert count == 2
        assert "exact" in summary

    def test_save_memory(self):
        summary, count = summarize_result("save_memory", {"data": {"saved": True}})
        assert summary == "Memory updated"
        assert count == 1

    def test_search_files_with_chunks(self):
        summary, count = summarize_result("search_files", {"data": [{"content": "x"}, {"content": "y"}]})
        assert count == 2
        assert "chunks" in summary

    def test_search_files_error_item(self):
        summary, count = summarize_result("search_files", {"data": [{"error": "no files"}]})
        assert summary == "No results"

    def test_mcp_string_result(self):
        summary, count = summarize_result("mcp__gmail__search", {"data": "email content here"})
        assert count == 1
        assert "MCP result" in summary

    def test_mcp_list_result(self):
        summary, count = summarize_result("mcp__drive__list", {"data": [1, 2, 3]})
        assert count == 1
        assert "3 items" in summary

    def test_unknown_tool_completed(self):
        summary, count = summarize_result("some_new_tool", {"data": {"ok": True}})
        assert summary == "Completed"
        assert count == 1


# ════════════════════════════════════════════════════════
#  execute_tool
# ════════════════════════════════════════════════════════

class TestExecuteTool:

    @pytest.mark.asyncio
    async def test_dispatch_search_entity(self):
        tools = MagicMock()
        tools.mcp_manager = None
        tools.search_entity = AsyncMock(return_value=[{"id": 1}])

        result = await execute_tool(tools, "search_entity", {"query": "Alice", "limit": 3})
        assert result == {"data": [{"id": 1}]}
        tools.search_entity.assert_awaited_once_with("Alice", 3)

    @pytest.mark.asyncio
    async def test_dispatch_search_messages_clamps_limit(self):
        tools = MagicMock()
        tools.mcp_manager = None
        tools.search_messages = AsyncMock(return_value=[])

        await execute_tool(tools, "search_messages", {"query": "test", "limit": 50})
        # Should clamp to min(50, 8) = 8
        tools.search_messages.assert_awaited_once_with("test", 8)

    @pytest.mark.asyncio
    async def test_dispatch_unknown_tool(self):
        tools = MagicMock()
        tools.mcp_manager = None
        result = await execute_tool(tools, "nonexistent_tool", {})
        assert "error" in result
        assert "Unknown tool" in result["error"]

    @pytest.mark.asyncio
    async def test_dispatch_tool_exception_returns_error(self):
        tools = MagicMock()
        tools.mcp_manager = None
        tools.search_entity = AsyncMock(side_effect=RuntimeError("DB down"))

        result = await execute_tool(tools, "search_entity", {"query": "x"})
        assert "error" in result
        assert "DB down" in result["error"]

    @pytest.mark.asyncio
    async def test_mcp_tool_routing(self):
        tools = MagicMock()
        tools.mcp_manager = AsyncMock()
        tools.mcp_manager.call_tool = AsyncMock(return_value={"data": "mcp result"})

        result = await execute_tool(tools, "mcp__gmail__search_emails", {"q": "test"})
        tools.mcp_manager.call_tool.assert_awaited_once_with("gmail", "search_emails", {"q": "test"})

    @pytest.mark.asyncio
    async def test_mcp_tool_no_manager(self):
        tools = MagicMock()
        tools.mcp_manager = None

        result = await execute_tool(tools, "mcp__gmail__search_emails", {"q": "test"})
        assert result == {"error": "MCP not configured"}

    @pytest.mark.asyncio
    async def test_dispatch_save_memory(self):
        tools = MagicMock()
        tools.mcp_manager = None
        tools.save_memory = AsyncMock(return_value={"saved": True, "memory_id": "m1"})

        result = await execute_tool(tools, "save_memory", {"content": "remember this", "topic": "General"})
        assert result == {"data": {"saved": True, "memory_id": "m1"}}
        tools.save_memory.assert_awaited_once_with("remember this", "General")

    @pytest.mark.asyncio
    async def test_dispatch_forget_memory(self):
        tools = MagicMock()
        tools.mcp_manager = None
        tools.forget_memory = AsyncMock(return_value={"removed": True})

        result = await execute_tool(tools, "forget_memory", {"memory_id": "m1"})
        assert result == {"data": {"removed": True}}
        tools.forget_memory.assert_awaited_once_with("m1")

    @pytest.mark.asyncio
    async def test_dispatch_defaults_when_args_missing(self):
        """Verify that missing optional args get sensible defaults."""
        tools = MagicMock()
        tools.mcp_manager = None
        tools.get_hierarchy = AsyncMock(return_value=[])

        await execute_tool(tools, "get_hierarchy", {"entity_name": "Project X"})
        # direction should default to "both"
        tools.get_hierarchy.assert_awaited_once_with("Project X", "both")

    @pytest.mark.asyncio
    async def test_dispatch_web_search(self):
        tools = MagicMock()
        tools.mcp_manager = None
        tools.web_search = AsyncMock(return_value=[{"url": "https://example.com"}])

        result = await execute_tool(tools, "web_search", {"query": "python", "limit": 3, "freshness": "pw"})
        assert result == {"data": [{"url": "https://example.com"}]}
        tools.web_search.assert_awaited_once_with("python", 3, "pw")