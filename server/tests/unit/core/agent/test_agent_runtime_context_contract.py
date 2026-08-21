from types import SimpleNamespace

import pytest

from core.agent.prompt_context import build_user_message, update_accumulators
from core.agent.run import AgentIdentity, AgentRun, AgentRunLimits
from core.agent.tool_runtime import summarize_result


def make_ctx(**overrides):
    data = {
        "user_name": "ada",
        "project_id": "project-1",
        "session_id": "session-1",
        "agent": AgentIdentity(
            config=SimpleNamespace(id="agent-1"),
            name="STELLA",
            persona="",
        ),
        "limits": AgentRunLimits(max_history_turns=2, max_accumulated_messages=2),
        "user_query": "What changed in profile behavior?",
        "run_id": "run-1",
    }
    data.update(overrides)
    return AgentRun.open(**data)


@pytest.mark.no_network
def test_build_user_message_trims_history_and_includes_runtime_context():
    ctx = make_ctx(
        history=[
            {"role": "user", "content": "oldest"},
            {
                "role": "assistant",
                "content": "middle",
                "timestamp": "2026-01-01T10:01:00+00:00",
            },
            {"role": "user", "content": "newest"},
        ],
        is_community=True,
        current_participants=["agent-1", "agent-2"],
        hot_topic_context={
            "Identity": {
                "entities": [
                    {
                        "name": "Ada",
                        "episodes": ["prefers scoped profile updates"],
                    }
                ]
            }
        },
    )
    ctx.call_count = 1
    ctx.last_error = "Duplicate call skipped"
    ctx.profiles.append({"id": 7, "canonical_name": "Ada"})
    ctx.profiles.append({"id": 8, "canonical_name": "Grace"})

    message = build_user_message(
        ctx,
        last_result=[
            {
                "tool": "search_entity",
                "result": {"data": [{"id": 7, "canonical_name": "Ada"}]},
            },
            {
                "tool": "edit_brain",
                "result": {
                    "data": {
                        "success": True,
                        "section": "Project Context",
                        "revision": 2,
                    }
                },
            },
            {"tool": "episode_check", "result": {"data": []}},
            {"tool": "search_messages", "error": "boom"},
        ],
    )

    assert "oldest" not in message
    assert "[10:01] AGENT: middle" in message
    assert "USER: newest" in message
    assert "**Participants:** agent-1, agent-2" in message
    assert "**Query:** What changed in profile behavior?" in message
    assert "**Calls remaining:** 11" in message
    assert "**Last action rejected:** Duplicate call skipped" in message
    assert "`search_entity`: Found 1 items" in message
    assert '`edit_brain`: {\n  "success": true,' in message
    assert '"section": "Project Context"' in message
    assert "`episode_check`: No results found." in message
    assert "`search_messages`: Error - boom" in message
    assert "[HOT: Identity]" in message
    assert "Ada: prefers scoped profile updates" in message
    assert "Previously retrieved entities: Grace" in message
    assert "**New entity results:**" in message


@pytest.mark.no_network
def test_build_user_message_omits_participants_outside_community():
    ctx = make_ctx(
        is_community=False,
        current_participants=["agent-1"],
    )

    message = build_user_message(ctx)

    assert "Participants" not in message


@pytest.mark.no_network
def test_update_accumulators_dedupes_and_trims_messages_by_score():
    ctx = make_ctx()

    update_accumulators(
        ctx,
        "search_messages",
        {
            "data": [
                {
                    "id": "msg_1",
                    "user_name": "ada",
                    "session_id": "session-1",
                    "score": 0.1,
                },
                {
                    "id": "msg_2",
                    "user_name": "ada",
                    "session_id": "session-1",
                    "score": 0.9,
                },
                {
                    "id": "msg_1",
                    "user_name": "ada",
                    "session_id": "session-1",
                    "score": 1.0,
                },
            ]
        },
    )
    update_accumulators(
        ctx,
        "search_messages",
        {
            "data": [
                {
                    "id": "msg_3",
                    "user_name": "ada",
                    "session_id": "session-2",
                    "score": 0.7,
                }
            ]
        },
    )

    assert [msg["id"] for msg in ctx.messages] == ["msg_2", "msg_3"]


@pytest.mark.no_network
def test_update_accumulators_dedupes_profiles_graph_files_and_sources():
    ctx = make_ctx()

    update_accumulators(
        ctx,
        "search_entity",
        {"data": [{"id": 1, "canonical_name": "Ada"}, {"id": 1}]},
    )
    update_accumulators(
        ctx,
        "get_connections",
        {
            "data": [
                {"source": "Ada", "target": "Knoggin", "score": 0.8},
                {"source": "Ada", "target": "Knoggin", "score": 0.5},
            ]
        },
    )
    update_accumulators(
        ctx,
        "get_recent_activity",
        {"data": [{"source": "Ada", "target": "Testing"}]},
    )
    update_accumulators(
        ctx,
        "find_path",
        {"data": [{"entity_a": "Ada", "entity_b": "Knoggin"}]},
    )
    update_accumulators(
        ctx,
        "find_path",
        {"data": [{"entity_a": "Ada", "entity_b": "Knoggin"}]},
    )
    update_accumulators(ctx, "episode_check", {"data": {"resolution": "exact"}})
    update_accumulators(ctx, "episode_check", {"data": {"resolution": "exact"}})
    update_accumulators(
        ctx,
        "search_documents",
        {
            "data": [
                {
                    "document_id": "file-1",
                    "chunk_index": 2,
                    "content": "profile plan",
                    "document_name": "plan.md",
                },
                {
                    "document_id": "file-1",
                    "chunk_index": 2,
                    "content": "duplicate",
                    "document_name": "plan.md",
                },
                {"error": "skip"},
            ]
        },
    )
    update_accumulators(
        ctx,
        "read_document",
        {
            "data": [
                {
                    "document_id": "file-1",
                    "chunk_index": "lines:10-12",
                    "content": "10: exact content",
                    "document_name": "plan.md",
                }
            ]
        },
    )
    update_accumulators(
        ctx,
        "web_search",
        {"data": [{"url": "https://example.test/a"}, {"url": "https://example.test/a"}]},
    )
    update_accumulators(
        ctx,
        "news_search",
        {"data": [{"url": "https://example.test/news"}]},
    )

    assert ctx.profiles == [{"id": 1, "canonical_name": "Ada"}]
    assert ctx.graph == [
        {"source": "Ada", "target": "Knoggin", "score": 0.8},
        {"source": "Ada", "target": "Testing"},
    ]
    assert ctx.paths == [{"entity_a": "Ada", "entity_b": "Knoggin"}]
    assert ctx.episodes == [{"resolution": "exact"}]
    assert [
        (msg["id"], msg["source_type"], msg["message"])
        for msg in ctx.messages
    ] == [
        ("document:file-1:2", "document", "profile plan"),
        (
            "document:file-1:lines:10-12",
            "document",
            "10: exact content",
        ),
    ]
    assert ctx.sources == [
        {"url": "https://example.test/a"},
        {"url": "https://example.test/news"},
    ]


@pytest.mark.no_network
def test_update_accumulators_caps_non_message_evidence_buckets():
    ctx = make_ctx(
        limits=AgentRunLimits(
            max_history_turns=2,
            max_accumulated_messages=2,
            max_accumulated_profiles=2,
            max_accumulated_graph=2,
            max_accumulated_paths=1,
            max_accumulated_episodes=1,
            max_accumulated_sources=1,
        )
    )

    update_accumulators(
        ctx,
        "search_entity",
        {
            "data": [
                {"id": 1, "canonical_name": "Ada"},
                {"id": 2, "canonical_name": "Grace"},
                {"id": 3, "canonical_name": "Katherine"},
            ]
        },
    )
    update_accumulators(
        ctx,
        "get_connections",
        {
            "data": [
                {"source": "Ada", "target": "Alpha"},
                {"source": "Ada", "target": "Beta"},
                {"source": "Ada", "target": "Gamma"},
            ]
        },
    )
    update_accumulators(
        ctx,
        "find_path",
        {
            "data": [
                {"entity_a": "Ada", "entity_b": "Alpha"},
                {"entity_a": "Ada", "entity_b": "Beta"},
            ]
        },
    )
    update_accumulators(
        ctx,
        "episode_check",
        {"data": [{"id": "episode-1"}, {"id": "episode-2"}]},
    )
    update_accumulators(
        ctx,
        "web_search",
        {
            "data": [
                {"url": "https://example.test/old"},
                {"url": "https://example.test/new"},
            ]
        },
    )
    update_accumulators(
        ctx,
        "search_documents",
        {
            "data": [
                {
                    "document_id": "file-1",
                    "chunk_index": 1,
                    "content": "one",
                },
                {
                    "document_id": "file-1",
                    "chunk_index": 2,
                    "content": "two",
                },
                {
                    "document_id": "file-1",
                    "chunk_index": 3,
                    "content": "three",
                },
            ]
        },
    )

    assert [profile["id"] for profile in ctx.profiles] == [2, 3]
    assert [(item["source"], item["target"]) for item in ctx.graph] == [
        ("Ada", "Beta"),
        ("Ada", "Gamma"),
    ]
    assert ctx.paths == [{"entity_a": "Ada", "entity_b": "Beta"}]
    assert ctx.episodes == [{"id": "episode-2"}]
    assert ctx.sources == [{"url": "https://example.test/new"}]
    assert [message["chunk_index"] for message in ctx.messages] == [2, 3]


@pytest.mark.no_network
def test_update_accumulators_ignores_errors_and_empty_results():
    ctx = make_ctx()

    update_accumulators(ctx, "search_messages", {"error": "failed"})
    update_accumulators(ctx, "search_messages", {"data": []})
    update_accumulators(ctx, "unknown", {"data": [{"id": "x"}]})

    assert ctx.has_any() is False


@pytest.mark.no_network
@pytest.mark.parametrize(
    ("tool_name", "result", "expected"),
    [
        ("search_messages", {"data": [{"id": 1}, {"id": 2}]}, ("Found 2 results", 2)),
        ("search_entity", {"data": []}, ("Found 0 results", 0)),
        ("find_path", {"data": [{"hop": 1}]}, ("Path found: 1 hops", 1)),
        ("find_path", {"data": []}, ("No path", 0)),
        (
            "episode_check",
            {"data": {"resolution": "exact", "results": [{}, {}]}},
            ("Resolved via exact (2 matches)", 2),
        ),
        ("episode_check", {"data": []}, ("No results", 0)),
        ("edit_brain", {"data": {"success": True}}, ("Brain updated", 1)),
        ("read_brain", {"data": {"content": "brain"}}, ("Brain loaded", 1)),
        (
            "search_documents",
            {"data": [{"id": "chunk"}]},
            ("Found 1 relevant chunks", 1),
        ),
        ("search_documents", {"data": [{"error": "nope"}]}, ("No results", 0)),
        ("list_documents", {"data": [{"document_id": "doc-1"}]}, ("Found 1 items", 1)),
        (
            "list_folder_uploads",
            {"data": [{"folder_root_id": "folder-1"}]},
            ("Found 1 items", 1),
        ),
        ("list_folder_tree", {"data": []}, ("Found 0 items", 0)),
        (
            "get_folder_upload_summary",
            {"data": {"folder_root_id": "folder-1"}},
            ("Loaded folder upload summary", 1),
        ),
        (
            "read_document",
            {"data": [{"content": "lines"}]},
            ("Read document content", 1),
        ),
        (
            "request_replanning",
            {"data": {"replanning": "stuck"}},
            ("Requested a new plan", 1),
        ),
        ("anything_else", {"data": {"ok": True}}, ("Completed", 1)),
        ("anything_else", {"data": None}, ("No results", 0)),
        ("anything_else", {"error": "boom"}, ("Error: boom", 0)),
    ],
)
def test_summarize_result_returns_stable_summaries(tool_name, result, expected):
    assert summarize_result(tool_name, result) == expected
