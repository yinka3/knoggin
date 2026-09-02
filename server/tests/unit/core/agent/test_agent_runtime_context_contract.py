from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from common.utils.time_utils import frozen_time
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
def test_topic_context_tool_accumulates_messages_as_evidence():
    ctx = make_ctx()
    result = {
        "data": {
            "Work": {
                "entities": [{"name": "Acme"}],
                "messages": [
                    {
                        "id": "msg_7",
                        "message": "The offer includes a leadership role.",
                        "timestamp": "2026-01-01T10:00:00+00:00",
                    }
                ],
            },
            "Finance": {
                "entities": [{"name": "Savings"}],
                "messages": [],
            },
        }
    }

    assert ctx.accumulate_tool_result("load_topic_context", result) is True
    assert ctx.new_evidence_gathered is True
    assert summarize_result("load_topic_context", result) == (
        "Loaded context for 2 topic(s) with 1 supporting message(s)",
        2,
    )
    assert ctx.messages == [
        {
            "id": "msg_7",
            "score": 1.0,
            "user_name": None,
            "session_id": None,
            "context": [
                {
                    "role": "assistant",
                    "timestamp": "2026-01-01T10:00:00+00:00",
                    "content": "The offer includes a leadership role.",
                    "is_hit": True,
                }
            ],
        }
    ]

    message = build_user_message(
        ctx,
        last_result=[{"tool": "load_topic_context", "result": result}],
    )

    assert "Loaded context for 2 topic(s)" in message
    assert "[TOPIC: Work]" in message
    assert "The offer includes a leadership role." in message


@pytest.mark.no_network
def test_topic_context_without_messages_does_not_count_as_new_evidence():
    ctx = make_ctx()

    assert ctx.accumulate_tool_result(
        "load_topic_context",
        {"data": {"Work": {"entities": [{"name": "Acme"}], "messages": []}}},
    ) is False
    assert ctx.new_evidence_gathered is False


@pytest.mark.no_network
def test_build_user_message_renders_discovered_sources_for_next_step():
    ctx = make_ctx(user_query="Research the latest profile behavior changes")
    result_data = [
        {
            "title": "Profile behavior release notes",
            "url": "https://example.test/release-notes",
            "snippet": "The release changed profile behavior.",
            "provider": "brave",
            "query": "profile behavior changes",
            "rank": 1,
            "source_kind": "web_search_result",
        },
        {
            "title": "Profile behavior news",
            "url": "https://example.test/news",
            "snippet": "A news report describes the change.",
            "provider": "brave",
            "query": "profile behavior changes",
            "rank": 2,
            "source_kind": "news_search_result",
        },
    ]
    update_accumulators(ctx, "web_search", {"data": result_data[:1]})
    update_accumulators(ctx, "news_search", {"data": result_data[1:]})

    message = build_user_message(
        ctx,
        last_result=[
            {"tool": "web_search", "result": {"data": result_data[:1]}},
            {"tool": "news_search", "result": {"data": result_data[1:]}},
        ],
    )

    assert "**New web sources (discovery only):**" in message
    assert "Title: Profile behavior release notes" in message
    assert "Provider: brave" in message
    assert "Query: profile behavior changes" in message
    assert "URL: https://example.test/release-notes" in message
    assert "Snippet (discovery only): The release changed profile behavior." in message
    assert "--- News search discovery result #2 ---" in message


@pytest.mark.no_network
def test_build_user_message_keeps_sources_after_a_later_non_web_tool_call():
    ctx = make_ctx()
    source = {
        "title": "Useful source",
        "url": "https://example.test/source",
        "snippet": "The source contains useful context.",
        "provider": "duckduckgo",
        "query": "useful context",
        "rank": 1,
    }
    update_accumulators(ctx, "web_search", {"data": [source]})
    update_accumulators(
        ctx,
        "search_entity",
        {"data": [{"id": 1, "canonical_name": "Ada"}]},
    )

    message = build_user_message(
        ctx,
        last_result={
            "tool": "search_entity",
            "result": {"data": [{"id": 1, "canonical_name": "Ada"}]},
        },
    )

    assert "**Previously discovered web sources:**" in message
    assert "Useful source" in message
    assert "https://example.test/source" in message
    assert "The source contains useful context." not in message


@pytest.mark.no_network
def test_update_accumulators_skips_non_source_search_status_items():
    ctx = make_ctx()

    update_accumulators(
        ctx,
        "web_search",
        {
            "data": [
                {"title": "No Results", "url": "", "snippet": "Nothing found."},
                {
                    "title": "Useful source",
                    "url": "https://example.test/source",
                    "snippet": "Useful snippet.",
                },
            ]
        },
    )

    assert [item["url"] for item in ctx.sources] == [
        "https://example.test/source"
    ]


@pytest.mark.no_network
def test_compact_evidence_trims_sources_with_other_evidence():
    ctx = make_ctx()
    ctx.sources = [
        {
            "title": f"Source {index}",
            "url": f"https://example.test/{index}",
            "snippet": f"Snippet {index}",
        }
        for index in range(6)
    ]

    ctx.compact_evidence("Condensed source evidence")

    assert ctx.evidence_summary == "Condensed source evidence"
    assert [item["title"] for item in ctx.sources] == [
        "Source 1",
        "Source 2",
        "Source 3",
        "Source 4",
        "Source 5",
    ]


@pytest.mark.no_network
def test_read_web_page_ranges_remain_distinct_and_visible_after_later_tool_calls():
    ctx = make_ctx()
    page_hash = "a" * 64
    first_range = {
        "title": "Research report",
        "url": "https://example.test/report",
        "content": "First observed range.",
        "start_line": 1,
        "end_line": 1,
        "content_hash": page_hash,
        "source_kind": "web_page",
    }
    second_range = {
        **first_range,
        "content": "Second observed range.",
        "start_line": 2,
        "end_line": 2,
    }
    update_accumulators(ctx, "read_web_page", {"data": [first_range]})
    update_accumulators(ctx, "read_web_page", {"data": [second_range]})
    update_accumulators(
        ctx,
        "search_entity",
        {"data": [{"id": 1, "canonical_name": "Ada"}]},
    )

    later_message = build_user_message(
        ctx,
        last_result={
            "tool": "search_entity",
            "result": {"data": [{"id": 1, "canonical_name": "Ada"}]},
        },
    )

    assert "**Previously read web content:**" in later_message
    assert "Webpage read lines 1-1" in later_message
    assert "Webpage read lines 2-2" in later_message
    assert "First observed range." not in later_message
    assert "Second observed range." not in later_message

    current_message = build_user_message(
        ctx,
        last_result={"tool": "read_web_page", "result": {"data": [second_range]}},
    )

    assert "**Web content actually read:**" in current_message
    assert "Content (untrusted external evidence):" in current_message
    assert "Second observed range." in current_message
    assert "**Previously read web content:**" in current_message
    assert "First observed range." not in current_message


@pytest.mark.no_network
def test_read_external_pdf_page_is_rendered_as_read_content_not_discovery():
    ctx = make_ctx()
    pdf_page = {
        "title": "External report",
        "url": "https://example.test/report.pdf",
        "content": "The observed PDF page passage.",
        "page_number": 2,
        "start_line": 1,
        "end_line": 1,
        "content_hash": "b" * 64,
        "source_kind": "web_pdf",
    }

    update_accumulators(ctx, "read_web_page", {"data": [pdf_page]})
    message = build_user_message(
        ctx,
        last_result={"tool": "read_web_page", "result": {"data": [pdf_page]}},
    )

    assert "**Web content actually read:**" in message
    assert "External PDF read page 2 lines 1-1" in message
    assert "The observed PDF page passage." in message
    assert "discovery only" not in message


@pytest.mark.no_network
def test_build_user_message_omits_participants_outside_community():
    ctx = make_ctx(
        is_community=False,
        current_participants=["agent-1"],
    )

    message = build_user_message(ctx)

    assert "Participants" not in message


@pytest.mark.no_network
def test_build_user_message_includes_absolute_and_elapsed_last_turn_time():
    ctx = make_ctx(
        last_turn_at=datetime(2026, 8, 24, 15, 30, tzinfo=timezone.utc),
    )

    with frozen_time(datetime(2026, 8, 24, 18, 5, tzinfo=timezone.utc)):
        message = build_user_message(ctx)

    assert (
        "**Last successful turn:** "
        "2026-08-24T15:30:00+00:00 (2h 35m ago)"
    ) in message


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
    episode_result = {
        "data": {
            "resolution": "exact",
            "results": [
                {
                    "episodes": [
                        {"episode_id": "ep-1", "summary": "Profile changed"}
                    ]
                }
            ],
        }
    }
    update_accumulators(ctx, "episode_check", episode_result)
    update_accumulators(ctx, "episode_check", episode_result)
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
        {
            "data": [
                {
                    "title": "Example A",
                    "url": "https://example.test/a",
                    "snippet": "A useful web result.",
                    "provider": "brave",
                    "query": "profile behavior",
                    "rank": 1,
                },
                {
                    "title": "Example A duplicate",
                    "url": "https://example.test/a",
                    "snippet": "A duplicate web result.",
                    "provider": "brave",
                    "query": "profile behavior",
                    "rank": 2,
                },
            ]
        },
    )
    update_accumulators(
        ctx,
        "news_search",
        {
            "data": [
                {
                    "title": "Example News",
                    "url": "https://example.test/news",
                    "snippet": "A useful news result.",
                    "provider": "brave",
                    "query": "profile behavior",
                    "rank": 1,
                }
            ]
        },
    )

    assert ctx.profiles == [{"id": 1, "canonical_name": "Ada"}]
    assert ctx.graph == [
        {"source": "Ada", "target": "Knoggin", "score": 0.8},
        {"source": "Ada", "target": "Testing"},
    ]
    assert ctx.paths == [{"entity_a": "Ada", "entity_b": "Knoggin"}]
    assert ctx.episodes == [
        {
            "episode_id": "ep-1",
            "summary": "Profile changed",
            "resolution": "exact",
        }
    ]
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
        {
            "title": "Example A",
            "url": "https://example.test/a",
            "snippet": "A useful web result.",
            "provider": "brave",
            "query": "profile behavior",
            "rank": 1,
            "source_kind": "web_search_result",
        },
        {
            "title": "Example News",
            "url": "https://example.test/news",
            "snippet": "A useful news result.",
            "provider": "brave",
            "query": "profile behavior",
            "rank": 1,
            "source_kind": "news_search_result",
        },
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
                {
                    "title": "Old result",
                    "url": "https://example.test/old",
                    "snippet": "Old snippet.",
                },
                {
                    "title": "New result",
                    "url": "https://example.test/new",
                    "snippet": "New snippet.",
                },
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
    assert ctx.sources == [
        {
            "title": "New result",
            "url": "https://example.test/new",
            "snippet": "New snippet.",
            "source_kind": "web_search_result",
        }
    ]
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
            "read_document",
            {"data": [{"content": "lines"}]},
            ("Read document content", 1),
        ),
        (
            "read_web_page",
            {"data": [{"content": "lines"}]},
            ("Read web content", 1),
        ),
        ("anything_else", {"data": {"ok": True}}, ("Completed", 1)),
        ("anything_else", {"data": None}, ("No results", 0)),
        ("anything_else", {"error": "boom"}, ("Error: boom", 0)),
    ],
)
def test_summarize_result_returns_stable_summaries(tool_name, result, expected):
    assert summarize_result(tool_name, result) == expected
