"""Tests for agent/formatters.py — all pure functions, zero external deps."""


from src.agent.formatters import (
    _normalize_timestamp,
    _format_timestamp,
    format_retrieved_messages,
    format_entity_results,
    format_graph_results,
    format_path_results,
    format_hierarchy_results,
    format_memory_context,
    format_hot_topic_context,
    format_fact_results,
    format_files_context,
    TS_MIN,
    TS_MAX,
)


class TestNormalizeTimestamp:

    def test_seconds(self):
        ts = 1700000000
        result = _normalize_timestamp(ts)
        assert result == ts

    def test_milliseconds(self):
        ts = 1700000000_000
        result = _normalize_timestamp(ts)
        assert result == 1700000000

    def test_microseconds(self):
        ts = 1700000000_000_000
        result = _normalize_timestamp(ts)
        assert result == 1700000000

    def test_nanoseconds(self):
        ts = 1700000000_000_000_000
        result = _normalize_timestamp(ts)
        assert result == 1700000000

    def test_out_of_bounds_low(self):
        assert _normalize_timestamp(100) is None

    def test_out_of_bounds_high(self):
        assert _normalize_timestamp(10**19) is None

    def test_boundary_min(self):
        result = _normalize_timestamp(TS_MIN)
        assert result == TS_MIN

    def test_boundary_max(self):
        result = _normalize_timestamp(TS_MAX)
        assert result == TS_MAX


class TestFormatTimestamp:

    def test_none_returns_unknown(self):
        assert _format_timestamp(None) == "unknown"

    def test_empty_string_returns_unknown(self):
        assert _format_timestamp("") == "unknown"

    def test_zero_returns_unknown(self):
        assert _format_timestamp(0) == "unknown"

    def test_iso_string_with_z(self):
        result = _format_timestamp("2025-01-15T14:30:00Z")
        assert "2025-01-15" in result
        assert "14:30" in result

    def test_iso_string_with_offset(self):
        result = _format_timestamp("2025-01-15T14:30:00+00:00")
        assert "2025-01-15" in result

    def test_unix_seconds(self):
        result = _format_timestamp(1700000000)
        assert "2023" in result
        assert "UTC" in result

    def test_unix_milliseconds(self):
        result = _format_timestamp(1700000000000)
        assert "2023" in result

    def test_invalid_string_returns_unknown(self):
        assert _format_timestamp("not-a-date") == "unknown"

    def test_out_of_range_numeric_returns_unknown(self):
        assert _format_timestamp(999) == "unknown"


class TestFormatRetrievedMessages:

    def test_empty(self):
        assert format_retrieved_messages([]) == "No messages found."
        assert format_retrieved_messages(None) == "No messages found."

    def test_single_hit_with_context(self):
        msgs = [{
            "score": 0.85,
            "context": [
                {"role": "user", "content": "Tell me about Alice", "timestamp": "2025-01-15T10:00:00Z", "is_hit": True},
                {"role": "assistant", "content": "Alice is a researcher", "timestamp": "2025-01-15T10:01:00Z", "is_hit": False},
            ]
        }]
        result = format_retrieved_messages(msgs)
        assert "Result #1" in result
        assert "0.85" in result
        assert ">>" in result
        assert "USER" in result
        assert "AGENT" in result
        assert "Tell me about Alice" in result

    def test_bad_timestamp_doesnt_crash(self):
        msgs = [{"score": 0.5, "context": [
            {"role": "user", "content": "hi", "timestamp": "garbage", "is_hit": True}
        ]}]
        result = format_retrieved_messages(msgs)
        assert "hi" in result

    def test_missing_timestamp(self):
        msgs = [{"score": 0.5, "context": [
            {"role": "user", "content": "hi", "is_hit": False}
        ]}]
        result = format_retrieved_messages(msgs)
        assert "hi" in result


class TestFormatEntityResults:

    def test_empty(self):
        assert format_entity_results([]) == "No entities found."

    def test_basic_entity(self):
        entities = [{
            "canonical_name": "Alice",
            "type": "person",
            "aliases": ["Ali"],
            "topic": "Identity",
            "last_mentioned": "2025-01-15T10:00:00Z",
            "facts": ["Works at Anthropic", "Lives in SF"],
        }]
        result = format_entity_results(entities)
        assert "=== Alice (person) ===" in result
        assert "Ali" in result
        assert "Identity" in result
        assert "Works at Anthropic" in result

    def test_no_facts(self):
        entities = [{"canonical_name": "Bob", "type": "person", "facts": []}]
        result = format_entity_results(entities)
        assert "None recorded" in result

    def test_connections_with_context(self):
        entities = [{
            "canonical_name": "Alice",
            "type": "person",
            "facts": [],
            "top_connections": [{
                "canonical_name": "Bob",
                "aliases": ["Robert"],
                "weight": 5,
                "context": "coworkers at Anthropic",
                "evidence": [
                    {"message": "Alice and Bob work together", "timestamp": 1700000000}
                ]
            }]
        }]
        result = format_entity_results(entities)
        assert "Bob" in result
        assert "Robert" in result
        assert "coworkers" in result
        assert "weight: 5" in result
        assert "Alice and Bob work together" in result

    def test_connections_without_context(self):
        entities = [{
            "canonical_name": "Alice",
            "type": "person",
            "facts": [],
            "top_connections": [{
                "canonical_name": "Carol",
                "aliases": [],
                "weight": 2,
                "evidence": []
            }]
        }]
        result = format_entity_results(entities)
        assert "Carol" in result
        assert "Context" not in result

    def test_evidence_limit(self):
        evidence = [{"message": f"msg {i}", "timestamp": 1700000000} for i in range(10)]
        entities = [{
            "canonical_name": "Alice",
            "type": "person",
            "facts": [],
            "top_connections": [{
                "canonical_name": "Bob",
                "aliases": [],
                "weight": 1,
                "evidence": evidence
            }]
        }]
        result = format_entity_results(entities, evidence_limit=3)
        assert "msg 0" in result
        assert "msg 2" in result
        assert "msg 3" not in result


class TestFormatGraphResults:

    def test_empty(self):
        assert format_graph_results([]) == "No connections found."

    def test_connection_edge(self):
        results = [{
            "source": "Alice",
            "target": "Bob",
            "connection_strength": 5,
            "last_seen": "2025-01-15T10:00:00Z",
            "context": "colleagues",
            "target_facts": ["Works at Anthropic", "PhD in CS"],
            "evidence": [{"message": "They collaborate on research", "timestamp": 1700000000}]
        }]
        result = format_graph_results(results)
        assert "Alice -> Bob" in result
        assert "colleagues" in result
        assert "Strength: 5" in result
        assert "Works at Anthropic" in result

    def test_activity_entity(self):
        results = [{
            "entity": "Project X",
            "time": "2025-01-15T10:00:00Z",
            "evidence": [{"message": "Discussed roadmap", "timestamp": 1700000000}]
        }]
        result = format_graph_results(results)
        assert "Activity: Project X" in result
        assert "Discussed roadmap" in result

    def test_unknown_shape_skipped(self):
        results = [{"random_key": "value"}]
        result = format_graph_results(results)
        assert result == ""


class TestFormatPathResults:

    def test_empty(self):
        assert format_path_results([]) == "No path found."

    def test_hidden_path(self):
        path = [{"hidden": True, "message": "Connection through inactive topic."}]
        result = format_path_results(path)
        assert "Connection through inactive topic." in result

    def test_hidden_path_default_message(self):
        path = [{"hidden": True}]
        result = format_path_results(path)
        assert "inactive topics" in result

    def test_single_hop(self):
        path = [{"step": 0, "entity_a": "Alice", "entity_b": "Bob",
                 "evidence": [{"message": "friends", "timestamp": 1700000000}]}]
        result = format_path_results(path)
        assert "Alice -> Bob" in result
        assert "1 hop" in result
        assert "hops" not in result
        assert "friends" in result

    def test_multi_hop(self):
        path = [
            {"step": 0, "entity_a": "Alice", "entity_b": "Bob", "evidence": []},
            {"step": 1, "entity_a": "Bob", "entity_b": "Carol", "evidence": []},
        ]
        result = format_path_results(path)
        assert "Alice -> Bob -> Carol" in result
        assert "2 hops" in result

    def test_locked_step(self):
        path = [{"step": 0, "entity_a": "A", "entity_b": "B",
                 "status": "LOCKED", "locked_reason": "Topic disabled"}]
        result = format_path_results(path)
        assert "LOCKED" in result
        assert "Topic disabled" in result


class TestFormatHierarchyResults:

    def test_empty(self):
        assert format_hierarchy_results([]) == "No hierarchy found."

    def test_with_ancestry_parents_children(self):
        results = [{
            "entity": "Module A",
            "ancestry": ["Root", "Project X"],
            "parents": [{"canonical_name": "Project X", "facts": ["Active project"]}],
            "children": [{"canonical_name": "Task 1", "facts": ["Due Friday", "Assigned to Bob"]}],
        }]
        result = format_hierarchy_results(results)
        assert "=== Module A ===" in result
        assert "Root → Project X" in result
        assert "↑ Project X" in result
        assert "Active project" in result
        assert "↓ Task 1" in result
        assert "Due Friday" in result

    def test_children_facts_capped_at_two(self):
        results = [{
            "entity": "X",
            "children": [{"canonical_name": "Y", "facts": ["f1", "f2", "f3"]}]
        }]
        result = format_hierarchy_results(results)
        assert "f1" in result
        assert "f2" in result
        assert "f3" not in result

    def test_no_parents_or_children(self):
        results = [{"entity": "Orphan"}]
        result = format_hierarchy_results(results)
        assert "=== Orphan ===" in result
        assert "Parents" not in result
        assert "Children" not in result


class TestFormatMemoryContext:

    def test_empty_dict(self):
        assert format_memory_context({}) == ""

    def test_none(self):
        assert format_memory_context(None) == ""

    def test_empty_entries(self):
        assert format_memory_context({"General": []}) == ""

    def test_single_topic(self):
        blocks = {
            "General": [
                {"id": "m1", "content": "User likes Python"},
                {"id": "m2", "content": "User works at Anthropic"},
            ]
        }
        result = format_memory_context(blocks)
        assert "[General]" in result
        assert "(m1) User likes Python" in result
        assert "(m2) User works at Anthropic" in result

    def test_multi_topic(self):
        blocks = {
            "General": [{"id": "m1", "content": "fact A"}],
            "Tech": [{"id": "m2", "content": "fact B"}],
        }
        result = format_memory_context(blocks)
        assert "[General]" in result
        assert "[Tech]" in result


class TestFormatHotTopicContext:

    def test_empty(self):
        assert format_hot_topic_context({}) == ""
        assert format_hot_topic_context(None) == ""

    def test_topic_with_entities_and_facts(self):
        context = {
            "AI Research": {
                "entities": [
                    {"name": "GPT-5", "facts": ["Released 2025", "Multimodal", "Extra fact"]},
                    {"name": "Claude", "facts": []},
                ]
            }
        }
        result = format_hot_topic_context(context)
        assert "[HOT: AI Research]" in result
        assert "GPT-5" in result
        assert "Released 2025" in result
        assert "Claude" in result

    def test_facts_capped_at_three(self):
        context = {
            "Tech": {
                "entities": [
                    {"name": "X", "facts": ["f1", "f2", "f3", "f4"]}
                ]
            }
        }
        result = format_hot_topic_context(context)
        assert "f3" in result
        assert "f4" not in result

    def test_entity_without_name_skipped(self):
        context = {"T": {"entities": [{"name": "", "facts": ["orphan"]}]}}
        result = format_hot_topic_context(context)
        assert "orphan" not in result


class TestFormatFactResults:

    def test_empty(self):
        assert format_fact_results([]) == "No facts found."

    def test_exact_match(self):
        results = [{
            "resolution": "exact",
            "results": [{
                "entity_name": "Alice",
                "similarity": 1.0,
                "facts": ["Works at Anthropic", "PhD in CS"],
            }]
        }]
        result = format_fact_results(results)
        assert "exact match" in result
        assert "Alice" in result
        assert "Works at Anthropic" in result

    def test_fact_as_dict(self):
        results = [{
            "resolution": "fuzzy",
            "results": [{
                "entity_name": "Bob",
                "similarity": 0.85,
                "facts": [{"content": "Lives in NYC"}],
            }]
        }]
        result = format_fact_results(results)
        assert "Lives in NYC" in result

    def test_no_facts_recorded(self):
        results = [{
            "resolution": "exact",
            "results": [{"entity_name": "Ghost", "similarity": 1.0, "facts": []}]
        }]
        result = format_fact_results(results)
        assert "No specific facts recorded" in result

    def test_fallback_resolution(self):
        results = [{
            "resolution": "fallback",
            "results": [{
                "score": 0.7,
                "context": [{"role": "user", "content": "clue msg", "timestamp": "", "is_hit": True}]
            }]
        }]
        result = format_fact_results(results)
        assert "Fallback" in result
        assert "clue msg" in result

    def test_multiple_entries(self):
        results = [
            {"resolution": "exact", "results": [{"entity_name": "A", "similarity": 1.0, "facts": ["f1"]}]},
            {"resolution": "fuzzy", "results": [{"entity_name": "B", "similarity": 0.8, "facts": ["f2"]}]},
        ]
        result = format_fact_results(results)
        assert "exact" in result
        assert "fuzzy" in result


class TestFormatFilesContext:

    def test_empty(self):
        assert format_files_context([]) == ""
        assert format_files_context(None) == ""

    def test_single_file(self):
        files = [{"original_name": "report.pdf", "size_bytes": 2048, "chunk_count": 5}]
        result = format_files_context(files)
        assert "report.pdf" in result
        assert "2KB" in result
        assert "5 chunks" in result

    def test_multiple_files(self):
        files = [
            {"original_name": "a.pdf", "size_bytes": 1024, "chunk_count": 3},
            {"original_name": "b.docx", "size_bytes": 5120, "chunk_count": 10},
        ]
        result = format_files_context(files)
        assert "a.pdf" in result
        assert "b.docx" in result

    def test_zero_size(self):
        files = [{"original_name": "empty.txt", "size_bytes": 0, "chunk_count": 0}]
        result = format_files_context(files)
        assert "0KB" in result