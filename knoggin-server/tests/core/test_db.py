"""Tests for src/db/reader.py, src/db/writer.py, src/db/store.py, and src/db/query_tools.py."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest

from src.db.reader import GraphReader
from src.db.writer import GraphWriter
from src.db.store import MemGraphStore
from src.db.query_tools import GraphToolQueries
from src.common.schema.dtypes import Fact




def make_mock_driver():
    driver = MagicMock()
    session = MagicMock()
    driver.session.return_value.__enter__ = MagicMock(return_value=session)
    driver.session.return_value.__exit__ = MagicMock(return_value=False)
    return driver, session


def make_record(data: dict):
    record = MagicMock()
    record.__getitem__ = lambda self, key: data[key]
    record.get = lambda key, default=None: data.get(key, default)
    record.keys = lambda: data.keys()
    record.items = lambda: data.items()
    record.values = lambda: data.values()
    record.__iter__ = lambda self: iter(data)
    record.__len__ = lambda self: len(data)
    record.data = lambda: dict(data)
    return record


class TestGraphReader:

    @pytest.fixture
    def reader(self):
        driver, session = make_mock_driver()
        return GraphReader(driver), session

    def test_get_message_text(self, reader):
        rdr, session = reader
        session.run.return_value.single.return_value = make_record({"content": "Hello world"})
        result = rdr.get_message_text(42)
        assert result == "Hello world"
        args = session.run.call_args
        assert args[0][1]["id"] == 42

    def test_get_message_text_not_found(self, reader):
        rdr, session = reader
        session.run.return_value.single.return_value = None
        assert rdr.get_message_text(999) == ""

    def test_get_message_text_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("connection lost")
        assert rdr.get_message_text(1) == ""

    def test_get_max_entity_id(self, reader):
        rdr, session = reader
        session.run.return_value.single.return_value = make_record({"max_id": 42})
        assert rdr.get_max_entity_id() == 42

    def test_get_max_entity_id_empty_graph(self, reader):
        rdr, session = reader
        session.run.return_value.single.return_value = make_record({"max_id": None})
        assert rdr.get_max_entity_id() == 0

    def test_get_max_entity_id_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("db down")
        assert rdr.get_max_entity_id() == 0

    def test_validate_existing_ids(self, reader):
        rdr, session = reader
        records = [make_record({"id": 1}), make_record({"id": 3})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        assert rdr.validate_existing_ids([1, 2, 3]) == {1, 3}

    def test_validate_existing_ids_empty_input(self, reader):
        rdr, session = reader
        assert rdr.validate_existing_ids([]) == set()
        session.run.assert_not_called()

    def test_validate_existing_ids_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("timeout")
        assert rdr.validate_existing_ids([1, 2]) is None

    def test_get_facts_for_entity_hydrates(self, reader):
        rdr, session = reader
        fact_record = make_record({
            "id": "fact_1", "source_entity_id": 10,
            "content": "Works at Anthropic",
            "valid_at": "2025-01-15T00:00:00+00:00", "invalid_at": None,
            "confidence": 0.95, "embedding": [0.1, 0.2], "source_msg_id": 5,
        })
        session.run.return_value.__iter__ = lambda self: iter([fact_record])
        facts = rdr.get_facts_for_entity(10, active_only=True)
        assert len(facts) == 1
        assert isinstance(facts[0], Fact)
        assert facts[0].content == "Works at Anthropic"

    def test_get_facts_for_entity_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("query failed")
        assert rdr.get_facts_for_entity(10) == []

    def test_get_facts_for_entity_inactive_included(self, reader):
        rdr, session = reader
        fact_record = make_record({
            "id": "fact_inactive", "source_entity_id": 10,
            "content": "Old job at Google",
            "valid_at": "2024-01-01T00:00:00+00:00",
            "invalid_at": "2025-01-01T00:00:00+00:00",
            "confidence": 0.9, "embedding": [], "source_msg_id": None,
        })
        session.run.return_value.__iter__ = lambda self: iter([fact_record])
        facts = rdr.get_facts_for_entity(10, active_only=False)
        assert len(facts) == 1
        assert facts[0].invalid_at is not None

    def test_get_facts_for_entities_empty_input(self, reader):
        rdr, session = reader
        assert rdr.get_facts_for_entities([]) == {}
        session.run.assert_not_called()

    def test_get_facts_for_entities_groups_by_id(self, reader):
        rdr, session = reader
        records = [
            make_record({
                "entity_id": 1, "id": "f1", "source_entity_id": 1,
                "content": "fact A", "valid_at": "2025-01-01T00:00:00+00:00",
                "invalid_at": None, "confidence": 1.0, "embedding": [],
                "source_msg_id": None,
            }),
            make_record({
                "entity_id": 2, "id": "f2", "source_entity_id": 2,
                "content": "fact B", "valid_at": "2025-01-01T00:00:00+00:00",
                "invalid_at": None, "confidence": 1.0, "embedding": [],
                "source_msg_id": None,
            }),
        ]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.get_facts_for_entities([1, 2, 3])
        assert len(result[1]) == 1
        assert len(result[2]) == 1
        assert result[3] == []

    def test_get_facts_for_entities_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("db down")
        assert rdr.get_facts_for_entities([1, 2, 3]) == {1: [], 2: [], 3: []}

    def test_get_entity_by_id_found(self, reader):
        rdr, session = reader
        record = make_record({
            "id": 1, "session_id": "s1", "canonical_name": "Alice",
            "aliases": ["alice"], "type": "person", "topic": "Identity",
            "last_mentioned": 1000, "last_updated": 1000,
            "last_profiled_msg_id": None,
        })
        session.run.return_value.single.return_value = record
        result = rdr.get_entity_by_id(1)
        assert result["canonical_name"] == "Alice"
        assert result["type"] == "person"

    def test_get_entity_by_id_not_found(self, reader):
        rdr, session = reader
        session.run.return_value.single.return_value = None
        assert rdr.get_entity_by_id(999) is None

    def test_get_entity_by_id_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("connection lost")
        assert rdr.get_entity_by_id(1) is None

    def test_get_messages_by_ids_empty(self, reader):
        rdr, session = reader
        assert rdr.get_messages_by_ids([]) == []
        session.run.assert_not_called()

    def test_get_messages_by_ids_happy(self, reader):
        rdr, session = reader
        records = [
            make_record({"id": 1, "role": "user", "content": "hello", "timestamp": "2025-01-01"}),
            make_record({"id": 2, "role": "assistant", "content": "hi there", "timestamp": "2025-01-01"}),
        ]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.get_messages_by_ids([1, 2])
        assert len(result) == 2
        assert result[0]["content"] == "hello"
        assert result[1]["role"] == "assistant"

    def test_get_messages_by_ids_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("timeout")
        assert rdr.get_messages_by_ids([1, 2]) == []

    def test_get_facts_from_message_happy(self, reader):
        rdr, session = reader
        records = [
            make_record({
                "id": "f1", "source_entity_id": 5, "content": "Works at Anthropic",
                "valid_at": "2025-01-01T00:00:00+00:00", "invalid_at": None,
                "confidence": 0.95, "embedding": [], "source_msg_id": 42,
            }),
        ]
        session.run.return_value.__iter__ = lambda self: iter(records)
        facts = rdr.get_facts_from_message(42)
        assert len(facts) == 1
        assert isinstance(facts[0], Fact)

    def test_get_facts_from_message_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("query failed")
        assert rdr.get_facts_from_message(42) == []

    def test_get_entity_embedding_found(self, reader):
        rdr, session = reader
        session.run.return_value.single.return_value = make_record({"embedding": [0.1, 0.2, 0.3]})
        assert rdr.get_entity_embedding(1) == [0.1, 0.2, 0.3]

    def test_get_entity_embedding_not_found(self, reader):
        rdr, session = reader
        session.run.return_value.single.return_value = None
        assert rdr.get_entity_embedding(999) == []

    def test_get_entity_embedding_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("db error")
        assert rdr.get_entity_embedding(1) == []

    def test_get_all_entities_for_hydration_happy(self, reader):
        rdr, session = reader
        records = [
            make_record({"id": 1, "canonical_name": "Alice", "aliases": ["al"],
                         "type": "person", "topic": "Identity", "session_id": "s1"}),
            make_record({"id": 2, "canonical_name": "Acme Corp", "aliases": [],
                         "type": "company", "topic": "Work", "session_id": "s1"}),
        ]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.get_all_entities_for_hydration()
        assert len(result) == 2
        assert result[0]["canonical_name"] == "Alice"

    def test_get_all_entities_for_hydration_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("db down")
        assert rdr.get_all_entities_for_hydration() == []

    def test_find_alias_collisions_happy(self, reader):
        rdr, session = reader
        records = [make_record({"id_a": 1, "id_b": 3}), make_record({"id_a": 2, "id_b": 5})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        assert rdr.find_alias_collisions() == [(1, 3), (2, 5)]

    def test_find_alias_collisions_none_found(self, reader):
        rdr, session = reader
        session.run.return_value.__iter__ = lambda self: iter([])
        assert rdr.find_alias_collisions() == []

    def test_get_orphan_entities_happy(self, reader):
        rdr, session = reader
        records = [make_record({"id": 5}), make_record({"id": 8})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.get_orphan_entities(protected_id=1, orphan_cutoff_ms=1000, stale_junk_cutoff_ms=2000)
        assert result == [5, 8]
        session.run.assert_called_once()

    def test_get_orphan_entities_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("query failed")
        assert rdr.get_orphan_entities() == []

    def test_get_neighbor_ids_happy(self, reader):
        rdr, session = reader
        records = [make_record({"neighbor_id": 2}), make_record({"neighbor_id": 3})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        assert rdr.get_neighbor_ids(1) == {2, 3}

    def test_get_neighbor_ids_no_neighbors(self, reader):
        rdr, session = reader
        session.run.return_value.__iter__ = lambda self: iter([])
        assert rdr.get_neighbor_ids(1) == set()

    def test_get_entities_by_names_happy(self, reader):
        rdr, session = reader
        records = [
            make_record({"id": 1, "canonical_name": "Alice", "type": "person",
                         "aliases": ["al"], "facts": ["Works at Anthropic"]}),
        ]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.get_entities_by_names(["Alice"])
        assert len(result) == 1
        assert result[0]["canonical_name"] == "Alice"

    def test_get_entities_by_names_no_match(self, reader):
        rdr, session = reader
        session.run.return_value.__iter__ = lambda self: iter([])
        assert rdr.get_entities_by_names(["Nobody"]) == []

    def test_get_parent_entities_happy(self, reader):
        rdr, session = reader
        records = [make_record({"id": 10, "canonical_name": "Anthropic",
                                "type": "company", "facts": ["AI safety company"]})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.get_parent_entities(5)
        assert result[0]["canonical_name"] == "Anthropic"

    def test_get_parent_entities_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("query failed")
        assert rdr.get_parent_entities(5) == []

    def test_get_child_entities_happy(self, reader):
        rdr, session = reader
        records = [make_record({"id": 20, "canonical_name": "Project X",
                                "type": "project", "facts": []})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        assert rdr.get_child_entities(10)[0]["canonical_name"] == "Project X"

    def test_get_child_entities_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("query failed")
        assert rdr.get_child_entities(10) == []

    def test_has_direct_edge_true(self, reader):
        rdr, session = reader
        session.run.return_value.single.return_value = make_record({"connected": True})
        assert rdr.has_direct_edge(1, 2) is True

    def test_has_direct_edge_false(self, reader):
        rdr, session = reader
        session.run.return_value.single.return_value = make_record({"connected": False})
        assert rdr.has_direct_edge(1, 2) is False

    def test_has_direct_edge_no_result(self, reader):
        rdr, session = reader
        session.run.return_value.single.return_value = None
        assert rdr.has_direct_edge(1, 2) is False

    def test_has_hierarchy_edge_true(self, reader):
        rdr, session = reader
        session.run.return_value.single.return_value = make_record({"exists": True})
        assert rdr.has_hierarchy_edge(1, 2) is True

    def test_has_hierarchy_edge_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("db error")
        assert rdr.has_hierarchy_edge(1, 2) is False

    def test_get_surrounding_messages_happy(self, reader):
        rdr, session = reader
        records = [
            make_record({"id": 1, "content": "A", "timestamp": "2025-01-01T00:00:00Z", "role": "user"}),
            make_record({"id": 2, "content": "B", "timestamp": "2025-01-01T00:10:00Z", "role": "assistant"})
        ]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.get_surrounding_messages(2)
        assert len(result) == 2
        assert result[0]["content"] == "A"
        assert result[1]["role"] == "assistant"

    def test_get_surrounding_messages_empty(self, reader):
        rdr, session = reader
        session.run.return_value.__iter__ = lambda self: iter([])
        assert rdr.get_surrounding_messages(999) == []

    def test_get_surrounding_messages_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("db error")
        assert rdr.get_surrounding_messages(1) == []

    def test_get_neighbor_entities_happy(self, reader):
        rdr, session = reader
        records = [make_record({"id": 5, "name": "Bob"})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.get_neighbor_entities(1)
        assert len(result) == 1
        assert result[0]["name"] == "Bob"

    def test_get_neighbor_entities_empty(self, reader):
        rdr, session = reader
        session.run.return_value.__iter__ = lambda self: iter([])
        assert rdr.get_neighbor_entities(1) == []

    def test_get_neighbor_entities_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("db error")
        assert rdr.get_neighbor_entities(1) == []

    def test_search_similar_entities_happy(self, reader):
        rdr, session = reader
        records = [make_record({"id": 5, "similarity": 0.9})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.search_similar_entities(1)
        assert result == [(5, 0.9)]

    def test_search_similar_entities_empty(self, reader):
        rdr, session = reader
        session.run.return_value.__iter__ = lambda self: iter([])
        assert rdr.search_similar_entities(1) == []

    def test_search_entities_by_embedding_happy(self, reader):
        rdr, session = reader
        records = [make_record({"id": 5, "similarity": 0.85})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.search_entities_by_embedding([0.1, 0.2])
        assert result == [(5, 0.85)]

    def test_search_entities_by_embedding_empty(self, reader):
        rdr, session = reader
        session.run.return_value.__iter__ = lambda self: iter([])
        assert rdr.search_entities_by_embedding([0.1]) == []

    def test_search_messages_vector_happy(self, reader):
        rdr, session = reader
        records = [make_record({"id": 10, "similarity": 0.95})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.search_messages_vector([0.1, 0.2])
        assert result == [(10, 0.95)]

    def test_search_messages_vector_empty(self, reader):
        rdr, session = reader
        session.run.return_value.__iter__ = lambda self: iter([])
        assert rdr.search_messages_vector([0.1]) == []

    def test_get_hierarchy_candidates_happy(self, reader):
        rdr, session = reader
        records = [make_record({
            "parent_id": 1, "parent_name": "Alice", "parent_embedding": [],
            "child_id": 5, "child_name": "Bob", "child_embedding": [], "weight": 5
        })]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.get_hierarchy_candidates("General", "person", ["project"], 2)
        assert len(result) == 1
        assert result[0]["child_name"] == "Bob"

    def test_get_hierarchy_candidates_empty(self, reader):
        rdr, session = reader
        session.run.return_value.__iter__ = lambda self: iter([])
        assert rdr.get_hierarchy_candidates("General", "person", ["project"], 2) == []

    def test_list_entities_happy(self, reader):
        rdr, session = reader
        count_res = MagicMock()
        count_res.single.return_value = make_record({"total": 1})
        data_res = MagicMock()
        data_res.__iter__ = lambda self: iter([make_record({"id": 5, "canonical_name": "Bob", "type": "person", "topic": "General"})])
        session.run.side_effect = [count_res, data_res]

        entities, total = rdr.list_entities()
        assert total == 1
        assert len(entities) == 1
        assert entities[0]["canonical_name"] == "Bob"

    def test_list_entities_empty(self, reader):
        rdr, session = reader
        count_res = MagicMock()
        count_res.single.return_value = make_record({"total": 0})
        session.run.side_effect = [count_res]
        assert rdr.list_entities() == ([], 0)

    def test_list_entities_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("db error")
        assert rdr.list_entities() == ([], 0)

    def test_list_preferences_happy(self, reader):
        rdr, session = reader
        records = [make_record({"id": "p1", "topic": "General", "kind": "rule", "content": "Always helpful"})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.list_preferences("s1")
        assert len(result) == 1
        assert result[0]["content"] == "Always helpful"

    def test_list_preferences_empty(self, reader):
        rdr, session = reader
        session.run.return_value.__iter__ = lambda self: iter([])
        assert rdr.list_preferences("s1") == []

    def test_list_preferences_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("db error")
        assert rdr.list_preferences("s1") == []

    def test_get_entity_relationships_happy(self, reader):
        rdr, session = reader
        records = [make_record({"source": "Alice", "target": "Bob", "context": "colleagues", "weight": 5, "last_seen_msg_id": 1000, "evidence_ids": []})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.get_entity_relationships(1)
        assert len(result) == 1
        assert result[0]["source"] == "Alice"

    def test_get_entity_relationships_empty(self, reader):
        rdr, session = reader
        session.run.return_value.__iter__ = lambda self: iter([])
        assert rdr.get_entity_relationships(1) == []

    def test_get_recent_facts_happy(self, reader):
        rdr, session = reader
        records = [make_record({"source_entity_id": 1, "content": "A", "valid_at": "2025-01-01T00:00:00Z"})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.get_recent_facts()
        assert len(result) == 1
        assert result[0]["content"] == "A"

    def test_get_recent_facts_empty(self, reader):
        rdr, session = reader
        session.run.return_value.__iter__ = lambda self: iter([])
        assert rdr.get_recent_facts() == []

    def test_get_recently_active_entities_happy(self, reader):
        rdr, session = reader
        records = [make_record({"id": 1, "canonical_name": "Bob", "type": "person", "topic": "General"})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.get_recently_active_entities()
        assert len(result) == 1
        assert result[0]["canonical_name"] == "Bob"

    def test_get_recently_active_entities_empty(self, reader):
        rdr, session = reader
        session.run.return_value.__iter__ = lambda self: iter([])
        assert rdr.get_recently_active_entities() == []

    def test_get_notable_entities_happy(self, reader):
        rdr, session = reader
        records = [make_record({"id": 1, "canonical_name": "Bob", "type": "person", "topic": "General"})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.get_notable_entities()
        assert len(result) == 1
        assert result[0]["canonical_name"] == "Bob"

    def test_get_notable_entities_empty(self, reader):
        rdr, session = reader
        session.run.return_value.__iter__ = lambda self: iter([])
        assert rdr.get_notable_entities() == []

    def test_get_graph_stats_happy(self, reader):
        rdr, session = reader
        session.run.return_value.single.return_value = make_record({
            "entities": 10, "facts": 50, "relationships": 25
        })
        assert rdr.get_graph_stats() == {"entities": 10, "facts": 50, "relationships": 25}

    def test_get_graph_stats_empty(self, reader):
        rdr, session = reader
        session.run.return_value.single.return_value = None
        assert rdr.get_graph_stats() == {"entities": 0, "facts": 0, "relationships": 0}

    def test_get_entity_count_by_type_happy(self, reader):
        rdr, session = reader
        records = [make_record({"type": "person", "count": 5}),
                   make_record({"type": "company", "count": 3})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        assert len(rdr.get_entity_count_by_type()) == 2

    def test_get_entity_count_by_type_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("db error")
        assert rdr.get_entity_count_by_type() == []

    def test_get_entity_count_by_topic_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("db error")
        assert rdr.get_entity_count_by_topic() == []

    def test_get_top_connected_entities_happy(self, reader):
        rdr, session = reader
        records = [make_record({"name": "Alice", "type": "person", "connections": 10})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = rdr.get_top_connected_entities(limit=5)
        assert result[0]["name"] == "Alice"

    def test_get_top_connected_entities_exception(self, reader):
        rdr, session = reader
        session.run.side_effect = Exception("db error")
        assert rdr.get_top_connected_entities() == []


class TestGraphWriter:

    @pytest.fixture
    def writer(self):
        driver, session = make_mock_driver()
        return GraphWriter(driver), session

    def test_save_message_logs_empty(self, writer):
        wr, session = writer
        assert wr.save_message_logs([]) is True
        session.run.assert_not_called()

    def test_save_message_logs_happy(self, writer):
        wr, session = writer
        msgs = [{"id": 1, "content": "hello", "role": "user",
                 "timestamp": "2025-01-01", "embedding": []}]
        wr.save_message_logs(msgs)
        session.execute_write.assert_called_once()

    def test_save_message_logs_exception_propagates(self, writer):
        wr, session = writer
        session.execute_write.side_effect = Exception("write failed")
        msgs = [{"id": 1, "content": "hello", "role": "user",
                 "timestamp": "2025-01-01", "embedding": []}]
        with pytest.raises(Exception, match="write failed"):
            wr.save_message_logs(msgs)

    def test_write_batch_defaults_aliases(self, writer):
        wr, session = writer
        wr.write_batch([{"id": 1, "canonical_name": "Alice", "type": "person", "confidence": 0.9}], [])
        session.execute_write.assert_called_once()

    def test_write_batch_defaults_confidence(self, writer):
        wr, session = writer
        wr.write_batch([], [{"entity_a": "Alice", "entity_b": "Bob", "entity_a_id": 1, "entity_b_id": 2}])
        session.execute_write.assert_called_once()

    def test_write_batch_combined(self, writer):
        wr, session = writer
        entities = [{"id": 1, "canonical_name": "Alice", "type": "person",
                     "confidence": 0.9, "session_id": "s1", "embedding": [], "topic": "Identity"}]
        rels = [{"entity_a": "Alice", "entity_b": "Bob", "entity_a_id": 1,
                 "entity_b_id": 2, "message_id": "msg_1", "context": "colleagues"}]
        assert wr.write_batch(entities, rels) is True
        session.execute_write.assert_called_once()

    def test_write_batch_returns_true(self, writer):
        wr, session = writer
        assert wr.write_batch([{"id": 1, "canonical_name": "Alice", "type": "person", "confidence": 0.9}], []) is True

    def test_create_facts_batch_empty(self, writer):
        wr, session = writer
        assert wr.create_facts_batch(1, []) == 0
        session.execute_write.assert_not_called()

    def test_create_facts_batch_happy(self, writer):
        wr, session = writer
        session.execute_write.return_value = 2
        facts = [
            Fact(id="f1", source_entity_id=1, content="fact 1",
                 valid_at=datetime.now(timezone.utc), embedding=[0.1]),
            Fact(id="f2", source_entity_id=1, content="fact 2",
                 valid_at=datetime.now(timezone.utc), embedding=[0.2]),
        ]
        assert wr.create_facts_batch(1, facts) == 2

    def test_create_facts_batch_exception_propagates(self, writer):
        wr, session = writer
        session.execute_write.side_effect = Exception("entity not found")
        facts = [Fact(id="f1", source_entity_id=999, content="test",
                      valid_at=datetime.now(timezone.utc), embedding=[0.1])]
        with pytest.raises(Exception, match="entity not found"):
            wr.create_facts_batch(999, facts)

    def test_invalidate_fact_success(self, writer):
        wr, session = writer
        session.execute_write.return_value = True
        assert wr.invalidate_fact("fact_123", datetime.now(timezone.utc)) is True

    def test_invalidate_fact_not_found(self, writer):
        wr, session = writer
        session.execute_write.return_value = False
        assert wr.invalidate_fact("nonexistent", datetime.now(timezone.utc)) is False

    def test_merge_entities_self_merge_rejected(self, writer):
        wr, session = writer
        assert wr.merge_entities(1, 1) is False
        session.execute_write.assert_not_called()

    def test_merge_entities_success(self, writer):
        wr, session = writer
        session.execute_write.return_value = True
        assert wr.merge_entities(1, 2) is True

    def test_merge_entities_not_found(self, writer):
        wr, session = writer
        session.execute_write.return_value = False
        assert wr.merge_entities(1, 999) is False

    def test_merge_entities_exception(self, writer):
        wr, session = writer
        session.execute_write.side_effect = Exception("deadlock")
        assert wr.merge_entities(1, 2) is False

    def test_delete_entity_success(self, writer):
        wr, session = writer
        session.execute_write.return_value = 1
        assert wr.delete_entity(42) is True

    def test_delete_entity_not_found(self, writer):
        wr, session = writer
        session.execute_write.return_value = 0
        assert wr.delete_entity(999) is False

    def test_delete_entity_exception(self, writer):
        wr, session = writer
        session.execute_write.side_effect = Exception("locked")
        assert wr.delete_entity(1) is False

    def test_bulk_delete_empty_input(self, writer):
        wr, session = writer
        assert wr.bulk_delete_entities([]) == 0
        session.execute_write.assert_not_called()

    def test_bulk_delete_success(self, writer):
        wr, session = writer
        session.execute_write.return_value = 3
        assert wr.bulk_delete_entities([1, 2, 3]) == 3

    def test_delete_old_facts_success(self, writer):
        wr, session = writer
        session.execute_write.return_value = 5
        assert wr.delete_old_invalidated_facts(datetime(2025, 1, 1, tzinfo=timezone.utc)) == 5

    def test_delete_old_facts_none_found(self, writer):
        wr, session = writer
        session.execute_write.return_value = 0
        assert wr.delete_old_invalidated_facts(datetime(2025, 1, 1, tzinfo=timezone.utc)) == 0

    def test_delete_old_facts_exception(self, writer):
        wr, session = writer
        session.execute_write.side_effect = Exception("db error")
        assert wr.delete_old_invalidated_facts(datetime(2025, 1, 1, tzinfo=timezone.utc)) == 0

    def test_update_aliases_empty_input(self, writer):
        wr, session = writer
        wr.update_entity_aliases({})
        session.execute_write.assert_not_called()

    def test_update_aliases_success(self, writer):
        wr, session = writer
        wr.update_entity_aliases({1: ["Alice", "Al"], 2: ["Bob"]})
        session.execute_write.assert_called_once()

    def test_update_entity_profile(self, writer):
        wr, session = writer
        wr.update_entity_profile(1, "Alice Johnson", [0.1, 0.2], 42)
        session.execute_write.assert_called_once()

    def test_update_entity_embedding(self, writer):
        wr, session = writer
        wr.update_entity_embedding(1, [0.1, 0.2, 0.3])
        session.execute_write.assert_called_once()

    def test_update_entity_checkpoint(self, writer):
        wr, session = writer
        wr.update_entity_checkpoint(1, 42)
        session.execute_write.assert_called_once()

    def test_create_hierarchy_edge_success(self, writer):
        wr, session = writer
        session.execute_write.return_value = True
        assert wr.create_hierarchy_edge(10, 20) is True

    def test_create_hierarchy_edge_exception(self, writer):
        wr, session = writer
        session.execute_write.side_effect = Exception("duplicate")
        assert wr.create_hierarchy_edge(10, 20) is False

    def test_cleanup_null_entities(self, writer):
        wr, session = writer
        session.execute_write.return_value = 3
        assert wr.cleanup_null_entities() == 3

    def test_create_preference_success(self, writer):
        wr, session = writer
        session.execute_write.return_value = "p1"
        assert wr.create_preference("session1", "General", "rule", "Be cool") == "p1"

    def test_create_preference_exception(self, writer):
        wr, session = writer
        session.execute_write.side_effect = Exception("db error")
        assert wr.create_preference("session1", "General", "rule", "Be cool") is False

    def test_delete_preference_success(self, writer):
        wr, session = writer
        session.execute_write.return_value = True
        assert wr.delete_preference("p1") is True

    def test_delete_preference_exception(self, writer):
        wr, session = writer
        session.execute_write.side_effect = Exception("db error")
        assert wr.delete_preference("p1") is False

    def test_delete_relationship_success(self, writer):
        wr, session = writer
        session.execute_write.return_value = True
        assert wr.delete_relationship(1, 2) is True

    def test_delete_relationship_not_found(self, writer):
        wr, session = writer
        session.execute_write.return_value = False
        assert wr.delete_relationship(1, 2) is False

    def test_delete_relationship_exception(self, writer):
        wr, session = writer
        session.execute_write.side_effect = Exception("db error")
        assert wr.delete_relationship(1, 2) is False


class TestGraphToolQueries:

    @pytest.fixture
    def tools(self):
        driver, session = make_mock_driver()
        return GraphToolQueries(driver), session

    def test_sanitize_fts_strips_operators(self):
        result = GraphToolQueries._sanitize_fts_query('hello + world -bad "quoted"')
        assert "+" not in result
        assert "-" not in result
        assert '"' not in result
        assert "hello" in result

    def test_sanitize_fts_collapses_whitespace(self):
        assert GraphToolQueries._sanitize_fts_query("hello    world") == "hello world"

    def test_sanitize_fts_empty_string(self):
        assert GraphToolQueries._sanitize_fts_query("") == ""

    def test_sanitize_fts_only_operators(self):
        assert GraphToolQueries._sanitize_fts_query('+-"*~^(){}[]').strip() == ""

    def test_build_path_data_simple(self):
        tq = GraphToolQueries(MagicMock())
        result = tq._build_path_data(
            ["Alice", "Bob", "Charlie"],
            ["Identity", "Work", "General"],
            [["msg_1", "msg_2"], ["msg_3"]]
        )
        assert len(result) == 2
        assert result[0]["entity_a"] == "Alice"
        assert result[0]["entity_b"] == "Bob"
        assert result[1]["entity_a"] == "Bob"
        assert result[1]["entity_b"] == "Charlie"

    def test_build_path_data_empty(self):
        assert GraphToolQueries(MagicMock())._build_path_data([], [], []) == []

    def test_search_messages_fts_happy(self, tools):
        tq, session = tools
        records = [make_record({"id": 1, "score": 0.95}), make_record({"id": 2, "score": 0.80})]
        session.run.return_value.__iter__ = lambda self: iter(records)
        assert len(tq.search_messages_fts("hello world")) == 2

    def test_search_messages_fts_exception(self, tools):
        tq, session = tools
        session.run.side_effect = Exception("fts error")
        assert tq.search_messages_fts("test") == []

    def test_get_related_entities_happy(self, tools):
        tq, session = tools
        records = [make_record({
            "source": "Alice", "target": "Bob", "target_facts": ["colleagues"],
            "connection_strength": 5, "evidence_ids": ["msg_1"],
            "confidence": 0.9, "last_seen": 1000, "context": "work"
        })]
        session.run.return_value.__iter__ = lambda self: iter(records)
        assert len(tq.get_related_entities(["Alice"])) == 1

    def test_get_related_entities_exception(self, tools):
        tq, session = tools
        session.run.side_effect = Exception("db error")
        assert tq.get_related_entities(["Alice"]) == []

    def test_get_hot_topic_context_happy(self, tools):
        tq, session = tools
        records = [make_record({
            "topic": "Work", "entities": [{"name": "Acme Corp"}], "message_ids": [1, 2, 3]
        })]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = tq.get_hot_topic_context_with_messages(["Work"])
        assert "Work" in result
        assert result["Work"]["message_ids"] == [1, 2, 3]

    def test_get_hot_topic_context_exception(self, tools):
        tq, session = tools
        session.run.side_effect = Exception("db error")
        assert tq.get_hot_topic_context_with_messages(["Work"]) == {}

    def test_search_entity_happy(self, tools):
        tq, session = tools
        records = [make_record({
            "id": 1, "canonical_name": "Alice", "type": "person",
            "topic": "General", "facts": [], "aliases": [],
            "conn_name": "Bob", "conn_aliases": [], "conn_weight": 5,
            "evidence_ids": [], "conn_context": "friends",
            "conn_facts": [], "parent_name": None, "children_count": 0,
            "last_mentioned": 1000, "last_updated": 1000
        })]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = tq.search_entity("Alice")
        assert len(result) == 1
        assert result[0]["canonical_name"] == "Alice"

    def test_search_entity_empty(self, tools):
        tq, session = tools
        session.run.return_value.__iter__ = lambda self: iter([])
        assert tq.search_entity("Alice") == []

    def test_search_entity_exception(self, tools):
        tq, session = tools
        session.run.side_effect = Exception("db error")
        assert tq.search_entity("Alice") == []

    def test_get_recent_activity_happy(self, tools):
        tq, session = tools
        records = [make_record({
            "source": "A", "target": "B", "target_facts": [],
            "connection_strength": 1, "evidence_ids": [],
            "confidence": 1.0, "last_seen": 1000, "context": ""
        })]
        session.run.return_value.__iter__ = lambda self: iter(records)
        result = tq.get_recent_activity("Alice")
        assert len(result) == 1
        assert result[0]["source"] == "A"

    def test_get_recent_activity_empty(self, tools):
        tq, session = tools
        session.run.return_value.__iter__ = lambda self: iter([])
        assert tq.get_recent_activity("Alice") == []

    def test_get_recent_activity_exception(self, tools):
        tq, session = tools
        session.run.side_effect = Exception("db error")
        assert tq.get_recent_activity("Alice") == []

    def test_find_shortest_path_found(self, tools):
        tq, session = tools
        record = make_record({
            "names": ["Alice", "Bob"],
            "node_topics": ["General", "General"],
            "evidence_ids": [[], []],
            "has_inactive": False
        })
        session.run.return_value.single.return_value = record
        result = tq._find_shortest_path("Alice", "Bob")
        assert result is not None
        assert result[0] == ["Alice", "Bob"]

    def test_find_shortest_path_not_found(self, tools):
        tq, session = tools
        session.run.return_value.single.return_value = None
        assert tq._find_shortest_path("Alice", "Bob") is None

    def test_find_active_only_path_found(self, tools):
        tq, session = tools
        record = make_record({
            "names": ["Alice", "Bob"],
            "node_topics": ["General", "General"],
            "evidence_ids": [[], []]
        })
        session.run.return_value.single.return_value = record
        result = tq._find_active_only_path("Alice", "Bob", ["General"])
        assert result is not None
        assert result[0] == ["Alice", "Bob"]

    def test_find_active_only_path_not_found(self, tools):
        tq, session = tools
        session.run.return_value.single.return_value = None
        assert tq._find_active_only_path("Alice", "Bob", ["General"]) is None

    def test_find_path_filtered_with_active_topics(self, tools):
        tq, session = tools
        tq._find_shortest_path = MagicMock(return_value=(["A", "B"], ["T1", "T2"], [[], []], True))
        tq._find_active_only_path = MagicMock(return_value=(["A", "B"], ["T1", "T2"], [[], []]))
        tq._build_path_data = MagicMock(return_value=[{"step": 0}])

        result, is_hidden = tq._find_path_filtered("A", "B", ["T1"])
        assert is_hidden is True
        assert len(result) == 1
        tq._find_shortest_path.assert_called_once()
        tq._find_active_only_path.assert_called_once()

    def test_find_path_filtered_fallback_to_shortest(self, tools):
        tq, session = tools
        tq._find_shortest_path = MagicMock(return_value=(["A", "B"], ["T1", "T2"], [[], []], True))
        tq._find_active_only_path = MagicMock(return_value=None)
        tq._build_path_data = MagicMock(return_value=[{"step": 0}])

        result, is_hidden = tq._find_path_filtered("A", "B", ["T1"])
        assert is_hidden is True
        assert len(result) == 0
        tq._find_shortest_path.assert_called_once()
        tq._find_active_only_path.assert_called_once()

    def test_find_path_filtered_no_active_topics(self, tools):
        tq, session = tools
        tq._find_shortest_path = MagicMock(return_value=(["A", "B"], ["T1", "T2"], [[], []], False))
        tq._find_active_only_path = MagicMock()
        tq._build_path_data = MagicMock(return_value=[{"step": 0}])

        result, is_hidden = tq._find_path_filtered("A", "B", ["T1"])
        assert is_hidden is False
        assert len(result) == 1
        tq._find_shortest_path.assert_called_once()
        tq._find_active_only_path.assert_not_called()


class TestMemGraphStore:

    @patch("src.db.store.GraphDatabase")
    def test_verify_conn_retries(self, mock_gdb):
        mock_driver = MagicMock()
        mock_gdb.driver.return_value = mock_driver
        mock_driver.verify_connectivity.side_effect = [
            Exception("not ready"), Exception("not ready"), None,
        ]
        with patch("src.db.store.time.sleep"):
            MemGraphStore(uri="bolt://localhost:7687")
        assert mock_driver.verify_connectivity.call_count == 3

    @patch("src.db.store.GraphDatabase")
    def test_verify_conn_sleep_calls(self, mock_gdb):
        mock_driver = MagicMock()
        mock_gdb.driver.return_value = mock_driver
        mock_driver.verify_connectivity.side_effect = [
            Exception("not ready"), Exception("not ready"), None,
        ]
        with patch("src.db.store.time.sleep") as mock_sleep:
            MemGraphStore(uri="bolt://localhost:7687")
        assert mock_sleep.call_count == 2
        mock_sleep.assert_has_calls([call(2), call(2)])

    @patch("src.db.store.GraphDatabase")
    def test_verify_conn_exhausted(self, mock_gdb):
        mock_driver = MagicMock()
        mock_gdb.driver.return_value = mock_driver
        mock_driver.verify_connectivity.side_effect = Exception("permanently down")
        with patch("src.db.store.time.sleep"):
            with pytest.raises(Exception, match="permanently down"):
                MemGraphStore(uri="bolt://localhost:7687")
        assert mock_driver.verify_connectivity.call_count == 5

    @patch("src.db.store.GraphDatabase")
    def test_setup_schema_swallows_errors(self, mock_gdb):
        mock_driver = MagicMock()
        mock_gdb.driver.return_value = mock_driver
        schema_session = MagicMock()
        schema_session.run.side_effect = Exception("index already exists")
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=schema_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)
        store = MemGraphStore(uri="bolt://localhost:7687")
        assert store._reader is not None
        assert store._writer is not None

    @patch("src.db.store.GraphDatabase")
    def test_close(self, mock_gdb):
        mock_driver = MagicMock()
        mock_gdb.driver.return_value = mock_driver
        store = MemGraphStore(uri="bolt://localhost:7687")
        store.close()
        mock_driver.close.assert_called_once()

    @patch("src.db.store.GraphDatabase")
    def test_close_idempotent(self, mock_gdb):
        mock_driver = MagicMock()
        mock_gdb.driver.return_value = mock_driver
        store = MemGraphStore(uri="bolt://localhost:7687")
        store.close()
        store.close()
        assert mock_driver.close.call_count == 2

    @patch("src.db.store.GraphDatabase")
    def test_delegations(self, mock_gdb):
        mock_driver = MagicMock()
        mock_gdb.driver.return_value = mock_driver
        store = MemGraphStore(uri="bolt://localhost:7687")

        store._reader = MagicMock()
        store.get_max_entity_id()
        store._reader.get_max_entity_id.assert_called_once()
        store.get_message_text(1)
        store._reader.get_message_text.assert_called_once_with(1)

        store._writer = MagicMock()
        store.save_message_logs([])
        store._writer.save_message_logs.assert_called_once_with([])
        store.create_facts_batch(1, [])
        store._writer.create_facts_batch.assert_called_once_with(1, [])
        store.merge_entities(1, 2)
        store._writer.merge_entities.assert_called_once_with(1, 2)

        store._tools = MagicMock()
        store.search_messages_fts("test")
        store._tools.search_messages_fts.assert_called_once_with("test", 50)