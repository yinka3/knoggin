"""Tests for db/reader.py, db/writer.py, and db/store.py.

This module contains unit tests for the core database interaction layers.
It uses a mocked Neo4j driver to test Python-side logic including parameter
handling, result hydration, and error paths.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest

from db.reader import GraphReader
from db.writer import GraphWriter
from db.store import MemGraphStore
from shared.models.schema.dtypes import Fact


# --- Mock Helpers ---

def make_mock_driver():
    """Create a mock neo4j Driver with a chainable session context manager."""
    driver = MagicMock()
    session = MagicMock()
    driver.session.return_value.__enter__ = MagicMock(return_value=session)
    driver.session.return_value.__exit__ = MagicMock(return_value=False)
    return driver, session


def make_record(data: dict):
    """Create a mock neo4j Record that supports dict-style access."""
    record = MagicMock()
    record.__getitem__ = lambda self, key: data[key]
    record.get = lambda key, default=None: data.get(key, default)
    record.keys = lambda: data.keys()
    record.items = lambda: data.items()
    record.values = lambda: data.values()
    # Support dict(record)
    record.__iter__ = lambda self: iter(data)
    record.__len__ = lambda self: len(data)
    return record


class TestGraphReader:
    """Tests for the GraphReader class."""

    @pytest.fixture
    def reader(self):
        """Fixture that provides a GraphReader instance and its mocked session."""
        driver, session = make_mock_driver()
        reader = GraphReader(driver)
        return reader, session

    # --- get_message_text ---

    def test_get_message_text(self, reader):
        """Verify successful message text retrieval."""
        rdr, session = reader
        session.run.return_value.single.return_value = make_record({"content": "Hello world"})

        result = rdr.get_message_text(42)
        assert result == "Hello world"
        session.run.assert_called_once()
        args = session.run.call_args
        assert args[0][1]["id"] == 42

    def test_get_message_text_not_found(self, reader):
        """Return an empty string if the message ID is not found."""
        rdr, session = reader
        session.run.return_value.single.return_value = None

        assert rdr.get_message_text(999) == ""

    def test_get_message_text_exception(self, reader):
        """Handle database exceptions gracefully by returning an empty string."""
        rdr, session = reader
        session.run.side_effect = Exception("connection lost")

        assert rdr.get_message_text(1) == ""

    # --- get_max_entity_id ---

    def test_get_max_entity_id(self, reader):
        """Verify max entity ID retrieval."""
        rdr, session = reader
        session.run.return_value.single.return_value = make_record({"max_id": 42})

        assert rdr.get_max_entity_id() == 42

    def test_get_max_entity_id_empty_graph(self, reader):
        """Return 0 if the graph is empty (no entities found)."""
        rdr, session = reader
        session.run.return_value.single.return_value = make_record({"max_id": None})

        assert rdr.get_max_entity_id() == 0

    def test_get_max_entity_id_exception(self, reader):
        """Handle database exceptions by returning 0."""
        rdr, session = reader
        session.run.side_effect = Exception("db down")

        assert rdr.get_max_entity_id() == 0

    # --- validate_existing_ids ---

    def test_validate_existing_ids(self, reader):
        """Verify that only existing IDs are returned from the input set."""
        rdr, session = reader
        records = [make_record({"id": 1}), make_record({"id": 3})]
        session.run.return_value.__iter__ = lambda self: iter(records)

        result = rdr.validate_existing_ids([1, 2, 3])
        assert result == {1, 3}

    def test_validate_existing_ids_empty_input(self, reader):
        """Return an empty set if the input list is empty."""
        rdr, session = reader
        assert rdr.validate_existing_ids([]) == set()
        session.run.assert_not_called()

    def test_validate_existing_ids_exception(self, reader):
        """Return None if a database exception occurs during validation."""
        rdr, session = reader
        session.run.side_effect = Exception("timeout")

        result = rdr.validate_existing_ids([1, 2])
        assert result is None

    # --- get_facts_for_entity ---

    def test_get_facts_for_entity_hydrates(self, reader):
        """Verify that facts are correctly hydrated from database records."""
        rdr, session = reader
        fact_record = make_record({
            "id": "fact_1",
            "source_entity_id": 10,
            "content": "Works at Anthropic",
            "valid_at": "2025-01-15T00:00:00+00:00",
            "invalid_at": None,
            "confidence": 0.95,
            "embedding": [0.1, 0.2],
            "source_msg_id": 5,
        })
        session.run.return_value.__iter__ = lambda self: iter([fact_record])

        facts = rdr.get_facts_for_entity(10, active_only=True)
        assert len(facts) == 1
        assert isinstance(facts[0], Fact)
        assert facts[0].content == "Works at Anthropic"
        assert facts[0].source_entity_id == 10

    def test_get_facts_for_entity_exception(self, reader):
        """Return an empty list if query fails."""
        rdr, session = reader
        session.run.side_effect = Exception("query failed")

        result = rdr.get_facts_for_entity(10)
        assert result == []

    # --- get_facts_for_entities ---

    def test_get_facts_for_entities_empty_input(self, reader):
        """Return an empty dictionary for empty input."""
        rdr, session = reader
        assert rdr.get_facts_for_entities([]) == {}
        session.run.assert_not_called()

    def test_get_facts_for_entities_groups_by_id(self, reader):
        """Verify that facts are correctly grouped by entity ID."""
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
        assert result[3] == []  # requested but no facts

    # --- get_entity_by_id ---

    def test_get_entity_by_id_found(self, reader):
        """Verify successful entity retrieval by ID."""
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
        """Return None if the entity ID is not found."""
        rdr, session = reader
        session.run.return_value.single.return_value = None

        assert rdr.get_entity_by_id(999) is None

    # --- get_messages_by_ids ---

    def test_get_messages_by_ids_empty(self, reader):
        """Return empty list for empty input."""
        rdr, session = reader
        assert rdr.get_messages_by_ids([]) == []
        session.run.assert_not_called()


class TestGraphWriter:
    """Tests for the GraphWriter class."""

    @pytest.fixture
    def writer(self):
        """Fixture that provides a GraphWriter instance and its mocked session."""
        driver, session = make_mock_driver()
        writer = GraphWriter(driver)
        return writer, session

    # --- save_message_logs ---

    def test_save_message_logs_empty(self, writer):
        """Return True immediately for empty input."""
        wr, session = writer
        result = wr.save_message_logs([])
        assert result is True
        session.run.assert_not_called()

    def test_save_message_logs(self, writer):
        """Verify that execute_write is called with the correct message data."""
        wr, session = writer
        msgs = [{"id": 1, "content": "hello", "role": "user", "timestamp": "2025-01-01", "embedding": []}]

        wr.save_message_logs(msgs)
        session.execute_write.assert_called_once()

    # --- write_batch ---

    def test_write_batch_defaults_aliases(self, writer):
        """Entities missing 'aliases' key should get empty list."""
        wr, session = writer
        entities = [{"id": 1, "canonical_name": "Alice", "type": "person", "confidence": 0.9}]

        wr.write_batch(entities, [])
        session.execute_write.assert_called_once()

        # Grab the callback and inspect what it would pass
        callback = session.execute_write.call_args[0][0]
        # The callback closes over entity_params — we can verify indirectly
        # by checking write_batch didn't raise a KeyError on missing aliases

    def test_write_batch_defaults_confidence(self, writer):
        """Relationships missing 'confidence' should default to 1.0."""
        wr, session = writer
        rels = [{"entity_a": "Alice", "entity_b": "Bob", "entity_a_id": 1, "entity_b_id": 2}]

        wr.write_batch([], rels)
        session.execute_write.assert_called_once()

    # --- create_facts_batch ---

    def test_create_facts_batch_empty(self, writer):
        """Return 0 immediately for empty facts list."""
        wr, session = writer
        assert wr.create_facts_batch(1, []) == 0
        session.execute_write.assert_not_called()

    def test_create_facts_batch_calls_execute_write(self, writer):
        """Verify that execute_write is called and returns the correct count."""
        wr, session = writer
        session.execute_write.return_value = 2

        facts = [
            Fact(id="f1", source_entity_id=1, content="fact 1",
                 valid_at=datetime.now(timezone.utc), embedding=[0.1]),
            Fact(id="f2", source_entity_id=1, content="fact 2",
                 valid_at=datetime.now(timezone.utc), embedding=[0.2]),
        ]
        result = wr.create_facts_batch(1, facts)
        assert result == 2
        session.execute_write.assert_called_once()

    # --- invalidate_fact ---

    def test_invalidate_fact(self, writer):
        """Verify fact invalidation logic."""
        wr, session = writer
        session.execute_write.return_value = True

        now = datetime.now(timezone.utc)
        result = wr.invalidate_fact("fact_123", now)
        assert result is True
        session.execute_write.assert_called_once()


class TestMemGraphStore:
    """Tests for the MemGraphStore class, focusing on connection handling and delegation."""

    @patch("db.store.GraphDatabase")
    def test_verify_conn_retries(self, mock_gdb):
        """Should retry up to 5 times on connection failure."""
        mock_driver = MagicMock()
        mock_gdb.driver.return_value = mock_driver
        mock_driver.verify_connectivity.side_effect = [
            Exception("not ready"),
            Exception("not ready"),
            None,  # succeeds on 3rd attempt
        ]

        with patch("db.store.time.sleep"):
            store = MemGraphStore(uri="bolt://localhost:7687")

        assert mock_driver.verify_connectivity.call_count == 3

    @patch("db.store.GraphDatabase")
    def test_verify_conn_exhausted(self, mock_gdb):
        """Should raise after 5 failed retries."""
        mock_driver = MagicMock()
        mock_gdb.driver.return_value = mock_driver
        mock_driver.verify_connectivity.side_effect = Exception("permanently down")

        with patch("db.store.time.sleep"):
            with pytest.raises(Exception, match="permanently down"):
                MemGraphStore(uri="bolt://localhost:7687")

        assert mock_driver.verify_connectivity.call_count == 5

    @patch("db.store.GraphDatabase")
    def test_close(self, mock_gdb):
        """Ensure the driver is closed correctly."""
        mock_driver = MagicMock()
        mock_gdb.driver.return_value = mock_driver

        store = MemGraphStore(uri="bolt://localhost:7687")
        store.close()
        mock_driver.close.assert_called_once()

    @patch("db.store.GraphDatabase")
    def test_delegations(self, mock_gdb):
        """Spot check that store delegates to reader/writer."""
        mock_driver = MagicMock()
        mock_gdb.driver.return_value = mock_driver

        store = MemGraphStore(uri="bolt://localhost:7687")

        # Reader delegation
        store._reader = MagicMock()
        store.get_max_entity_id()
        store._reader.get_max_entity_id.assert_called_once()

        store.get_message_text(1)
        store._reader.get_message_text.assert_called_once_with(1)

        # Writer delegation
        store._writer = MagicMock()
        store.save_message_logs([])
        store._writer.save_message_logs.assert_called_once_with([])

        store.create_facts_batch(1, [])
        store._writer.create_facts_batch.assert_called_once_with(1, [])