"""Tests for shared/models/schema/dtypes.py.

This module contains unit tests for the core data types used across the system,
ensuring correct serialization, de-serialization, and default behavior.
"""

import json
from datetime import datetime, timezone

import numpy as np
import pytest

from shared.models.schema.dtypes import (
    BatchResult,
    DLQEntry,
    EntityPair,
    Fact,
    MessageConnections,
    MessageData,
)


class TestFact:
    """Tests for the Fact data type."""

    def test_from_record_full(self):
        """Verify Fact instantiation from a complete database record."""
        record = {
            "id": "fact_abc123",
            "source_entity_id": 42,
            "content": "Works at Anthropic",
            "valid_at": "2025-01-15T10:30:00+00:00",
            "invalid_at": "2025-06-01T00:00:00+00:00",
            "confidence": 0.95,
            "embedding": [0.1, 0.2, 0.3],
            "source_msg_id": 7,
            "source": "extraction",
        }
        fact = Fact.from_record(record)

        assert fact.id == "fact_abc123"
        assert fact.source_entity_id == 42
        assert fact.content == "Works at Anthropic"
        assert fact.confidence == 0.95
        assert fact.embedding == [0.1, 0.2, 0.3]
        assert fact.source_msg_id == 7
        assert fact.source == "extraction"
        assert isinstance(fact.valid_at, datetime)
        assert isinstance(fact.invalid_at, datetime)

    def test_from_record_minimal(self):
        """Missing optional fields should fall back to defaults during Fact instantiation."""
        record = {
            "id": "fact_min",
            "source_entity_id": 1,
            "content": "Primary user",
            "valid_at": "2025-01-01T00:00:00+00:00",
        }
        fact = Fact.from_record(record)

        assert fact.invalid_at is None
        assert fact.confidence == 1.0
        assert fact.embedding == []
        assert fact.source_msg_id is None
        assert fact.source == "user"

    def test_parse_dt_string(self):
        """Verify correct parsing of ISO-8601 datetime strings."""
        iso = "2025-03-08T14:30:00+00:00"
        result = Fact._parse_dt(iso)
        assert isinstance(result, datetime)
        assert result.year == 2025
        assert result.month == 3

    def test_parse_dt_datetime_passthrough(self):
        """Ensure datetime objects are passed through unchanged."""
        dt = datetime(2025, 6, 1, tzinfo=timezone.utc)
        assert Fact._parse_dt(dt) is dt


class TestBatchResult:
    """Tests for the BatchResult data type."""

    def test_defaults(self):
        """Verify default values for a new BatchResult instance."""
        br = BatchResult()
        assert br.entity_ids == []
        assert br.new_entity_ids == set()
        assert br.alias_updated_ids == set()
        assert br.alias_updates == {}
        assert br.extraction_result is None
        assert br.message_embeddings == {}
        assert br.success is True
        assert br.error is None

    def test_round_trip(self):
        """Full to_dict -> JSON -> from_dict round-trip should preserve all fields and types."""
        original = BatchResult(
            entity_ids=[1, 2, 3],
            new_entity_ids={2, 3},
            alias_updated_ids={1},
            alias_updates={1: ["Bob", "Robert"]},
            extraction_result=[
                MessageConnections(
                    message_id=10,
                    entity_pairs=[
                        EntityPair(
                            entity_a="Alice",
                            entity_b="Bob",
                            confidence=0.9,
                            context="colleagues",
                        )
                    ],
                )
            ],
            message_embeddings={10: [0.1, 0.2], 11: [0.3, 0.4]},
            success=True,
            error=None,
        )

        serialized = json.dumps(original.to_dict())
        restored = BatchResult.from_dict(json.loads(serialized))

        assert restored.entity_ids == [1, 2, 3]
        assert restored.new_entity_ids == {2, 3}
        assert isinstance(restored.new_entity_ids, set)
        assert restored.alias_updated_ids == {1}
        assert restored.alias_updates == {1: ["Bob", "Robert"]}
        assert isinstance(list(restored.alias_updates.keys())[0], int)

        # message_embeddings keys must survive as int after JSON round-trip
        assert 10 in restored.message_embeddings
        assert 11 in restored.message_embeddings
        assert isinstance(list(restored.message_embeddings.keys())[0], int)

        mc = restored.extraction_result[0]
        assert mc.message_id == 10
        assert mc.entity_pairs[0].entity_a == "Alice"
        assert mc.entity_pairs[0].confidence == 0.9
        assert mc.entity_pairs[0].context == "colleagues"

    def test_round_trip_empty(self):
        """Empty BatchResult round-trip should preserve essential defaults."""
        original = BatchResult()
        serialized = json.dumps(original.to_dict())
        restored = BatchResult.from_dict(json.loads(serialized))

        assert restored.entity_ids == []
        assert restored.extraction_result is None
        assert restored.success is True

    def test_to_dict_numpy_embeddings(self):
        """Numpy arrays in message_embeddings must be converted to lists for JSON serialization."""
        br = BatchResult(
            message_embeddings={1: np.array([0.5, 0.6, 0.7])}
        )
        d = br.to_dict()

        assert isinstance(d["message_embeddings"][1], list)
        assert d["message_embeddings"][1] == pytest.approx([0.5, 0.6, 0.7])
        # Must be JSON-serializable (numpy arrays aren't)
        json.dumps(d)


class TestDLQEntry:
    """Tests for the DLQEntry data type."""

    def test_round_trip(self):
        """Verify to_json -> from_json round-trip for DLQEntry."""
        entry = DLQEntry(
            messages=[{"id": 1, "message": "hello"}],
            session_text="[USER]: hello",
            error="TIMEOUT",
            attempt=2,
        )
        restored = DLQEntry.from_json(entry.to_json())

        assert restored.messages == [{"id": 1, "message": "hello"}]
        assert restored.session_text == "[USER]: hello"
        assert restored.error == "TIMEOUT"
        assert restored.attempt == 2
        assert restored.batch_size == 1

    def test_is_transient(self):
        """Verify detection of transient errors in DLQ entries."""
        entry = DLQEntry(
            messages=[],
            session_text="",
            error="Connection refused: TIMEOUT after 30s",
        )
        assert entry.is_transient(["TIMEOUT", "rate_limit"]) is True
        assert entry.is_transient(["PERMANENT_FAIL"]) is False


class TestMessageData:
    """Tests for the MessageData data type."""

    def test_defaults(self):
        """Verify default field values for a new MessageData instance."""
        md = MessageData(message="test input")
        assert md.message == "test input"
        assert md.id == -1
        assert isinstance(md.timestamp, datetime)
        assert md.timestamp.tzinfo is not None