"""Tests for src/common/schema/dtypes.py — core data types, serialization, and defaults."""

import json
from datetime import datetime, timezone

import numpy as np
import pytest

from src.common.schema.dtypes import (
    AgentConfig,
    BatchResult,
    DLQEntry,
    EntityItem,
    EntityPair,
    Fact,
    FactMergeResult,
    MessageConnections,
    MessageData,
    ProfileUpdate,
    ResolutionResult,
    BaseResult,
    CompleteResult,
    ClarificationResult,
    ToolCall,
    FinalResponse,
    ClarificationRequest,
)


class TestFact:

    def test_from_record_full(self):
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
        iso = "2025-03-08T14:30:00+00:00"
        result = Fact._parse_dt(iso)
        assert isinstance(result, datetime)
        assert result.year == 2025
        assert result.month == 3

    def test_parse_dt_datetime_passthrough(self):
        dt = datetime(2025, 6, 1, tzinfo=timezone.utc)
        assert Fact._parse_dt(dt) is dt

    def test_parse_dt_none_returns_none(self):
        assert Fact._parse_dt(None) is None

    def test_from_record_invalid_valid_at_raises(self):
        record = {
            "id": "f_bad",
            "source_entity_id": 1,
            "content": "test",
            "valid_at": "not-a-date",
        }
        with pytest.raises(ValueError):
            Fact.from_record(record)

    def test_from_record_embedding_none_becomes_empty_list(self):
        record = {
            "id": "f_emb",
            "source_entity_id": 1,
            "content": "test",
            "valid_at": "2025-01-01T00:00:00+00:00",
            "embedding": None,
        }
        fact = Fact.from_record(record)
        assert fact.embedding == []

    def test_from_record_empty_list_embedding_stays_empty(self):
        record = {
            "id": "f_emb2",
            "source_entity_id": 1,
            "content": "test",
            "valid_at": "2025-01-01T00:00:00+00:00",
            "embedding": [],
        }
        fact = Fact.from_record(record)
        assert fact.embedding == []


class TestFactToDict:

    @pytest.fixture
    def sample_fact(self):
        return Fact(
            id="fact_td1",
            source_entity_id=10,
            content="Works at Anthropic",
            valid_at=datetime(2025, 3, 1, tzinfo=timezone.utc),
            invalid_at=datetime(2025, 6, 1, tzinfo=timezone.utc),
            confidence=0.95,
            embedding=[0.1, 0.2, 0.3],
            source_msg_id=7,
            source="extraction",
        )

    def test_default_excludes_embedding(self, sample_fact):
        d = sample_fact.to_dict()
        assert "embedding" not in d
        assert d["id"] == "fact_td1"
        assert d["content"] == "Works at Anthropic"
        assert d["confidence"] == 0.95
        assert d["source_msg_id"] == 7
        assert d["source"] == "extraction"

    def test_datetimes_become_iso_strings(self, sample_fact):
        d = sample_fact.to_dict()
        assert isinstance(d["valid_at"], str)
        assert isinstance(d["invalid_at"], str)
        datetime.fromisoformat(d["valid_at"])
        datetime.fromisoformat(d["invalid_at"])

    def test_custom_exclude(self, sample_fact):
        d = sample_fact.to_dict(exclude={"confidence", "source"})
        assert "confidence" not in d
        assert "source" not in d
        assert "embedding" in d
        assert d["embedding"] == [0.1, 0.2, 0.3]

    def test_empty_exclude_includes_everything(self, sample_fact):
        d = sample_fact.to_dict(exclude=set())
        assert "embedding" in d
        assert d["embedding"] == [0.1, 0.2, 0.3]
        for field_name in Fact.__dataclass_fields__:
            assert field_name in d

    def test_none_invalid_at_serializes_as_none(self):
        fact = Fact(
            id="f_none", source_entity_id=1, content="test",
            valid_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        d = fact.to_dict()
        assert d["invalid_at"] is None


class TestBatchResult:

    def test_defaults(self):
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
        original = BatchResult()
        serialized = json.dumps(original.to_dict())
        restored = BatchResult.from_dict(json.loads(serialized))

        assert restored.entity_ids == []
        assert restored.extraction_result is None
        assert restored.success is True

    def test_to_dict_numpy_embeddings(self):
        br = BatchResult(
            message_embeddings={1: np.array([0.5, 0.6, 0.7])}
        )
        d = br.to_dict()

        assert isinstance(d["message_embeddings"][1], list)
        assert d["message_embeddings"][1] == pytest.approx([0.5, 0.6, 0.7])
        json.dumps(d)  # must be JSON-serializable (numpy arrays aren't)

    def test_from_dict_sparse_input(self):
        sparse = {"entity_ids": [1, 2], "success": False}
        result = BatchResult.from_dict(sparse)

        assert result.entity_ids == [1, 2]
        assert result.success is False
        assert result.new_entity_ids == set()
        assert result.alias_updated_ids == set()
        assert result.alias_updates == {}
        assert result.extraction_result is None
        assert result.message_embeddings == {}
        assert result.error is None

    def test_to_dict_none_extraction_result(self):
        br = BatchResult(extraction_result=None)
        d = br.to_dict()
        assert d["extraction_result"] == []

    def test_to_dict_keys_are_original_types(self):
        br = BatchResult(message_embeddings={10: [0.1], 20: [0.2]})
        d = br.to_dict()
        assert all(isinstance(k, int) for k in d["message_embeddings"])

    def test_to_dict_alias_updates_keys_become_strings(self):
        br = BatchResult(alias_updates={1: ["Alice", "Al"], 2: ["Bob"]})
        d = br.to_dict()
        assert all(isinstance(k, str) for k in d["alias_updates"])

    def test_from_dict_completely_empty(self):
        result = BatchResult.from_dict({})
        assert result.entity_ids == []
        assert result.success is True
        assert result.extraction_result is None


class TestDLQEntry:

    def test_round_trip(self):
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
        entry = DLQEntry(
            messages=[],
            session_text="",
            error="Connection refused: TIMEOUT after 30s",
        )
        assert entry.is_transient(["TIMEOUT", "rate_limit"]) is True
        assert entry.is_transient(["PERMANENT_FAIL"]) is False

    def test_batch_size_computed_on_init(self):
        entry = DLQEntry(
            messages=[{"id": 1}, {"id": 2}, {"id": 3}],
            session_text="test",
            error="ERR",
        )
        assert entry.batch_size == 3

    def test_batch_size_zero_for_empty_messages(self):
        entry = DLQEntry(messages=[], session_text="", error="ERR")
        assert entry.batch_size == 0

    def test_from_json_extra_keys_raises(self):
        payload = json.dumps({
            "messages": [],
            "session_text": "",
            "error": "ERR",
            "attempt": 1,
            "timestamp": 1000.0,
            "batch_size": 0,
            "version": 2,
        })
        with pytest.raises(TypeError):
            DLQEntry.from_json(payload)

    def test_is_transient_empty_error_string(self):
        entry = DLQEntry(messages=[], session_text="", error="")
        assert entry.is_transient(["TIMEOUT", "rate_limit"]) is False

    def test_is_transient_empty_transient_list(self):
        entry = DLQEntry(messages=[], session_text="", error="TIMEOUT occurred")
        assert entry.is_transient([]) is False

    def test_is_transient_case_sensitive(self):
        entry = DLQEntry(messages=[], session_text="", error="timeout after 30s")
        assert entry.is_transient(["TIMEOUT"]) is False
        assert entry.is_transient(["timeout"]) is True


class TestMessageData:

    def test_defaults(self):
        md = MessageData(message="test input")
        assert md.message == "test input"
        assert md.id == -1
        assert isinstance(md.timestamp, datetime)
        assert md.timestamp.tzinfo is not None


class TestAgentConfig:

    def test_defaults(self):
        cfg = AgentConfig(id="a1", name="Test", persona="helpful")
        assert cfg.temperature == 0.7
        assert cfg.is_default is False
        assert cfg.is_spawned is False
        assert cfg.spawned_by is None
        assert cfg.instructions is None
        assert cfg.model is None
        assert cfg.enabled_tools is None
        assert isinstance(cfg.created_at, datetime)
        assert cfg.created_at.tzinfo is not None

    def test_round_trip(self):
        original = AgentConfig(
            id="agent_1",
            name="Research Bot",
            persona="You are a research assistant.",
            instructions="Focus on accuracy.",
            model="gemini-2.5-flash",
            temperature=0.3,
            enabled_tools=["search", "graph_query"],
            is_default=True,
            is_spawned=True,
            spawned_by="agent_0",
        )
        restored = AgentConfig.from_dict(original.to_dict())

        assert restored.id == original.id
        assert restored.name == original.name
        assert restored.persona == original.persona
        assert restored.instructions == original.instructions
        assert restored.model == original.model
        assert restored.temperature == 0.3
        assert restored.enabled_tools == ["search", "graph_query"]
        assert restored.is_default is True
        assert restored.is_spawned is True
        assert restored.spawned_by == "agent_0"
        assert restored.created_at.year == original.created_at.year

    def test_from_dict_created_at_as_string(self):
        data = {
            "id": "a2", "name": "Bot", "persona": "test",
            "created_at": "2025-06-15T12:00:00+00:00",
        }
        cfg = AgentConfig.from_dict(data)
        assert isinstance(cfg.created_at, datetime)
        assert cfg.created_at.year == 2025
        assert cfg.created_at.month == 6

    def test_from_dict_created_at_none_uses_now(self):
        data = {"id": "a3", "name": "Bot", "persona": "test"}
        cfg = AgentConfig.from_dict(data)
        assert isinstance(cfg.created_at, datetime)
        delta = (datetime.now(timezone.utc) - cfg.created_at).total_seconds()
        assert delta < 2

    def test_from_dict_created_at_as_datetime(self):
        dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        data = {"id": "a4", "name": "Bot", "persona": "test", "created_at": dt}
        cfg = AgentConfig.from_dict(data)
        assert cfg.created_at is dt

    def test_to_dict_created_at_is_iso_string(self):
        cfg = AgentConfig(id="a5", name="Bot", persona="test")
        d = cfg.to_dict()
        assert isinstance(d["created_at"], str)
        datetime.fromisoformat(d["created_at"])


class TestSimpleDataclasses:

    def test_entity_item(self):
        ei = EntityItem(msg_id=3, name="Alice", label="person", topic="Identity", confidence=0.9)
        assert ei.msg_id == 3
        assert ei.name == "Alice"
        assert ei.label == "person"
        assert ei.topic == "Identity"
        assert ei.confidence == 0.9

    def test_profile_update(self):
        pu = ProfileUpdate(canonical_name="Alice", facts=["Works at Anthropic", "Lives in SF"])
        assert pu.canonical_name == "Alice"
        assert len(pu.facts) == 2

    def test_fact_merge_result(self):
        fmr = FactMergeResult(to_invalidate=["f1", "f2"], new_contents=["merged fact"])
        assert len(fmr.to_invalidate) == 2
        assert fmr.new_contents == ["merged fact"]


class TestResolutionResult:

    def test_instantiation(self):
        rr = ResolutionResult(
            entity_ids=[1, 2],
            new_ids={2},
            alias_ids={1},
            entity_msg_map={1: [10], 2: [11]},
            alias_updates={1: ["Bob"]}
        )
        assert rr.entity_ids == [1, 2]
        assert rr.new_ids == {2}
        assert rr.alias_updates[1] == ["Bob"]
        assert rr.entity_msg_map[1] == [10]


class TestAgentResponseTypes:

    def test_base_result(self):
        br = BaseResult(status="success", state="done", tools_used=["search"])
        assert br.status == "success"
        assert br.tools_used == ["search"]

    def test_complete_result(self):
        cr = CompleteResult(
            status="success", state="done", tools_used=[],
            response="All good", messages=[], profiles=[], graph=[]
        )
        assert cr.response == "All good"

    def test_clarification_result(self):
        clr = ClarificationResult(
            status="pending", state="waiting", tools_used=[],
            question="What is your name?"
        )
        assert clr.question == "What is your name?"

    def test_tool_call(self):
        tc = ToolCall(name="get_weather", args={"loc": "NY"}, thinking="Need weather")
        assert tc.name == "get_weather"
        assert tc.args == {"loc": "NY"}

    def test_final_response(self):
        fr = FinalResponse(content="It's sunny.", usage={"tokens": 10}, sources=[])
        assert fr.content == "It's sunny."

    def test_clarification_request(self):
        cr = ClarificationRequest(question="Are you sure?", usage=None)
        assert cr.question == "Are you sure?"