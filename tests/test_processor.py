"""
Tests for main/processor.py.

This module validates the BatchProcessor orchestration logic.
All dependencies are mocked, including Memgraph, Redis, and LLM services.
"""

import json
import numpy as np
import pytest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, AsyncMock, patch

from main.processor import BatchProcessor, _safe_json
from shared.config.topics_config import TopicConfig
from shared.models.schema.dtypes import (
    BatchResult,
    EntityPair,
    Fact,
    MessageConnections,
)
from datetime import datetime, timezone


# --- Helpers ---

FAKE_EMBEDDING = [0.1] * 1024


def make_processor(**overrides):
    """Build a BatchProcessor with fully mocked dependencies."""
    store = MagicMock()
    store.get_facts_for_entity.return_value = []
    store.get_neighbor_ids.return_value = set()
    store.validate_existing_ids.return_value = set()

    llm = MagicMock()
    llm.call_llm = AsyncMock(return_value=None)

    resolver = MagicMock()
    resolver.get_id.return_value = None
    resolver.compute_batch_embeddings.return_value = []
    resolver.get_candidate_ids.return_value = []
    resolver.validate_existing.return_value = (None, False, [])
    resolver.entity_profiles = {}
    resolver.embedding_service = MagicMock()
    resolver.embedding_service.encode = MagicMock(return_value=[FAKE_EMBEDDING])

    nlp = MagicMock()
    nlp.extract_mentions = AsyncMock(return_value=[])

    redis = MagicMock()
    redis.rpush = AsyncMock(return_value=True)

    tc = TopicConfig({
        "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
        "Identity": {"active": True, "labels": ["person"], "hierarchy": {}, "aliases": []},
        "Work": {"active": True, "labels": ["company", "project"], "hierarchy": {}, "aliases": ["career", "job"]},
    })

    defaults = dict(
        session_id="test-session",
        redis_client=redis,
        llm=llm,
        ent_resolver=resolver,
        nlp_pipe=nlp,
        store=store,
        cpu_executor=ThreadPoolExecutor(max_workers=2),
        user_name="Yinka",
        topic_config=tc,
        get_next_ent_id=AsyncMock(side_effect=iter(range(100, 200))),
    )
    defaults.update(overrides)

    proc = BatchProcessor(**defaults)
    return proc, store, llm, resolver, nlp, redis


MESSAGES = [
    {"id": 1, "message": "I just started at Palantir working on the Foundry platform", "role": "user"},
    {"id": 2, "message": "My manager Priya Sharma has been helping with onboarding", "role": "user"},
]


# --- Orchestration Tests ---

class TestRunOrchestration:
    """Validates the high-level orchestration logic of BatchProcessor.run()."""

    @pytest.mark.asyncio
    async def test_empty_messages(self):
        proc, *_ = make_processor()

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc.run([], "")

        assert result.success is True
        assert result.entity_ids == []
        assert result.extraction_result is None

    @pytest.mark.asyncio
    async def test_no_mentions_returns_embeddings_only(self):
        """When NLP finds no entities, we still compute message embeddings."""
        proc, _, _, resolver, nlp, _ = make_processor()
        nlp.extract_mentions = AsyncMock(return_value=[])
        resolver.compute_batch_embeddings.return_value = [FAKE_EMBEDDING, FAKE_EMBEDDING]

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc.run(MESSAGES, "[USER]: test")

        assert result.success is True
        assert len(result.message_embeddings) == 2
        assert 1 in result.message_embeddings
        assert 2 in result.message_embeddings
        assert result.entity_ids == []

    @pytest.mark.asyncio
    async def test_full_flow_chains_stages(self):
        """run() should chain: extract_mentions → resolve → connections."""
        proc, _, llm, resolver, nlp, _ = make_processor()

        # NLP returns mentions
        nlp.extract_mentions = AsyncMock(return_value=[
            (1, "Palantir", "company", "Work"),
            (2, "Priya Sharma", "person", "Identity"),
        ])
        resolver.compute_batch_embeddings.return_value = [FAKE_EMBEDDING, FAKE_EMBEDDING]

        # Resolution: both are new entities
        resolver.get_candidate_ids.return_value = []
        resolver.register_entity.return_value = FAKE_EMBEDDING

        # Connections: LLM returns one connection
        llm.call_llm = AsyncMock(return_value="""
        <connections>
        MSG 1 | Palantir; Priya Sharma | 0.85 | colleagues
        </connections>
        """)
        resolver.entity_profiles = {
            100: {"canonical_name": "Palantir", "type": "company"},
            101: {"canonical_name": "Priya Sharma", "type": "person"},
        }
        resolver.get_mentions_for_id.return_value = []

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc.run(MESSAGES, "[USER]: context")

        assert result.success is True
        assert len(result.entity_ids) >= 2
        assert len(result.new_entity_ids) >= 2
        assert result.extraction_result is not None

    @pytest.mark.asyncio
    async def test_connection_extraction_returns_none(self):
        """If VP-03 connection LLM returns None, result should flag failure."""
        proc, _, llm, resolver, nlp, _ = make_processor()

        nlp.extract_mentions = AsyncMock(return_value=[
            (1, "Palantir", "company", "Work"),
        ])
        resolver.compute_batch_embeddings.return_value = [FAKE_EMBEDDING, FAKE_EMBEDDING]
        resolver.get_candidate_ids.return_value = []
        resolver.register_entity.return_value = FAKE_EMBEDDING
        resolver.entity_profiles = {
            100: {"canonical_name": "Palantir", "type": "company"},
        }
        resolver.get_mentions_for_id.return_value = []

        # VP-03 returns None (LLM timeout)
        llm.call_llm = AsyncMock(return_value=None)

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc.run(MESSAGES, "[USER]: context")

        assert result.success is False
        assert "VP-03" in result.error

    @pytest.mark.asyncio
    async def test_exception_during_processing(self):
        """Unexpected exception should be caught, result.success=False."""
        proc, _, _, resolver, nlp, _ = make_processor()

        nlp.extract_mentions = AsyncMock(side_effect=RuntimeError("NLP crashed"))
        resolver.compute_batch_embeddings.return_value = [FAKE_EMBEDDING, FAKE_EMBEDDING]

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc.run(MESSAGES, "[USER]: context")

        assert result.success is False
        assert "NLP crashed" in result.error

    @pytest.mark.asyncio
    async def test_user_entity_appended(self):
        """User entity should be appended to entity_ids if not already present."""
        proc, _, llm, resolver, nlp, _ = make_processor()

        nlp.extract_mentions = AsyncMock(return_value=[
            (1, "Palantir", "company", "Work"),
        ])
        resolver.compute_batch_embeddings.return_value = [FAKE_EMBEDDING, FAKE_EMBEDDING]
        resolver.get_candidate_ids.return_value = []
        resolver.register_entity.return_value = FAKE_EMBEDDING
        resolver.entity_profiles = {
            100: {"canonical_name": "Palantir", "type": "company"},
        }
        resolver.get_mentions_for_id.return_value = []
        # User entity exists with id=1
        resolver.get_id.return_value = 1

        llm.call_llm = AsyncMock(return_value="<connections>\n</connections>")

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc.run(MESSAGES, "[USER]: context")

        # User id=1 should be in entity_ids even though NLP didn't extract it
        assert 1 in result.entity_ids

    @pytest.mark.asyncio
    async def test_embeddings_attached_to_messages(self):
        """Each message should have embedding attached after processing."""
        proc, _, _, resolver, nlp, _ = make_processor()

        nlp.extract_mentions = AsyncMock(return_value=[])
        emb_1 = [0.1, 0.2, 0.3]
        emb_2 = [0.4, 0.5, 0.6]
        resolver.compute_batch_embeddings.return_value = [emb_1, emb_2]

        msgs = [
            {"id": 10, "message": "first message", "role": "user"},
            {"id": 11, "message": "second message", "role": "user"},
        ]

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc.run(msgs, "")

        assert result.message_embeddings[10] == emb_1
        assert result.message_embeddings[11] == emb_2


# --- Mention Extraction and Normalization ---

class TestExtractMentionsNormalization:
    """Tests the normalization of extracted topics during mention processing."""

    @pytest.mark.asyncio
    async def test_normalizes_topics(self):
        proc, *_ = make_processor()
        proc.nlp.extract_mentions = AsyncMock(return_value=[
            (1, "Palantir", "company", "career"),  # alias for Work
            (2, "Priya", "person", "Identity"),
        ])

        result = await proc._extract_mentions(MESSAGES, "test-session")

        topics = [topic for _, _, _, topic in result]
        assert "Work" in topics  # "career" normalized to "Work"
        assert "Identity" in topics

    @pytest.mark.asyncio
    async def test_invalid_topic_filtered(self):
        """Mentions with unresolvable topics should be skipped."""
        proc, *_ = make_processor()
        # Deactivate General so unknown topics can't fall back
        proc.topic_config = TopicConfig({
            "General": {"active": False, "labels": [], "hierarchy": {}, "aliases": []},
            "Work": {"active": True, "labels": ["company"], "hierarchy": {}, "aliases": []},
        })
        proc.nlp.extract_mentions = AsyncMock(return_value=[
            (1, "Palantir", "company", "Work"),
            (2, "Something", "thing", "FakeTopic"),
        ])

        result = await proc._extract_mentions(MESSAGES, "test-session")

        names = [name for _, name, _, _ in result]
        assert "Palantir" in names
        assert "Something" not in names

    @pytest.mark.asyncio
    async def test_empty_topic_defaults_to_general(self):
        proc, *_ = make_processor()
        proc.nlp.extract_mentions = AsyncMock(return_value=[
            (1, "Palantir", "company", ""),
        ])

        result = await proc._extract_mentions(MESSAGES, "test-session")
        assert len(result) == 1
        assert result[0][3] == "General"


# --- Mention Resolution ---

class TestResolveMentions:
    """Tests the resolution logic for newly extracted mentions against existing entities."""

    @pytest.mark.asyncio
    async def test_new_entity_created(self):
        proc, _, _, resolver, _, _ = make_processor()
        resolver.get_candidate_ids.return_value = []
        resolver.register_entity.return_value = FAKE_EMBEDDING
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING]

        mentions = [(1, "Palantir", "company", "Work")]
        messages = [{"id": 1, "message": "Working at Palantir"}]

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc._resolve_mentions(mentions, messages)

        assert len(result.new_ids) == 1
        resolver.register_entity.assert_called_once()

    @pytest.mark.asyncio
    async def test_existing_entity_matched(self):
        proc, _, _, resolver, _, _ = make_processor()
        # Candidate above threshold
        resolver.get_candidate_ids.return_value = [(5, 0.95)]
        resolver.validate_existing.return_value = (5, False, [])
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING]

        mentions = [(1, "Palantir", "company", "Work")]
        messages = [{"id": 1, "message": "Working at Palantir"}]

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc._resolve_mentions(mentions, messages)

        assert 5 in result.entity_ids
        assert len(result.new_ids) == 0

    @pytest.mark.asyncio
    async def test_alias_added_on_match(self):
        proc, _, _, resolver, _, _ = make_processor()
        resolver.get_candidate_ids.return_value = [(5, 0.92)]
        resolver.validate_existing.return_value = (5, True, ["Palantir Tech"])
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING]

        mentions = [(1, "Palantir Tech", "company", "Work")]
        messages = [{"id": 1, "message": "Palantir Tech is great"}]

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc._resolve_mentions(mentions, messages)

        resolver.commit_new_aliases.assert_called_once_with(5, ["Palantir Tech"])
        assert 5 in result.alias_ids
        assert result.alias_updates[5] == ["Palantir Tech"]

    @pytest.mark.asyncio
    async def test_batch_dedup(self):
        """Same entity mentioned twice — should only register once."""
        proc, _, _, resolver, _, _ = make_processor()
        resolver.get_candidate_ids.return_value = []
        resolver.register_entity.return_value = FAKE_EMBEDDING
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING]

        mentions = [
            (1, "Palantir", "company", "Work"),
            (2, "Palantir", "company", "Work"),
        ]
        messages = [
            {"id": 1, "message": "Working at Palantir"},
            {"id": 2, "message": "Palantir is great"},
        ]

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc._resolve_mentions(mentions, messages)

        # register_entity called once, second mention deduped
        assert resolver.register_entity.call_count == 1
        # But both msg_ids should map to the entity
        ent_id = list(result.new_ids)[0]
        assert 1 in result.entity_msg_map[ent_id]
        assert 2 in result.entity_msg_map[ent_id]

    @pytest.mark.asyncio
    async def test_below_threshold_creates_new(self):
        proc, store, _, resolver, _, _ = make_processor()
        # Candidate below threshold
        resolver.get_candidate_ids.return_value = [(5, 0.70)]
        store.validate_existing_ids.return_value = {5}
        resolver.register_entity.return_value = FAKE_EMBEDDING
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING]

        mentions = [(1, "Palantir Tech", "company", "Work")]
        messages = [{"id": 1, "message": "Palantir Tech stuff"}]

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc._resolve_mentions(mentions, messages)

        assert len(result.new_ids) == 1
        # Should NOT match entity 5
        assert 5 not in result.entity_ids

    # NOTE: test_zombie_candidate_evicted removed — the zombie eviction branch
    # in _resolve_mentions is commented out (unreachable: _boost_candidates only
    # adds to scores). Restore this test when negative signals are implemented.

    @pytest.mark.asyncio
    async def test_register_entity_exception(self):
        """If register_entity throws, that entity is excluded but processing continues."""
        proc, _, _, resolver, _, _ = make_processor()
        resolver.get_candidate_ids.return_value = []
        resolver.register_entity.side_effect = RuntimeError("GPU OOM")
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING, FAKE_EMBEDDING]

        mentions = [
            (1, "Entity A", "thing", "General"),
            (2, "Entity B", "thing", "General"),
        ]
        messages = [
            {"id": 1, "message": "Entity A here"},
            {"id": 2, "message": "Entity B here"},
        ]

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc._resolve_mentions(mentions, messages)

        # Both failed to register — no entity_ids
        assert len(result.entity_ids) == 0
        assert len(result.new_ids) == 0


# --- Dead Letter Queue (DLQ) Management ---

class TestMoveToDLQ:
    """Tests the logic for moving failed message batches to the dead letter queue."""

    @pytest.mark.asyncio
    async def test_processing_stage_includes_session_text(self):
        proc, _, _, _, _, redis = make_processor()

        with patch("main.processor.emit", new_callable=AsyncMock):
            success = await proc.move_to_dead_letter(
                messages=[{"id": 1, "message": "test"}],
                error="NLP_CRASH",
                stage="processing",
                session_text="[USER]: test",
            )

        assert success is True
        call_args = redis.rpush.call_args[0]
        entry = json.loads(call_args[1])
        assert entry["stage"] == "processing"
        assert entry["session_text"] == "[USER]: test"
        assert "batch_result" not in entry

    @pytest.mark.asyncio
    async def test_graph_write_stage_includes_batch_result(self):
        proc, _, _, _, _, redis = make_processor()

        batch = BatchResult(
            entity_ids=[1, 2],
            extraction_result=[
                MessageConnections(message_id=1, entity_pairs=[
                    EntityPair(entity_a="A", entity_b="B", confidence=0.9)
                ])
            ],
        )

        with patch("main.processor.emit", new_callable=AsyncMock):
            success = await proc.move_to_dead_letter(
                messages=[{"id": 1, "message": "test"}],
                error="GRAPH_WRITE_TIMEOUT",
                stage="graph_write",
                batch_result=batch,
            )

        assert success is True
        call_args = redis.rpush.call_args[0]
        entry = json.loads(call_args[1])
        assert entry["stage"] == "graph_write"
        assert "batch_result" in entry
        assert entry["batch_result"]["entity_ids"] == [1, 2]
        assert "session_text" not in entry

    @pytest.mark.asyncio
    async def test_redis_failure_returns_false(self):
        proc, _, _, _, _, redis = make_processor()
        redis.rpush = AsyncMock(side_effect=Exception("Redis down"))

        with patch("main.processor.emit", new_callable=AsyncMock):
            success = await proc.move_to_dead_letter(
                messages=[{"id": 1, "message": "test"}],
                error="some error",
                stage="processing",
            )

        assert success is False

    @pytest.mark.asyncio
    async def test_numpy_arrays_serialized(self):
        proc, _, _, _, _, redis = make_processor()

        msgs = [{"id": 1, "message": "test", "embedding": np.array([0.1, 0.2])}]

        with patch("main.processor.emit", new_callable=AsyncMock):
            success = await proc.move_to_dead_letter(
                messages=msgs,
                error="test",
                stage="processing",
            )

        assert success is True
        # Verify it serialized without numpy TypeError
        call_args = redis.rpush.call_args[0]
        entry = json.loads(call_args[1])
        assert entry["messages"][0]["embedding"] == [0.1, 0.2]

    @pytest.mark.asyncio
    async def test_attempt_greater_than_one_serialized(self):
        """DLQ entry with attempt=3 should have that value in the payload."""
        proc, _, _, _, _, redis = make_processor()

        with patch("main.processor.emit", new_callable=AsyncMock):
            success = await proc.move_to_dead_letter(
                messages=[{"id": 1, "message": "test"}],
                error="RETRY_EXHAUSTED",
                stage="processing",
                session_text="context",
                attempt=3,
            )

        assert success is True
        call_args = redis.rpush.call_args[0]
        entry = json.loads(call_args[1])
        assert entry["attempt"] == 3
        assert entry["error"] == "RETRY_EXHAUSTED"

    @pytest.mark.asyncio
    async def test_dlq_no_session_text_for_graph_stage(self):
        """graph_write stage should NOT include session_text even if passed."""
        proc, _, _, _, _, redis = make_processor()

        batch = BatchResult(entity_ids=[1])

        with patch("main.processor.emit", new_callable=AsyncMock):
            await proc.move_to_dead_letter(
                messages=[{"id": 1, "message": "test"}],
                error="GRAPH_DOWN",
                stage="graph_write",
                session_text="this should be excluded",
                batch_result=batch,
            )

        call_args = redis.rpush.call_args[0]
        entry = json.loads(call_args[1])
        assert "session_text" not in entry
        assert "batch_result" in entry

    @pytest.mark.asyncio
    async def test_dlq_no_batch_result_for_processing_stage(self):
        """processing stage should NOT include batch_result even if passed."""
        proc, _, _, _, _, redis = make_processor()

        batch = BatchResult(entity_ids=[1])

        with patch("main.processor.emit", new_callable=AsyncMock):
            await proc.move_to_dead_letter(
                messages=[{"id": 1, "message": "test"}],
                error="NLP_CRASH",
                stage="processing",
                session_text="context",
                batch_result=batch,
            )

        call_args = redis.rpush.call_args[0]
        entry = json.loads(call_args[1])
        assert "session_text" in entry
        assert "batch_result" not in entry


# --- JSON Serialization Helpers ---

class TestSafeJson:
    """Tests internal utility for safe JSON serialization of numpy types."""

    def test_numpy_array(self):
        assert _safe_json(np.array([1.0, 2.0])) == [1.0, 2.0]

    def test_numpy_integer(self):
        assert _safe_json(np.int64(42)) == 42

    def test_numpy_float(self):
        assert _safe_json(np.float32(3.14)) == pytest.approx(3.14, abs=1e-5)

    def test_unsupported_type_raises(self):
        with pytest.raises(TypeError):
            _safe_json({"nested": "dict"})

    def test_nested_numpy_in_json_dumps(self):
        """json.dumps with default=_safe_json should handle numpy inside dicts."""
        payload = {
            "embedding": np.array([0.1, 0.2]),
            "score": np.float64(0.95),
            "count": np.int64(42),
        }
        serialized = json.dumps(payload, default=_safe_json)
        parsed = json.loads(serialized)
        assert parsed["embedding"] == pytest.approx([0.1, 0.2])
        assert parsed["score"] == pytest.approx(0.95)
        assert parsed["count"] == 42

    def test_regular_types_dont_hit_safe_json(self):
        """Standard Python types bypass _safe_json entirely — json.dumps handles them."""
        payload = {"name": "Alice", "ids": [1, 2], "score": 0.9}
        serialized = json.dumps(payload, default=_safe_json)
        assert json.loads(serialized) == payload



# ════════════════════════════════════════════════════════
#  _extract_connections
# ════════════════════════════════════════════════════════

class TestExtractConnections:

    @pytest.mark.asyncio
    async def test_empty_entity_ids_returns_empty(self):
        proc, _, llm, resolver, _, _ = make_processor()
        resolver.entity_profiles = {}

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc._extract_connections([], {}, MESSAGES, "context")

        assert result == []
        llm.call_llm.assert_not_called()

    @pytest.mark.asyncio
    async def test_custom_connection_prompt_used(self):
        """When connection_prompt is set, it should be used instead of the default."""
        custom_prompt = "Custom prompt for {user_name}. Find connections."
        proc, _, llm, resolver, _, _ = make_processor(connection_prompt=custom_prompt)

        resolver.entity_profiles = {
            100: {"canonical_name": "Palantir", "type": "company"},
        }
        resolver.get_mentions_for_id.return_value = []
        llm.call_llm = AsyncMock(return_value="<connections>\nMSG 1 | NO CONNECTIONS\n</connections>")

        with patch("main.processor.emit", new_callable=AsyncMock):
            await proc._extract_connections([100], {100: [1]}, MESSAGES, "context")

        # Verify the custom prompt was passed as system arg
        call_args = llm.call_llm.call_args
        system_arg = call_args[0][0] if call_args[0] else call_args[1].get("system", "")
        assert "Custom prompt for Yinka" in system_arg

    @pytest.mark.asyncio
    async def test_default_prompt_when_no_custom(self):
        """Without connection_prompt, the default from get_connection_reasoning_prompt is used."""
        proc, _, llm, resolver, _, _ = make_processor(connection_prompt=None)

        resolver.entity_profiles = {
            100: {"canonical_name": "Palantir", "type": "company"},
        }
        resolver.get_mentions_for_id.return_value = []
        llm.call_llm = AsyncMock(return_value="<connections>\nMSG 1 | NO CONNECTIONS\n</connections>")

        with patch("main.processor.emit", new_callable=AsyncMock):
            await proc._extract_connections([100], {100: [1]}, MESSAGES, "context")

        call_args = llm.call_llm.call_args
        system_arg = call_args[0][0] if call_args[0] else call_args[1].get("system", "")
        # Default prompt contains VEGAPUNK or connection-related instructions
        assert "connection" in system_arg.lower() or "VEGAPUNK" in system_arg

    @pytest.mark.asyncio
    async def test_llm_returns_valid_connections(self):
        proc, _, llm, resolver, _, _ = make_processor()

        resolver.entity_profiles = {
            100: {"canonical_name": "Palantir", "type": "company"},
            101: {"canonical_name": "Priya Sharma", "type": "person"},
        }
        resolver.get_mentions_for_id.return_value = []

        llm.call_llm = AsyncMock(return_value="""
        <connections>
        MSG 1 | Palantir; Priya Sharma | 0.85 | colleagues
        </connections>
        """)

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc._extract_connections(
                [100, 101], {100: [1], 101: [2]}, MESSAGES, "context"
            )

        assert len(result) == 1
        assert result[0].entity_pairs[0].entity_a == "Palantir"


# ════════════════════════════════════════════════════════
#  run() edge cases
# ════════════════════════════════════════════════════════

class TestRunEdgeCases:

    @pytest.mark.asyncio
    async def test_empty_session_text(self):
        """Empty session_text should not cause errors in connection extraction."""
        proc, _, llm, resolver, nlp, _ = make_processor()

        nlp.extract_mentions = AsyncMock(return_value=[
            (1, "Palantir", "company", "Work"),
        ])
        resolver.compute_batch_embeddings.return_value = [FAKE_EMBEDDING, FAKE_EMBEDDING]
        resolver.get_candidate_ids.return_value = []
        resolver.register_entity.return_value = FAKE_EMBEDDING
        resolver.entity_profiles = {
            100: {"canonical_name": "Palantir", "type": "company"},
        }
        resolver.get_mentions_for_id.return_value = []
        llm.call_llm = AsyncMock(return_value="<connections>\nMSG 1 | NO CONNECTIONS\n</connections>")

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc.run(MESSAGES, "")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_embeddings_failure_caught(self):
        """compute_batch_embeddings raising should be caught, result.success=False."""
        proc, _, _, resolver, nlp, _ = make_processor()
        nlp.extract_mentions = AsyncMock(return_value=[])
        resolver.compute_batch_embeddings.side_effect = RuntimeError("CUDA OOM")

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc.run(MESSAGES, "context")

        assert result.success is False
        assert "CUDA OOM" in result.error

    @pytest.mark.asyncio
    async def test_missing_message_key_caught(self):
        """Messages without 'message' key should raise KeyError, caught by outer try."""
        proc, _, _, resolver, nlp, _ = make_processor()
        nlp.extract_mentions = AsyncMock(return_value=[])
        resolver.compute_batch_embeddings.return_value = [FAKE_EMBEDDING]

        bad_messages = [{"id": 1, "role": "user"}]  # no 'message' key

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc.run(bad_messages, "context")

        assert result.success is False
        assert "message" in result.error.lower() or "key" in result.error.lower()


# ════════════════════════════════════════════════════════
#  _resolve_mentions — mixed scenarios
# ════════════════════════════════════════════════════════

class TestResolveMentionsMixed:

    @pytest.mark.asyncio
    async def test_mixed_new_and_existing(self):
        """Batch with one existing entity match and one new entity."""
        proc, _, _, resolver, _, _ = make_processor()

        # First mention: matches existing entity 5
        # Second mention: no candidates, creates new
        call_count = [0]
        def mock_candidates(name, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return [(5, 0.95)]  # existing match
            return []  # new entity

        resolver.get_candidate_ids.side_effect = mock_candidates
        resolver.validate_existing.return_value = (5, False, [])
        resolver.register_entity.return_value = FAKE_EMBEDDING
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING, FAKE_EMBEDDING]

        mentions = [
            (1, "Palantir", "company", "Work"),
            (2, "Priya Sharma", "person", "Identity"),
        ]
        messages = [
            {"id": 1, "message": "Working at Palantir"},
            {"id": 2, "message": "My manager Priya Sharma"},
        ]

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc._resolve_mentions(mentions, messages)

        assert 5 in result.entity_ids  # existing
        assert len(result.new_ids) == 1  # one new
        assert len(result.entity_ids) == 2  # total

    @pytest.mark.asyncio
    async def test_separate_entities_get_separate_msg_maps(self):
        """Two different entities from different messages should have distinct msg_id lists."""
        proc, _, _, resolver, _, _ = make_processor()

        resolver.get_candidate_ids.return_value = []
        resolver.register_entity.return_value = FAKE_EMBEDDING
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING, FAKE_EMBEDDING]

        mentions = [
            (1, "Palantir", "company", "Work"),
            (2, "Priya Sharma", "person", "Identity"),
        ]
        messages = [
            {"id": 1, "message": "Working at Palantir"},
            {"id": 2, "message": "My manager Priya Sharma"},
        ]

        with patch("main.processor.emit", new_callable=AsyncMock):
            result = await proc._resolve_mentions(mentions, messages)

        # Two new entities with separate IDs
        ids = list(result.new_ids)
        assert len(ids) == 2
        # Each should map to its own message
        for eid in ids:
            assert len(result.entity_msg_map[eid]) == 1
        # The msg_ids should be different
        mapped_msgs = [result.entity_msg_map[eid][0] for eid in ids]
        assert set(mapped_msgs) == {1, 2}

class TestBoostCandidates:

    @pytest.mark.asyncio
    async def test_numbered_yes_no(self):
        """Standard numbered format: '1. YES\\n2. NO'"""
        proc, store, mock_llm, _, _, _ = make_processor()
        mock_llm.call_llm.return_value = "1. YES\n2. NO"
        now = datetime.now(timezone.utc)
        store.get_facts_for_entity.return_value = [
            Fact(id="f1", source_entity_id=1, content="Works at Anthropic", valid_at=now)
        ]

        result = await proc._boost_candidates(
            [(1, 0.80, 100), (2, 0.75, 101)],
            {100: "I work at Anthropic", 101: "Going to the store"},
            set(),
        )
        assert result[1] == pytest.approx(0.85)  # 0.80 + 0.05
        assert result[2] == pytest.approx(0.75)  # NO, stays at base

    @pytest.mark.asyncio
    async def test_bare_yes_no_lines(self):
        """LLM returns just 'YES\\nNO' without numbering."""
        proc, store, mock_llm, _, _, _ = make_processor()
        mock_llm.call_llm.return_value = "YES\nNO"

        now = datetime.now(timezone.utc)
        store.get_facts_for_entity.return_value = [
            Fact(id="f1", source_entity_id=1, content="fact", valid_at=now)
        ]

        result = await proc._boost_candidates(
            [(1, 0.80, 100), (2, 0.75, 101)],
            {100: "msg A", 101: "msg B"},
            set(),
        )
        assert result[1] == pytest.approx(0.85)
        assert result[2] == pytest.approx(0.75)

    @pytest.mark.asyncio
    async def test_colon_format(self):
        """LLM uses colon format: '1: YES\\n2: NO'"""
        proc, store, mock_llm, _, _, _ = make_processor()
        mock_llm.call_llm.return_value = "1: YES\n2: NO\n3: YES"

        now = datetime.now(timezone.utc)
        store.get_facts_for_entity.return_value = [
            Fact(id="f1", source_entity_id=1, content="fact", valid_at=now)
        ]

        result = await proc._boost_candidates(
            [(1, 0.80, 100), (2, 0.75, 101), (3, 0.70, 102)],
            {100: "msg A", 101: "msg B", 102: "msg C"},
            set(),
        )
        assert result[1] == pytest.approx(0.85)
        assert result[2] == pytest.approx(0.75)
        assert result[3] == pytest.approx(0.75)

    @pytest.mark.asyncio
    async def test_garbled_response_falls_back(self):
        """LLM returns nonsense — all candidates keep base scores."""
        proc, store, mock_llm, _, _, _ = make_processor()
        mock_llm.call_llm.return_value = "I think the answer depends on the context and nuance of the situation."

        now = datetime.now(timezone.utc)
        store.get_facts_for_entity.return_value = [
            Fact(id="f1", source_entity_id=1, content="fact", valid_at=now)
        ]

        result = await proc._boost_candidates(
            [(1, 0.80, 100), (2, 0.75, 101)],
            {100: "msg A", 101: "msg B"},
            set(),
        )
        assert result[1] == pytest.approx(0.80)
        assert result[2] == pytest.approx(0.75)

    @pytest.mark.asyncio
    async def test_none_response_falls_back(self):
        """LLM returns None (timeout) — base scores preserved."""
        proc, store, mock_llm, _, _, _ = make_processor()
        mock_llm.call_llm.return_value = None

        now = datetime.now(timezone.utc)
        store.get_facts_for_entity.return_value = [
            Fact(id="f1", source_entity_id=1, content="fact", valid_at=now)
        ]

        result = await proc._boost_candidates(
            [(1, 0.80, 100)],
            {100: "msg A"},
            set(),
        )
        assert result[1] == pytest.approx(0.80)

    @pytest.mark.asyncio
    async def test_llm_exception_falls_back(self):
        """LLM raises exception — base scores preserved, no crash."""
        proc, store, mock_llm, _, _, _ = make_processor()
        mock_llm.call_llm.side_effect = Exception("LLM is down")
        proc.llm.call_llm = AsyncMock(side_effect=Exception("API timeout"))

        now = datetime.now(timezone.utc)
        store.get_facts_for_entity.return_value = [
            Fact(id="f1", source_entity_id=1, content="fact", valid_at=now)
        ]

        result = await proc._boost_candidates(
            [(1, 0.80, 100)],
            {100: "msg A"},
            set(),
        )
        assert result[1] == pytest.approx(0.80)

    @pytest.mark.asyncio
    async def test_fewer_lines_than_candidates(self):
        """LLM returns fewer YES/NO lines than candidates — extras keep base score."""
        proc, store, mock_llm, _, _, _ = make_processor()
        mock_llm.call_llm.return_value = "1. YES"

        now = datetime.now(timezone.utc)
        store.get_facts_for_entity.return_value = [
            Fact(id="f1", source_entity_id=1, content="fact", valid_at=now)
        ]

        result = await proc._boost_candidates(
            [(1, 0.80, 100), (2, 0.75, 101), (3, 0.70, 102)],
            {100: "msg A", 101: "msg B", 102: "msg C"},
            set(),
        )
        assert result[1] == pytest.approx(0.85)
        # Candidates 2 and 3 have no matching YES/NO line — keep base
        assert result[2] == pytest.approx(0.75)
        assert result[3] == pytest.approx(0.70)

    @pytest.mark.asyncio
    async def test_no_facts_skips_llm(self):
        """Candidate with no facts should keep base score without LLM call."""
        proc, store, mock_llm, _, _, _ = make_processor()
        mock_llm.call_llm.return_value = "should not be called"
        store.get_facts_for_entity.return_value = []

        result = await proc._boost_candidates(
            [(1, 0.80, 100)],
            {100: "msg A"},
            set(),
        )
        assert result[1] == pytest.approx(0.80)
        proc.llm.call_llm.assert_not_called()

    @pytest.mark.asyncio
    async def test_co_occurrence_boost(self):
        """Signal 4: neighbor overlap with batch entities adds up to 0.05."""
        proc, store, mock_llm, _, _, _ = make_processor()
        mock_llm.call_llm.return_value = None
        store.get_facts_for_entity.return_value = []
        store.get_neighbor_ids.return_value = {5, 6}

        result = await proc._boost_candidates(
            [(1, 0.80, 100)],
            {100: "msg A"},
            {5, 7},  # entity 5 is both a neighbor and in batch
        )
        # base 0.80 + co-occurrence 0.03 (1 overlap * 0.03)
        assert result[1] == pytest.approx(0.83)