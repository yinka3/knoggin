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