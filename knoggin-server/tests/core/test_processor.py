"""Tests for src/core/batch_processor.py — orchestration, resolution, DLQ, and connection extraction."""

import json
import numpy as np
import pytest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, AsyncMock, patch

from src.core.batch_processor import BatchProcessor, _safe_json
from src.common.config.topics_config import TopicConfig
from src.common.schema.dtypes import (
    BatchResult,
    EntityPair,
    Fact,
    MessageConnections,
)
from datetime import datetime, timezone


FAKE_EMBEDDING = [0.1] * 1024


def make_processor(**overrides):
    store = MagicMock()
    store.get_facts_for_entity.return_value = []
    store.get_neighbor_ids.return_value = set()
    store.validate_existing_ids.return_value = set()

    llm = MagicMock()
    llm.call_llm = AsyncMock(return_value=None)

    resolver = MagicMock()
    resolver.get_id.return_value = None
    resolver.get_candidate_ids = AsyncMock(return_value=[])
    resolver.register_entity = AsyncMock(return_value=FAKE_EMBEDDING)
    resolver.resolve_entity_name = AsyncMock(return_value=None)
    resolver.validate_existing.return_value = (None, False, [])
    resolver.entity_profiles = {}
    resolver.embedding_service = MagicMock()
    resolver.embedding_service.encode = AsyncMock(return_value=[FAKE_EMBEDDING] * 5)

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


class TestRunOrchestration:

    @pytest.mark.asyncio
    async def test_empty_messages(self):
        proc, *_ = make_processor()

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc.run([], "")

        assert result.success is True
        assert result.entity_ids == []
        assert result.extraction_result is None

    @pytest.mark.asyncio
    async def test_no_mentions_returns_embeddings_only(self):
        proc, _, _, resolver, nlp, _ = make_processor()
        nlp.extract_mentions = AsyncMock(return_value=[])
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING, FAKE_EMBEDDING]

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc.run(MESSAGES, "[USER]: test")

        assert result.success is True
        assert len(result.message_embeddings) == 2
        assert 1 in result.message_embeddings
        assert 2 in result.message_embeddings
        assert result.entity_ids == []

    @pytest.mark.asyncio
    async def test_full_flow_chains_stages(self):
        proc, _, llm, resolver, nlp, _ = make_processor()

        nlp.extract_mentions = AsyncMock(return_value=[
            (1, "Palantir", "company", "Work"),
            (2, "Priya Sharma", "person", "Identity"),
        ])
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING, FAKE_EMBEDDING]
        resolver.get_candidate_ids.return_value = []
        resolver.register_entity.return_value = FAKE_EMBEDDING

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

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc.run(MESSAGES, "[USER]: context")

        assert result.success is True
        assert len(result.entity_ids) >= 2
        assert len(result.new_entity_ids) >= 2
        assert result.extraction_result is not None

    @pytest.mark.asyncio
    async def test_connection_extraction_returns_none(self):
        proc, _, llm, resolver, nlp, _ = make_processor()

        nlp.extract_mentions = AsyncMock(return_value=[
            (1, "Palantir", "company", "Work"),
        ])
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING, FAKE_EMBEDDING]
        resolver.get_candidate_ids.return_value = []
        resolver.register_entity.return_value = FAKE_EMBEDDING
        resolver.entity_profiles = {
            100: {"canonical_name": "Palantir", "type": "company"},
        }
        resolver.get_mentions_for_id.return_value = []
        llm.call_llm = AsyncMock(return_value=None)

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc.run(MESSAGES, "[USER]: context")

        assert result.success is False
        assert "VP-03" in result.error

    @pytest.mark.asyncio
    async def test_exception_during_processing(self):
        proc, _, _, resolver, nlp, _ = make_processor()

        nlp.extract_mentions = AsyncMock(side_effect=RuntimeError("NLP crashed"))
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING, FAKE_EMBEDDING]

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc.run(MESSAGES, "[USER]: context")

        assert result.success is False
        assert "NLP crashed" in result.error

    @pytest.mark.asyncio
    async def test_user_entity_appended(self):
        proc, _, llm, resolver, nlp, _ = make_processor()

        nlp.extract_mentions = AsyncMock(return_value=[
            (1, "Palantir", "company", "Work"),
        ])
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING, FAKE_EMBEDDING]
        resolver.get_candidate_ids.return_value = []
        resolver.register_entity.return_value = FAKE_EMBEDDING
        resolver.entity_profiles = {
            100: {"canonical_name": "Palantir", "type": "company"},
        }
        resolver.get_mentions_for_id.return_value = []
        resolver.get_id.return_value = 1

        llm.call_llm = AsyncMock(return_value="<connections>\n</connections>")

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc.run(MESSAGES, "[USER]: context")

        assert 1 in result.entity_ids

    @pytest.mark.asyncio
    async def test_embeddings_attached_to_messages(self):
        proc, _, _, resolver, nlp, _ = make_processor()

        nlp.extract_mentions = AsyncMock(return_value=[])
        emb_1 = [0.1, 0.2, 0.3]
        emb_2 = [0.4, 0.5, 0.6]
        resolver.embedding_service.encode.return_value = [emb_1, emb_2]

        msgs = [
            {"id": 10, "message": "first message", "role": "user"},
            {"id": 11, "message": "second message", "role": "user"},
        ]

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc.run(msgs, "")

        assert result.message_embeddings[10] == emb_1
        assert result.message_embeddings[11] == emb_2


class TestExtractMentionsNormalization:

    @pytest.mark.asyncio
    async def test_normalizes_topics(self):
        proc, *_ = make_processor()
        proc.nlp.extract_mentions = AsyncMock(return_value=[
            (1, "Palantir", "company", "career"),  # alias for Work
            (2, "Priya", "person", "Identity"),
        ])

        result = await proc._extract_mentions(MESSAGES, "test-session")

        topics = [topic for _, _, _, topic in result]
        assert "Work" in topics
        assert "Identity" in topics

    @pytest.mark.asyncio
    async def test_invalid_topic_filtered(self):
        proc, *_ = make_processor()
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


class TestResolveMentions:

    @pytest.mark.asyncio
    async def test_new_entity_created(self):
        proc, _, _, resolver, _, _ = make_processor()
        resolver.get_candidate_ids.return_value = []
        resolver.register_entity.return_value = FAKE_EMBEDDING
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING]

        mentions = [(1, "Palantir", "company", "Work")]
        messages = [{"id": 1, "message": "Working at Palantir"}]

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc._resolve_mentions(mentions, messages)

        assert len(result.new_ids) == 1
        resolver.register_entity.assert_called_once()

    @pytest.mark.asyncio
    async def test_existing_entity_matched(self):
        proc, _, _, resolver, _, _ = make_processor()
        resolver.get_candidate_ids.return_value = [(5, 0.95)]
        resolver.validate_existing.return_value = (5, False, [])
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING]

        mentions = [(1, "Palantir", "company", "Work")]
        messages = [{"id": 1, "message": "Working at Palantir"}]

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
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

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc._resolve_mentions(mentions, messages)

        resolver.commit_new_aliases.assert_called_once_with(5, ["Palantir Tech"])
        assert 5 in result.alias_ids
        assert result.alias_updates[5] == ["Palantir Tech"]

    @pytest.mark.asyncio
    async def test_batch_dedup(self):
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

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc._resolve_mentions(mentions, messages)

        assert resolver.register_entity.call_count == 1
        ent_id = list(result.new_ids)[0]
        assert 1 in result.entity_msg_map[ent_id]
        assert 2 in result.entity_msg_map[ent_id]

    @pytest.mark.asyncio
    async def test_below_threshold_creates_new(self):
        proc, store, _, resolver, _, _ = make_processor()
        resolver.get_candidate_ids.return_value = [(5, 0.70)]
        store.validate_existing_ids.return_value = {5}
        resolver.register_entity.return_value = FAKE_EMBEDDING
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING]

        mentions = [(1, "Palantir Tech", "company", "Work")]
        messages = [{"id": 1, "message": "Palantir Tech stuff"}]

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc._resolve_mentions(mentions, messages)

        assert len(result.new_ids) == 1
        assert 5 not in result.entity_ids

    # NOTE: test_zombie_candidate_evicted removed — the zombie eviction branch
    # in _resolve_mentions is commented out (unreachable). Restore when negative
    # signals are implemented.

    @pytest.mark.asyncio
    async def test_register_entity_exception(self):
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

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc._resolve_mentions(mentions, messages)

        assert len(result.entity_ids) == 0
        assert len(result.new_ids) == 0


class TestMoveToDLQ:

    @pytest.mark.asyncio
    async def test_processing_stage_includes_session_text(self):
        proc, _, _, _, _, redis = make_processor()

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
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

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
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

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
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

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            success = await proc.move_to_dead_letter(
                messages=msgs,
                error="test",
                stage="processing",
            )

        assert success is True
        call_args = redis.rpush.call_args[0]
        entry = json.loads(call_args[1])
        assert entry["messages"][0]["embedding"] == [0.1, 0.2]

    @pytest.mark.asyncio
    async def test_attempt_greater_than_one_serialized(self):
        proc, _, _, _, _, redis = make_processor()

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
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
        proc, _, _, _, _, redis = make_processor()

        batch = BatchResult(entity_ids=[1])

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
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
        proc, _, _, _, _, redis = make_processor()

        batch = BatchResult(entity_ids=[1])

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
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


class TestSafeJson:

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
        payload = {"name": "Alice", "ids": [1, 2], "score": 0.9}
        serialized = json.dumps(payload, default=_safe_json)
        assert json.loads(serialized) == payload


class TestExtractConnections:

    @pytest.mark.asyncio
    async def test_empty_entity_ids_returns_empty(self):
        proc, _, llm, resolver, _, _ = make_processor()
        resolver.entity_profiles = {}

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc._extract_connections([], {}, MESSAGES, "context")

        assert result == []
        llm.call_llm.assert_not_called()

    @pytest.mark.asyncio
    async def test_custom_connection_prompt_used(self):
        custom_prompt = "Custom prompt for {user_name}. Find connections."
        proc, _, llm, resolver, _, _ = make_processor(connection_prompt=custom_prompt)

        resolver.entity_profiles = {
            100: {"canonical_name": "Palantir", "type": "company"},
        }
        resolver.get_mentions_for_id.return_value = []
        llm.call_llm = AsyncMock(return_value="<connections>\nMSG 1 | NO CONNECTIONS\n</connections>")

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            await proc._extract_connections([100], {100: [1]}, MESSAGES, "context")

        call_args = llm.call_llm.call_args
        system_arg = call_args[0][0] if call_args[0] else call_args[1].get("system", "")
        assert "Custom prompt for Yinka" in system_arg

    @pytest.mark.asyncio
    async def test_default_prompt_when_no_custom(self):
        proc, _, llm, resolver, _, _ = make_processor(connection_prompt=None)

        resolver.entity_profiles = {
            100: {"canonical_name": "Palantir", "type": "company"},
        }
        resolver.get_mentions_for_id.return_value = []
        llm.call_llm = AsyncMock(return_value="<connections>\nMSG 1 | NO CONNECTIONS\n</connections>")

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            await proc._extract_connections([100], {100: [1]}, MESSAGES, "context")

        call_args = llm.call_llm.call_args
        system_arg = call_args[0][0] if call_args[0] else call_args[1].get("system", "")
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

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc._extract_connections(
                [100, 101], {100: [1], 101: [2]}, MESSAGES, "context"
            )

        assert len(result) == 1
        assert result[0].entity_pairs[0].entity_a == "Palantir"


class TestRunEdgeCases:

    @pytest.mark.asyncio
    async def test_empty_session_text(self):
        proc, _, llm, resolver, nlp, _ = make_processor()

        nlp.extract_mentions = AsyncMock(return_value=[
            (1, "Palantir", "company", "Work"),
        ])
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING, FAKE_EMBEDDING]
        resolver.get_candidate_ids.return_value = []
        resolver.register_entity.return_value = FAKE_EMBEDDING
        resolver.entity_profiles = {
            100: {"canonical_name": "Palantir", "type": "company"},
        }
        resolver.get_mentions_for_id.return_value = []
        llm.call_llm = AsyncMock(return_value="<connections>\nMSG 1 | NO CONNECTIONS\n</connections>")

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc.run(MESSAGES, "")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_embeddings_failure_caught(self):
        proc, _, _, resolver, nlp, _ = make_processor()
        nlp.extract_mentions = AsyncMock(return_value=[])
        resolver.embedding_service.encode.side_effect = RuntimeError("CUDA OOM")

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc.run(MESSAGES, "context")

        assert result.success is False
        assert "CUDA OOM" in result.error

    @pytest.mark.asyncio
    async def test_missing_message_key_caught(self):
        proc, _, _, resolver, nlp, _ = make_processor()
        nlp.extract_mentions = AsyncMock(return_value=[])
        resolver.embedding_service.encode.return_value = [FAKE_EMBEDDING]

        bad_messages = [{"id": 1, "role": "user"}]  # no 'message' key

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc.run(bad_messages, "context")

        assert result.success is False
        assert "message" in result.error.lower() or "key" in result.error.lower()


class TestResolveMentionsMixed:

    @pytest.mark.asyncio
    async def test_mixed_new_and_existing(self):
        proc, _, _, resolver, _, _ = make_processor()

        call_count = [0]
        def mock_candidates(name, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return [(5, 0.95)]
            return []

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

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc._resolve_mentions(mentions, messages)

        assert 5 in result.entity_ids
        assert len(result.new_ids) == 1
        assert len(result.entity_ids) == 2

    @pytest.mark.asyncio
    async def test_separate_entities_get_separate_msg_maps(self):
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

        with patch("src.core.batch_processor.emit", new_callable=AsyncMock):
            result = await proc._resolve_mentions(mentions, messages)

        ids = list(result.new_ids)
        assert len(ids) == 2
        for eid in ids:
            assert len(result.entity_msg_map[eid]) == 1
        mapped_msgs = [result.entity_msg_map[eid][0] for eid in ids]
        assert set(mapped_msgs) == {1, 2}


class TestBoostCandidates:

    @pytest.mark.asyncio
    async def test_numbered_yes_no(self):
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
        assert result[1] == pytest.approx(0.85)
        assert result[2] == pytest.approx(0.75)

    @pytest.mark.asyncio
    async def test_bare_yes_no_lines(self):
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
        assert result[2] == pytest.approx(0.75)
        assert result[3] == pytest.approx(0.70)

    @pytest.mark.asyncio
    async def test_no_facts_skips_llm(self):
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
        proc, store, mock_llm, _, _, _ = make_processor()
        mock_llm.call_llm.return_value = None
        store.get_facts_for_entity.return_value = []
        store.get_neighbor_ids.return_value = {5, 6}

        result = await proc._boost_candidates(
            [(1, 0.80, 100)],
            {100: "msg A"},
            {5, 7},  # entity 5 is both a neighbor and in batch
        )
        assert result[1] == pytest.approx(0.83)