"""Real PostgreSQL/AGE/pgvector/Redis ingestion recovery contracts."""

import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from common.conf.domain_config import CompiledDomain, DomainConfig
from common.schema.ingestion.extraction import (
    RelationshipExtraction,
    RelationshipMention,
)
from common.schema.primitives import Message
from common.schema.settings import DLQSettings, IngestionSettings
from core.ingestion.graph_commit import write_ingestion_batch_to_graph
from core.ingestion.pipeline import IngestionPipeline
from core.ingestion.recovery.dlq_state import DLQ_STATUS_COMPLETED, ensure_dlq_id
from core.ingestion.recovery.replay_job import DLQReplayJob
from core.ingestion.worker import IngestionWorker
from core.knowledge.entity.resolver import EntityResolver
from core.knowledge.store import KnowledgeStore
from core.session.context import Session
from infrastructure.job.base import JobContext
from infrastructure.redis_client import RedisKeys
from tests.integration.ingestion.test_server_flow import _session


class _DeterministicMentionProcessor:
    """Deterministic extraction at the model boundary, with production pipeline state."""

    # IngestionPipeline snapshots these production text-processor settings into
    # each durable batch before it calls the deterministic extraction hook.
    gliner_threshold = 0.85
    vp01_min_confidence = 0.8
    llm_ner = False

    async def extract_mentions(self, batch):
        message_id = int(batch.messages[0]["id"])
        return [
            (message_id, "Ada Lovelace", "person", "Identity"),
            (message_id, "Grace Hopper", "person", "Identity"),
        ]


class _RecoveryEmbeddingService:
    async def encode(self, texts, **_kwargs):
        return [[0.25] * 1024 for _ in texts]

    async def encode_single(self, _text, **_kwargs):
        return [0.25] * 1024


class _DeterministicRelationshipLLM:
    extraction_model = "recovery-contract-model"

    async def generate_structured(self, *, response_model, **_kwargs):
        assert response_model is RelationshipExtraction
        return RelationshipExtraction(
            connections=[
                RelationshipMention(
                    msg_id="m1",
                    entity_a="Ada Lovelace",
                    entity_b="Grace Hopper",
                    relationship="met",
                    confidence=1.0,
                    context="Ada Lovelace met Grace Hopper.",
                )
            ]
        )


class _RedisProxy:
    def __init__(self, client):
        self.client = client

    def __getattr__(self, name):
        return getattr(self.client, name)


class _FailOnceStore:
    def __init__(self, store):
        self.store = store
        self.failed = False

    def __getattr__(self, name):
        return getattr(self.store, name)

    async def write_batch(self, *args, **kwargs):
        if not self.failed:
            self.failed = True
            raise ConnectionError("ConnectionError: main graph transaction unavailable")
        return await self.store.write_batch(*args, **kwargs)


class _FailOnceDirtyRedis(_RedisProxy):
    def __init__(self, client):
        super().__init__(client)
        self.failed = False

    async def sadd(self, key, *values):
        if not self.failed:
            self.failed = True
            raise ConnectionError("ConnectionError: dirty marking unavailable")
        return await self.client.sadd(key, *values)


class _FailOnceCheckpointRedis(_RedisProxy):
    def __init__(self, client):
        super().__init__(client)
        self.failed = False

    async def eval(self, *args, **kwargs):
        if not self.failed:
            self.failed = True
            raise ConnectionError("ConnectionError: checkpoint unavailable")
        return await self.client.eval(*args, **kwargs)


def _compiled_domain() -> CompiledDomain:
    return DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {
                "General": {"active": True},
                "Identity": {"active": True},
            },
            "entity_types": {
                "Identity": {
                    "topic": "Identity",
                    "labels": ["person", "me"],
                },
                "General": {"topic": "General", "labels": []},
            },
        }
    ).compile()


def _runtime(scope, *, postgres, redis, store=None):
    embedding = _RecoveryEmbeddingService()
    store = store or KnowledgeStore(postgres, embedding)
    entities = EntityResolver(
        knowledge_store=store,
        embedding_service=embedding,
        project_id=scope["project_id"],
        readable_project_ids=[scope["project_id"]],
    )
    pipeline = IngestionPipeline(
        project_id=scope["project_id"],
        redis_client=redis,
        llm=_DeterministicRelationshipLLM(),
        entities=entities,
        processor=_DeterministicMentionProcessor(),
        cpu_executor=None,
        user_name=scope["user_name"],
        compiled_domain=_compiled_domain(),
        get_next_ent_id=store.allocate_entity_id,
        knowledge_store=store,
    )
    resources = SimpleNamespace(
        postgres=postgres,
        redis=redis,
        knowledge_store=store,
        embedding=embedding,
    )
    context = _session(
        resources,
        user_name=scope["user_name"],
        project_id=scope["project_id"],
        session_id=scope["session_id"],
    )
    worker = IngestionWorker(
        user_name=scope["user_name"],
        session_id=scope["session_id"],
        knowledge_store=store,
        processor=pipeline,
        redis=redis,
        get_session_context=context.get_conversation_context,
        write_to_graph=lambda batch: _write_graph(
            batch, store=store, entities=entities, redis=redis
        ),
        settings=IngestionSettings(
            batch_size=1,
            batch_debounce_seconds=0,
            batch_timeout=10,
            ingestion_batch_settle_delay_seconds=0,
            checkpoint_interval=1,
        ),
    )
    context.consumer = worker
    return context, worker, pipeline, store, entities


async def _write_graph(batch, *, store, entities, redis):
    await write_ingestion_batch_to_graph(
        batch,
        knowledge_store=store,
        entities=entities,
        redis_client=redis,
    )
    return True, None


def _configure_session(monkeypatch):
    monkeypatch.setattr(
        Session,
        "current_config",
        property(
            lambda self: SimpleNamespace(
                developer_settings=SimpleNamespace(
                    limits=SimpleNamespace(conversation_context_turns=100),
                    ingestion=SimpleNamespace(message_edit_window_seconds=1),
                )
            )
        ),
    )


async def _accept_and_flush(context, worker, content):
    if worker._task is None:
        worker.start()
    message = await context.add(
        Message(
            content=content,
            timestamp=datetime(2026, 8, 1, 17, 0, tzinfo=timezone.utc),
        )
    )
    # A manual flush does not bypass the production edit window.
    await asyncio.sleep(1.05)
    await worker.flush()
    return message


async def _assert_graph_state(scope, postgres, message_id):
    assert await postgres.fetch_one(
        "SELECT count(*) AS count FROM entities WHERE project_id = %s",
        (scope["project_id"],),
    ) == {"count": 2}
    assert await postgres.fetch_one(
        "SELECT count(*) AS count FROM relationships WHERE project_id = %s",
        (scope["project_id"],),
    ) == {"count": 1}
    assert await postgres.fetch_one(
        "SELECT count(*) AS count FROM relationship_evidence_refs "
        "WHERE message_id = %s",
        (message_id,),
    ) == {"count": 1}
    assert await postgres.fetch_one(
        "SELECT count(*) AS count FROM message_entity_refs WHERE message_id = %s",
        (message_id,),
    ) == {"count": 2}


async def _replay_once(scope, *, redis, pipeline, graph_writer, max_attempts=2):
    replay = DLQReplayJob(
        entities=pipeline.entities,
        processor=pipeline,
        write_to_graph=graph_writer,
        redis_client=redis,
        settings=DLQSettings(max_attempts=max_attempts),
    )
    result = await replay.execute(JobContext(scope["user_name"], scope["project_id"]))
    return result


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_pipeline_persists_entities_relationships_and_checkpoint(
    real_server_scope, monkeypatch
):
    """The production pipeline crosses extraction, graph, AGE, and checkpoint boundaries."""

    _configure_session(monkeypatch)
    scope = real_server_scope
    context, worker, _pipeline, store, _entities = _runtime(
        scope,
        postgres=scope["postgres"],
        redis=scope["redis"],
    )
    try:
        message = await _accept_and_flush(
            context, worker, "Ada Lovelace met Grace Hopper."
        )
        await _assert_graph_state(scope, scope["postgres"], message.id)
        assert await scope["redis"].get(
            RedisKeys.last_processed(scope["user_name"], scope["session_id"])
        ) == str(message.id)
        assert (
            await scope["redis"].llen(
                RedisKeys.buffer(scope["user_name"], scope["session_id"])
            )
            == 0
        )
    finally:
        await worker.stop()


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_graph_transaction_failure_replays_without_resolver_cache(
    real_server_scope, monkeypatch
):
    """A process-loss-style graph failure replays serialized graph commands."""

    _configure_session(monkeypatch)
    scope = real_server_scope
    base_store = KnowledgeStore(scope["postgres"], _RecoveryEmbeddingService())
    failing_store = _FailOnceStore(base_store)
    context, worker, pipeline, _store, entities = _runtime(
        scope,
        postgres=scope["postgres"],
        redis=scope["redis"],
        store=failing_store,
    )
    try:
        message = await _accept_and_flush(
            context, worker, "Ada Lovelace met Grace Hopper."
        )
        queued = await scope["redis"].lrange(
            RedisKeys.dlq(scope["user_name"], scope["project_id"]), 0, -1
        )
        assert len(queued) == 1
        assert "graph_write" in queued[0]

        # The retry is reconstructed from Redis and must not depend on the
        # resolver cache that was intentionally cleared after the failed write.
        async def graph_writer(batch):
            return await _write_graph(
                batch, store=base_store, entities=entities, redis=scope["redis"]
            )

        result = await _replay_once(
            scope,
            redis=scope["redis"],
            pipeline=pipeline,
            graph_writer=graph_writer,
        )
        assert result.summary == "Processed 1: 1 retried, 0 parked"
        await _assert_graph_state(scope, scope["postgres"], message.id)
        assert (
            await scope["redis"].hget(
                RedisKeys.dlq_state(scope["user_name"], scope["project_id"]),
                ensure_dlq_id(json.loads(queued[0])),
            )
            == DLQ_STATUS_COMPLETED
        )
    finally:
        await worker.stop()


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_alias_persistence_failure_replays_checkpoint_only(
    real_server_scope, monkeypatch
):
    """A response loss after aliases commit does not repeat graph effects."""

    _configure_session(monkeypatch)
    scope = real_server_scope
    context, worker, pipeline, store, entities = _runtime(
        scope,
        postgres=scope["postgres"],
        redis=scope["redis"],
    )
    original_graph_writer = worker.write_to_graph
    failed = True

    async def fail_after_alias_persistence(batch):
        nonlocal failed
        result = await original_graph_writer(batch)
        if failed:
            failed = False
            raise ConnectionError(
                "ConnectionError: response lost after alias persistence"
            )
        return result

    worker.write_to_graph = fail_after_alias_persistence
    try:
        message = await _accept_and_flush(
            context, worker, "Ada Lovelace met Grace Hopper."
        )
        assert await scope["postgres"].fetch_one(
            "SELECT count(*) AS count FROM entity_aliases "
            "WHERE entity_id IN (SELECT entity_id FROM entities WHERE project_id = %s)",
            (scope["project_id"],),
        ) == {"count": 2}
        assert (
            await scope["redis"].llen(
                RedisKeys.dlq(scope["user_name"], scope["project_id"])
            )
            == 1
        )

        result = await _replay_once(
            scope,
            redis=scope["redis"],
            pipeline=pipeline,
            graph_writer=lambda batch: _write_graph(
                batch, store=store, entities=entities, redis=scope["redis"]
            ),
        )
        assert result.summary == "Processed 1: 1 retried, 0 parked"
        await _assert_graph_state(scope, scope["postgres"], message.id)
        assert await scope["redis"].get(
            RedisKeys.last_processed(scope["user_name"], scope["session_id"])
        ) == str(message.id)
    finally:
        await worker.stop()


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_dirty_marking_failure_replays_after_postgres_commit(
    real_server_scope, monkeypatch
):
    """A Redis dirty-set outage after SQL commit is replay-safe."""

    _configure_session(monkeypatch)
    scope = real_server_scope
    redis = _FailOnceDirtyRedis(scope["redis"])
    context, worker, pipeline, store, entities = _runtime(
        scope, postgres=scope["postgres"], redis=redis
    )
    try:
        message = await _accept_and_flush(
            context, worker, "Ada Lovelace met Grace Hopper."
        )
        assert (
            await scope["redis"].llen(
                RedisKeys.dlq(scope["user_name"], scope["project_id"])
            )
            == 1
        )
        result = await _replay_once(
            scope,
            redis=redis,
            pipeline=pipeline,
            graph_writer=lambda batch: _write_graph(
                batch, store=store, entities=entities, redis=redis
            ),
        )
        assert result.summary == "Processed 1: 1 retried, 0 parked"
        await _assert_graph_state(scope, scope["postgres"], message.id)
    finally:
        await worker.stop()


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_checkpoint_failure_replays_after_graph_commit(
    real_server_scope, monkeypatch
):
    """A checkpoint failure leaves graph state durable and replays only the checkpoint."""

    _configure_session(monkeypatch)
    scope = real_server_scope
    redis = _FailOnceCheckpointRedis(scope["redis"])
    context, worker, pipeline, store, entities = _runtime(
        scope, postgres=scope["postgres"], redis=redis
    )
    try:
        message = await _accept_and_flush(
            context, worker, "Ada Lovelace met Grace Hopper."
        )
        await _assert_graph_state(scope, scope["postgres"], message.id)
        assert (
            await scope["redis"].llen(
                RedisKeys.dlq(scope["user_name"], scope["project_id"])
            )
            == 1
        )
        result = await _replay_once(
            scope,
            redis=redis,
            pipeline=pipeline,
            graph_writer=lambda batch: _write_graph(
                batch, store=store, entities=entities, redis=redis
            ),
        )
        assert result.summary == "Processed 1: 1 retried, 0 parked"
        assert await scope["redis"].get(
            RedisKeys.last_processed(scope["user_name"], scope["session_id"])
        ) == str(message.id)
        await _assert_graph_state(scope, scope["postgres"], message.id)
    finally:
        await worker.stop()


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_durable_claim_completion_is_idempotent(
    real_server_scope, monkeypatch
):
    """A completed Postgres claim is not redriven by a later worker drain."""

    _configure_session(monkeypatch)
    scope = real_server_scope
    context, worker, _pipeline, _store, _entities = _runtime(
        scope, postgres=scope["postgres"], redis=scope["redis"]
    )
    try:
        message = await _accept_and_flush(context, worker, "Ada met Grace.")
        assert await scope["redis"].get(
            RedisKeys.last_processed(scope["user_name"], scope["session_id"])
        ) == str(message.id)

        await worker._drain_durable_queue()
        await _assert_graph_state(scope, scope["postgres"], message.id)
        assert await scope["postgres"].fetch_one(
            "SELECT ingestion_state FROM messages WHERE message_id = %s",
            (message.id,),
        ) == {"ingestion_state": "processed"}
    finally:
        await worker.stop()


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_dlq_persistence_keeps_failed_durable_claim_blocked(
    real_server_scope, monkeypatch
):
    """A graph failure is captured once without silently redriving the claim."""

    _configure_session(monkeypatch)
    scope = real_server_scope
    context, worker, pipeline, store, entities = _runtime(
        scope, postgres=scope["postgres"], redis=scope["redis"]
    )
    original_graph_writer = worker.write_to_graph
    failed = True

    async def fail_graph_once(batch):
        nonlocal failed
        if failed:
            failed = False
            return False, "transient graph outage"
        return await original_graph_writer(batch)

    worker.write_to_graph = fail_graph_once
    try:
        await _accept_and_flush(context, worker, "Ada Lovelace met Grace Hopper.")
        dlq_key = RedisKeys.dlq(scope["user_name"], scope["project_id"])
        assert await scope["redis"].llen(dlq_key) == 1

        await worker._drain_durable_queue()
        assert await scope["redis"].llen(dlq_key) == 1
        assert await scope["postgres"].fetch_one(
            "SELECT ingestion_state FROM messages WHERE session_id = %s",
            (scope["session_id"],),
        ) == {"ingestion_state": "blocked"}
        assert (
            await scope["redis"].llen(
                RedisKeys.dlq_parked(scope["user_name"], scope["project_id"])
            )
            == 0
        )
    finally:
        await worker.stop()


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_worker_shutdown_drains_active_batch_and_restarts_cleanly(
    real_server_scope, monkeypatch
):
    """Shutdown during graph work leaves no half-acknowledged source batch."""

    _configure_session(monkeypatch)
    scope = real_server_scope
    context, worker, _pipeline, store, _entities = _runtime(
        scope,
        postgres=scope["postgres"],
        redis=scope["redis"],
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    original_write = worker.write_to_graph

    async def blocked_graph_write(batch):
        entered.set()
        await release.wait()
        return await original_write(batch)

    worker.write_to_graph = blocked_graph_write
    worker.start()
    try:
        await context.add(
            Message(
                content="Ada Lovelace met Grace Hopper.",
                timestamp=datetime(2026, 8, 1, 17, 1, tzinfo=timezone.utc),
            )
        )
        await asyncio.sleep(1.05)
        worker.signal()
        await asyncio.wait_for(entered.wait(), timeout=5)
        stopping = asyncio.create_task(worker.stop())
        await asyncio.sleep(0)
        release.set()
        await stopping

        assert (
            await scope["redis"].get(
                RedisKeys.last_processed(scope["user_name"], scope["session_id"])
            )
            is not None
        )
        assert await scope["postgres"].fetch_one(
            "SELECT count(*) AS count FROM entities WHERE project_id = %s",
            (scope["project_id"],),
        ) == {"count": 2}
    finally:
        await worker.stop()
