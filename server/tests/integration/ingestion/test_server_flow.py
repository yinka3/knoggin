"""Real PostgreSQL/Redis contracts for composed server workflows."""

import asyncio
import json
import os
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from common.schema.agent.identity import AgentConfig
from common.schema.episode.generation import LLMEpisodeDecision
from common.schema.ingestion.contracts import EpisodeEligibility
from common.schema.primitives import Message
from common.schema.settings import (
    DLQSettings,
    EpisodeSettings,
    IngestionSettings,
    RedisConnectionSettings,
)
from common.schema.source.references import SourceReferenceCandidate
from core.agent.executor import AgentExecutor
from core.agent.run import AgentIdentity, AgentRun, AgentRunLimits
from core.agent.sources.pasted_text import build_pasted_text_candidates
from core.agent.tools.registry import Tools
from core.ingestion.batch import IngestionBatch
from core.ingestion.pipeline import IngestionPipeline
from core.ingestion.recovery.dlq_state import DLQ_STATUS_COMPLETED
from core.ingestion.recovery.replay_job import DLQReplayJob
from core.ingestion.worker import IngestionWorker
from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
from core.knowledge.entity.resolver import EntityResolver
from core.knowledge.episodes.job import EpisodeJob
from core.knowledge.store import KnowledgeStore
from core.session.context import Session
from infrastructure.job.base import JobContext
from infrastructure.postgres_client import PostgresClient
from infrastructure.redis_client import AsyncRedisClient, RedisKeys
from infrastructure.work_record import WorkRecord


class _DeterministicEpisodeLLM:
    async def generate_structured(self, **kwargs):
        assert kwargs["response_model"] is LLMEpisodeDecision
        return LLMEpisodeDecision(
            action="create",
            summary="The accepted message is now durable episodic memory.",
            new_developments=["The complete server path is grounded in its source."],
            message_influences=[
                {
                    "message_id": "m1",
                    "influence_weight": 1.0,
                    "influence_reason": "The accepted message records the decision.",
                }
            ],
        )


class _DeterministicEmbeddingService:
    async def encode(self, texts):
        assert len(texts) == 1
        return [[0.25] * 1024]


class _DeterministicAgentLLM:
    """Drive the real executor through retrieval and final synthesis."""

    agent_model = "architect"
    extraction_model = "librarian"

    def __init__(self):
        self.steps = [
            (
                "search_messages",
                '{"query": "grounded", "limit": 3}',
                "search-1",
            ),
            (
                "submit_answer",
                '{"content": "The source is grounded."}',
                "answer-1",
            ),
            (
                "submit_answer",
                '{"content": "The source is grounded and retrievable."}',
                "answer-2",
            ),
        ]
        self.calls = []

    async def stream_with_tools(self, **kwargs):
        self.calls.append(kwargs)
        name, arguments, call_id = self.steps.pop(0)
        yield {
            "event": "tool_calls",
            "data": {
                "content": f"Calling {name}",
                "calls": [{"name": name, "arguments": arguments, "id": call_id}],
            },
        }
        yield {
            "event": "step_completed",
            "data": {
                "usage": {
                    "prompt_tokens": 3,
                    "completion_tokens": 2,
                    "total_tokens": 5,
                    "approximate": False,
                }
            },
        }

    def count_tokens(self, text):
        return len(text.split())

    async def generate_text(self, **_kwargs):
        return "The source is grounded and retrievable."


class _NoEntityProcessor:
    """Complete the pipeline boundary without requiring an external LLM."""

    def __init__(self, project_id: str, user_name: str):
        self.project_id = project_id
        self.user_name = user_name
        self.knowledge_store = object()

    def open_batch(self, messages, session_text, *, session_id):
        return IngestionBatch.open(
            user_name=self.user_name,
            project_id=self.project_id,
            session_id=session_id,
            messages=messages,
            session_text=session_text,
        )

    async def process(self, batch: IngestionBatch):
        batch.validate_input()
        batch.mark_extracted()
        batch.set_resolution(
            entity_ids=[],
            new_entity_ids=[],
            alias_updated_ids=[],
            entity_message_map={},
            alias_updates={},
            candidate_suggestions=[],
        )
        batch.set_relationship_observations([])
        batch.complete()

    async def move_to_dead_letter(self, *args, **kwargs):
        raise AssertionError("the deterministic success path must not use the DLQ")


class _SignalCounter:
    def __init__(self):
        self.calls = 0

    def signal(self):
        self.calls += 1


class _FailOnceStore:
    """Delegate to the real store while injecting one message-log failure."""

    def __init__(self, store):
        self.store = store
        self.fail_message_logs = True

    async def save_message_logs(self, messages):
        if self.fail_message_logs:
            self.fail_message_logs = False
            raise ConnectionError("simulated message-log outage")
        return await self.store.save_message_logs(messages)


class _CheckpointDlqProcessor(_NoEntityProcessor):
    """Use the production DLQ serializer with the deterministic processor."""

    def __init__(self, project_id, user_name, store, redis):
        super().__init__(project_id, user_name)
        self._dlq_pipeline = IngestionPipeline(
            project_id=project_id,
            redis_client=redis,
            llm=None,
            entities=None,
            processor=None,
            cpu_executor=None,
            user_name=user_name,
            topic_config=None,
            get_next_ent_id=None,
            knowledge_store=store,
        )

    async def move_to_dead_letter(self, *args, **kwargs):
        return await self._dlq_pipeline.move_to_dead_letter(*args, **kwargs)


class _FailOnceEvalRedis:
    """Delegate to real Redis while dropping one checkpoint execution."""

    def __init__(self, client):
        self.client = client
        self.fail_eval = True

    def __getattr__(self, name):
        return getattr(self.client, name)

    async def eval(self, *args, **kwargs):
        if self.fail_eval:
            self.fail_eval = False
            raise ConnectionError("ConnectionError: simulated checkpoint outage")
        return await self.client.eval(*args, **kwargs)


@pytest.fixture
async def real_server_scope():
    dsn = os.environ.get(
        "KNOGGIN_TEST_DATABASE_URL",
        "postgresql://knoggin:knoggin@localhost:5432/knoggin_db",
    )
    postgres = PostgresClient(dsn=dsn, min_size=1, max_size=2)
    await postgres.connect()
    redis_manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    redis = await redis_manager.connect()
    suffix = uuid.uuid4().hex[:12]
    user_name = "server_flow_test_user"
    project_id = f"server-flow-project-{suffix}"
    session_id = f"server-flow-session-{suffix}"
    await postgres.execute(
        "INSERT INTO projects (project_id, user_name, name) VALUES (%s, %s, %s)",
        (project_id, user_name, "Server flow integration"),
    )
    await postgres.execute(
        "INSERT INTO sessions (session_id, user_name, project_id) VALUES (%s, %s, %s)",
        (session_id, user_name, project_id),
    )
    try:
        yield {
            "postgres": postgres,
            "redis": redis,
            "redis_manager": redis_manager,
            "user_name": user_name,
            "project_id": project_id,
            "session_id": session_id,
        }
    finally:
        await ProjectDeletionWriter(postgres).delete_project(
            user_name=user_name,
            project_id=project_id,
        )
        keys = set(RedisKeys.project_cleanup_keys(user_name, project_id))
        keys.update(RedisKeys.session_keys(user_name, session_id))
        async for key in redis.scan_iter(
            match=RedisKeys.message_dedup_pattern(user_name, session_id)
        ):
            keys.add(key)
        if keys:
            await redis.delete(*keys)
        await redis_manager.close()
        await postgres.close()


def _session(resources, *, user_name, project_id, session_id):
    context = Session(user_name, [], resources)
    context.session_id = session_id
    context.project_id = project_id
    context.project = SimpleNamespace(
        scheduler=object(),
        record_session_activity=lambda: asyncio.sleep(0),
    )
    return context


def _prepared_graph_callback(store):
    async def write_graph(batch: IngestionBatch):
        eligible_messages = [
            EpisodeEligibility(message_id=int(message["id"]))
            for message in batch.messages
        ]
        batch.set_graph_write_buffers(
            graph_work_unit=WorkRecord.for_graph_write(batch.scope),
            safe_entity_ids=set(),
            graph_alias_updates=[],
            entity_writes=[],
            relationship_writes=[],
            message_entity_refs=[],
            eligible_messages=eligible_messages,
            skipped_relationships=[],
            zombie_entity_ids=set(),
            dirty_entity_ids=set(),
        )
        batch.seal_for_commit()
        await store.write_batch(
            [],
            [],
            eligible_messages=batch.eligible_messages,
            scope=batch.scope,
        )
        batch.mark_graph_committed()
        return True, None

    return write_graph


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_server_flow_reaches_episode_and_grounded_answer(
    real_server_scope,
    monkeypatch,
):
    """Session acceptance survives the worker boundary and becomes an episode."""

    scope = real_server_scope
    postgres = scope["postgres"]
    redis = scope["redis"]
    store = KnowledgeStore(postgres, _DeterministicEmbeddingService())
    resources = SimpleNamespace(
        postgres=postgres,
        redis=redis,
        knowledge_store=store,
        embedding=_DeterministicEmbeddingService(),
    )
    monkeypatch.setattr(
        Session,
        "current_config",
        property(lambda self: SimpleNamespace(developer_settings=SimpleNamespace(
            limits=SimpleNamespace(conversation_context_turns=100)
        )),),
    )
    context = _session(
        resources,
        user_name=scope["user_name"],
        project_id=scope["project_id"],
        session_id=scope["session_id"],
    )
    processor = _NoEntityProcessor(scope["project_id"], scope["user_name"])
    worker = IngestionWorker(
        user_name=scope["user_name"],
        session_id=scope["session_id"],
        knowledge_store=store,
        processor=processor,
        redis=redis,
        get_session_context=context.get_conversation_context,
        write_to_graph=_prepared_graph_callback(store),
        settings=IngestionSettings(
            batch_size=1,
            batch_debounce_seconds=0,
            batch_timeout=10,
            checkpoint_interval=1,
        ),
    )
    context.consumer = worker
    worker.start()

    try:
        accepted = await context.add(
            Message(
                content="The complete server path must remain grounded.",
                timestamp=datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
            )
        )
        await worker.flush()

        assert await redis.llen(RedisKeys.buffer(scope["user_name"], scope["session_id"])) == 0
        assert await redis.get(
            RedisKeys.last_processed(scope["user_name"], scope["session_id"])
        ) == str(accepted.id)
        message = await postgres.fetch_one(
            "SELECT role, content, episode_eligible FROM messages "
            "WHERE message_id = %s",
            (accepted.id,),
        )
        assert message == {
            "role": "user",
            "content": "The complete server path must remain grounded.",
            "episode_eligible": True,
        }

        episode_job = EpisodeJob(
            knowledge_store=store,
            settings=EpisodeSettings(batch_multiple=1, max_message_count=1),
            ingestion_settings=IngestionSettings(batch_size=1),
            llm=_DeterministicEpisodeLLM(),
            embedding_service=_DeterministicEmbeddingService(),
        )
        build = await episode_job.process_next_window(
            user_name=scope["user_name"],
            project_id=scope["project_id"],
            session_id=scope["session_id"],
        )
        assert build is not None
        assert build.outcome_episode_id

        agent_run_id = f"run-{uuid.uuid4().hex}"
        source_candidates = build_pasted_text_candidates(
            project_id=scope["project_id"],
            session_id=scope["session_id"],
            source_message_id=accepted.id,
            message_content="The complete server path must remain grounded.",
            agent_run_id=agent_run_id,
            spans=[{"start_char": 0, "end_char": 46}],
        )
        resolver = EntityResolver(
            store,
            _DeterministicEmbeddingService(),
            scope["project_id"],
            [scope["project_id"]],
        )
        tools = Tools(
            scope["user_name"],
            resolver,
            scope["session_id"],
            knowledge_store=store,
            postgres=postgres,
            redis=redis,
        )
        run = AgentRun.open(
            user_name=scope["user_name"],
            project_id=scope["project_id"],
            session_id=scope["session_id"],
            user_query="Use the grounded server path source.",
            run_id=agent_run_id,
            agent=AgentIdentity(
                config=AgentConfig(
                    id="server-flow-agent",
                    name="Server Flow Agent",
                    persona={
                        "attention_bias": "evidence",
                        "reasoning_style": "methodical",
                        "social_temperament": "calm",
                        "communication_signature": "clear",
                        "productive_flaw": "overexplains",
                    },
                ),
                name="Server Flow Agent",
                persona="Careful and evidence-led",
            ),
            limits=AgentRunLimits(max_attempts=3, max_calls=2),
            enabled_tools=["search_messages"],
            initial_source_candidates=source_candidates,
        )
        try:
            agent_events = [
                event
                async for event in AgentExecutor(
                    run,
                    _DeterministicAgentLLM(),
                    tools,
                ).execute(enabled_tools=["search_messages"])
            ]
        finally:
            await tools.close()

        response = next(event for event in agent_events if event["event"] == "response")
        assert any(event["event"] == "tool_end" for event in agent_events)
        persisted_candidates = [
            SourceReferenceCandidate.model_validate(candidate)
            for candidate in response["data"]["sources_consulted"]
        ]
        assert len(persisted_candidates) == 1
        assert persisted_candidates[0].source_message_id == accepted.id
        await context.add_assistant_turn(
            response["data"]["content"],
            datetime(2026, 8, 1, 12, 1, tzinfo=timezone.utc),
            user_msg_id=accepted.id,
            source_candidates=persisted_candidates,
        )
        assistant = await postgres.fetch_one(
            "SELECT message_id FROM messages WHERE session_id = %s AND role = 'assistant'",
            (scope["session_id"],),
        )
        assert assistant is not None
        answer = await store.get_assistant_message_with_sources(
            int(assistant["message_id"]),
            user_name=scope["user_name"],
            project_id=scope["project_id"],
            session_id=scope["session_id"],
        )
        assert answer is not None
        assert len(answer.sources_consulted) == 1
        assert answer.sources_consulted[0].source_message_id == accepted.id
    finally:
        await worker.stop()


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_concurrent_sessions_accept_one_message_once(
    real_server_scope,
    monkeypatch,
):
    """Separate runtime instances converge on one idempotent message identity."""

    scope = real_server_scope
    store = KnowledgeStore(scope["postgres"], _DeterministicEmbeddingService())
    resources = SimpleNamespace(
        postgres=scope["postgres"],
        redis=scope["redis"],
        knowledge_store=store,
        embedding=_DeterministicEmbeddingService(),
    )
    monkeypatch.setattr(
        Session,
        "current_config",
        property(lambda self: SimpleNamespace(developer_settings=SimpleNamespace(
            limits=SimpleNamespace(conversation_context_turns=100)
        )),),
    )
    contexts = [
        _session(
            resources,
            user_name=scope["user_name"],
            project_id=scope["project_id"],
            session_id=scope["session_id"],
        )
        for _ in range(2)
    ]
    for context in contexts:
        context.consumer = _SignalCounter()

    timestamp = datetime(2026, 8, 1, 13, 0, tzinfo=timezone.utc)
    results = await asyncio.gather(
        *[
            context.add(Message(content="same accepted turn", timestamp=timestamp))
            for context in contexts
        ]
    )

    assert {result.id for result in results} == {results[0].id}
    rows = await scope["postgres"].fetch_all(
        "SELECT message_id, content FROM messages "
        "WHERE session_id = %s AND role = 'user'",
        (scope["session_id"],),
    )
    assert rows == [{"message_id": results[0].id, "content": "same accepted turn"}]
    buffered = await scope["redis"].lrange(
        RedisKeys.buffer(scope["user_name"], scope["session_id"]),
        0,
        -1,
    )
    assert len(buffered) == 1
    assert json.loads(buffered[0])["id"] == results[0].id


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_worker_keeps_buffer_after_message_log_failure(
    real_server_scope,
    monkeypatch,
):
    """A message-log outage leaves the Redis buffer available for retry."""

    scope = real_server_scope
    real_store = KnowledgeStore(scope["postgres"], _DeterministicEmbeddingService())
    failing_store = _FailOnceStore(real_store)
    resources = SimpleNamespace(
        postgres=scope["postgres"],
        redis=scope["redis"],
        knowledge_store=real_store,
        embedding=_DeterministicEmbeddingService(),
    )
    monkeypatch.setattr(
        Session,
        "current_config",
        property(lambda self: SimpleNamespace(developer_settings=SimpleNamespace(
            limits=SimpleNamespace(conversation_context_turns=100)
        )),),
    )
    context = _session(
        resources,
        user_name=scope["user_name"],
        project_id=scope["project_id"],
        session_id=scope["session_id"],
    )
    processor = _NoEntityProcessor(scope["project_id"], scope["user_name"])
    processor.move_to_dead_letter = lambda *args, **kwargs: asyncio.sleep(0, result=False)
    worker = IngestionWorker(
        user_name=scope["user_name"],
        session_id=scope["session_id"],
        knowledge_store=failing_store,
        processor=processor,
        redis=scope["redis"],
        get_session_context=context.get_conversation_context,
        write_to_graph=_prepared_graph_callback(real_store),
        settings=IngestionSettings(
            batch_size=1,
            batch_debounce_seconds=0,
            batch_timeout=10,
            checkpoint_interval=1,
        ),
    )
    context.consumer = worker
    worker.start()

    try:
        accepted = await context.add(
            Message(
                content="Retry this message-log boundary.",
                timestamp=datetime(2026, 8, 1, 14, 0, tzinfo=timezone.utc),
            )
        )
        await worker.flush()
        buffer_key = RedisKeys.buffer(scope["user_name"], scope["session_id"])
        assert await scope["redis"].llen(buffer_key) == 1
        assert await scope["postgres"].fetch_one(
            "SELECT message_id FROM messages WHERE message_id = %s",
            (accepted.id,),
        )

        await worker.flush()
        assert await scope["redis"].llen(buffer_key) == 0
        assert await scope["redis"].get(
            RedisKeys.last_processed(scope["user_name"], scope["session_id"])
        ) == str(accepted.id)
    finally:
        await worker.stop()


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_worker_checkpoint_failure_replays_through_dlq(
    real_server_scope,
    monkeypatch,
):
    """A checkpoint outage becomes a real DLQ item and replays once."""

    scope = real_server_scope
    real_store = KnowledgeStore(scope["postgres"], _DeterministicEmbeddingService())
    redis_proxy = _FailOnceEvalRedis(scope["redis"])
    resources = SimpleNamespace(
        postgres=scope["postgres"],
        redis=scope["redis"],
        knowledge_store=real_store,
        embedding=_DeterministicEmbeddingService(),
    )
    monkeypatch.setattr(
        Session,
        "current_config",
        property(lambda self: SimpleNamespace(developer_settings=SimpleNamespace(
            limits=SimpleNamespace(conversation_context_turns=100)
        )),),
    )
    context = _session(
        resources,
        user_name=scope["user_name"],
        project_id=scope["project_id"],
        session_id=scope["session_id"],
    )
    processor = _CheckpointDlqProcessor(
        scope["project_id"], scope["user_name"], real_store, redis_proxy
    )
    worker = IngestionWorker(
        user_name=scope["user_name"],
        session_id=scope["session_id"],
        knowledge_store=real_store,
        processor=processor,
        redis=redis_proxy,
        get_session_context=context.get_conversation_context,
        write_to_graph=_prepared_graph_callback(real_store),
        settings=IngestionSettings(
            batch_size=1,
            batch_debounce_seconds=0,
            batch_timeout=10,
            checkpoint_interval=1,
        ),
    )
    context.consumer = worker
    worker.start()

    try:
        accepted = await context.add(
            Message(
                content="Retry this checkpoint boundary.",
                timestamp=datetime(2026, 8, 1, 15, 0, tzinfo=timezone.utc),
            )
        )
        await worker.flush()

        dlq_key = RedisKeys.dlq(scope["user_name"], scope["project_id"])
        queued = await scope["redis"].lrange(dlq_key, 0, -1)
        assert len(queued) == 1
        assert json.loads(queued[0])["stage"] == "checkpoint"
        assert await scope["redis"].llen(
            RedisKeys.buffer(scope["user_name"], scope["session_id"])
        ) == 0

        replay = DLQReplayJob(
            entities=SimpleNamespace(project_id=scope["project_id"]),
            processor=SimpleNamespace(knowledge_store=real_store),
            write_to_graph=None,
            redis_client=scope["redis"],
            settings=DLQSettings(max_attempts=2),
        )
        monkeypatch.setattr("core.ingestion.recovery.replay_job.emit", lambda *a, **k: asyncio.sleep(0))
        result = await replay.execute(
            JobContext(scope["user_name"], scope["project_id"])
        )

        assert result.summary == "Processed 1: 1 retried, 0 parked"
        assert await scope["redis"].get(
            RedisKeys.last_processed(scope["user_name"], scope["session_id"])
        ) == str(accepted.id)
        assert await scope["redis"].hget(
            RedisKeys.dlq_state(scope["user_name"], scope["project_id"]),
            json.loads(queued[0])["dlq_id"],
        ) == DLQ_STATUS_COMPLETED
    finally:
        await worker.stop()
