"""Real PostgreSQL/Redis contracts for composed server workflows."""

import asyncio
import json
import os
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from common.schema.agent.identity import AgentConfig
from common.schema.episode.generation import (
    LLMEpisodeDecision,
    LLMEpisodeWindowDecision,
)
from common.schema.ingestion.contracts import IngestionCommit
from common.schema.primitives import Message
from common.schema.settings import (
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
from core.ingestion.worker import IngestionWorker
from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
from core.knowledge.entity.resolver import EntityResolver
from core.knowledge.episodes.job import EpisodeJob
from core.knowledge.store import KnowledgeStore
from infrastructure.postgres_client import PostgresClient
from infrastructure.redis_client import AsyncRedisClient, RedisKeys
from runtime.session_runtime import SessionRuntime as Session
from tests.fixtures.factories import make_domain_config
from tests.fixtures.ingestion import ingestion_policy


class _DeterministicEpisodeLLM:
    async def generate_structured(self, **kwargs):
        assert kwargs["response_model"] is LLMEpisodeWindowDecision
        return LLMEpisodeWindowDecision(
            proposals=[
                LLMEpisodeDecision(
                    action="create",
                    summary="The accepted message is now durable episodic memory.",
                    new_developments=[
                        "The complete server path is grounded in its source."
                    ],
                    message_influences=[
                        {
                            "message_id": "m1",
                            "influence_weight": 1.0,
                            "influence_reason": "The accepted message records the decision.",
                        }
                    ],
                )
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

    def capture_policy(self):
        return ingestion_policy()

    def open_batch(self, messages, session_text, *, session_id, policy, batch_id=None):
        return IngestionBatch.open(
            user_name=self.user_name,
            project_id=self.project_id,
            session_id=session_id,
            messages=messages,
            session_text=session_text,
            policy=policy,
            batch_id=batch_id,
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
        )
        batch.set_relationship_observations([])
        batch.complete()


class _SignalCounter:
    def __init__(self):
        self.calls = 0

    def signal(self):
        self.calls += 1


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
        """
        INSERT INTO projects (project_id, user_name, name, domain_config)
        VALUES (%s, %s, %s, %s)
        """,
        (
            project_id,
            user_name,
            "Server flow integration",
            json.dumps(asdict(make_domain_config())),
        ),
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
    context = Session(user_name, resources)
    context.session_id = session_id
    context.project_id = project_id
    context.project = SimpleNamespace(
        scheduler=object(),
        record_session_activity=lambda: asyncio.sleep(0),
    )
    return context


def _prepared_graph_callback(store):
    async def write_graph(batch: IngestionBatch):
        return await store.commit_ingestion(
            IngestionCommit(
                scope=batch.scope,
                batch_id=batch.batch_id,
                message_ids=tuple(int(message["id"]) for message in batch.messages),
            )
        )

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
        property(
            lambda self: SimpleNamespace(
                developer_settings=SimpleNamespace(
                    limits=SimpleNamespace(conversation_context_turns=100),
                    ingestion=SimpleNamespace(message_edit_window_seconds=1),
                )
            ),
        ),
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
            ingestion_batch_settle_delay_seconds=0,
        ),
    )
    context.consumer = worker
    worker.start()

    try:
        accepted_messages = []
        for index in range(8):
            content = (
                "The complete server path must remain grounded."
                if index == 7
                else f"Server-flow episode context {index + 1}."
            )
            accepted_messages.append(
                await context.add(
                    Message(
                        content=content,
                        timestamp=datetime(2026, 8, 1, 12, index, tzinfo=timezone.utc),
                    )
                )
            )
        await asyncio.sleep(1.05)
        accepted = accepted_messages[-1]
        await worker.flush()

        message = await postgres.fetch_one(
            "SELECT role, content, episode_eligible, ingestion_state FROM messages "
            "WHERE message_id = %s",
            (accepted.id,),
        )
        assert message == {
            "role": "user",
            "content": "The complete server path must remain grounded.",
            "episode_eligible": True,
            "ingestion_state": "processed",
        }

        episode_job = EpisodeJob(
            knowledge_store=store,
            settings=EpisodeSettings(),
            episode_window_size=8,
            llm=_DeterministicEpisodeLLM(),
            embedding_service=_DeterministicEmbeddingService(),
        )
        build = await episode_job.process_next_window(
            user_name=scope["user_name"],
            project_id=scope["project_id"],
        )
        assert build is not None
        assert len(build.final_episodes) == 1

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
        property(
            lambda self: SimpleNamespace(
                developer_settings=SimpleNamespace(
                    limits=SimpleNamespace(conversation_context_turns=100),
                    ingestion=SimpleNamespace(message_edit_window_seconds=1),
                )
            ),
        ),
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
    assert (
        await scope["redis"].get(
            RedisKeys.heartbeat_counter(scope["user_name"], scope["session_id"])
        )
        == "1"
    )


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_worker_processes_message_persisted_during_acceptance(
    real_server_scope,
    monkeypatch,
):
    """Acceptance persists the canonical message before the worker processes it."""

    scope = real_server_scope
    real_store = KnowledgeStore(scope["postgres"], _DeterministicEmbeddingService())
    resources = SimpleNamespace(
        postgres=scope["postgres"],
        redis=scope["redis"],
        knowledge_store=real_store,
        embedding=_DeterministicEmbeddingService(),
    )
    monkeypatch.setattr(
        Session,
        "current_config",
        property(
            lambda self: SimpleNamespace(
                developer_settings=SimpleNamespace(
                    limits=SimpleNamespace(conversation_context_turns=100),
                    ingestion=SimpleNamespace(message_edit_window_seconds=1),
                )
            ),
        ),
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
        knowledge_store=real_store,
        processor=processor,
        redis=scope["redis"],
        get_session_context=context.get_conversation_context,
        write_to_graph=_prepared_graph_callback(real_store),
        settings=IngestionSettings(
            batch_size=1,
            batch_debounce_seconds=0,
            batch_timeout=10,
            ingestion_batch_settle_delay_seconds=0,
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
        await asyncio.sleep(1.05)
        await worker.flush()
        assert await scope["postgres"].fetch_one(
            "SELECT ingestion_state FROM messages WHERE message_id = %s",
            (accepted.id,),
        ) == {"ingestion_state": "processed"}
    finally:
        await worker.stop()
