"""Real PostgreSQL contracts for composed server workflows."""

import asyncio
import json
import os
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from common.conf.manager import ConfigManager
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
)
from common.schema.source.references import SourceReferenceCandidate
from core.agent.executor import AgentExecutor
from core.agent.orchestrator import AgentOrchestrator
from core.agent.run import AgentIdentity, AgentRun, AgentRunLimits
from core.agent.sources.pasted_text import build_pasted_text_candidates
from core.agent.tools.registry import Tools
from core.ingestion.batch import IngestionBatch
from core.ingestion.worker import IngestionWorker
from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
from core.knowledge.documents import DocumentService
from core.knowledge.entity.resolver import EntityResolver
from core.knowledge.episodes.job import EpisodeJob
from core.knowledge.retrieval import KnowledgeRetrieval
from core.knowledge.store import KnowledgeStore
from infrastructure.postgres_client import PostgresClient
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

    async def encode_single(self, text):
        return [0.25] * 1024


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


class _DeterministicDocumentAgentLLM(_DeterministicAgentLLM):
    """Drive the canonical runtime through document search and synthesis."""

    def __init__(self):
        super().__init__()
        self.steps = [
            (
                "search_documents",
                '{"query": "violet launch phrase", "limit": 1}',
                "document-search-1",
            ),
            (
                "submit_answer",
                '{"content": "The document records the violet launch phrase."}',
                "answer-1",
            ),
            (
                "submit_answer",
                '{"content": "The document records the violet launch phrase."}',
                "answer-2",
            ),
        ]


class _StaticAgentManager:
    """Small durable-agent boundary substitute for a deterministic runtime test."""

    def __init__(self, config: AgentConfig):
        self._config = config

    async def get_default_agent_id(self) -> str:
        return self._config.id

    async def get_agent(self, agent_id: str) -> AgentConfig | None:
        return self._config if agent_id == self._config.id else None

    async def mark_turn_completed(self, agent_id: str) -> bool:
        return agent_id == self._config.id


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
            "user_name": user_name,
            "project_id": project_id,
            "session_id": session_id,
        }
    finally:
        await ProjectDeletionWriter(postgres).delete_project(
            user_name=user_name,
            project_id=project_id,
        )
        await postgres.close()


def _session(resources, *, user_name, project_id, session_id):
    context = Session(user_name, resources)
    context.session_id = session_id
    context.project_id = project_id
    context.project = SimpleNamespace(
        scheduler=object(),
        record_session_activity=lambda: asyncio.sleep(0),
        readable_project_ids=[project_id],
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
@pytest.mark.no_network
async def test_real_server_flow_reaches_episode_and_grounded_answer(
    real_server_scope,
    monkeypatch,
):
    """Session acceptance survives the worker boundary and becomes an episode."""

    scope = real_server_scope
    postgres = scope["postgres"]
    store = KnowledgeStore(postgres, _DeterministicEmbeddingService())
    resources = SimpleNamespace(
        postgres=postgres,
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
        retrieval = KnowledgeRetrieval(
            project_id=scope["project_id"],
            readable_project_ids=[scope["project_id"]],
            user_name=scope["user_name"],
            entities=resolver,
            embedding_service=resolver.embedding_service,
            knowledge_store=store,
            postgres=postgres,
        )
        tools = Tools(
            scope["user_name"],
            resolver,
            scope["session_id"],
            knowledge_retrieval=retrieval,
            knowledge_store=store,
            postgres=postgres,
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
                ).execute()
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
@pytest.mark.no_network
async def test_real_document_request_persists_document_source_provenance(
    real_server_scope,
    monkeypatch,
):
    """One canonical turn carries document-tool evidence through to durable answer refs."""

    scope = real_server_scope
    postgres = scope["postgres"]
    embedding = _DeterministicEmbeddingService()
    llm = _DeterministicDocumentAgentLLM()
    store = KnowledgeStore(postgres, embedding)
    resources = SimpleNamespace(
        postgres=postgres,
        knowledge_store=store,
        embedding=embedding,
        llm_service=llm,
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
            )
        ),
    )

    documents = DocumentService(
        project_id=scope["project_id"],
        postgres_client=postgres,
        embedding_service=embedding,
        document_rerank_enabled=False,
    )
    document = await documents.submit_document(
        content=b"The violet launch phrase is durable and documented.\n",
        original_name="launch-note.md",
        relative_path="notes/launch-note.md",
        visibility_scope="project",
    )
    assert document["status"] == "indexed"

    resolver = EntityResolver(
        store,
        embedding,
        scope["project_id"],
        [scope["project_id"]],
    )
    retrieval = KnowledgeRetrieval(
        project_id=scope["project_id"],
        readable_project_ids=[scope["project_id"]],
        user_name=scope["user_name"],
        entities=resolver,
        embedding_service=embedding,
        knowledge_store=store,
        postgres=postgres,
    )
    context = _session(
        resources,
        user_name=scope["user_name"],
        project_id=scope["project_id"],
        session_id=scope["session_id"],
    )
    context.project = SimpleNamespace(
        scheduler=object(),
        record_session_activity=lambda: asyncio.sleep(0),
        readable_project_ids=[scope["project_id"]],
        entities=resolver,
        compiled_domain=make_domain_config().compile(),
        knowledge_retrieval=retrieval,
        workspace_service=None,
    )
    context.document_service = documents
    context.consumer = _SignalCounter()

    agent = AgentConfig(
        id="document-flow-agent",
        name="Document Flow Agent",
        persona={
            "attention_bias": "evidence",
            "reasoning_style": "methodical",
            "social_temperament": "calm",
            "communication_signature": "clear",
            "productive_flaw": "overexplains",
        },
        enabled_tools=["search_documents"],
    )
    orchestrator = AgentOrchestrator(
        _StaticAgentManager(agent),
        config_provider=ConfigManager,
    )

    events = [
        event
        async for event in context.run_agent_stream(
            Message(
                content="What is the violet launch phrase?",
                timestamp=datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
            ),
            orchestrator=orchestrator,
            enabled_tools=["search_documents"],
        )
    ]

    assert not [event for event in events if event["event"] == "error"]
    response = next(event for event in events if event["event"] == "response")
    assert any(event["event"] == "tool_end" for event in events)
    assert len(response["data"]["source_ref_ids"]) == 1

    answer = await store.get_assistant_message_with_sources(
        response["data"]["assistant_message_id"],
        user_name=scope["user_name"],
        project_id=scope["project_id"],
        session_id=scope["session_id"],
    )
    assert answer is not None
    assert len(answer.sources_consulted) == 1
    source = answer.sources_consulted[0]
    assert source.source_kind == "text_document"
    assert source.document_id == document["document_id"]
    assert source.source_project_id == scope["project_id"]
    assert source.contributing_message_id == response["data"]["assistant_message_id"]

    source_row = await postgres.fetch_one(
        """
            SELECT project_id, source_project_id, document_id::text AS document_id, content_hash,
               encounter_kind, tool_call_id
        FROM message_source_refs
        WHERE source_ref_id = %s
        """,
        (response["data"]["source_ref_ids"][0],),
    )
    assert source_row == {
        "project_id": scope["project_id"],
        "source_project_id": scope["project_id"],
        "document_id": document["document_id"],
        "content_hash": document["content_hash"],
        "encounter_kind": "document_search",
        "tool_call_id": "document-search-1",
    }


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
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


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
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
