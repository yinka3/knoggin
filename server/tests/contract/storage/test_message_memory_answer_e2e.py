"""Service-backed message -> episodic memory -> answer retrieval contract."""

import hashlib
import json
import os
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import pytest_asyncio

import core.knowledge.retrieval as retrieval_module
from common.schema.episode.generation import (
    LLMEpisodeDecision,
    LLMEpisodeWindowDecision,
)
from common.schema.primitives import Message
from common.schema.settings import (
    EpisodeSettings,
)
from common.schema.source.locators import PastedTextLocator
from common.schema.source.references import SourceReferenceCandidate
from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
from core.knowledge.db.writers.session_deletion_writer import SessionDeletionWriter
from core.knowledge.episodes.job import EpisodeJob
from core.knowledge.retrieval import KnowledgeRetrieval
from core.knowledge.store import KnowledgeStore
from infrastructure.postgres_client import PostgresClient
from runtime.session_runtime import SessionRuntime as Session
from tests.fixtures.factories import make_domain_config
from tests.fixtures.fakes import FakeConfigValue, FakeIngestionWorker


@pytest_asyncio.fixture
async def isolated_e2e_scope():
    """Use one unique project and clean only that project after the test."""

    dsn = os.environ.get(
        "KNOGGIN_TEST_DATABASE_URL",
        "postgresql://knoggin:knoggin@localhost:5432/knoggin_db",
    )
    client = PostgresClient(dsn=dsn, min_size=1, max_size=2)
    await client.connect()
    suffix = uuid.uuid4().hex[:12]
    user_name = "e2e_test_user"
    project_id = f"e2e-project-{suffix}"
    session_id = f"e2e-session-{suffix}"
    project_created = False
    try:
        await client.execute(
            """
            INSERT INTO projects (project_id, user_name, name, domain_config)
            VALUES (%s, %s, %s, %s)
            """,
            (
                project_id,
                user_name,
                "Message memory e2e",
                json.dumps(asdict(make_domain_config())),
            ),
        )
        project_created = True
        await client.execute(
            """
            INSERT INTO sessions (session_id, user_name, project_id)
            VALUES (%s, %s, %s)
            """,
            (session_id, user_name, project_id),
        )
        yield {
            "client": client,
            "user_name": user_name,
            "project_id": project_id,
            "session_id": session_id,
        }
    finally:
        if project_created:
            await ProjectDeletionWriter(client).delete_project(
                user_name=user_name,
                project_id=project_id,
            )
        await client.close()


class DeterministicEpisodeLLM:
    """Return a valid local-reference decision without network access."""

    async def generate_structured(self, **kwargs):
        assert kwargs["response_model"] is LLMEpisodeWindowDecision
        return LLMEpisodeWindowDecision(
            proposals=[
                LLMEpisodeDecision(
                    action="create",
                    summary="The team agreed to use durable episodic memory for retrieval.",
                    new_developments=["The memory path is now grounded in source messages."],
                    message_influences=["message:1", "message:2"],
                )
            ],
        )


class DeterministicEmbeddingService:
    async def encode(self, texts):
        assert len(texts) == 1
        return [[0.25] * 1024]


class EpisodeRetrievalTool(KnowledgeRetrieval):
    """Minimal real-store wiring for the canonical retrieval boundary."""

    def __init__(self, knowledge_store):
        super().__init__(
            project_id="project-1",
            readable_project_ids=["project-1"],
            user_name="ada",
            entities=SimpleNamespace(),
            embedding_service=None,
            knowledge_store=knowledge_store,
            postgres=SimpleNamespace(),
        )

    async def search_messages(self, query, *, session_id, limit=None):
        raise AssertionError(f"raw-message fallback was not expected for {query!r}")


@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_messages_become_grounded_memory_and_are_returned_as_answer_context(
    isolated_e2e_scope,
    monkeypatch,
):
    """Persist one eligible message window, then retrieve its answer context."""

    real_postgres_client = isolated_e2e_scope["client"]
    user_name = isolated_e2e_scope["user_name"]
    project_id = isolated_e2e_scope["project_id"]
    session_id = isolated_e2e_scope["session_id"]
    message_rows = await real_postgres_client.fetch_all(
        "SELECT nextval('public.message_id_seq') AS message_id "
        "FROM generate_series(1, 8)"
    )
    message_ids = [int(row["message_id"]) for row in message_rows]
    first_message_id, second_message_id = message_ids[:2]
    contents = [
        "We need durable episodic memory for retrieval.",
        "Agreed: source messages must ground every memory answer.",
        *[f"Supporting retrieval context {index}." for index in range(3, 9)],
    ]
    for index in range(0, len(message_ids), 2):
        user_message_id = message_ids[index]
        assistant_message_id = message_ids[index + 1]
        await real_postgres_client.execute(
            """
            INSERT INTO messages (
                user_name, session_id, message_id, project_id, role, content,
                timestamp_ms, user_msg_id, lifecycle_state, ingestion_state
            )
            VALUES
                (%s, %s, %s, %s, 'user', %s, %s, %s, 'sealed', 'processed'),
                (%s, %s, %s, %s, 'assistant', %s, %s, %s, 'sealed', 'excluded')
            """,
            (
                user_name,
                session_id,
                user_message_id,
                project_id,
                contents[index],
                1700000000000 + index * 1000,
                user_message_id,
                user_name,
                session_id,
                assistant_message_id,
                project_id,
                contents[index + 1],
                1700000000000 + (index + 1) * 1000,
                user_message_id,
            ),
        )

    embedding_service = DeterministicEmbeddingService()
    knowledge_store = KnowledgeStore(real_postgres_client, embedding_service)
    job = EpisodeJob(
        knowledge_store=knowledge_store,
        settings=EpisodeSettings(),
        episode_window_size=8,
        llm=DeterministicEpisodeLLM(),
        embedding_service=embedding_service,
    )

    build = await job.process_next_window(
        user_name=user_name,
        project_id=project_id,
    )

    assert build is not None
    assert len(build.final_episodes) == 1
    episode_id = build.final_episodes[0].episode_id
    assert build.final_episodes[0].generator_metadata["decision_action"] == "create"

    episode = await knowledge_store.get_episode(
        episode_id,
        user_name=user_name,
        project_id=project_id,
        session_id=session_id,
    )
    assert episode is not None
    assert episode.summary.startswith("The team agreed")
    assert [message.message_id for message in episode.messages] == [
        first_message_id,
        second_message_id,
    ]
    assert episode.embedding == [0.25] * 1024

    checkpoint = await knowledge_store.get_episode_checkpoint(
        user_name=user_name,
        project_id=project_id,
        session_id=session_id,
    )
    assert checkpoint.last_evaluated_message_id == message_ids[-1]
    assert checkpoint.last_evaluated_timestamp_ms == 1700000007000

    async def no_op_emit(*args, **kwargs):
        return None

    monkeypatch.setattr(retrieval_module, "emit", no_op_emit)
    tool = EpisodeRetrievalTool(knowledge_store)
    tool.user_name = user_name
    tool.project_id = project_id
    tool.session_id = session_id
    tool.readable_project_ids = [project_id]
    result = await tool.episode_check("episodic memory", session_id=session_id)

    assert result["resolution"] == "question"
    retrieved = result["results"][0]["episodes"]
    assert len(retrieved) == 1
    assert retrieved[0]["episode_id"] == episode_id
    assert retrieved[0]["summary"].startswith("The team agreed")
    assert [evidence["message_id"] for evidence in retrieved[0]["evidence"]] == [
        first_message_id,
        second_message_id,
    ]


@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.integration
@pytest.mark.no_network
async def test_session_add_and_assistant_sources_are_durable_in_postgres(
    isolated_e2e_scope,
    monkeypatch,
):
    """Persist a user turn, answer, and pasted-text source through Session."""

    real_postgres_client = isolated_e2e_scope["client"]
    user_name = isolated_e2e_scope["user_name"]
    project_id = isolated_e2e_scope["project_id"]
    session_id = isolated_e2e_scope["session_id"]
    embedding_service = DeterministicEmbeddingService()
    knowledge_store = KnowledgeStore(real_postgres_client, embedding_service)
    session_deleted = False

    monkeypatch.setattr(
        Session,
        "current_config",
        property(lambda self: FakeConfigValue(conversation_context_turns=100)),
    )
    context = Session(
        user_name,
        SimpleNamespace(
            postgres=real_postgres_client,
            knowledge_store=knowledge_store,
            embedding=embedding_service,
        ),
        session_id=session_id,
        project_id=project_id,
        project=SimpleNamespace(
            scheduler=object(),
            readable_project_ids=[project_id],
        ),
        model=None,
        agent_id=None,
        enabled_tools=None,
    )
    context.ingestion_worker = FakeIngestionWorker()

    try:
        accepted, _created = await context._accept_user_message(
            Message(
                content="The source note says durable memory must stay grounded.",
                timestamp=datetime(2026, 7, 31, 12, 0, tzinfo=timezone.utc),
            )
        )

        persisted_user = await real_postgres_client.fetch_one(
            """
            SELECT role, content, project_id, session_id
            FROM messages
            WHERE message_id = %s
            """,
            (accepted.id,),
        )
        assert persisted_user == {
            "role": "user",
            "content": "The source note says durable memory must stay grounded.",
            "project_id": project_id,
            "session_id": session_id,
        }
        assert context.ingestion_worker.signaled == 1

        excerpt = "durable memory must stay grounded"
        source_candidate = SourceReferenceCandidate(
            project_id=project_id,
            session_id=session_id,
            source_kind="user_pasted_text",
            source_message_id=accepted.id,
            content_hash=hashlib.sha256(excerpt.encode("utf-8")).hexdigest(),
            locator=PastedTextLocator(start_char=0, end_char=len(excerpt)),
            excerpt=excerpt,
            metadata={"pasted_text": True},
            encounter_kind="user_pasted_text",
            agent_run_id=f"run-{uuid.uuid4().hex}",
            result_position=0,
        )
        await context.add_assistant_turn(
            "I will keep the memory answer grounded in that source.",
            datetime(2026, 7, 31, 12, 1, tzinfo=timezone.utc),
            user_msg_id=accepted.id,
            source_candidates=[source_candidate],
        )

        assistant_row = await real_postgres_client.fetch_one(
            """
            SELECT message_id, role, content
            FROM messages
            WHERE user_name = %s AND session_id = %s AND role = 'assistant'
            """,
            (user_name, session_id),
        )
        assert assistant_row is not None
        assert assistant_row["role"] == "assistant"
        answer = await knowledge_store.get_assistant_message_with_sources(
            int(assistant_row["message_id"]),
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        assert answer is not None
        assert answer.content == assistant_row["content"]
        assert len(answer.sources_consulted) == 1
        assert answer.sources_consulted[0].source_message_id == accepted.id
        assert (
            answer.sources_consulted[0].contributing_message_id
            == int(assistant_row["message_id"])
        )
    finally:
        await SessionDeletionWriter(real_postgres_client).delete_session(
            user_name=user_name,
            session_id=session_id,
        )
        session_deleted = True

    assert session_deleted is True
