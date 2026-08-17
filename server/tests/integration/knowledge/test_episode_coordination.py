"""Service-backed episode coordination and recovery contracts."""

import asyncio
import os
import uuid

import pytest

from common.schema.episode.generation import LLMEpisodeDecision
from common.schema.settings import EpisodeSettings, IngestionSettings
from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
from core.knowledge.episodes.job import EpisodeJob
from core.knowledge.store import KnowledgeStore
from infrastructure.postgres_client import PostgresClient


class DeterministicEpisodeLLM:
    async def generate_structured(self, **kwargs):
        assert kwargs["response_model"] is LLMEpisodeDecision
        return LLMEpisodeDecision(
            action="create",
            summary="The durable episode coordination path completed.",
            new_developments=["The source window was persisted exactly once."],
            message_influences=[
                {"message_id": "m1", "influence_weight": 0.8},
                {"message_id": "m2", "influence_weight": 0.9},
            ],
        )


class BarrierEpisodeLLM(DeterministicEpisodeLLM):
    def __init__(self):
        self.started = 0
        self.ready = asyncio.Event()

    async def generate_structured(self, **kwargs):
        self.started += 1
        if self.started == 2:
            self.ready.set()
        await self.ready.wait()
        return await super().generate_structured(**kwargs)


class DeterministicEmbeddingService:
    async def encode(self, texts):
        assert len(texts) == 1
        return [[0.25] * 1024]


class FailOnceEmbeddingService(DeterministicEmbeddingService):
    def __init__(self):
        self.failed = False

    async def encode(self, texts):
        if not self.failed:
            self.failed = True
            raise ConnectionError("simulated embedding outage")
        return await super().encode(texts)


@pytest.fixture
async def episode_service_scope():
    dsn = os.environ.get(
        "KNOGGIN_TEST_DATABASE_URL",
        "postgresql://knoggin:knoggin@localhost:5432/knoggin_db",
    )
    postgres = PostgresClient(dsn=dsn, min_size=1, max_size=8)
    await postgres.connect()
    suffix = uuid.uuid4().hex[:12]
    user_name = "episode_coordination_test_user"
    scopes = []
    try:
        for index in (1, 2):
            project_id = f"episode-coord-project-{suffix}-{index}"
            session_id = f"episode-coord-session-{suffix}-{index}"
            await postgres.execute(
                "INSERT INTO projects (project_id, user_name, name) VALUES (%s, %s, %s)",
                (project_id, user_name, "Episode coordination contract"),
            )
            await postgres.execute(
                "INSERT INTO sessions (session_id, user_name, project_id) VALUES (%s, %s, %s)",
                (session_id, user_name, project_id),
            )
            scopes.append(
                {
                    "project_id": project_id,
                    "session_id": session_id,
                }
            )
        yield {
            "postgres": postgres,
            "user_name": user_name,
            "scopes": scopes,
        }
    finally:
        for scope in scopes:
            await ProjectDeletionWriter(postgres).delete_project(
                user_name=user_name,
                project_id=scope["project_id"],
            )
        await postgres.close()


async def _insert_eligible_window(postgres, *, user_name, project_id, session_id):
    rows = await postgres.fetch_all(
        "SELECT nextval('public.message_id_seq') AS message_id FROM generate_series(1, 2)"
    )
    message_ids = [int(row["message_id"]) for row in rows]
    await postgres.execute(
        """
        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, episode_eligible
        )
        VALUES
            (%s, %s, %s, %s, 'user', 'First durable coordination message.',
             1700000000000, TRUE),
            (%s, %s, %s, %s, 'assistant', 'Second durable coordination message.',
             1700000001000, TRUE)
        """,
        (
            user_name,
            session_id,
            message_ids[0],
            project_id,
            user_name,
            session_id,
            message_ids[1],
            project_id,
        ),
    )
    return message_ids


def _episode_job(store, llm, embedding=None):
    embedding = embedding or DeterministicEmbeddingService()
    return EpisodeJob(
        knowledge_store=store,
        settings=EpisodeSettings(batch_multiple=1, max_message_count=2),
        ingestion_settings=IngestionSettings(batch_size=2),
        llm=llm,
        embedding_service=embedding,
    )


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_real_episode_jobs_converge_when_same_window_runs_concurrently(
    episode_service_scope,
):
    resources = episode_service_scope
    scope = resources["scopes"][0]
    message_ids = await _insert_eligible_window(
        resources["postgres"],
        user_name=resources["user_name"],
        **scope,
    )
    store = KnowledgeStore(resources["postgres"], DeterministicEmbeddingService())
    job = _episode_job(store, BarrierEpisodeLLM())

    results = await asyncio.gather(
        job.process_next_window(
            user_name=resources["user_name"],
            **scope,
        ),
        job.process_next_window(
            user_name=resources["user_name"],
            **scope,
        ),
    )

    assert sum(result is not None for result in results) == 1
    row = await resources["postgres"].fetch_one(
        """
        SELECT count(*) AS episode_count
        FROM episodes
        WHERE project_id = %s AND session_id = %s
        """,
        (scope["project_id"], scope["session_id"]),
    )
    assert row == {"episode_count": 1}
    checkpoint = await store.get_episode_checkpoint(
        user_name=resources["user_name"],
        **scope,
    )
    assert checkpoint.last_evaluated_message_id == message_ids[-1]
    assert checkpoint.last_evaluated_timestamp_ms == 1700000001000


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_real_episode_persistence_failure_rolls_back_for_retry(
    episode_service_scope,
):
    resources = episode_service_scope
    scope = resources["scopes"][0]
    await _insert_eligible_window(
        resources["postgres"],
        user_name=resources["user_name"],
        **scope,
    )
    embedding = DeterministicEmbeddingService()
    store = KnowledgeStore(resources["postgres"], embedding)
    writer = store._episode_writer
    original_write_episode = writer._write_episode
    failed = False

    async def fail_after_relational_write(cur, episode, source_message_ids, **kwargs):
        nonlocal failed
        await original_write_episode(cur, episode, source_message_ids, **kwargs)
        if not failed:
            failed = True
            raise ConnectionError("simulated episode transaction outage")

    writer._write_episode = fail_after_relational_write
    job = _episode_job(store, DeterministicEpisodeLLM())
    try:
        with pytest.raises(ConnectionError, match="transaction outage"):
            await job.process_next_window(
                user_name=resources["user_name"],
                **scope,
            )
    finally:
        writer._write_episode = original_write_episode

    assert await resources["postgres"].fetch_one(
        "SELECT count(*) AS count FROM episodes WHERE project_id = %s AND session_id = %s",
        (scope["project_id"], scope["session_id"]),
    ) == {"count": 0}

    retry = await job.process_next_window(
        user_name=resources["user_name"],
        **scope,
    )
    assert retry is not None
    assert await resources["postgres"].fetch_one(
        "SELECT count(*) AS count FROM episodes WHERE project_id = %s AND session_id = %s",
        (scope["project_id"], scope["session_id"]),
    ) == {"count": 1}


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_real_episode_embedding_failure_leaves_window_retryable(
    episode_service_scope,
):
    resources = episode_service_scope
    scope = resources["scopes"][0]
    await _insert_eligible_window(
        resources["postgres"],
        user_name=resources["user_name"],
        **scope,
    )
    embedding = FailOnceEmbeddingService()
    store = KnowledgeStore(resources["postgres"], embedding)
    job = _episode_job(store, DeterministicEpisodeLLM(), embedding)

    with pytest.raises(ConnectionError, match="embedding outage"):
        await job.process_next_window(
            user_name=resources["user_name"],
            **scope,
        )

    assert await resources["postgres"].fetch_one(
        "SELECT count(*) AS count FROM episodes WHERE project_id = %s AND session_id = %s",
        (scope["project_id"], scope["session_id"]),
    ) == {"count": 0}
    retry = await job.process_next_window(
        user_name=resources["user_name"],
        **scope,
    )
    assert retry is not None
    assert await resources["postgres"].fetch_one(
        "SELECT count(*) AS count FROM episodes WHERE project_id = %s AND session_id = %s",
        (scope["project_id"], scope["session_id"]),
    ) == {"count": 1}


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_real_episode_jobs_keep_project_and_session_checkpoints_isolated(
    episode_service_scope,
):
    resources = episode_service_scope
    message_ids = {}
    for scope in resources["scopes"]:
        message_ids[scope["project_id"]] = await _insert_eligible_window(
            resources["postgres"],
            user_name=resources["user_name"],
            **scope,
        )
    store = KnowledgeStore(resources["postgres"], DeterministicEmbeddingService())
    job = _episode_job(store, DeterministicEpisodeLLM())

    results = await asyncio.gather(
        *(
            job.process_next_window(
                user_name=resources["user_name"],
                **scope,
            )
            for scope in resources["scopes"]
        )
    )

    assert all(result is not None for result in results)
    rows = await resources["postgres"].fetch_all(
        """
        SELECT project_id, session_id, count(*) AS episode_count
        FROM episodes
        WHERE project_id = ANY(%s)
        GROUP BY project_id, session_id
        ORDER BY project_id
        """,
        ([scope["project_id"] for scope in resources["scopes"]],),
    )
    assert [dict(row) for row in rows] == [
        {
            "project_id": scope["project_id"],
            "session_id": scope["session_id"],
            "episode_count": 1,
        }
        for scope in sorted(resources["scopes"], key=lambda item: item["project_id"])
    ]
    for scope in resources["scopes"]:
        checkpoint = await store.get_episode_checkpoint(
            user_name=resources["user_name"],
            **scope,
        )
        assert (
            checkpoint.last_evaluated_message_id == message_ids[scope["project_id"]][-1]
        )
