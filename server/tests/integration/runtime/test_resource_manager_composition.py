"""Real model, PostgreSQL, and Redis runtime lifecycle contracts."""

import asyncio
import os

import pytest

from infrastructure.model_work import ModelWorkPriority
from runtime.resources import ResourceManager


def _local_snapshot(
    huggingface_hub,
    model_name: str,
    *,
    revision_env: str,
) -> str:
    kwargs = {"local_files_only": True}
    revision = os.environ.get(revision_env)
    if revision:
        kwargs["revision"] = revision
    try:
        return huggingface_hub.snapshot_download(
            model_name,
            **kwargs,
        )
    except Exception as exc:
        if os.environ.get("KNOGGIN_REQUIRE_LOCAL_MODELS") == "1":
            pytest.fail(
                f"Required runtime model artifact {model_name!r} is unavailable: {exc}"
            )
        pytest.skip(f"Runtime model artifact {model_name!r} is unavailable: {exc}")


@pytest.fixture
async def real_resource_manager(monkeypatch):
    huggingface_hub = pytest.importorskip("huggingface_hub")
    embedding_model = _local_snapshot(
        huggingface_hub,
        os.environ.get(
            "KNOGGIN_SEMANTIC_SMOKE_MODEL",
            os.environ.get("KNOGGIN_EMBEDDING_MODEL", "dunzhang/stella_en_1.5B_v5"),
        ),
        revision_env="KNOGGIN_SEMANTIC_SMOKE_MODEL_REVISION",
    )
    reranker_model = _local_snapshot(
        huggingface_hub,
        os.environ.get(
            "KNOGGIN_SEMANTIC_SMOKE_RERANKER_MODEL",
            os.environ.get("KNOGGIN_RERANKER_MODEL", "BAAI/bge-reranker-large"),
        ),
        revision_env="KNOGGIN_SEMANTIC_SMOKE_RERANKER_REVISION",
    )
    gliner_model = _local_snapshot(
        huggingface_hub,
        os.environ.get("KNOGGIN_GLINER_MODEL", "urchade/gliner_large-v2.1"),
        revision_env="KNOGGIN_GLINER_REVISION",
    )
    _local_snapshot(
        huggingface_hub,
        os.environ.get("KNOGGIN_GLINER_BASE_MODEL", "microsoft/deberta-v3-large"),
        revision_env="KNOGGIN_GLINER_BASE_REVISION",
    )

    monkeypatch.setenv(
        "DATABASE_URL",
        os.environ.get(
            "KNOGGIN_TEST_DATABASE_URL",
            "postgresql://knoggin:knoggin@localhost:5432/knoggin_db",
        ),
    )
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("KNOGGIN_EMBEDDING_MODEL", embedding_model)
    monkeypatch.setenv("KNOGGIN_RERANKER_MODEL", reranker_model)
    monkeypatch.setenv("KNOGGIN_GLINER_MODEL", gliner_model)
    monkeypatch.setenv(
        "KNOGGIN_EMBEDDING_BACKEND",
        os.environ.get("KNOGGIN_EMBEDDING_BACKEND", "onnx"),
    )
    monkeypatch.setenv(
        "KNOGGIN_ONNX_PROVIDER",
        os.environ.get("KNOGGIN_ONNX_PROVIDER", "CPUExecutionProvider"),
    )
    monkeypatch.setenv("HF_HUB_OFFLINE", "1")

    ResourceManager._instance = None
    ResourceManager._lock = None
    manager = await ResourceManager.initialize(num_workers=2)
    try:
        yield manager
    finally:
        await manager.shutdown()


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.model
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_real_runtime_startup_and_shutdown_drains_active_work(
    real_resource_manager,
):
    manager = real_resource_manager
    postgres = manager.postgres
    redis_manager = manager.redis_manager

    assert manager.redis is not None
    assert await manager.redis.ping() is True
    assert manager.embedding is not None
    assert manager.embedding.embedding_dim > 0
    assert manager.spacy is not None
    assert manager.gliner is not None
    assert manager.work_snapshot()["background_work"]["queued"] == 0

    ingestion_started = asyncio.Event()
    ingestion_release = asyncio.Event()

    async def active_ingestion():
        ingestion_started.set()
        await ingestion_release.wait()
        return "ingestion-complete"

    ingestion_task = asyncio.create_task(
        manager.background_work.submit(
            "runtime-composition-project",
            active_ingestion,
            name="active-ingestion",
        )
    )
    await ingestion_started.wait()

    agent_tool_started = asyncio.Event()
    agent_tool_release = asyncio.Event()

    async def active_agent_tool():
        agent_tool_started.set()
        await agent_tool_release.wait()
        return "agent-tool-complete"

    agent_tool_task = asyncio.create_task(
        manager.model_work.submit(
            active_agent_tool,
            priority=ModelWorkPriority.FOREGROUND,
            name="active-agent-tool",
        )
    )
    await agent_tool_started.wait()

    shutdown_task = asyncio.create_task(manager.shutdown())
    await asyncio.sleep(0)
    ingestion_release.set()
    agent_tool_release.set()

    await asyncio.gather(shutdown_task, ingestion_task, agent_tool_task)

    assert ingestion_task.result() == "ingestion-complete"
    assert agent_tool_task.result() == "agent-tool-complete"
    assert manager.postgres is None
    assert manager.redis_manager is None
    assert manager.embedding is None
    assert manager.llm_service is None
    assert manager.work_snapshot() == {
        "postgres": None,
        "background_work": None,
        "model_work": None,
    }
    assert postgres._pool is None
    assert redis_manager._client is None
    assert ResourceManager._instance is None

    # A second close is a supported no-op after both complete and partial boots.
    await manager.shutdown()
