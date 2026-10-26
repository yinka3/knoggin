"""Real model and PostgreSQL runtime lifecycle contracts."""

import asyncio
import os

import pytest

from common.conf.manager import ConfigManager
from core.ingestion import vp01
from infrastructure.model_work import ModelWorkPriority
from runtime.resources import RuntimeResources


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
        if os.environ.get("REQUIRE_LOCAL_MODELS") == "1":
            pytest.fail(
                f"Required runtime model artifact {model_name!r} is unavailable: {exc}"
            )
        pytest.skip(f"Runtime model artifact {model_name!r} is unavailable: {exc}")


@pytest.fixture
async def real_resource_manager(monkeypatch, tmp_path, test_database_url):
    huggingface_hub = pytest.importorskip("huggingface_hub")
    embedding_model = _local_snapshot(
        huggingface_hub,
        os.environ.get(
            "SEMANTIC_SMOKE_MODEL",
            os.environ.get("EMBEDDING_MODEL", "BAAI/bge-base-en-v1.5"),
        ),
        revision_env="SEMANTIC_SMOKE_MODEL_REVISION",
    )
    reranker_model = _local_snapshot(
        huggingface_hub,
        os.environ.get(
            "SEMANTIC_SMOKE_RERANKER_MODEL",
            os.environ.get("RERANKER_MODEL", "BAAI/bge-reranker-base"),
        ),
        revision_env="SEMANTIC_SMOKE_RERANKER_REVISION",
    )
    vp01_model = _local_snapshot(
        huggingface_hub,
        os.environ.get("VP01_MODEL", "fastino/gliner2.5-base-v1"),
        revision_env="VP01_REVISION",
    )
    monkeypatch.setattr(vp01, "GLINER25_ENGLISH_MODEL", vp01_model)

    monkeypatch.setenv("DATABASE_URL", test_database_url)
    monkeypatch.setenv("EMBEDDING_MODEL", embedding_model)
    monkeypatch.setenv("RERANKER_MODEL", reranker_model)
    monkeypatch.setenv(
        "EMBEDDING_BACKEND",
        os.environ.get("EMBEDDING_BACKEND", "onnx"),
    )
    monkeypatch.setenv(
        "ONNX_PROVIDER",
        os.environ.get("ONNX_PROVIDER", "CPUExecutionProvider"),
    )
    monkeypatch.setenv("HF_HUB_OFFLINE", "1")

    # Resources read live settings from an application-owned ConfigManager.
    monkeypatch.setattr(ConfigManager, "_instance", None)
    config_manager = ConfigManager(tmp_path)
    config_manager.load(require_valid=True)
    manager = await RuntimeResources.create(thread_workers=2, config_manager=config_manager)
    try:
        yield manager
    finally:
        await manager.shutdown()


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.model
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_real_runtime_shutdown_cancels_background_and_drains_model_work(
    real_resource_manager,
):
    manager = real_resource_manager
    postgres = manager.postgres
    assert manager.embedding is not None
    assert manager.embedding.embedding_dim > 0
    assert manager.spacy is not None
    assert manager.vp01 is not None
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
    # Model work drains; background ingestion is never released, so only
    # shutdown cancelling it can end it (no race on event-loop ticks).
    agent_tool_release.set()

    shutdown_result, ingestion_result, agent_tool_result = await asyncio.wait_for(
        asyncio.gather(
            shutdown_task,
            ingestion_task,
            agent_tool_task,
            return_exceptions=True,
        ),
        timeout=60,
    )

    assert shutdown_result is None
    assert isinstance(ingestion_result, asyncio.CancelledError)
    assert agent_tool_result == "agent-tool-complete"
    assert manager.postgres is None
    assert manager.embedding is None
    assert manager.llm_service is None
    assert manager.work_snapshot() == {
        "postgres": None,
        "background_work": None,
        "model_work": None,
    }
    assert postgres._pool is None

    # A second close is a supported no-op after both complete and partial boots.
    await manager.shutdown()
