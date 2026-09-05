"""Failure-stage cleanup contracts for runtime resource composition."""

from types import SimpleNamespace

import pytest

from common.exceptions import DependencyError
from common.schema.settings import LLMSettings, RootConfig
from runtime import resources as resources_module


@pytest.mark.no_network
@pytest.mark.parametrize(
    "failure",
    [
        "background_start",
        "model_start",
        "tokenizer",
        "embedding",
        "spacy",
        "vp01",
        "postgres",
    ],
)
async def test_resource_manager_cleans_every_partial_startup_stage(
    monkeypatch, failure
):
    created = SimpleNamespace(
        executors=[],
        background=[],
        model_work=[],
        llm=[],
        embedding=[],
        postgres=[],
    )

    class FakeExecutor:
        def __init__(self, max_workers):
            self.shutdown_calls = []
            created.executors.append(self)

        def shutdown(self, wait=True):
            self.shutdown_calls.append(wait)

    class FakeBackground:
        def __init__(self, **_kwargs):
            self.shutdown_calls = 0
            created.background.append(self)

        async def start(self):
            if failure == "background_start":
                raise RuntimeError("background_start failed")

        async def shutdown(self):
            self.shutdown_calls += 1

    class FakeModelWork:
        def __init__(self, *_args, **_kwargs):
            self.shutdown_calls = 0
            created.model_work.append(self)

        async def start(self):
            if failure == "model_start":
                raise RuntimeError("model_start failed")

        async def run_blocking(self, operation, **_kwargs):
            return operation()

        async def shutdown(self):
            self.shutdown_calls += 1

    class FakeLLM:
        def __init__(self, **_kwargs):
            self.closed = False
            created.llm.append(self)

        async def load_tokenizer(self):
            if failure == "tokenizer":
                raise RuntimeError("tokenizer failed")

        def update_settings(self, _settings):
            return None

        async def close(self):
            self.closed = True

    class FakeEmbedding:
        def __init__(self, **_kwargs):
            self.cleaned = False
            created.embedding.append(self)

        def set_model_work_coordinator(self, _model_work):
            return None

        async def load_models(self):
            if failure == "embedding":
                raise RuntimeError("embedding failed")

        def cleanup(self):
            self.cleaned = True

    class FakeKnowledgeStore:
        def __init__(self, **_kwargs):
            return None

    class FakePostgres:
        def __init__(self, _dsn=None, **_kwargs):
            self.closed = False
            created.postgres.append(self)

        async def connect(self):
            if failure == "postgres":
                raise RuntimeError("postgres failed")

        async def close(self):
            self.closed = True

    class FakeProcessor:
        def add_pipe(self, _name):
            return None

    class FakeSpacy:
        @staticmethod
        def load(_name, exclude=None):
            if failure == "spacy":
                raise RuntimeError("spacy failed")
            return FakeProcessor()

    class FakeGLiNER25VP01Adapter:
        @staticmethod
        def load(*, language, device=None):
            if failure == "vp01":
                raise RuntimeError("vp01 failed")
            assert language == "en"
            assert device == "cpu"
            return object()

    config = RootConfig(
        llm=LLMSettings(
            api_key="",
            base_url="https://llm.example/v1",
            agent_model="agent",
            extraction_model="extract",
            merge_model="merge",
        )
    )
    config_manager = SimpleNamespace(
        config=config,
        subscribe=lambda *_args, **_kwargs: lambda: None,
    )
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setenv("KNOGGIN_GPU", "false")
    monkeypatch.setattr(
        resources_module.ConfigManager,
        "get",
        staticmethod(lambda: config_manager),
    )
    monkeypatch.setattr(resources_module, "ThreadPoolExecutor", FakeExecutor)
    monkeypatch.setattr(resources_module, "BackgroundWorkCoordinator", FakeBackground)
    monkeypatch.setattr(resources_module, "ModelWorkCoordinator", FakeModelWork)
    monkeypatch.setattr(resources_module, "LLMService", FakeLLM)
    monkeypatch.setattr(resources_module, "EmbeddingService", FakeEmbedding)
    monkeypatch.setattr(resources_module, "KnowledgeStore", FakeKnowledgeStore)
    monkeypatch.setattr(resources_module, "PostgresClient", FakePostgres)
    monkeypatch.setattr(resources_module, "spacy", FakeSpacy)
    monkeypatch.setattr(
        resources_module,
        "GLiNER25VP01Adapter",
        FakeGLiNER25VP01Adapter,
    )
    with pytest.raises(DependencyError, match=failure):
        await resources_module.RuntimeResources.create(num_workers=2)

    assert all(resource.shutdown_calls for resource in created.background)
    assert all(resource.shutdown_calls for resource in created.model_work)
    assert all(resource.closed for resource in created.llm)
    assert all(resource.cleaned for resource in created.embedding)
    assert all(resource.closed for resource in created.postgres)
    assert all(executor.shutdown_calls == [False] for executor in created.executors)


@pytest.mark.no_network
async def test_runtime_resources_preserves_startup_error_when_cleanup_also_fails(
    monkeypatch,
):
    async def fail_start(self, *, num_workers):
        raise DependencyError("primary startup failure")

    async def fail_teardown(self, *, wait):
        return (
            resources_module.RuntimeResourceShutdownFailure(
                phase="background work",
                error=RuntimeError("cleanup failure"),
            ),
        )

    monkeypatch.setattr(resources_module.RuntimeResources, "_start", fail_start)
    monkeypatch.setattr(resources_module.RuntimeResources, "_teardown", fail_teardown)

    with pytest.raises(DependencyError, match="primary startup failure"):
        await resources_module.RuntimeResources.create()
