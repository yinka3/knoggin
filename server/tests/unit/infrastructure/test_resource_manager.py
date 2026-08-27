from concurrent.futures import Future
from unittest.mock import MagicMock

import pytest
import torch

from common.exceptions import ConfigurationError, DependencyError
from common.schema.settings import LLMSettings, RootConfig
from runtime import resources as resources_module


class FakePostgresClient:
    def __init__(self, dsn):
        self.dsn = dsn
        self.connected = False
        self.closed = False

    async def connect(self):
        self.connected = True

    async def close(self):
        self.closed = True


@pytest.mark.no_network
async def test_resource_manager_passes_base_url_and_subscribes_llm_updates(
    monkeypatch, tmp_path
):
    captured_llm_kwargs = {}
    unsubscribe_calls = []
    subscribe_calls = []
    configure_coordination_log = MagicMock()

    class FakeConfigManager:
        def __init__(self):
            self.config = RootConfig(
                llm=LLMSettings(
                    api_key="key",
                    base_url="https://llm.example/v1",
                    agent_model="agent",
                    extraction_model="extract",
                    merge_model="merge",
                )
            )

        def subscribe(self, callback, path=None):
            subscribe_calls.append((callback, path))
            if path == "llm":
                callback(self.config.llm)
            elif path == "developer_settings.coordination_log":
                callback(self.config.developer_settings.coordination_log)
            else:
                raise AssertionError(f"unexpected subscription path: {path}")

            def unsubscribe():
                unsubscribe_calls.append(path)

            return unsubscribe

    class FakeKnowledgeStore:
        def __init__(self, postgres_client, embedding_service):
            self.postgres_client = postgres_client
            self.embedding_service = embedding_service

    class FakeLLMService:
        def __init__(self, **kwargs):
            captured_llm_kwargs.update(kwargs)
            self.updated_settings = []

        async def load_tokenizer(self):
            pass

        def update_settings(self, settings):
            self.updated_settings.append(settings)

        async def close(self):
            pass

    class FakeEmbeddingService:
        def __init__(
            self,
            embedding_model=None,
            reranker_model=None,
            nli_model=None,
            device=None,
            batch_size=None,
        ):
            self.embedding_model = embedding_model
            self.reranker_model = reranker_model
            self.nli_model = nli_model
            self.device = device

        async def load_models(self):
            pass

        def cleanup(self):
            pass

    class FakeProcessor:
        def add_pipe(self, name):
            pass

    class FakeSpacy:
        @staticmethod
        def load(name, exclude=None):
            return FakeProcessor()

    class FakeGlinerModel:
        def to(self, device):
            pass

    class FakeGLiNER:
        @staticmethod
        def from_pretrained(name):
            return FakeGlinerModel()

    fake_config = FakeConfigManager()
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setenv("KNOGGIN_GPU", "false")
    monkeypatch.setenv("KNOGGIN_EMBEDDING_MODEL", "custom/embedder")
    monkeypatch.setenv("KNOGGIN_RERANKER_MODEL", "custom/reranker")
    monkeypatch.setenv("KNOGGIN_NLI_MODEL", "custom/nli")
    monkeypatch.delenv("KNOGGIN_LLM_TRACE", raising=False)
    monkeypatch.setattr(
        resources_module.ConfigManager, "get", staticmethod(lambda: fake_config)
    )
    load_dotenv = MagicMock()
    monkeypatch.setattr(resources_module, "load_dotenv", load_dotenv)
    monkeypatch.setattr(resources_module, "KnowledgeStore", FakeKnowledgeStore)
    monkeypatch.setattr(resources_module, "PostgresClient", FakePostgresClient)
    monkeypatch.setattr(
        resources_module,
        "configure_coordination_log",
        configure_coordination_log,
    )
    monkeypatch.setattr(resources_module, "LLMService", FakeLLMService)
    monkeypatch.setattr(resources_module, "EmbeddingService", FakeEmbeddingService)
    monkeypatch.setattr(resources_module, "spacy", FakeSpacy)
    monkeypatch.setattr(resources_module, "GLiNER", FakeGLiNER)
    manager = await resources_module.RuntimeResources.create()

    load_dotenv.assert_called_once_with()
    assert captured_llm_kwargs["base_url"] == "https://llm.example/v1"
    assert captured_llm_kwargs["trace_logger"] is None
    assert manager.embedding.embedding_model == "custom/embedder"
    assert manager.embedding.reranker_model == "custom/reranker"
    assert manager.embedding.nli_model == "custom/nli"
    assert manager.knowledge_store.postgres_client is manager.postgres
    assert subscribe_calls == [
        (configure_coordination_log, "developer_settings.coordination_log"),
        (manager.llm_service.update_settings, "llm"),
    ]
    assert configure_coordination_log.call_args_list == [
        ((fake_config.config.developer_settings.coordination_log,),),
        ((fake_config.config.developer_settings.coordination_log,),),
    ]
    assert manager.llm_service.updated_settings == [fake_config.config.llm]

    await manager.shutdown()

    assert unsubscribe_calls == ["developer_settings.coordination_log", "llm"]


@pytest.mark.no_network
async def test_resource_manager_raises_if_database_url_missing(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(
        ConfigurationError, match="DATABASE_URL environment variable is not set"
    ):
        await resources_module.RuntimeResources.create()


@pytest.mark.no_network
async def test_resource_manager_cleans_up_when_postgres_startup_fails(monkeypatch):
    knowledge_store_instances = []
    embedding_instances = []
    llm_instances = []
    executor_instances = []

    class FakeConfigManager:
        def __init__(self):
            self.config = RootConfig(
                llm=LLMSettings(
                    api_key="key",
                    base_url="https://llm.example/v1",
                    agent_model="agent",
                    extraction_model="extract",
                    merge_model="merge",
                )
            )

        def subscribe(self, callback, path=None):
            return lambda: None

    class RecordingKnowledgeStore:
        def __init__(self, postgres_client, embedding_service):
            knowledge_store_instances.append(self)

    class FailingPostgresClient(FakePostgresClient):
        instances = []

        def __init__(self, dsn):
            super().__init__(dsn)
            self.__class__.instances.append(self)

        async def connect(self):
            raise ConnectionError("Postgres unavailable")

    class FakeLLMService:
        def __init__(self, **kwargs):
            self.closed = False
            llm_instances.append(self)

        async def load_tokenizer(self):
            pass

        def update_settings(self, settings):
            pass

        async def close(self):
            self.closed = True

    class FakeEmbeddingService:
        def __init__(self, **kwargs):
            self.cleaned_up = False
            embedding_instances.append(self)

        async def load_models(self):
            pass

        def cleanup(self):
            self.cleaned_up = True

    class FakeProcessor:
        def add_pipe(self, name):
            pass

    class FakeSpacy:
        @staticmethod
        def load(name, exclude=None):
            return FakeProcessor()

    class FakeGlinerModel:
        def to(self, device):
            pass

    class FakeGLiNER:
        @staticmethod
        def from_pretrained(name):
            return FakeGlinerModel()

    class FakeExecutor:
        def __init__(self, max_workers):
            self.shutdown_calls = []
            executor_instances.append(self)

        def shutdown(self, wait=True):
            self.shutdown_calls.append(wait)

        def submit(self, fn, *args, **kwargs):
            future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

    fake_config = FakeConfigManager()
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setenv("KNOGGIN_GPU", "false")
    monkeypatch.setattr(
        resources_module.ConfigManager,
        "get",
        staticmethod(lambda: fake_config),
    )
    monkeypatch.setattr(resources_module, "KnowledgeStore", RecordingKnowledgeStore)
    monkeypatch.setattr(
        resources_module,
        "PostgresClient",
        FailingPostgresClient,
    )
    monkeypatch.setattr(resources_module, "LLMService", FakeLLMService)
    monkeypatch.setattr(resources_module, "EmbeddingService", FakeEmbeddingService)
    monkeypatch.setattr(resources_module, "spacy", FakeSpacy)
    monkeypatch.setattr(resources_module, "GLiNER", FakeGLiNER)
    monkeypatch.setattr(resources_module, "ThreadPoolExecutor", FakeExecutor)
    with pytest.raises(DependencyError, match="Postgres unavailable"):
        await resources_module.RuntimeResources.create()

    assert knowledge_store_instances == []
    assert FailingPostgresClient.instances[0].closed is True
    assert embedding_instances == []
    assert llm_instances == []
    assert executor_instances[0].shutdown_calls == [False]


@pytest.mark.no_network
async def test_resource_manager_resolves_gpu_cuda(monkeypatch, tmp_path):
    class FakeKnowledgeStore:
        def __init__(self, postgres_client, embedding_service):
            self.postgres_client = postgres_client

    class FakeLLMService:
        def __init__(self, **kwargs):
            pass

        async def load_tokenizer(self):
            pass

        def update_settings(self, **kwargs):
            pass

        async def close(self):
            pass

    class FakeEmbeddingService:
        def __init__(
            self,
            embedding_model=None,
            reranker_model=None,
            nli_model=None,
            device=None,
            batch_size=None,
        ):
            self.device = device

        async def load_models(self):
            pass

        def cleanup(self):
            pass

    class FakeSpacy:
        @staticmethod
        def load(name, exclude=None):
            return MagicMock()

    class FakeGLiNER:
        @staticmethod
        def from_pretrained(name):
            return MagicMock()

    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setenv("KNOGGIN_GPU", "true")
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)

    monkeypatch.setattr(resources_module.ConfigManager, "get", lambda: MagicMock())
    monkeypatch.setattr(resources_module, "KnowledgeStore", FakeKnowledgeStore)
    monkeypatch.setattr(resources_module, "PostgresClient", FakePostgresClient)
    monkeypatch.setattr(resources_module, "LLMService", FakeLLMService)
    monkeypatch.setattr(resources_module, "EmbeddingService", FakeEmbeddingService)
    monkeypatch.setattr(resources_module, "spacy", FakeSpacy)
    monkeypatch.setattr(resources_module, "GLiNER", FakeGLiNER)

    manager = await resources_module.RuntimeResources.create()
    assert manager.embedding.device.type == "cuda"
    await manager.shutdown()


@pytest.mark.no_network
async def test_resource_manager_resolves_gpu_mps(monkeypatch, tmp_path):
    # Same mocks as cuda
    class FakeKnowledgeStore:
        def __init__(self, postgres_client, embedding_service):
            self.postgres_client = postgres_client

    class FakeLLMService:
        def __init__(self, **kwargs):
            pass

        async def load_tokenizer(self):
            pass

        def update_settings(self, **kwargs):
            pass

        async def close(self):
            pass

    class FakeEmbeddingService:
        def __init__(
            self,
            embedding_model=None,
            reranker_model=None,
            nli_model=None,
            device=None,
            batch_size=None,
        ):
            self.device = device

        async def load_models(self):
            pass

        def cleanup(self):
            pass

    class FakeSpacy:
        @staticmethod
        def load(name, exclude=None):
            return MagicMock()

    class FakeGLiNER:
        @staticmethod
        def from_pretrained(name):
            return MagicMock()

    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setenv("KNOGGIN_GPU", "true")
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)

    # Mock torch.backends.mps.is_available
    if not hasattr(torch, "backends"):

        class FakeBackends:
            pass

        torch.backends = FakeBackends()
    if not hasattr(torch.backends, "mps"):

        class FakeMPS:
            @staticmethod
            def is_available():
                return True

        torch.backends.mps = FakeMPS()
    else:
        monkeypatch.setattr(torch.backends.mps, "is_available", lambda: True)

    monkeypatch.setattr(resources_module.ConfigManager, "get", lambda: MagicMock())
    monkeypatch.setattr(resources_module, "KnowledgeStore", FakeKnowledgeStore)
    monkeypatch.setattr(resources_module, "PostgresClient", FakePostgresClient)
    monkeypatch.setattr(resources_module, "LLMService", FakeLLMService)
    monkeypatch.setattr(resources_module, "EmbeddingService", FakeEmbeddingService)
    monkeypatch.setattr(resources_module, "spacy", FakeSpacy)
    monkeypatch.setattr(resources_module, "GLiNER", FakeGLiNER)

    manager = await resources_module.RuntimeResources.create()
    assert manager.embedding.device.type == "mps"
    await manager.shutdown()


@pytest.mark.no_network
async def test_resource_manager_resolves_cpu_when_gpu_false(monkeypatch, tmp_path):
    class FakeKnowledgeStore:
        def __init__(self, postgres_client, embedding_service):
            self.postgres_client = postgres_client

    class FakeLLMService:
        def __init__(self, **kwargs):
            pass

        async def load_tokenizer(self):
            pass

        def update_settings(self, **kwargs):
            pass

        async def close(self):
            pass

    class FakeEmbeddingService:
        def __init__(
            self,
            embedding_model=None,
            reranker_model=None,
            nli_model=None,
            device=None,
            batch_size=None,
        ):
            self.device = device

        async def load_models(self):
            pass

        def cleanup(self):
            pass

    class FakeSpacy:
        @staticmethod
        def load(name, exclude=None):
            return MagicMock()

    class FakeGLiNER:
        @staticmethod
        def from_pretrained(name):
            return MagicMock()

    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setenv("KNOGGIN_GPU", "false")
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)  # Should ignore this

    monkeypatch.setattr(resources_module.ConfigManager, "get", lambda: MagicMock())
    monkeypatch.setattr(resources_module, "KnowledgeStore", FakeKnowledgeStore)
    monkeypatch.setattr(resources_module, "PostgresClient", FakePostgresClient)
    monkeypatch.setattr(resources_module, "LLMService", FakeLLMService)
    monkeypatch.setattr(resources_module, "EmbeddingService", FakeEmbeddingService)
    monkeypatch.setattr(resources_module, "spacy", FakeSpacy)
    monkeypatch.setattr(resources_module, "GLiNER", FakeGLiNER)

    manager = await resources_module.RuntimeResources.create()
    assert manager.embedding.device.type == "cpu"
    await manager.shutdown()


@pytest.mark.no_network
async def test_runtime_resources_shutdown_attempts_every_phase_and_aggregates_errors():
    calls = []

    def failing_unsubscribe():
        calls.append("unsubscribe")
        raise RuntimeError("unsubscribe failed")

    class AsyncResource:
        def __init__(self, name, *, fail=False):
            self.name = name
            self.fail = fail
            self.close_calls = 0

        async def shutdown(self):
            self.close_calls += 1
            calls.append(self.name)
            if self.fail:
                raise RuntimeError(f"{self.name} failed")

        async def close(self):
            await self.shutdown()

    class Executor:
        def __init__(self):
            self.shutdown_calls = 0

        def shutdown(self, *, wait):
            self.shutdown_calls += 1
            calls.append("executor")

    class Embedding:
        def __init__(self):
            self.cleanup_calls = 0

        def cleanup(self):
            self.cleanup_calls += 1
            calls.append("embedding")

    resources = resources_module.RuntimeResources()
    background = AsyncResource("background", fail=True)
    model_work = AsyncResource("model_work")
    postgres = AsyncResource("postgres")
    llm = AsyncResource("llm")
    executor = Executor()
    embedding = Embedding()
    resources.config_unsubscribers = [failing_unsubscribe]
    resources.background_work = background
    resources.model_work = model_work
    resources.executor = executor
    resources.postgres = postgres
    resources.embedding = embedding
    resources.llm_service = llm

    with pytest.raises(resources_module.RuntimeResourcesShutdownError) as error:
        await resources.shutdown()

    assert [failure.phase for failure in error.value.failures] == [
        "configuration unsubscribe 1",
        "background work",
    ]
    assert calls == [
        "unsubscribe",
        "background",
        "model_work",
        "executor",
        "postgres",
        "embedding",
        "llm",
    ]
    assert background.close_calls == model_work.close_calls == 1
    assert postgres.close_calls == llm.close_calls == 1
    assert executor.shutdown_calls == embedding.cleanup_calls == 1

    with pytest.raises(resources_module.RuntimeResourcesShutdownError) as repeated_error:
        await resources.shutdown()

    assert repeated_error.value is error.value
    assert calls == [
        "unsubscribe",
        "background",
        "model_work",
        "executor",
        "postgres",
        "embedding",
        "llm",
    ]
