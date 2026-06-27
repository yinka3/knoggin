from unittest.mock import MagicMock

import pytest
import torch

from common.exceptions import ConfigurationError, DependencyError
from common.schema.settings import LLMSettings, RootConfig
from infrastructure import resources as resources_module


@pytest.mark.no_network
async def test_resource_manager_passes_base_url_and_subscribes_llm_updates(
    monkeypatch, tmp_path
):
    captured_llm_kwargs = {}
    unsubscribe_calls = []
    subscribe_calls = []
    redis_client = object()
    redis_instances = []
    event_calls = []

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
            callback(self.config.llm)

            def unsubscribe():
                unsubscribe_calls.append(path)

            return unsubscribe

    class FakeKnowledgeStore:
        def __init__(self, dsn, embedding_service):
            self.dsn = dsn
            self.embedding_service = embedding_service
            self.postgres = object()
            self.connected = False
            self.closed = False

        async def connect(self):
            self.connected = True

        async def close(self):
            self.closed = True

    class FakeAsyncRedisClient:
        def __init__(self, settings):
            self.settings = settings
            self.closed = False
            redis_instances.append(self)

        async def connect(self):
            return redis_client

        async def close(self):
            self.closed = True
            event_calls.append(("close", redis_client))

    class FakeCommunityEmitter:
        def bind_redis(self, redis):
            event_calls.append(("bind", redis))

        def unbind_redis(self, redis):
            event_calls.append(("unbind", redis))

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
        def __init__(self, embedding_model=None, reranker_model=None, device=None):
            self.embedding_model = embedding_model
            self.reranker_model = reranker_model
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
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv(
        "KNOGGIN_DOCUMENT_STORAGE_DIR",
        str(tmp_path / "documents"),
    )
    monkeypatch.setenv("KNOGGIN_GPU", "false")
    monkeypatch.setenv("KNOGGIN_EMBEDDING_MODEL", "custom/embedder")
    monkeypatch.setenv("KNOGGIN_RERANKER_MODEL", "custom/reranker")
    monkeypatch.delenv("KNOGGIN_LLM_TRACE", raising=False)
    monkeypatch.setattr(
        resources_module.ConfigManager, "get", staticmethod(lambda: fake_config)
    )
    load_dotenv = MagicMock()
    monkeypatch.setattr(resources_module, "load_dotenv", load_dotenv)
    monkeypatch.setattr(resources_module, "KnowledgeStore", FakeKnowledgeStore)
    monkeypatch.setattr(resources_module, "AsyncRedisClient", FakeAsyncRedisClient)
    monkeypatch.setattr(
        resources_module.CommunityEventEmitter,
        "get",
        staticmethod(lambda: FakeCommunityEmitter()),
    )
    monkeypatch.setattr(resources_module, "LLMService", FakeLLMService)
    monkeypatch.setattr(resources_module, "EmbeddingService", FakeEmbeddingService)
    monkeypatch.setattr(resources_module, "spacy", FakeSpacy)
    monkeypatch.setattr(resources_module, "GLiNER", FakeGLiNER)
    resources_module.ResourceManager._instance = None
    resources_module.ResourceManager._lock = None

    manager = await resources_module.ResourceManager.initialize()

    load_dotenv.assert_called_once_with()
    assert manager.redis is redis_client
    assert manager.redis_manager is redis_instances[0]
    assert redis_instances[0].settings.url == "redis://localhost:6379/0"
    assert event_calls == [("bind", redis_client)]
    assert captured_llm_kwargs["base_url"] == "https://llm.example/v1"
    assert captured_llm_kwargs["trace_logger"] is None
    assert manager.embedding.embedding_model == "custom/embedder"
    assert manager.embedding.reranker_model == "custom/reranker"
    assert manager.postgres is manager.knowledge_store.postgres
    assert manager.document_storage_root == (tmp_path / "documents").resolve()
    assert manager.document_storage_root.is_dir()
    assert subscribe_calls == [(manager.llm_service.update_settings, "llm")]
    assert manager.llm_service.updated_settings == [fake_config.config.llm]

    await manager.shutdown()

    assert unsubscribe_calls == ["llm"]
    assert redis_instances[0].closed is True
    assert event_calls == [
        ("bind", redis_client),
        ("unbind", redis_client),
        ("close", redis_client),
    ]


@pytest.mark.no_network
async def test_resource_manager_raises_if_database_url_missing(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    resources_module.ResourceManager._instance = None
    resources_module.ResourceManager._lock = None

    with pytest.raises(
        ConfigurationError, match="DATABASE_URL environment variable is not set"
    ):
        await resources_module.ResourceManager.initialize()


@pytest.mark.no_network
async def test_resource_manager_cleans_up_when_postgres_startup_fails(monkeypatch):
    redis_client = object()
    event_calls = []
    knowledge_store_instances = []
    redis_instances = []
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

    class FailingKnowledgeStore:
        def __init__(self, dsn, embedding_service):
            self.postgres = object()
            self.closed = False
            knowledge_store_instances.append(self)

        async def connect(self):
            raise ConnectionError("Postgres unavailable")

        async def close(self):
            self.closed = True

    class FakeAsyncRedisClient:
        def __init__(self, settings):
            self.closed = False
            redis_instances.append(self)

        async def connect(self):
            return redis_client

        async def close(self):
            self.closed = True

    class FakeCommunityEmitter:
        def bind_redis(self, redis):
            event_calls.append(("bind", redis))

        def unbind_redis(self, redis):
            event_calls.append(("unbind", redis))

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

    fake_config = FakeConfigManager()
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("KNOGGIN_GPU", "false")
    monkeypatch.setattr(
        resources_module.ConfigManager,
        "get",
        staticmethod(lambda: fake_config),
    )
    monkeypatch.setattr(resources_module, "KnowledgeStore", FailingKnowledgeStore)
    monkeypatch.setattr(resources_module, "AsyncRedisClient", FakeAsyncRedisClient)
    monkeypatch.setattr(
        resources_module.CommunityEventEmitter,
        "get",
        staticmethod(lambda: FakeCommunityEmitter()),
    )
    monkeypatch.setattr(resources_module, "LLMService", FakeLLMService)
    monkeypatch.setattr(resources_module, "EmbeddingService", FakeEmbeddingService)
    monkeypatch.setattr(resources_module, "spacy", FakeSpacy)
    monkeypatch.setattr(resources_module, "GLiNER", FakeGLiNER)
    monkeypatch.setattr(resources_module, "ThreadPoolExecutor", FakeExecutor)
    resources_module.ResourceManager._instance = None
    resources_module.ResourceManager._lock = None

    with pytest.raises(DependencyError, match="Postgres unavailable"):
        await resources_module.ResourceManager.initialize()

    assert resources_module.ResourceManager._instance is None
    assert knowledge_store_instances[0].closed is True
    assert redis_instances[0].closed is True
    assert embedding_instances[0].cleaned_up is True
    assert llm_instances[0].closed is True
    assert executor_instances[0].shutdown_calls == [False]
    assert event_calls == [
        ("bind", redis_client),
        ("unbind", redis_client),
    ]


@pytest.mark.no_network
async def test_resource_manager_resolves_gpu_cuda(monkeypatch, tmp_path):
    class FakeKnowledgeStore:
        def __init__(self, dsn, embedding_service):
            self.postgres = object()

        async def connect(self):
            pass

        async def close(self):
            pass

    class FakeAsyncRedisClient:
        def __init__(self, settings):
            self.raw = object()

        async def connect(self):
            return self.raw

        async def close(self):
            pass

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
        def __init__(self, embedding_model=None, reranker_model=None, device=None):
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
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv(
        "KNOGGIN_DOCUMENT_STORAGE_DIR",
        str(tmp_path / "documents"),
    )
    monkeypatch.setenv("KNOGGIN_GPU", "true")
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)

    monkeypatch.setattr(resources_module.ConfigManager, "get", lambda: MagicMock())
    monkeypatch.setattr(resources_module, "KnowledgeStore", FakeKnowledgeStore)
    monkeypatch.setattr(resources_module, "AsyncRedisClient", FakeAsyncRedisClient)
    monkeypatch.setattr(resources_module, "LLMService", FakeLLMService)
    monkeypatch.setattr(resources_module, "EmbeddingService", FakeEmbeddingService)
    monkeypatch.setattr(resources_module, "spacy", FakeSpacy)
    monkeypatch.setattr(resources_module, "GLiNER", FakeGLiNER)

    resources_module.ResourceManager._instance = None
    resources_module.ResourceManager._lock = None

    manager = await resources_module.ResourceManager.initialize()
    assert manager.embedding.device.type == "cuda"
    await manager.shutdown()


@pytest.mark.no_network
async def test_resource_manager_resolves_gpu_mps(monkeypatch, tmp_path):
    # Same mocks as cuda
    class FakeKnowledgeStore:
        def __init__(self, dsn, embedding_service):
            self.postgres = object()

        async def connect(self):
            pass

        async def close(self):
            pass

    class FakeAsyncRedisClient:
        def __init__(self, settings):
            self.raw = object()

        async def connect(self):
            return self.raw

        async def close(self):
            pass

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
        def __init__(self, embedding_model=None, reranker_model=None, device=None):
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
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv(
        "KNOGGIN_DOCUMENT_STORAGE_DIR",
        str(tmp_path / "documents"),
    )
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
    monkeypatch.setattr(resources_module, "AsyncRedisClient", FakeAsyncRedisClient)
    monkeypatch.setattr(resources_module, "LLMService", FakeLLMService)
    monkeypatch.setattr(resources_module, "EmbeddingService", FakeEmbeddingService)
    monkeypatch.setattr(resources_module, "spacy", FakeSpacy)
    monkeypatch.setattr(resources_module, "GLiNER", FakeGLiNER)

    resources_module.ResourceManager._instance = None
    resources_module.ResourceManager._lock = None

    manager = await resources_module.ResourceManager.initialize()
    assert manager.embedding.device.type == "mps"
    await manager.shutdown()


@pytest.mark.no_network
async def test_resource_manager_resolves_cpu_when_gpu_false(monkeypatch, tmp_path):
    class FakeKnowledgeStore:
        def __init__(self, dsn, embedding_service):
            self.postgres = object()

        async def connect(self):
            pass

        async def close(self):
            pass

    class FakeAsyncRedisClient:
        def __init__(self, settings):
            self.raw = object()

        async def connect(self):
            return self.raw

        async def close(self):
            pass

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
        def __init__(self, embedding_model=None, reranker_model=None, device=None):
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
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv(
        "KNOGGIN_DOCUMENT_STORAGE_DIR",
        str(tmp_path / "documents"),
    )
    monkeypatch.setenv("KNOGGIN_GPU", "false")
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)  # Should ignore this

    monkeypatch.setattr(resources_module.ConfigManager, "get", lambda: MagicMock())
    monkeypatch.setattr(resources_module, "KnowledgeStore", FakeKnowledgeStore)
    monkeypatch.setattr(resources_module, "AsyncRedisClient", FakeAsyncRedisClient)
    monkeypatch.setattr(resources_module, "LLMService", FakeLLMService)
    monkeypatch.setattr(resources_module, "EmbeddingService", FakeEmbeddingService)
    monkeypatch.setattr(resources_module, "spacy", FakeSpacy)
    monkeypatch.setattr(resources_module, "GLiNER", FakeGLiNER)

    resources_module.ResourceManager._instance = None
    resources_module.ResourceManager._lock = None

    manager = await resources_module.ResourceManager.initialize()
    assert manager.embedding.device.type == "cpu"
    await manager.shutdown()
