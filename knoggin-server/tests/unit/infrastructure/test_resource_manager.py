import pytest
import torch

from common.exceptions import ConfigurationError
from common.schema.settings import LLMSettings, RootConfig
from infrastructure import resources as resources_module


@pytest.mark.no_network
async def test_resource_manager_passes_base_url_and_subscribes_llm_updates(monkeypatch):
    captured_llm_kwargs = {}
    unsubscribe_calls = []
    subscribe_calls = []

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
            callback(**self.config.llm.model_dump())

            def unsubscribe():
                unsubscribe_calls.append(path)

            return unsubscribe

    class FakeGraphClient:
        def __init__(self, dsn):
            self.dsn = dsn
            self.connected = False
            self.closed = False

        async def connect(self):
            self.connected = True

        async def close(self):
            self.closed = True

    class FakeAsyncRedisClient:
        @staticmethod
        async def get_instance():
            return object()

        @staticmethod
        async def close_redis():
            pass

    class FakeLLMService:
        def __init__(self, **kwargs):
            captured_llm_kwargs.update(kwargs)
            self.updated_settings = []

        async def load_tokenizer(self):
            pass

        def update_settings(self, **kwargs):
            self.updated_settings.append(kwargs)

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

    class FakeEntityManager:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    fake_config = FakeConfigManager()
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setenv("KNOGGIN_GPU", "false")
    monkeypatch.setenv("KNOGGIN_EMBEDDING_MODEL", "custom/embedder")
    monkeypatch.setenv("KNOGGIN_RERANKER_MODEL", "custom/reranker")
    monkeypatch.setattr(
        resources_module.ConfigManager, "get", staticmethod(lambda: fake_config)
    )
    monkeypatch.setattr(resources_module, "GraphClient", FakeGraphClient)
    monkeypatch.setattr(resources_module, "AsyncRedisClient", FakeAsyncRedisClient)
    monkeypatch.setattr(resources_module, "LLMService", FakeLLMService)
    monkeypatch.setattr(resources_module, "EmbeddingService", FakeEmbeddingService)
    monkeypatch.setattr(resources_module, "EntityManager", FakeEntityManager)
    monkeypatch.setattr(resources_module, "spacy", FakeSpacy)
    monkeypatch.setattr(resources_module, "GLiNER", FakeGLiNER)
    resources_module.ResourceManager._instance = None
    resources_module.ResourceManager._lock = None

    manager = await resources_module.ResourceManager.initialize()

    assert captured_llm_kwargs["base_url"] == "https://llm.example/v1"
    assert manager.embedding.embedding_model == "custom/embedder"
    assert manager.embedding.reranker_model == "custom/reranker"
    assert subscribe_calls == [(manager.llm_service.update_settings, "llm")]
    assert manager.llm_service.updated_settings == [
        {
            "api_key": "key",
            "base_url": "https://llm.example/v1",
            "agent_model": "agent",
            "extraction_model": "extract",
            "merge_model": "merge",
        }
    ]

    await manager.shutdown()

    assert unsubscribe_calls == ["llm"]


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
async def test_resource_manager_resolves_gpu_cuda(monkeypatch):
    class FakeGraphClient:
        def __init__(self, dsn): pass
        async def connect(self): pass
        async def close(self): pass

    class FakeAsyncRedisClient:
        @staticmethod
        async def get_instance(): return object()
        @staticmethod
        async def close_redis(): pass

    class FakeLLMService:
        def __init__(self, **kwargs): pass
        async def load_tokenizer(self): pass
        def update_settings(self, **kwargs): pass
        async def close(self): pass

    class FakeEmbeddingService:
        def __init__(self, embedding_model=None, reranker_model=None, device=None):
            self.device = device
        async def load_models(self): pass
        def cleanup(self): pass

    class FakeSpacy:
        @staticmethod
        def load(name, exclude=None): return MagicMock()

    class FakeGLiNER:
        @staticmethod
        def from_pretrained(name): return MagicMock()

    from unittest.mock import MagicMock
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setenv("KNOGGIN_GPU", "true")
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)

    monkeypatch.setattr(resources_module.ConfigManager, "get", lambda: MagicMock())
    monkeypatch.setattr(resources_module, "GraphClient", FakeGraphClient)
    monkeypatch.setattr(resources_module, "AsyncRedisClient", FakeAsyncRedisClient)
    monkeypatch.setattr(resources_module, "LLMService", FakeLLMService)
    monkeypatch.setattr(resources_module, "EmbeddingService", FakeEmbeddingService)
    monkeypatch.setattr(resources_module, "EntityManager", MagicMock)
    monkeypatch.setattr(resources_module, "spacy", FakeSpacy)
    monkeypatch.setattr(resources_module, "GLiNER", FakeGLiNER)

    resources_module.ResourceManager._instance = None
    resources_module.ResourceManager._lock = None

    manager = await resources_module.ResourceManager.initialize()
    assert manager.embedding.device.type == "cuda"
    await manager.shutdown()


@pytest.mark.no_network
async def test_resource_manager_resolves_gpu_mps(monkeypatch):
    # Same mocks as cuda
    class FakeGraphClient:
        def __init__(self, dsn): pass
        async def connect(self): pass
        async def close(self): pass

    class FakeAsyncRedisClient:
        @staticmethod
        async def get_instance(): return object()
        @staticmethod
        async def close_redis(): pass

    class FakeLLMService:
        def __init__(self, **kwargs): pass
        async def load_tokenizer(self): pass
        def update_settings(self, **kwargs): pass
        async def close(self): pass

    class FakeEmbeddingService:
        def __init__(self, embedding_model=None, reranker_model=None, device=None):
            self.device = device
        async def load_models(self): pass
        def cleanup(self): pass

    class FakeSpacy:
        @staticmethod
        def load(name, exclude=None): return MagicMock()

    class FakeGLiNER:
        @staticmethod
        def from_pretrained(name): return MagicMock()

    from unittest.mock import MagicMock
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
            def is_available(): return True
        torch.backends.mps = FakeMPS()
    else:
        monkeypatch.setattr(torch.backends.mps, "is_available", lambda: True)

    monkeypatch.setattr(resources_module.ConfigManager, "get", lambda: MagicMock())
    monkeypatch.setattr(resources_module, "GraphClient", FakeGraphClient)
    monkeypatch.setattr(resources_module, "AsyncRedisClient", FakeAsyncRedisClient)
    monkeypatch.setattr(resources_module, "LLMService", FakeLLMService)
    monkeypatch.setattr(resources_module, "EmbeddingService", FakeEmbeddingService)
    monkeypatch.setattr(resources_module, "EntityManager", MagicMock)
    monkeypatch.setattr(resources_module, "spacy", FakeSpacy)
    monkeypatch.setattr(resources_module, "GLiNER", FakeGLiNER)

    resources_module.ResourceManager._instance = None
    resources_module.ResourceManager._lock = None

    manager = await resources_module.ResourceManager.initialize()
    assert manager.embedding.device.type == "mps"
    await manager.shutdown()


@pytest.mark.no_network
async def test_resource_manager_resolves_cpu_when_gpu_false(monkeypatch):
    class FakeGraphClient:
        def __init__(self, dsn): pass
        async def connect(self): pass
        async def close(self): pass

    class FakeAsyncRedisClient:
        @staticmethod
        async def get_instance(): return object()
        @staticmethod
        async def close_redis(): pass

    class FakeLLMService:
        def __init__(self, **kwargs): pass
        async def load_tokenizer(self): pass
        def update_settings(self, **kwargs): pass
        async def close(self): pass

    class FakeEmbeddingService:
        def __init__(self, embedding_model=None, reranker_model=None, device=None):
            self.device = device
        async def load_models(self): pass
        def cleanup(self): pass

    class FakeSpacy:
        @staticmethod
        def load(name, exclude=None): return MagicMock()

    class FakeGLiNER:
        @staticmethod
        def from_pretrained(name): return MagicMock()

    from unittest.mock import MagicMock
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setenv("KNOGGIN_GPU", "false")
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True) # Should ignore this

    monkeypatch.setattr(resources_module.ConfigManager, "get", lambda: MagicMock())
    monkeypatch.setattr(resources_module, "GraphClient", FakeGraphClient)
    monkeypatch.setattr(resources_module, "AsyncRedisClient", FakeAsyncRedisClient)
    monkeypatch.setattr(resources_module, "LLMService", FakeLLMService)
    monkeypatch.setattr(resources_module, "EmbeddingService", FakeEmbeddingService)
    monkeypatch.setattr(resources_module, "EntityManager", MagicMock)
    monkeypatch.setattr(resources_module, "spacy", FakeSpacy)
    monkeypatch.setattr(resources_module, "GLiNER", FakeGLiNER)

    resources_module.ResourceManager._instance = None
    resources_module.ResourceManager._lock = None

    manager = await resources_module.ResourceManager.initialize()
    assert manager.embedding.device.type == "cpu"
    await manager.shutdown()
