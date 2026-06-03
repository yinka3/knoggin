import pytest

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
        def __init__(self, device):
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
    monkeypatch.setattr(resources_module.ConfigManager, "get", staticmethod(lambda: fake_config))
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
