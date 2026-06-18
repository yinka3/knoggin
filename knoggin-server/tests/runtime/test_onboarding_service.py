import inspect

import pytest

from common.schema.settings import RootConfig
from knoggin_server.session import onboarding
from knoggin_server.session.onboarding import OnboardingService


class FakeConfigManager:
    def __init__(self, config=None):
        self.config = config or RootConfig()
        self.updates = []

    def update_settings(self, updates):
        self.updates.append(updates)
        data = self.config.model_dump()
        data.update(updates)
        self.config = RootConfig(**data)
        return True


class RecordingProjectManager:
    def __init__(self):
        self.create_calls = []

    async def create_project(self, name, description=None):
        self.create_calls.append({"name": name, "description": description})
        return {
            "id": "project-1",
            "name": name,
            "description": description,
        }


class RecordingSessionManager:
    def __init__(self, context):
        self.context = context
        self.create_calls = []

    async def create_session(
        self,
        *,
        project_id,
        model=None,
        agent_id=None,
        enabled_tools=None,
    ):
        self.create_calls.append(
            {
                "model": model,
                "agent_id": agent_id,
                "enabled_tools": enabled_tools,
                "project_id": project_id,
            }
        )
        return self.context


class RecordingContext:
    def __init__(self, session_id="session-1", fail_add=False):
        self.session_id = session_id
        self.fail_add = fail_add
        self.messages = []

    async def add(self, message):
        if self.fail_add:
            raise RuntimeError("ingestion unavailable")
        self.messages.append(message)
        return message


@pytest.mark.runtime
@pytest.mark.no_network
def test_complete_user_onboarding_sets_empty_config_identity():
    config_manager = FakeConfigManager()
    service = OnboardingService(config_manager=config_manager)

    result = service.complete_user_onboarding(
        "  Ada  ",
        aliases=[" A. Lovelace ", "", "A. Lovelace"],
    )

    assert result["user_name"] == "Ada"
    assert result["user_aliases"] == ["A. Lovelace"]
    assert result["configured_at"]
    assert config_manager.updates[0]["user_name"] == "Ada"
    assert "user_facts" not in config_manager.updates[0]


@pytest.mark.runtime
@pytest.mark.no_network
def test_complete_user_onboarding_allows_repeat_for_same_user():
    config = RootConfig(
        user_name="Ada",
        user_aliases=["Countess"],
        user_facts=["first fact"],
        configured_at="2026-01-01T00:00:00+00:00",
    )
    config_manager = FakeConfigManager(config)
    service = OnboardingService(config_manager=config_manager)

    result = service.complete_user_onboarding(
        "Ada",
        aliases=["Enchantress of Numbers"],
    )

    assert result["user_aliases"] == ["Enchantress of Numbers"]
    assert result["configured_at"] == "2026-01-01T00:00:00+00:00"
    assert config_manager.config.user_facts == ["first fact"]


@pytest.mark.runtime
@pytest.mark.no_network
def test_complete_user_onboarding_rejects_renaming_storage_scope():
    config_manager = FakeConfigManager(RootConfig(user_name="Ada"))
    service = OnboardingService(config_manager=config_manager)

    with pytest.raises(ValueError, match="Changing user_name"):
        service.complete_user_onboarding("Grace")

    assert config_manager.updates == []


@pytest.mark.runtime
@pytest.mark.no_network
def test_complete_user_onboarding_does_not_touch_runtime_dependencies():
    service = OnboardingService(config_manager=FakeConfigManager())

    service.complete_user_onboarding("Ada")

    assert service.project_manager is None
    assert service.session_manager is None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_project_quickstart_creates_project_session_and_seeds_one_message():
    config = RootConfig(user_name="Ada")
    context = RecordingContext()
    project_manager = RecordingProjectManager()
    session_manager = RecordingSessionManager(context)
    service = OnboardingService(
        project_manager=project_manager,
        session_manager=session_manager,
        config_manager=FakeConfigManager(config),
    )

    result = await service.create_project_quickstart(
        "  Research  ",
        description="  Build a graph memory system.  ",
        kickoff_note="  Focus on ingestion first.  ",
        facts=[" I am building Knoggin. ", "", "I am building Knoggin."],
        preferences=[" I prefer concise plans. "],
        model="test-model",
        agent_id="agent-1",
        enabled_tools=["search"],
    )

    assert result == {
        "project": {
            "id": "project-1",
            "name": "Research",
            "description": "Build a graph memory system.",
        },
        "session_id": "session-1",
        "project_id": "project-1",
        "seeded": True,
        "seed_error": None,
    }
    assert project_manager.create_calls == [
        {"name": "Research", "description": "Build a graph memory system."}
    ]
    assert session_manager.create_calls == [
        {
            "model": "test-model",
            "agent_id": "agent-1",
            "enabled_tools": ["search"],
            "project_id": "project-1",
        }
    ]
    assert len(context.messages) == 1
    seed = context.messages[0].content
    assert "I prefer concise plans." in seed
    assert seed.count("I am building Knoggin.") == 1
    assert "Build a graph memory system." in seed
    assert "Focus on ingestion first." in seed


@pytest.mark.runtime
@pytest.mark.no_network
async def test_project_quickstart_skips_blank_seed_context():
    context = RecordingContext()
    service = OnboardingService(
        project_manager=RecordingProjectManager(),
        session_manager=RecordingSessionManager(context),
        config_manager=FakeConfigManager(RootConfig(user_name="Ada")),
    )

    result = await service.create_project_quickstart(
        "Scratch",
        description=" ",
        kickoff_note=" ",
        facts=[" "],
        preferences=[""],
    )

    assert result["seeded"] is False
    assert result["seed_error"] is None
    assert context.messages == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_project_quickstart_seed_failure_does_not_roll_back_project_or_session():
    context = RecordingContext(fail_add=True)
    project_manager = RecordingProjectManager()
    session_manager = RecordingSessionManager(context)
    service = OnboardingService(
        project_manager=project_manager,
        session_manager=session_manager,
        config_manager=FakeConfigManager(
            RootConfig(user_name="Ada")
        ),
    )

    result = await service.create_project_quickstart(
        "Research",
        facts=["Remember this."],
    )

    assert result["project_id"] == "project-1"
    assert result["session_id"] == "session-1"
    assert result["seeded"] is False
    assert result["seed_error"] == "ingestion unavailable"
    assert project_manager.create_calls == [{"name": "Research", "description": None}]
    assert session_manager.create_calls[0]["project_id"] == "project-1"


@pytest.mark.runtime
@pytest.mark.no_network
def test_old_heavy_onboarding_pipeline_is_retired():
    source = inspect.getsource(onboarding)

    assert not hasattr(onboarding, "run_setup")
    assert "run_setup" not in source
    assert "\"onboarding\"" not in source
    assert "'onboarding'" not in source
