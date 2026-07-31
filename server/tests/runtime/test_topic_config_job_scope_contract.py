import pytest

from common.conf.topics_config import TopicConfig
from common.schema.settings import TopicSchema
from core.agent.tools.topic_tools import TopicTools
from infrastructure.redis_client import RedisKeys
from tests.fixtures.fakes import FakeRedis


class RecordingPostgres:
    def __init__(self, result=1):
        self.result = result
        self.calls = []

    async def execute(self, query, params=None):
        self.calls.append((query, params))
        return self.result


class TopicHarness(TopicTools):
    def __init__(self, postgres, redis):
        self.postgres = postgres
        self.redis = redis
        self.user_name = "ada"
        self.project_id = "project-1"
        self.topic_config = TopicConfig(
            {
                "General": TopicSchema(active=True),
                "Identity": TopicSchema(active=True, labels=["person"]),
            }
        )
        self.entities = object()
        self.active_topics = []
        self.refreshes = 0
        self.topic_refresh_callback = self._refresh

    def _refresh(self):
        self.refreshes += 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_update_topics_persists_project_config_and_refreshes_runtime():
    postgres = RecordingPostgres()
    redis = FakeRedis()
    tools = TopicHarness(postgres, redis)

    result = await tools.update_topics(
        add_topics=[
            {
                "name": "Research",
                "labels": ["paper"],
                "aliases": ["study"],
            }
        ],
        reasoning="The project now contains a stable research domain.",
    )

    assert result["success"] is True
    assert result["added"] == ["Research"]
    assert "Research" in result["active_topics"]
    assert tools.refreshes == 1
    assert "UPDATE public.projects" in postgres.calls[0][0]
    assert await redis.get(
        RedisKeys.project_heartbeat_counter("ada", "project-1")
    ) == "0"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_update_topics_rolls_back_when_postgres_write_fails():
    postgres = RecordingPostgres(result=0)
    tools = TopicHarness(postgres, FakeRedis())
    before = tools.topic_config.snapshot()

    result = await tools.update_topics(
        add_topics=[{"name": "Research", "labels": ["paper"]}]
    )

    assert "error" in result
    assert tools.topic_config.snapshot() == before
    assert tools.refreshes == 0


@pytest.mark.runtime
@pytest.mark.no_network
async def test_update_topics_rejects_invalid_change_without_persisting():
    postgres = RecordingPostgres()
    tools = TopicHarness(postgres, FakeRedis())

    result = await tools.update_topics(
        add_topics=[{"name": "", "labels": []}]
    )

    assert "error" in result
    assert postgres.calls == []
