import pytest

from common.conf.topics_config import TopicConfig
from common.schema.contracts import TopicConfigResult, TopicDetail
from common.schema.settings import TopicSchema
from infrastructure.job.base import JobContext
from infrastructure.redis_client import RedisKeys
from knoggin_server.knowledge.jobs.topics_job import TopicConfigJob
from tests.fixtures.fakes import FakeKnowledgeStore, FakeRedis


class RecordingLLM:
    merge_model = "fake-merge"

    def __init__(self):
        self.calls = []

    async def generate_structured(self, **kwargs):
        self.calls.append(kwargs)
        return TopicConfigResult(
            topics={"General": TopicDetail(active=True, labels=[])}
        )


async def noop_update(new_config):
    return None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_topic_config_job_uses_project_heartbeat_and_session_buffers():
    redis = FakeRedis()
    knowledge_store = FakeKnowledgeStore()
    job = TopicConfigJob(
        llm=RecordingLLM(),
        topic_config=TopicConfig({"General": TopicSchema(active=True)}),
        update_callback=noop_update,
        redis_client=redis,
        knowledge_store=knowledge_store,
        interval_msgs=2,
    )
    ctx = JobContext(user_name="ada", project_id="project-1")

    await redis.set(RedisKeys.project_heartbeat_counter("ada", "project-1"), 2)
    await redis.sadd(RedisKeys.project_sessions("ada", "project-1"), "session-1")
    await redis.rpush(RedisKeys.buffer("ada", "session-1"), "pending")

    assert await job.should_run(ctx) is False

    await redis.ltrim(RedisKeys.buffer("ada", "session-1"), 1, -1)

    assert await job.should_run(ctx) is True


@pytest.mark.runtime
@pytest.mark.no_network
async def test_topic_config_job_reads_project_messages_from_graph():
    redis = FakeRedis()
    knowledge_store = FakeKnowledgeStore()
    knowledge_store.recent_project_messages = [
        {
            "id": 1,
            "user_name": "ada",
            "session_id": "session-1",
            "project_id": "project-1",
            "role": "user",
            "content": "I am learning planning systems.",
        }
    ]
    llm = RecordingLLM()
    job = TopicConfigJob(
        llm=llm,
        topic_config=TopicConfig({"General": TopicSchema(active=True)}),
        update_callback=noop_update,
        redis_client=redis,
        knowledge_store=knowledge_store,
        interval_msgs=1,
    )
    ctx = JobContext(user_name="ada", project_id="project-1")

    result = await job.execute(ctx)

    assert result.success is True
    assert "I am learning planning systems." in llm.calls[0]["user"]
    assert await redis.get(RedisKeys.project_heartbeat_counter("ada", "project-1")) == "0"
