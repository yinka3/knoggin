from common.conf.topics_config import TopicConfig
from common.schema.settings import TopicSchema
from knoggin_server.project.state import ProjectState
from tests.fixtures.fakes import FakePipeline, FakeRedis, FakeScheduler


def make_topic_config():
    return TopicConfig(
        {
            "General": TopicSchema(active=True, labels=[], hierarchy={}, aliases=[]),
            "Identity": TopicSchema(
                active=True, labels=["person"], hierarchy={}, aliases=["me"]
            ),
        }
    )


def make_project_state(project_id="global", redis=None, scheduler=None):
    redis = redis or FakeRedis()
    scheduler = scheduler or FakeScheduler()
    return ProjectState(
        project_id=project_id,
        topic_config=make_topic_config(),
        entities=object(),
        pipeline=FakePipeline(),
        scheduler=scheduler,
        user_name="ada",
        redis_client=redis,
        readable_project_ids=[project_id],
    )
