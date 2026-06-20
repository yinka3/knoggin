from pathlib import Path

from common.conf.topics_config import TopicConfig
from common.schema.settings import TopicSchema
from knoggin_server.project.state import ProjectState
from tests.fixtures.fakes import (
    FakePipeline,
    FakePostgresClient,
    FakeRedis,
    FakeScheduler,
)


def make_topic_config():
    return TopicConfig(
        {
            "General": TopicSchema(active=True, labels=[], hierarchy={}, aliases=[]),
            "Identity": TopicSchema(
                active=True, labels=["person"], hierarchy={}, aliases=["me"]
            ),
        }
    )


def make_project_state(
    project_id="project-1",
    redis=None,
    scheduler=None,
    postgres=None,
    file_storage_root=None,
):
    redis = redis or FakeRedis()
    scheduler = scheduler or FakeScheduler()
    postgres = postgres or FakePostgresClient()
    file_storage_root = file_storage_root or Path("data/files")
    return ProjectState(
        project_id=project_id,
        topic_config=make_topic_config(),
        entities=object(),
        pipeline=FakePipeline(),
        scheduler=scheduler,
        user_name="ada",
        redis_client=redis,
        postgres_client=postgres,
        file_storage_root=file_storage_root,
        readable_project_ids=[project_id],
    )
