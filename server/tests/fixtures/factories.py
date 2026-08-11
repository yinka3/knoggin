from common.conf.domain_config import DomainConfig
from common.conf.topics_config import TopicConfig
from common.schema.settings import TopicSchema
from core.project.state import ProjectState
from tests.fixtures.fakes import (
    FakeEmbeddingService,
    FakePipeline,
    FakePostgresClient,
    FakeRedis,
    FakeScheduler,
)


def make_topic_config():
    return TopicConfig(
        {
            "General": TopicSchema(active=True, labels=[], aliases=[]),
            "Identity": TopicSchema(active=True, labels=["person"], aliases=["me"]),
        }
    )


def make_domain_config(version=1):
    return DomainConfig.from_mapping(
        {
            "version": version,
            "topics": {
                "Identity": {"active": True},
                "General": {"active": True},
            },
            "entity_types": {
                "Identity": {
                    "topic": "Identity",
                    "labels": ["person", "identity"],
                },
                "Concept": {
                    "topic": "General",
                    "labels": ["concept", "thing"],
                },
            },
        }
    )


def make_project_state(
    project_id="project-1",
    redis=None,
    scheduler=None,
    postgres=None,
    embedding=None,
    domain_config=None,
):
    redis = redis or FakeRedis()
    scheduler = scheduler or FakeScheduler()
    postgres = postgres or FakePostgresClient()
    embedding = embedding or FakeEmbeddingService()
    return ProjectState(
        project_id=project_id,
        topic_config=make_topic_config(),
        entities=object(),
        pipeline=FakePipeline(),
        scheduler=scheduler,
        user_name="ada",
        redis_client=redis,
        postgres_client=postgres,
        embedding_service=embedding,
        domain_config=domain_config or make_domain_config(),
        readable_project_ids=[project_id],
    )
