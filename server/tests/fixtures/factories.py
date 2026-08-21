from types import SimpleNamespace

from common.conf.domain_config import DomainConfig
from core.knowledge.documents import DocumentService
from core.project.domain_config_store import DomainConfigStore
from runtime.project_runtime import ProjectRuntime
from tests.fixtures.fakes import (
    FakeEmbeddingService,
    FakePipeline,
    FakePostgresClient,
    FakeRedis,
    FakeScheduler,
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
    batch_processor=None,
    background_work=None,
    entities=None,
    pipeline=None,
):
    redis = redis or FakeRedis()
    scheduler = scheduler or FakeScheduler()
    postgres = postgres or FakePostgresClient()
    embedding = embedding or FakeEmbeddingService()
    entities = entities if entities is not None else object()
    retrieval = SimpleNamespace(set_active_topics=lambda _: None)
    return ProjectRuntime(
        project_id=project_id,
        entities=entities,
        knowledge_retrieval=retrieval,
        pipeline=pipeline if pipeline is not None else FakePipeline(),
        scheduler=scheduler,
        user_name="ada",
        domain_config=domain_config or make_domain_config(),
        readable_project_ids=[project_id],
        document_service=DocumentService(
            project_id=project_id,
            postgres_client=postgres,
            embedding_service=embedding,
        ),
        domain_config_store=DomainConfigStore(postgres),
        batch_processor=batch_processor,
        background_work=background_work,
    )
