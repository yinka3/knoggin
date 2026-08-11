"""Explicit ingestion-policy builders for tests."""

from common.conf.domain_config import CompiledDomain, DomainConfig
from common.conf.topics_config import TopicConfig
from common.schema.settings import (
    EntityResolutionSettings,
    IngestionSettings,
    TextProcessorSettings,
)
from core.ingestion.policy import IngestionPolicy


def ingestion_policy(
    *,
    ingestion: IngestionSettings | None = None,
    text_processor: TextProcessorSettings | None = None,
    entity_resolution: EntityResolutionSettings | None = None,
    topics: TopicConfig | None = None,
    compiled_domain: CompiledDomain | None = None,
) -> IngestionPolicy:
    topics = topics or TopicConfig.seed()
    if compiled_domain is None:
        topic_snapshot = topics.snapshot()
        compiled_domain = DomainConfig.from_mapping(
            {
                "version": 0,
                "topics": {
                    name: {"active": schema.active}
                    for name, schema in topic_snapshot.items()
                },
                "entity_types": {
                    name: {
                        "topic": name,
                        "labels": list(schema.labels),
                    }
                    for name, schema in topic_snapshot.items()
                },
            }
        ).compile()
    return IngestionPolicy.capture(
        ingestion=ingestion or IngestionSettings(),
        text_processor=text_processor or TextProcessorSettings(),
        entity_resolution=entity_resolution or EntityResolutionSettings(),
        topic_config=topics,
        compiled_domain=compiled_domain,
    )
