"""Explicit ingestion-policy builders for tests."""

from common.conf.domain_config import CompiledDomain, DomainConfig
from common.schema.settings import EntityResolutionSettings, TextProcessorSettings
from core.ingestion.policy import IngestionPolicy


def ingestion_policy(
    *,
    text_processor: TextProcessorSettings | None = None,
    entity_resolution: EntityResolutionSettings | None = None,
    compiled_domain: CompiledDomain | None = None,
) -> IngestionPolicy:
    if compiled_domain is None:
        compiled_domain = DomainConfig.from_mapping(
            {
                "version": 0,
                "topics": {
                    "Identity": {"active": True},
                    "General": {"active": True},
                },
                "entity_types": {
                    "Identity": {
                        "topic": "Identity",
                        "labels": ["person", "identity"],
                    },
                    "General": {"topic": "General", "labels": []},
                },
            }
        ).compile()
    return IngestionPolicy.capture(
        text_processor=text_processor or TextProcessorSettings(),
        entity_resolution=entity_resolution or EntityResolutionSettings(),
        compiled_domain=compiled_domain,
    )
