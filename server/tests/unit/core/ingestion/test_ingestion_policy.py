from dataclasses import FrozenInstanceError

import pytest

from common.conf.domain_config import DomainConfig
from core.ingestion.batch import IngestionBatch
from core.ingestion.policy import IngestionPolicy
from tests.fixtures.ingestion import ingestion_policy


@pytest.mark.ingestion
@pytest.mark.no_network
def test_ingestion_policy_captures_immutable_domain_rules_for_one_batch():
    first_domain = DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"Identity": {"active": True}},
            "entity_types": {"Identity": {"topic": "Identity", "labels": ["identity"]}},
        }
    ).compile()
    second_domain = DomainConfig.from_mapping(
        {
            "version": 2,
            "topics": {"Identity": {"active": True}},
            "entity_types": {
                "Identity": {
                    "topic": "Identity",
                    "labels": ["identity", "test_label"],
                }
            },
        }
    ).compile()

    first = ingestion_policy(compiled_domain=first_domain)
    second = ingestion_policy(compiled_domain=second_domain)

    assert first.domain.resolve_entity_type("test_label") is None
    assert second.domain.resolve_entity_type("test_label") == "Identity"
    with pytest.raises(FrozenInstanceError):
        first.resolution_threshold = 0.0


@pytest.mark.ingestion
@pytest.mark.no_network
def test_ingestion_batch_requires_an_explicit_policy():
    with pytest.raises(TypeError, match="policy"):
        IngestionBatch.open(
            user_name="ada",
            project_id="project-1",
            session_id="session-1",
            messages=[],
            session_text="",
        )

    batch = IngestionBatch.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        messages=[],
        session_text="",
        policy=ingestion_policy(),
    )
    assert isinstance(batch.policy, IngestionPolicy)
    assert batch.policy.domain.version == 0
