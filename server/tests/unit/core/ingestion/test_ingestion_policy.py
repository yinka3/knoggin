import pytest

from common.conf.domain_config import DomainConfig
from core.ingestion.batch import IngestionBatch
from tests.fixtures.ingestion import ingestion_policy


@pytest.mark.ingestion
@pytest.mark.no_network
def test_ingestion_policy_detaches_domain_rules_and_versions_the_snapshot():
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
            "entity_types": {"Identity": {"topic": "Identity", "labels": ["identity", "test_label"]}},
        }
    ).compile()
    first = ingestion_policy(compiled_domain=first_domain)
    second = ingestion_policy(compiled_domain=second_domain)

    assert first.domain.resolve_entity_type("test_label") is None
    assert second.domain.resolve_entity_type("test_label") == "Identity"
    assert first.version != second.version


@pytest.mark.ingestion
@pytest.mark.no_network
def test_ingestion_policy_round_trip_rejects_tampering():
    policy = ingestion_policy()
    payload = policy.to_dict()

    assert type(policy).from_dict(payload) == policy

    payload["checkpoint_interval"] = policy.checkpoint_interval + 1
    with pytest.raises(ValueError, match="version"):
        type(policy).from_dict(payload)


@pytest.mark.ingestion
@pytest.mark.no_network
def test_ingestion_policy_reads_a_legacy_redundant_topic_snapshot():
    policy = ingestion_policy()
    payload = policy.to_dict()
    legacy_topics = {
        "rules": [
            {
                "name": "Identity",
                "active": True,
                "labels": ["person"],
                "aliases": [],
            }
        ]
    }
    payload["topics"] = legacy_topics
    values = {key: value for key, value in payload.items() if key != "version"}
    payload["version"] = type(policy)._version_for(values)

    restored = type(policy).from_dict(payload)

    assert restored.version == payload["version"]
    assert restored.domain.resolve_entity_type("person") == "Identity"


@pytest.mark.ingestion
@pytest.mark.no_network
def test_ingestion_policy_captures_and_round_trips_compiled_domain():
    domain = DomainConfig.from_mapping(
        {
            "version": 9,
            "topics": {"Software Development": {"active": True}},
            "entity_types": {
                "Project": {
                    "topic": "Software Development",
                    "labels": ["project"],
                }
            },
        }
    ).compile()
    policy = ingestion_policy(compiled_domain=domain)

    payload = policy.to_dict()
    assert payload["domain"]["version"] == 9
    assert payload["domain"]["label_to_entity_type"] == {"project": "Project"}
    assert type(policy).from_dict(payload) == policy

    payload["domain"]["label_to_entity_type"]["mutated"] = "Project"
    with pytest.raises(ValueError, match="version"):
        type(policy).from_dict(payload)


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
    assert batch.work_unit.metadata["policy_version"] == batch.policy.version
