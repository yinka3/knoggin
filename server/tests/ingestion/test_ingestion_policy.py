import pytest

from common.conf.domain_config import DomainConfig
from common.conf.topics_config import TopicConfig
from core.ingestion.batch import IngestionBatch
from tests.fixtures.ingestion import ingestion_policy


@pytest.mark.ingestion
@pytest.mark.no_network
def test_ingestion_policy_detaches_topic_rules_and_versions_the_snapshot():
    topics = TopicConfig.seed()
    first = ingestion_policy(topics=topics)

    updated_topics = topics.snapshot()
    updated_topics["Identity"].labels.append("test_label")
    topics.replace(updated_topics)
    second = ingestion_policy(topics=topics)

    assert first.topics.label_topics("test_label") == ()
    assert second.topics.label_topics("test_label") == ("Identity",)
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
