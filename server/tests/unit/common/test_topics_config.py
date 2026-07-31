import pytest

from common.conf.topics_config import TopicConfig
from common.schema.settings import TopicSchema


@pytest.mark.unit
@pytest.mark.no_network
def test_validate_hot_topics_uses_active_aliases_without_general_fallback():
    topic_config = TopicConfig(
        {
            "General": TopicSchema(active=True),
            "Research": TopicSchema(active=True, aliases=["deep work"]),
            "Archive": TopicSchema(active=False, aliases=["old"]),
        }
    )

    assert topic_config.validate_hot_topics(
        ["deep work", "unknown", "old", "Research", "Research", ""]
    ) == ["Research"]


@pytest.mark.unit
@pytest.mark.no_network
def test_topic_schema_normalizes_terms_and_rejects_duplicate_or_blank_values():
    schema = TopicSchema(
        labels=[" Person ", "Organization"],
        aliases=[" Team ", "collaborators"],
    )

    assert schema.labels == ["person", "organization"]
    assert schema.aliases == ["team", "collaborators"]
    with pytest.raises(ValueError, match="duplicates"):
        TopicSchema(labels=["Person", " person "])
    with pytest.raises(ValueError, match="must not be blank"):
        TopicSchema(aliases=["   "])


@pytest.mark.unit
@pytest.mark.no_network
def test_topic_config_rejects_cross_topic_alias_collisions():
    with pytest.raises(ValueError, match="canonical topic name"):
        TopicConfig(
            {
                "Identity": TopicSchema(active=True),
                "Research": TopicSchema(aliases=["identity"]),
            }
        )

    with pytest.raises(ValueError, match="another topic alias"):
        TopicConfig(
            {
                "Identity": TopicSchema(active=True, aliases=["self"]),
                "Research": TopicSchema(aliases=["self"]),
            }
        )


@pytest.mark.unit
@pytest.mark.no_network
def test_topic_config_projections_do_not_expose_mutable_internal_state():
    topic_config = TopicConfig(
        {
            "Identity": TopicSchema(active=True),
            "Research": TopicSchema(active=True, labels=["study"]),
        }
    )

    raw = topic_config.raw
    raw["Research"].labels.append("mutated")
    active = topic_config.active_topics
    active.append("mutated")

    assert topic_config.get_labels_for_topic("Research") == ["study"]
    assert topic_config.active_topics == ["Identity", "Research"]


@pytest.mark.unit
@pytest.mark.no_network
def test_topic_config_migrates_obsolete_hierarchy_from_persisted_data():
    topic_config = TopicConfig(
        {
            "Identity": {
                "active": True,
                "labels": ["person"],
                "hierarchy": {"person": ["person"]},
            }
        }
    )

    assert topic_config.raw["Identity"].model_dump() == {
        "active": True,
        "hot": False,
        "labels": ["person"],
        "aliases": [],
    }
